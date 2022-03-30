//
// Copyright (c) 2017, 2020 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//
use async_std::sync::Arc;
use async_std::sync::{Mutex, RwLock};
use async_std::task::sleep;
use flume::{Receiver, Sender};
use futures::join;
use futures::select;
use futures::stream::StreamExt;
use log::{debug, error, info, trace, warn};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fs;
use std::io::Write;
use std::str;
use std::str::FromStr;
use std::time::Duration;
use zenoh::net::protocol::io::SplitBuffer;
use zenoh::prelude::Sample;
use zenoh::prelude::*;
use zenoh::prelude::{KeyExpr, Value};
use zenoh::queryable;
use zenoh::queryable::EVAL;
use zenoh::time::Timestamp;
use zenoh::Session;
use zenoh_backend_traits::config::ReplicaConfig;
use zenoh_backend_traits::Query;
use zenoh_core::Result as ZResult;

#[path = "digest.rs"]
pub mod digest;
pub use digest::*;

const ERA: &str = "era";
const INTERVALS: &str = "intervals";
const SUBINTERVALS: &str = "subintervals";
const CONTENTS: &str = "contents";

pub enum StorageMessage {
    Stop,
    GetStatus(async_std::channel::Sender<serde_json::Value>),
}

struct ReplicaData {
    stable_log: RwLock<HashMap<String, Timestamp>>, // log entries until the snapshot time
    volatile_log: RwLock<HashMap<String, Timestamp>>, // log entries after the snapshot time
    digests_published: RwLock<HashSet<u64>>, // checksum of all digests generated and published by this replica
    digests_processed: RwLock<HashSet<u64>>, // checksum of all digests received by the replica
    last_snapshot_time: RwLock<Timestamp>,   // the latest snapshot time
    last_interval: RwLock<u64>,              // the latest interval
    digest: RwLock<Option<Digest>>,          // the current stable digest
}

pub struct Replica {
    name: String,          // name of replica  -- UUID(zenoh)-<storage_type>-<storage_name>
    session: Arc<Session>, // zenoh session used by the replica
    key_expr: String,      // key expression of the storage to be functioning as a replica
    storage: Mutex<Box<dyn zenoh_backend_traits::Storage>>, // -- the actual value being stored
    in_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>,
    out_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>,
    replica_config: Option<ReplicaConfig>, // replica configuration - if some, replica, if none, normal storage
    replica_data: Option<ReplicaData>,
}

// functions to start services required by a replica
impl Replica {
    pub async fn initialize_replica(
        config: Option<ReplicaConfig>,
        session: Arc<Session>,
        storage: Box<dyn zenoh_backend_traits::Storage>,
        in_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>,
        out_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>,
        key_expr: &str,
        admin_key: &str,
        log: HashMap<String, Timestamp>,
    ) -> ZResult<Sender<StorageMessage>> {
        // Ex: /@/router/43C9AE839B274A259B437F78CC081D6A/status/plugins/storage_manager/storages/demo1 -> 390CEC11A1E34977A1C609A35BC015E6/demo1 (/memory needed????)
        let parts: Vec<&str> = admin_key.split('/').collect();
        let uuid = parts[3];
        let storage_name = parts[8];
        // let storage_name = parts[10];
        let name = format!("{}/{}", uuid, storage_name);

        if config.is_some() {
            info!("[REPLICA]Opening session...");

            let (interval, time) = Replica::get_latest_snapshot_interval_time(
                config.as_ref().unwrap().propagation_delay,
                config.as_ref().unwrap().delta,
            );

            let replica = Replica {
                name,
                session,
                key_expr: key_expr.to_string(),
                storage: Mutex::new(storage),
                in_interceptor,
                out_interceptor,
                replica_config: config,
                replica_data: Some(ReplicaData {
                    stable_log: RwLock::new(HashMap::<String, Timestamp>::new()),
                    volatile_log: RwLock::new(HashMap::<String, Timestamp>::new()),
                    digests_published: RwLock::new(HashSet::<u64>::new()),
                    digests_processed: RwLock::new(HashSet::<u64>::new()),
                    last_snapshot_time: RwLock::new(time),
                    last_interval: RwLock::new(interval),
                    digest: RwLock::new(None),
                }),
            };

            replica.initialize_log(log).await;
            replica.initialize_digest().await;

            // channel to queue digests to be aligned
            let (tx_digest, rx_digest) = flume::unbounded();
            // digest sub
            let digest_sub = replica.start_digest_sub(tx_digest);
            // eval for align
            let align_eval = replica.start_align_eval();
            // aligner
            let aligner = replica.start_aligner(rx_digest);
            // digest pub
            let digest_pub = replica.start_digest_pub();

            //updating snapshot time
            let snapshot_task = replica.update_snapshot_task();

            //actual storage
            let storage_task = replica.start_storage_queryable_subscriber();

            let result = join!(
                digest_sub,
                align_eval,
                aligner,
                digest_pub,
                snapshot_task,
                storage_task,
            );

            result.5
        } else {
            let replica = Replica {
                name,
                session,
                key_expr: key_expr.to_string(),
                storage: Mutex::new(storage),
                in_interceptor,
                out_interceptor,
                replica_config: None,
                replica_data: None,
            };

            replica.start_storage_queryable_subscriber().await
        }
    }

    pub async fn start_digest_sub(&self, tx: Sender<(String, Digest)>) {
        let mut received = HashMap::<String, Timestamp>::new();

        let digest_key = format!("{}**", self.get_digest_key());

        debug!(
            "[DIGEST_SUB]Creating Subscriber named {} on '{}'...",
            self.name, digest_key
        );
        let mut subscriber = self.session.subscribe(&digest_key).await.unwrap();

        loop {
            let sample = subscriber.receiver().next().await;
            let sample = sample.unwrap();
            let from = &sample.key_expr.as_str()[self.get_digest_key().len()..];
            debug!(
                "[DIGEST_SUB]>> [Digest Subscriber] From {} Received {} ('{}': '{}')",
                from,
                sample.kind,
                sample.key_expr.as_str(),
                sample.value.payload
            );
            let digest: Digest =
                serde_json::from_str(&format!("{:?}", sample.value.payload))
                    .unwrap();
            let ts = digest.timestamp;
            let to_be_processed = self
                .processing_needed(
                    from,
                    digest.timestamp,
                    digest.checksum,
                    received.clone(),
                )
                .await;
            if to_be_processed {
                debug!("[DIGEST_SUB] sending {} to aligner", digest.checksum);
                tx.send_async((from.to_string(), digest)).await.unwrap();
            };
            received.insert(from.to_string(), ts);
        }
    }

    pub async fn start_digest_pub(&self) {
        let digest_key = format!("{}{}", self.get_digest_key(), self.name);

        debug!(
            "[DIGEST_PUB]Declaring digest on key expression '{}'...",
            digest_key
        );
        let expr_id = self.session.declare_expr(&digest_key).await.unwrap();
        debug!("[DIGEST_PUB] => ExprId {}", expr_id);

        debug!("[DIGEST_PUB]Declaring publication on '{}'...", expr_id);
        self.session.declare_publication(expr_id).await.unwrap();

        loop {
            sleep(self.replica_config.as_ref().unwrap().publication_interval).await;

            self.update_stable_log().await;

            let replica_data = self.replica_data.as_ref().unwrap();
            let digest = replica_data.digest.read().await;
            let digest = digest.as_ref().unwrap().compress();
            let digest_json = serde_json::to_string(&digest).unwrap();
            let mut digests_published = replica_data.digests_published.write().await;
            digests_published.insert(digest.checksum);
            drop(digests_published);
            drop(digest);

            debug!(
                "[DIGEST_PUB]Putting Digest ('{}': '{}')...",
                expr_id, digest_json
            );
            self.session.put(expr_id, digest_json).await.unwrap();
        }
    }

    pub async fn start_align_eval(&self) {
        let digest_key = format!("{}{}/**", self.get_digest_key(), self.name);

        debug!("[ALIGN_EVAL]Creating Queryable on '{}'...", digest_key);
        let mut queryable = self
            .session
            .queryable(&digest_key)
            .kind(EVAL)
            .await
            .unwrap();

        loop {
            let query = queryable.receiver().next().await;
            let query = query.unwrap();
            debug!(
                "[ALIGN_EVAL]>> [Queryable ] Received Query '{}'",
                query.selector()
            );
            let selector = query.selector().clone();
            let (era, interval, subinterval, content) = self.parse_selector(selector.clone());
            debug!("[ALIGN_EVAL] Parsed selector era: {:?}, interval:{:?}, subinterval:{:?}, content:{:?}", era, interval, subinterval, content);
            let value = self.get_value(era, interval, subinterval, content).await;
            let value = match value {
                Some(v) => v,
                None => String::from(""),
            };
            debug!("[ALIGN_EVAL] value for the query is {}", value);
            query.reply(Sample::new(
                selector.key_selector.as_str().to_string(),
                value,
            ));
        }
    }

    pub async fn start_aligner(&self, rx: Receiver<(String, Digest)>) {
        while let Ok((from, incoming_digest)) = rx.recv_async().await {
            debug!(
                "[ALIGNER]Processing digest: {:?} from {}",
                incoming_digest, from
            );
            if self.in_processed(incoming_digest.checksum).await {
                debug!("[ALIGNER]Skipping already processed digest");
                continue;
            } else {
                // process this digest
                self.process_incoming_digest(incoming_digest, &from).await;
            }
        }
    }

    pub async fn update_snapshot_task(&self) {
        sleep(Duration::from_secs(2)).await;
        loop {
            sleep(self.replica_config.as_ref().unwrap().delta).await;
            let replica_data = self.replica_data.as_ref().unwrap();
            let mut last_snapshot_time = replica_data.last_snapshot_time.write().await;
            let mut last_interval = replica_data.last_interval.write().await;
            let (interval, time) = Replica::get_latest_snapshot_interval_time(
                self.replica_config.as_ref().unwrap().propagation_delay,
                self.replica_config.as_ref().unwrap().delta,
            );
            *last_interval = interval;
            *last_snapshot_time = time;
            drop(last_interval);
            drop(last_snapshot_time);
            self.update_stable_log().await;
        }
    }

    async fn start_storage_queryable_subscriber(&self) -> ZResult<Sender<StorageMessage>> {
        let (tx, rx) = flume::bounded(1);

        // TODO:do we need a task here? if yes, why?
        // task::spawn(async move {
        // subscribe on key_expr
        let mut storage_sub = match self.session.subscribe(&self.key_expr).await {
            Ok(storage_sub) => storage_sub,
            Err(e) => {
                error!("Error starting storage {} : {}", self.name, e);
                return Err(e);
            }
        };

        // answer to queries on key_expr
        let mut storage_queryable = match self
            .session
            .queryable(&self.key_expr)
            .kind(queryable::STORAGE)
            .await
        {
            Ok(storage_queryable) => storage_queryable,
            Err(e) => {
                error!("Error starting storage {} : {}", self.name, e);
                return Err(e);
            }
        };

        loop {
            select!(
                // on sample for key_expr
                sample = storage_sub.next() => {
                    // Call incoming data interceptor (if any)
                    self.process_sample(sample.unwrap()).await;
                },
                // on query on key_expr
                query = storage_queryable.next() => {
                    let q = query.unwrap();
                    // wrap zenoh::Query in zenoh_backend_traits::Query
                    // with outgoing interceptor
                    let query = Query::new(q, self.out_interceptor.clone());
                    let mut storage = self.storage.lock().await;
                    if let Err(e) = storage.on_query(query).await {
                        warn!("Storage {} raised an error receiving a query: {}", self.name, e);
                    }
                    drop(storage);
                },
                // on storage handle drop
                message = rx.recv_async() => {
                    match message {
                        Ok(StorageMessage::Stop) => {
                            trace!("Dropping storage {}", self.name);
                            return Ok(tx);
                        },
                        Ok(StorageMessage::GetStatus(tx)) => {
                            let storage = self.storage.lock().await;
                            std::mem::drop(tx.send(storage.get_admin_status()).await);
                            drop(storage);
                        }
                        Err(e) => {
                            error!("Storage Message Channel Error: {}", e);
                            return Err(e.into());
                        },
                    };
                }
            );
        }
    }

    async fn process_sample(&self, sample: Sample) {
        debug!("[STORAGE] Processing sample: {}", sample);
        let mut sample = if let Some(ref interceptor) = self.in_interceptor {
            interceptor(sample)
        } else {
            sample
        };

        sample.ensure_timestamp();

        let mut storage = self.storage.lock().await;
        let result = storage.on_sample(sample.clone()).await;
        // let storage_result = match result {
        //     Ok(ts) => Some(ts),
        //     Err(e) => {
        //         warn!("Storage {} raised an error receiving a sample: {}", self.name, e);
        //         None
        //     }
        // };
        if result.is_ok() {
            // let timestamp = timestamp.unwrap();
            self.update_log(sample.key_expr.as_str().to_string(), *sample.get_timestamp().unwrap()).await;
        }
    }

    async fn processing_needed(
        &self,
        from: &str,
        ts: Timestamp,
        checksum: u64,
        received: HashMap<String, Timestamp>,
    ) -> bool {
        if *from == self.name {
            debug!("[DIGEST_SUB]Dropping own digest with checksum {}", checksum);
            return false;
        }
        // TODO: test this part
        if received.contains_key(from) && *received.get(from).unwrap() > ts {
            // not the latest from that replica
            debug!("[DIGEST_SUB]Dropping older digest at {} from {}", ts, from);
            return false;
        }
        //
        if checksum == 0 {
            return false;
        }
        !self.in_processed(checksum).await
    }

    async fn in_processed(&self, checksum: u64) -> bool {
        let replica_data = self.replica_data.as_ref().unwrap();
        let processed_set = replica_data.digests_processed.read().await;
        let processed = processed_set.contains(&checksum);
        drop(processed_set);
        if processed {
            debug!("[DIGEST_SUB]Dropping {} since already processed", checksum);
            true
        } else {
            false
        }
    }

    fn get_digest_key(&self) -> String {
        let key_expr = self.key_expr.clone();
        let align_prefix = self
            .replica_config
            .as_ref()
            .unwrap()
            .align_prefix
            .to_string();
        if key_expr.ends_with("**") {
            format!("{}{}", align_prefix, key_expr.strip_suffix("**").unwrap())
        } else if key_expr.ends_with('/') {
            format!("{}{}", align_prefix, key_expr)
        } else {
            format!("{}{}/", align_prefix, key_expr)
        }
    }
}

// maintain log and digest
impl Replica {
    async fn initialize_log(&self, log: HashMap<String, Timestamp>) {
        let replica_data = self.replica_data.as_ref().unwrap();
        let last_snapshot_time = replica_data.last_snapshot_time.read().await;

        let mut stable_log = replica_data.stable_log.write().await;
        let mut volatile_log = replica_data.volatile_log.write().await;
        for (k, ts) in log {
            // depending on the associated timestamp, either to stable_log or volatile log
            // entries until last_snapshot_time goes to stable
            if ts > *last_snapshot_time {
                (*volatile_log).insert(k, ts);
            } else {
                (*stable_log).insert(k, ts);
            }
        }
        drop(volatile_log);
        drop(stable_log);

        drop(last_snapshot_time);

        self.flush().await;
    }

    async fn initialize_digest(&self) {
        let now = zenoh::time::new_reception_timestamp();
        let replica_data = self.replica_data.as_ref().unwrap();
        let log_locked = replica_data.stable_log.read().await;
        let latest_interval = replica_data.last_interval.read().await;
        let latest_snapshot_time = replica_data.last_snapshot_time.read().await;
        let digest = Digest::create_digest(
            now,
            DigestConfig {
                propagation_delay: self.replica_config.as_ref().unwrap().propagation_delay,
                delta: self.replica_config.as_ref().unwrap().delta,
                sub_intervals: self.replica_config.as_ref().unwrap().subintervals,
                hot: self.replica_config.as_ref().unwrap().hot,
                warm: self.replica_config.as_ref().unwrap().warm,
            },
            (*log_locked).values().copied().collect(),
            *latest_interval,
            *latest_snapshot_time,
        );
        drop(latest_interval);
        drop(latest_snapshot_time);

        let mut digest_lock = replica_data.digest.write().await;
        *digest_lock = Some(digest);
        drop(digest_lock);
    }

    async fn update_log(&self, key: String, ts: Timestamp) {
        let replica_data = self.replica_data.as_ref().unwrap();
        let last_snapshot_time = replica_data.last_snapshot_time.read().await;
        let last_interval = replica_data.last_interval.read().await;
        let mut redundant_content = HashSet::new();
        let mut new_stable_content = HashSet::new();
        if ts > *last_snapshot_time {
            let mut log = replica_data.volatile_log.write().await;
            (*log).insert(key, ts);
            drop(log);
        } else {
            let mut log = replica_data.stable_log.write().await;
            let redundant = (*log).insert(key, ts);
            if redundant.is_some() {
                redundant_content.insert(redundant.unwrap());
            }
            drop(log);
            new_stable_content.insert(ts);
        }
        let mut digest = replica_data.digest.write().await;
        let updated_digest = Digest::update_digest(
            digest.as_ref().unwrap().clone(),
            *last_interval,
            *last_snapshot_time,
            new_stable_content,
            redundant_content,
        )
        .await;
        *digest = Some(updated_digest);
    }

    async fn update_stable_log(&self) {
        let replica_data = self.replica_data.as_ref().unwrap();
        let last_snapshot_time = replica_data.last_snapshot_time.read().await;
        let last_interval = replica_data.last_interval.read().await;
        let volatile = replica_data.volatile_log.read().await;
        let mut stable = replica_data.stable_log.write().await;
        let mut still_volatile = HashMap::new();
        let mut new_stable = HashSet::new();
        let mut redundant_stable = HashSet::new();
        for (k, ts) in volatile.clone() {
            if ts > *last_snapshot_time {
                still_volatile.insert(k, ts);
            } else {
                let redundant = stable.insert(k.to_string(), ts);
                if redundant.is_some() {
                    redundant_stable.insert(redundant.unwrap());
                }
                new_stable.insert(ts);
            }
        }
        drop(stable);
        drop(volatile);

        let mut volatile = replica_data.volatile_log.write().await;
        *volatile = still_volatile;
        drop(volatile);

        let mut digest = replica_data.digest.write().await;
        let updated_digest = Digest::update_digest(
            digest.as_ref().unwrap().clone(),
            *last_interval,
            *last_snapshot_time,
            new_stable,
            redundant_stable,
        )
        .await;
        *digest = Some(updated_digest);

        self.flush().await;
    }

    async fn flush(&self) {
        let log_filename = format!("{}_log.json", self.name.replace('/', "-"));
        let mut log_file = fs::File::create(log_filename).unwrap();

        let replica_data = self.replica_data.as_ref().unwrap();
        let log = replica_data.stable_log.read().await;
        let l = serde_json::to_string(&(*log)).unwrap();
        log_file.write_all(l.as_bytes()).unwrap();
        drop(log);
    }

    fn get_latest_snapshot_interval_time(
        propagation_delay: Duration,
        delta: Duration,
    ) -> (u64, Timestamp) {
        let now = zenoh::time::new_reception_timestamp();
        let latest_interval = (now
            .get_time()
            .to_system_time()
            .duration_since(EPOCH_START)
            .unwrap()
            .as_millis()
            - propagation_delay.as_millis())
            / delta.as_millis();
        let latest_snapshot_time = zenoh::time::Timestamp::new(
            zenoh::time::NTP64::from(Duration::from_millis(
                u64::try_from(delta.as_millis() * latest_interval).unwrap(),
            )),
            *now.get_id(),
        );
        (
            u64::try_from(latest_interval).unwrap(),
            latest_snapshot_time,
        )
    }

    // async fn save_digest(&self) {
    //     let digest = self.digest.read().await;
    //     let rep_digest = digest.as_ref().unwrap();
    //     let j = serde_json::to_string(&rep_digest).unwrap();
    //     let filename = self.get_digest_filename(rep_digest.timestamp);
    //     let mut file = File::create(filename).unwrap();
    //     file.write_all(j.as_bytes()).unwrap();
    // }

    // fn get_digest_filename(&self, timestamp: zenoh::time::Timestamp) -> String {
    //     let ts = format!("{}-{}", timestamp.get_time(), timestamp.get_id());
    //     format!(
    //         "/Users/sreeja/work/zenoh-storage-alignment/digest/{}-{}.json",
    //         ts, self.name
    //     )
    // }
}

// functions to query data as needed for alignment
impl Replica {
    //identify alignment requirements -> {hot => [subintervals], warm => [intervals], cold => []}
    async fn process_incoming_digest(&self, other: Digest, from: &str) {
        let checksum = other.checksum;
        let timestamp = other.timestamp;
        let missing_content = self.get_missing_content(other, from).await;
        debug!("[REPLICA] Missing content is {:?}", missing_content);

        if !missing_content.is_empty() {
            let missing_data = self
                .get_missing_data(&missing_content, timestamp, from)
                .await;

            debug!(
                "[REPLICA] Missing data is {:?}",
                missing_data
            );

            for (key, (ts, value)) in missing_data {
                let sample = Sample::new(key, value).with_timestamp(ts);
                debug!(
                    "[REPLICA] Adding sample {:?}",
                    sample
                );
                self.process_sample(sample).await;
            }

            self.flush().await;

            // TODO: if missing content is not identified, should we still consider it as processed?

            let replica_data = self.replica_data.as_ref().unwrap();
            let mut processed = replica_data.digests_processed.write().await;
            (*processed).insert(checksum);
            drop(processed);
        }
    }

    async fn get_missing_data(
        &self,
        missing_content: &[Timestamp],
        timestamp: Timestamp,
        from: &str,
    ) -> HashMap<KeyExpr<'static>, (Timestamp, Value)> {
        let mut result = HashMap::new();
        let properties = format!(
            "timestamp={};{}={}",
            timestamp,
            CONTENTS,
            serde_json::to_string(missing_content).unwrap()
        );
        let reply = self
            .perform_query(from.to_string(), properties.clone())
            .await;
        // debug!(
        //     "[ALIGNER] << Reply for missing data with property {} is {:?}",
        //     properties, reply
        // );
        // Note: reply is an Option<String>
        // reply.unwrap() gives json string
        // serde_json::from_str(reply.unwrap()) gives HashMap<key(string), (value (json string), timestamp)>
        // for each entry in this, convert it into result required structure and return

        if reply.is_some() {
            let reply_data: HashMap<String, (String, Timestamp)> =
                serde_json::from_str(&reply.unwrap()).unwrap();
            for (k, (v, ts)) in reply_data {
                result.insert(KeyExpr::from(k), (ts, Value::from(v)));
            }
        }
        result
    }

    async fn get_missing_content(&self, other: Digest, from: &str) -> Vec<Timestamp> {
        // get my digest
        let replica_data = self.replica_data.as_ref().unwrap();
        let digest = replica_data.digest.read().await;
        let this = digest.as_ref().unwrap();

        // get first level diff of digest wrt other - subintervals, of HOT, intervals of WARM and COLD if misaligned
        let mis_eras = this.get_era_diff(other.eras.clone());
        let mut missing_content = Vec::new();
        if mis_eras.contains(&EraType::Cold) {
            // perform cold alignment
            let mut cold_data = self
                .perform_cold_alignment(this, from.to_string(), other.timestamp)
                .await;
            missing_content.append(&mut cold_data);
            // debug!(
            //     "************** the missing content after cold alignment is {:?}",
            //     missing_content
            // );
        }
        if mis_eras.contains(&EraType::Warm) {
            // perform warm alignment
            let mut warm_data = self
                .perform_warm_alignment(this, from.to_string(), other.clone())
                .await;
            missing_content.append(&mut warm_data);
            // debug!(
            //     "************** the missing content after warm alignment is {:?}",
            //     missing_content
            // );
        }
        if mis_eras.contains(&EraType::Hot) {
            // perform hot alignment
            let mut hot_data = self
                .perform_hot_alignment(this, from.to_string(), other)
                .await;
            missing_content.append(&mut hot_data);
            // debug!(
            //     "************** the missing content after hot alignment is {:?}",
            //     missing_content
            // );
        }
        missing_content.into_iter().collect()
    }

    //perform cold alignment
    // if COLD misaligned, ask for interval hashes for cold for the other digest timestamp and replica
    // for misaligned intervals, ask subinterval hashes
    // for misaligned subintervals, ask content
    async fn perform_cold_alignment(
        &self,
        this: &Digest,
        other_rep: String,
        timestamp: Timestamp,
    ) -> Vec<Timestamp> {
        let properties = format!("timestamp={};{}=cold", timestamp, ERA);
        let reply_content = self.perform_query(other_rep.to_string(), properties).await;
        let other_intervals : HashMap<u64, u64> =
            serde_json::from_str(&reply_content.unwrap()).unwrap_or_default();
        // get era diff
        let diff_intervals = this.get_interval_diff(other_intervals.clone());
        if !diff_intervals.is_empty() {
            let mut diff_string = Vec::new();
            for each_int in diff_intervals {
                diff_string.push(each_int.to_string());
            }
            let properties = format!(
                "timestamp={};{}=[{}]",
                timestamp,
                INTERVALS,
                diff_string.join(",")
            );
            let reply_content = self.perform_query(other_rep.to_string(), properties).await;
            let other_subintervals =
                serde_json::from_str(&reply_content.unwrap()).unwrap_or_default();
            // get intervals diff
            let diff_subintervals = this.get_subinterval_diff(other_subintervals);
            debug!(
                "[ALIGNER] The subintervals that need alignment are : {:?}",
                diff_subintervals
            );
            if !diff_subintervals.is_empty() {
                let mut diff_string = Vec::new();
                for each_sub in diff_subintervals {
                    diff_string.push(each_sub.to_string());
                }
                let properties = format!(
                    "timestamp={};{}=[{}]",
                    timestamp,
                    SUBINTERVALS,
                    diff_string.join(",")
                );
                let reply_content = self.perform_query(other_rep.to_string(), properties).await;
                let other_content =
                    serde_json::from_str(&reply_content.unwrap()).unwrap_or_default();
                // get subintervals diff
                let result = this.get_full_content_diff(other_content);
                debug!("[ALIGNER] The missing content is {:?}", result);
                return result;
            }
        }
        Vec::new()
    }

    async fn perform_query(&self, from: String, properties: String) -> Option<String> {
        let selector = format!("{}{}?({})", self.get_digest_key(), from, properties);
        debug!("[ALIGNER]Sending Query '{}'...", selector);
        let mut replies = self.session.get(&selector).await.unwrap();
        if let Some(reply) = replies.next().await {
            debug!(
                "[ALIGNER]>> Received ('{}': '{}')",
                reply.sample.key_expr.as_str(),
                reply.sample.value.payload
            );
            // reply_content =
            // format!("{:?}", reply.sample.value.payload);
            // return Some((reply.sample.key_expr, (reply.sample.timestamp.unwrap(), reply.sample.value)));
            return Some(format!("{:?}", reply.sample.value));
        }
        None
    }

    //peform warm alignment
    // if WARM misaligned, ask for subinterval hashes of misaligned intervals
    // for misaligned subintervals, ask content
    async fn perform_warm_alignment(
        &self,
        this: &Digest,
        other_rep: String,
        other: Digest,
    ) -> Vec<Timestamp> {
        // get interval hashes for WARM intervals from other
        let other_intervals = other.get_era_content(EraType::Warm);
        // get era diff
        let diff_intervals = this.get_interval_diff(other_intervals);

        // properties = timestamp=xxx,intervals=[www,ee,rr]
        if !diff_intervals.is_empty() {
            let mut diff_string = Vec::new();
            for each_int in diff_intervals {
                diff_string.push(each_int.to_string());
            }
            let properties = format!(
                "timestamp={};{}=[{}]",
                other.timestamp,
                INTERVALS,
                diff_string.join(",")
            );
            let reply_content = self.perform_query(other_rep.to_string(), properties).await;
            let other_subintervals =
                serde_json::from_str(&reply_content.unwrap()).unwrap_or_default();
            // get intervals diff
            let diff_subintervals = this.get_subinterval_diff(other_subintervals);
            if !diff_subintervals.is_empty() {
                let mut diff_string = Vec::new();
                // properties = timestamp=xxx,subintervals=[www,ee,rr]
                for each_sub in diff_subintervals {
                    diff_string.push(each_sub.to_string());
                }
                let properties = format!(
                    "timestamp={};{}=[{}]",
                    other.timestamp,
                    SUBINTERVALS,
                    diff_string.join(",")
                );
                let reply_content = self.perform_query(other_rep.to_string(), properties).await;
                let other_content =
                    serde_json::from_str(&reply_content.unwrap()).unwrap_or_default();
                // get subintervals diff
                return this.get_full_content_diff(other_content);
            }
        }
        Vec::new()
    }

    //perform hot alignment
    // if HOT misaligned, ask for content(timestamps) of misaligned subintervals
    async fn perform_hot_alignment(
        &self,
        this: &Digest,
        other_rep: String,
        other: Digest,
    ) -> Vec<Timestamp> {
        // get interval hashes for HOT intervals from other
        let other_intervals = other.get_era_content(EraType::Hot);
        // get era diff
        let diff_intervals = this.get_interval_diff(other_intervals);

        // get subintervals for mismatching intervals from other
        let other_subintervals = other.get_interval_content(diff_intervals);
        // get intervals diff
        let diff_subintervals = this.get_subinterval_diff(other_subintervals);

        if !diff_subintervals.is_empty() {
            let mut diff_string = Vec::new();
            // properties = timestamp=xxx,subintervals=[www,ee,rr]
            for each_sub in diff_subintervals {
                diff_string.push(each_sub.to_string());
            }
            let properties = format!(
                "timestamp={};{}=[{}]",
                other.timestamp,
                SUBINTERVALS,
                diff_string.join(",")
            );
            let reply_content = self.perform_query(other_rep.to_string(), properties).await;
            let other_content =
                serde_json::from_str(&reply_content.unwrap()).unwrap_or_default();
            // get subintervals diff
            return this.get_full_content_diff(other_content);
        }
        Vec::new()
    }
}

// replying queries
impl Replica {
    fn parse_selector(
        &self,
        selector: Selector,
    ) -> (
        // Timestamp,
        Option<EraType>,
        Option<Vec<u64>>,
        Option<Vec<u64>>,
        Option<Vec<Timestamp>>,
    ) {
        let properties = selector.parse_value_selector().unwrap().properties; // note: this is a hashmap
        debug!("[ALIGN QUERYABLE] Properties are ************** : {:?}", properties);
        let era = if properties.get(ERA).is_none() {
            None
        } else {
            Some(EraType::from_str(properties.get("era").unwrap()).unwrap())
        };
        let intervals = if properties.get(INTERVALS).is_none() {
            None
        } else {
            let mut intervals = properties.get(INTERVALS).unwrap().to_string();
            intervals.remove(0);
            intervals.pop();
            Some(
                intervals
                    .split(',')
                    .map(|x| x.parse::<u64>().unwrap())
                    .collect::<Vec<u64>>(),
            )
        };
        let subintervals = if properties.get(SUBINTERVALS).is_none() {
            None
        } else {
            let mut subintervals = properties.get(SUBINTERVALS).unwrap().to_string();
            subintervals.remove(0);
            subintervals.pop();
            Some(
                subintervals
                    .split(',')
                    .map(|x| x.parse::<u64>().unwrap())
                    .collect::<Vec<u64>>(),
            )
        };
        let contents = if properties.get(CONTENTS).is_none() {
            None
        } else {
            let contents = serde_json::from_str(properties.get(CONTENTS).unwrap()).unwrap();
            Some(contents)
        };
        (era, intervals, subintervals, contents)
    }

    async fn get_value(
        &self,
        // timestamp: Timestamp,
        era: Option<EraType>,
        intervals: Option<Vec<u64>>,
        subintervals: Option<Vec<u64>>,
        contents: Option<Vec<Timestamp>>,
    ) -> Option<String> {
        // TODO: timestamp is useless????
        // debug!("***********************asking value");
        if era.is_some() {
            // debug!("*************asking for era content");
            let intervals = self.get_intervals(era.unwrap()).await;
            return Some(serde_json::to_string(&intervals).unwrap());
        }
        if intervals.is_some() {
            // debug!(
            //     "*************asking for intervals {:?}",
            //     intervals.as_ref().unwrap()
            // );
            let mut subintervals = HashMap::new();
            for each in intervals.unwrap() {
                subintervals.extend(self.get_subintervals(each).await);
            }
            return Some(serde_json::to_string(&subintervals).unwrap());
        }

        if subintervals.is_some() {
            // debug!(
            //     "*************asking for subintervals {:?}",
            //     subintervals.as_ref().unwrap()
            // );
            let mut content = HashMap::new();
            for each in subintervals.unwrap() {
                content.extend(self.get_content(each).await);
            }
            return Some(serde_json::to_string(&content).unwrap());
        }

        if contents.is_some() {
            // debug!(
            //     "*************asking for content {:?}",
            //     contents.as_ref().unwrap()
            // );
            //  TODO: club into a single query to DB
            let mut result = HashMap::new();
            for each in contents.unwrap() {
                let entry = self.get_entry_with_ts(each).await;
                // debug!(
                //     "*********************** entry for content {} is {:?}",
                //     each, entry
                // );
                if entry.is_some() {
                    let entry = entry.unwrap();
                    result.insert(
                        entry.key_expr.as_str().to_string(),
                        (entry.value.as_json().unwrap(), entry.timestamp),
                    );
                }
            }
            return Some(serde_json::to_string(&result).unwrap());
        }

        None
    }

    // TODO: replace this and directly read from storage calling storage infra
    // TODO: query on /keyexpr/key?starttime=ts;stoptime=ts
    async fn get_entry_with_ts(&self, timestamp: Timestamp) -> Option<Sample> {
        // get corresponding key from log
        // let mut key: Option<String> = None;
        let mut key = None;
        let replica_data = self.replica_data.as_ref().unwrap();
        let log = replica_data.stable_log.read().await;
        // debug!("**************** log is {:?}", *log);
        for (k, ts) in &*log {
            // debug!("************** searching for {} in log", timestamp);
            if *ts == timestamp {
                // debug!("**************** got corresponding key {} ", k);
                key = Some(k.to_string());
            }
        }
        // None

        if key.is_some() {
            let mut replies = self.session.get(&key.unwrap()).await.unwrap();
            if let Some(reply) = replies.next().await {
                // println!(
                //     ">> Received ('{}': '{}')",
                //     reply.data.key_expr.as_str(),
                //     String::from_utf8_lossy(&reply.data.value.payload.contiguous())
                // )
                if reply.sample.timestamp.is_some() {
                    if reply.sample.timestamp.unwrap() > timestamp {
                    // data must have been already overridden
                    return None;
                    } else if reply.sample.timestamp.unwrap() < timestamp {
                        error!("Data in the storage is older than requested.");
                        return None;
                    } else {
                        return Some(reply.sample);
                    }
                }
            }
        }
        None
        // TODO: query storage for this key, getting value + timestamp.. if timestamp is the same as the requested one, return the key-value pair. If it is older, raise an error and if it is newer, send an empty response since the data is no longer valid.

        // if key.is_none() {
        //     warn!("Corresponding entry for timestamp {} not found in log, might have been replaced.", timestamp);
        //     return None;
        // } else {
        //     let key = key.unwrap();
        //     let selector = format!("{}?starttime={};stoptime={}", key, timestamp, timestamp);
        //     let mut replies = self.session.get(&selector).await.unwrap();
        //     while let Some(reply) = replies.next().await {
        //         // println!(
        //         //     ">> Received ('{}': '{}')",
        //         //     reply.data.key_expr.as_str(),
        //         //     String::from_utf8_lossy(&reply.data.value.payload.contiguous())
        //         // );
        //         return Some(serde_json::from_str(reply.data).unwrap());
        //     }
        //     None
        // }
        //
        // let storage = self.storage.read().await;
        // for (k, v) in &(*storage) {
        //     if v.0.to_string() == ts.to_string() {
        //         //TODO: find a better way to compare
        //         return serde_json::to_string(&(k.to_string(), (v.0, v.1.clone()))).unwrap();
        //     }
        // }
        // drop(storage);
        // OVERWRITTEN_DATA.to_string()
    }

    async fn get_intervals(&self, era: EraType) -> HashMap<u64, u64> {
        let replica_data = self.replica_data.as_ref().unwrap();
        let digest = replica_data.digest.read().await;
        digest.as_ref().unwrap().get_era_content(era)
    }

    async fn get_subintervals(&self, interval: u64) -> HashMap<u64, u64> {
        let replica_data = self.replica_data.as_ref().unwrap();
        let digest = replica_data.digest.read().await;
        let mut intervals = HashSet::new();
        intervals.insert(interval);
        digest.as_ref().unwrap().get_interval_content(intervals)
    }

    async fn get_content(&self, subinterval: u64) -> HashMap<u64, Vec<zenoh::time::Timestamp>> {
        let replica_data = self.replica_data.as_ref().unwrap();
        let digest = replica_data.digest.read().await;
        let mut subintervals = HashSet::new();
        subintervals.insert(subinterval);
        digest
            .as_ref()
            .unwrap()
            .get_subinterval_content(subintervals)
    }
}

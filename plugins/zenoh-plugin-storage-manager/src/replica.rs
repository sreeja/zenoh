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
use async_std::sync::{RwLock, Mutex};
use async_std::task::sleep;
use flume::{Receiver, Sender};
// use async_std::task;
use futures::select;
use futures::stream::StreamExt;
// use futures::FutureExt;
use futures::join;
// use futures::prelude::*;
// use std::array::IntoIter;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fs;
// use std::fs::File;
use std::io::Write;
// use std::iter::FromIterator;
use std::str;
use std::str::FromStr;
use std::time::Duration;
use zenoh::prelude::Sample;
use zenoh::queryable::EVAL;
use zenoh::time::Timestamp;
use zenoh::Session;
use async_std::sync::Arc;
// use log::{debug, info};
use log::{debug, error, trace, warn, info};
use zenoh::prelude::{KeyExpr, Value};
use zenoh_core::Result as ZResult;
use zenoh::queryable;
use zenoh_backend_traits::Query;
use zenoh::prelude::*;

#[path = "digest.rs"]
pub mod digest;
pub use digest::*;

const ALIGN_PREFIX: &str = "/@-digest";
// const OVERWRITTEN_DATA: &str = "IRRELEVANT";
const PUBLICATION_INTERVAL: Duration = Duration::from_secs(5);

pub enum StorageMessage {
    Stop,
    GetStatus(async_std::channel::Sender<serde_json::Value>),
}

pub struct Replica {
    name: String,                                          // name of replica  -- to be replaced by UUID(zenoh)/<storage_type>/<storage_name>
    session: Arc<Session>,                               // zenoh session used by the replica
    key_expr: String, // key expression of the storage to be functioning as a replica
    stable_log: RwLock<HashMap<String, Timestamp>>, // log entries until the snapshot time
    volatile_log: RwLock<HashMap<String, Timestamp>>, // log entries after the snapshot time
    // storage: RwLock<HashMap<String, (Timestamp, String)>>, // key, (timestamp, value) -- the actual value being stored
    storage: Mutex<Box<dyn zenoh_backend_traits::Storage>>,
    in_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>,
    out_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>,
    digests_published: RwLock<HashSet<u64>>, // checksum of all digests generated and published by this replica
    digests_processed: RwLock<HashSet<u64>>, // checksum of all digests received by the replica
    last_snapshot_time: RwLock<Timestamp>, // the latest snapshot time
    last_interval: RwLock<u64>, // the latest interval
    digest: RwLock<Option<Digest>>, // the current stable digest
}

// functions to start services required by a replica
impl Replica {
    pub async fn initialize_replica(session: Arc<Session>, storage: Box<dyn zenoh_backend_traits::Storage>, in_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>, out_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>, key_expr: &str, admin_key: String, log: HashMap<String, Timestamp>) -> Replica {
        info!("[REPLICA]Openning session...");

        let (interval, time) = Replica::get_latest_snapshot_interval_time();

        // let key_expr = if key_expr.ends_with("**") { key_expr.strip_suffix("**").unwrap() } 
        //                 else { key_expr };

        //TODO:get refined key_expr from key_expr and name from admin_key

        let replica = Replica {
            name: admin_key,
            session: session,
            key_expr: key_expr.to_string(),
            stable_log: RwLock::new(HashMap::<String, Timestamp>::new()),
            volatile_log: RwLock::new(HashMap::<String, Timestamp>::new()),
            storage: Mutex::new(storage),
            in_interceptor: in_interceptor,
            out_interceptor:out_interceptor,
            digests_published: RwLock::new(HashSet::<u64>::new()),
            digests_processed: RwLock::new(HashSet::<u64>::new()),
            last_snapshot_time: RwLock::new(time),
            last_interval: RwLock::new(interval),
            digest: RwLock::new(None),
        };

        replica.initialize_log(log).await;
        replica.initialize_digest().await;

        replica
    }


    pub async fn start_replica(&self) -> ZResult<Sender<StorageMessage>>{
        // channel to queue digests to be aligned
        let (tx_digest, rx_digest) = flume::unbounded();
        // digest sub
        let digest_sub = self.start_digest_sub(tx_digest);
        // eval for align
        let align_eval = self.start_align_eval();
        // aligner
        let aligner = self.start_aligner(rx_digest);
        // digest pub
        let digest_pub = self.start_digest_pub();

        //updating snapshot time
        let snapshot_task = self.update_snapshot_task();

        let storage_task = self.start_storage_queryable_subscriber();

        let result = join!(
            digest_sub,
            align_eval,
            aligner,
            digest_pub,
            snapshot_task,
            storage_task,
        );

        result.5
    }

    pub async fn start_digest_sub(&self, tx: Sender<(String, Digest)>) {
        let key_expr = format!("{}{}**", ALIGN_PREFIX, self.key_expr);
        let mut received = HashMap::<String, Timestamp>::new();

        debug!(
            "[DIGEST_SUB]Creating Subscriber named {} on '{}'...",
            self.name, key_expr
        );
        let mut subscriber = self.session.subscribe(&key_expr).await.unwrap();

        loop {
            let sample = subscriber.receiver().next().await;
            let sample = sample.unwrap();
            let from = sample
                .key_expr
                .as_str()
                .split("/")
                .collect::<Vec<&str>>()
                .last()
                .unwrap()
                .clone();
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
                    &(from.to_string()),
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
        let key_expr = format!("{}{}{}", ALIGN_PREFIX, self.key_expr, self.name);

        debug!(
            "[DIGEST_PUB]Declaring digest on key expression '{}'...",
            key_expr
        );
        let expr_id = self.session.declare_expr(&key_expr).await.unwrap();
        println!("[DIGEST_PUB] => ExprId {}", expr_id);

        println!("[DIGEST_PUB]Declaring publication on '{}'...", expr_id);
        self.session.declare_publication(expr_id).await.unwrap();

        loop {
            sleep(PUBLICATION_INTERVAL).await;

            self.update_stable_log().await;

            let digest = self.digest.read().await;
            let digest = digest.as_ref().unwrap().compress();
            let digest_json = serde_json::to_string(&digest).unwrap();
            // self.save_digest().await;
            let mut digests_published = self.digests_published.write().await;
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
        let key_expr = format!("{}{}{}/**", ALIGN_PREFIX, self.key_expr, self.name);

        debug!("[ALIGN_EVAL]Creating Queryable on '{}'...", key_expr);
        let mut queryable = self.session.queryable(&key_expr).kind(EVAL).await.unwrap();

        loop {
            let query = queryable.receiver().next().await;
            let query = query.unwrap();
            debug!(
                "[ALIGN_EVAL]>> [Queryable ] Received Query '{}'",
                query.selector()
            );
            let selector = query.selector().key_selector;
            let selector = selector.as_str().to_lowercase();
            let (timestamp, era, interval, subinterval, content) = self.parse_selector(&selector);
            debug!("[ALIGN_EVAL] Parsed selector timestamp: {}, era: {}, interval:{:?}, subinterval:{:?}", timestamp, era, interval, subinterval);
            let value = self.get_value(era, interval, subinterval, content).await;
            let value = match value {
                Some(v) => v,
                None => String::from(""),
            };
            debug!("[ALIGN_EVAL] value for the query is {}", value);
            query.reply(Sample::new(selector, value));
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
            sleep(DELTA).await;
            let mut last_snapshot_time = self.last_snapshot_time.write().await;
            let mut last_interval = self.last_interval.write().await;
            let (interval, time) = Replica::get_latest_snapshot_interval_time();
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
            let mut storage_queryable = match self.session.queryable(&self.key_expr).kind(queryable::STORAGE).await
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
        // });
        // tx
    }

    async fn process_sample(&self, sample:Sample) {
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
        from: &String,
        ts: Timestamp,
        checksum: u64,
        received: HashMap<String, Timestamp>,
    ) -> bool {
        if from.to_string() == self.name {
            debug!("[DIGEST_SUB]Dropping own digest with checksum {}", checksum);
            return false;
        }
        // TODO: test this part
        if received.contains_key(from) && received.get(from).unwrap().clone() > ts {
            // not the latest from that replica
            debug!("[DIGEST_SUB]Dropping older digest at {} from {}", ts, from);
            return false;
        }
        //
        !self.in_processed(checksum).await
    }

    async fn in_processed(&self, checksum: u64) -> bool {
        let processed_set = self.digests_processed.read().await;
        let processed = processed_set.contains(&checksum);
        drop(processed_set);
        if processed {
            debug!("[DIGEST_SUB]Dropping {} since already processed", checksum);
            true
        } else {
            false
        }
    }
}

// maintain log and digest
impl Replica {
    async fn initialize_log(&self, log: HashMap<String, Timestamp>) {
        let last_snapshot_time = self.last_snapshot_time.read().await;

        let mut stable_log = self.stable_log.write().await;
        let mut volatile_log = self.volatile_log.write().await;
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
        let log_locked = self.stable_log.read().await;
        let latest_interval = self.last_interval.read().await;
        let latest_snapshot_time = self.last_snapshot_time.read().await;
        let digest = Digest::create_digest(
            now.into(),
            (*log_locked).values().map(|ts| *ts).collect(),
            *latest_interval,
            *latest_snapshot_time,
        );
        drop(latest_interval);
        drop(latest_snapshot_time);

        let mut digest_lock = self.digest.write().await;
        *digest_lock = Some(digest);
        drop(digest_lock);
    }

    async fn update_log(&self, key: String, ts: Timestamp) {
        let last_snapshot_time = self.last_snapshot_time.read().await;
        let last_interval = self.last_interval.read().await;
        let mut redundant_content = HashSet::new();
        let mut new_stable_content = HashSet::new();
        if ts > *last_snapshot_time {
            let mut log = self.volatile_log.write().await;
            (*log).insert(key, ts);
            drop(log);
        } else {
            let mut log = self.stable_log.write().await;
            let redundant = (*log).insert(key, ts);
            if redundant.is_some() {
                redundant_content.insert(redundant.unwrap());
            }
            drop(log);
            new_stable_content.insert(ts);
        }
        let mut digest = self.digest.write().await;
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
        let last_snapshot_time = self.last_snapshot_time.read().await;
        let last_interval = self.last_interval.read().await;
        let volatile = self.volatile_log.read().await;
        let mut stable = self.stable_log.write().await;
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

        let mut volatile = self.volatile_log.write().await;
        *volatile = still_volatile;
        drop(volatile);

        let mut digest = self.digest.write().await;
        let updated_digest = Digest::update_digest(
            digest.as_ref().unwrap().clone(),
            *last_interval,
            *last_snapshot_time,
            new_stable,
            redundant_stable,
        )
        .await;
        *digest = Some(updated_digest);
    }

    async fn flush(&self) {
        // let storage_filename = format!("{}.json", self.name);
        let log_filename = format!("{}_log.json", self.name.replace("/", "-"));
        // let mut file = fs::File::create(storage_filename).unwrap();
        let mut log_file = fs::File::create(log_filename).unwrap();

        // let storage = self.storage.read().await;
        let log = self.stable_log.read().await;
        // let j = serde_json::to_string(&(*storage)).unwrap();
        // file.write_all(j.as_bytes()).unwrap();
        let l = serde_json::to_string(&(*log)).unwrap();
        log_file.write_all(l.as_bytes()).unwrap();
        drop(log);
        // drop(storage);
    }

    fn get_latest_snapshot_interval_time() -> (u64, Timestamp) {
        let now = zenoh::time::new_reception_timestamp();
        let latest_interval = (now
            .get_time()
            .to_system_time()
            .duration_since(EPOCH_START)
            .unwrap()
            .as_millis()
            - PROPAGATION_DELAY.as_millis())
            / DELTA.as_millis();
        let latest_snapshot_time = zenoh::time::Timestamp::new(
            zenoh::time::NTP64::from(Duration::from_millis(
                u64::try_from(DELTA.as_millis() * latest_interval).unwrap(),
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
    //     // in real zenoh application, use this line instead
    //     // let filename = format!("{}-{}", timestamp.get_time().to_duration().as_nanos().to_string(), timestamp.get_id());
    // }
}

// functions to query data as needed for alignment
impl Replica {
    //identify alignment requirements -> {hot => [subintervals], warm => [intervals], cold => []}
    async fn process_incoming_digest(&self, other: Digest, from: &String) {
        let checksum = other.checksum;
        let timestamp = other.timestamp;
        let missing_content = self.get_missing_content(other, from).await;
        debug!("[REPLICA] Missing content is {:?}", missing_content);

        let missing_data = self
            .get_missing_data(&missing_content, timestamp, from)
            .await;

        for (key, (ts, value)) in missing_data {
            let sample = Sample::new(key, value).with_timestamp(ts);
            self.process_sample(sample).await;
        }

        self.flush().await;

        let mut processed = self.digests_processed.write().await;
        (*processed).insert(checksum);
        drop(processed);
    }

    async fn get_missing_data(
        &self,
        missing_content: &Vec<Timestamp>,
        timestamp: Timestamp,
        from: &String,
    ) -> HashMap<KeyExpr<'static>, (Timestamp, Value)> {
        let mut result = HashMap::new();
        for content in missing_content {
            let selector = format!(
                "{}{}{}/{}/*/*/*/{}",
                ALIGN_PREFIX,
                self.key_expr,
                from.to_string(),
                timestamp,
                content
            );
            let reply_content = self.perform_query(selector).await;
            if reply_content.is_some() {
                let (k, (ts, v)): (KeyExpr<'static>, (Timestamp, Value)) = reply_content.unwrap();
                result.insert(k, (ts, v));
            }
        }
        result
    }

    async fn get_missing_content(&self, other: Digest, from: &String) -> Vec<Timestamp> {
        // get my digest
        let digest = self.digest.read().await;
        let this = digest.as_ref().unwrap();

        // get first level diff of digest wrt other - subintervals, of HOT, intervals of WARM and COLD if misaligned
        let mis_eras = this.get_era_diff(other.eras.clone());
        let mut missing_content = Vec::new();
        if mis_eras.contains(&EraType::Cold) {
            // perform cold alignment
            let mut cold_data = self
                .perform_cold_alignment(this, from.to_string(), other.timestamp.clone())
                .await;
            missing_content.append(&mut cold_data);
        }
        if mis_eras.contains(&EraType::Warm) {
            // perform warm alignment
            let mut warm_data = self
                .perform_warm_alignment(this, from.to_string(), other.clone())
                .await;
            missing_content.append(&mut warm_data);
        }
        if mis_eras.contains(&EraType::Hot) {
            // perform hot alignment
            let mut hot_data = self
                .perform_hot_alignment(this, from.to_string(), other)
                .await;
            missing_content.append(&mut hot_data);
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
        let selector = format!(
            "{}{}{}/{}/cold/*",
            ALIGN_PREFIX,
            self.key_expr,
            other_rep.to_string(),
            timestamp
        );
        let reply_content = self.perform_query(selector).await;
        // while reply_content.is_none() {
        //     let reply_content = self.perform_query(selector).await;
        // }
        let other_intervals = serde_json::from_str(&String::from_utf8_lossy(&reply_content.unwrap().1.1.payload.contiguous())).unwrap_or(HashMap::new());
        // get era diff
        let diff_intervals = this.get_interval_diff(other_intervals);
        let mut diff_subintervals = HashSet::<u64>::new();
        for each_int in diff_intervals {
            // get subintervals for mismatching intervals from other_rep
            let selector = format!(
                "{}{}{}/{}/*/{}",
                ALIGN_PREFIX,
                self.key_expr,
                other_rep.to_string(),
                timestamp,
                each_int
            );
            let reply_content = self.perform_query(selector).await;
            let other_subintervals = serde_json::from_str(&String::from_utf8_lossy(&reply_content.unwrap().1.1.payload.contiguous())).unwrap_or(HashMap::new());
            // get intervals diff
            let diff = this.get_subinterval_diff(other_subintervals);
            debug!("[ALIGNER] Diff in interval {} is {:?}", each_int, diff);
            diff_subintervals.extend(&diff);
        }
        debug!(
            "[ALIGNER] The subintervals that need alignment are : {:?}",
            diff_subintervals
        );

        let mut diff_content = Vec::new();
        for each_sub in diff_subintervals {
            //get content for mismatching intervals from other_rep
            let selector = format!(
                "{}{}{}/{}/*/*/{}",
                ALIGN_PREFIX,
                self.key_expr,
                other_rep.to_string(),
                timestamp,
                each_sub
            );
            let reply_content = self.perform_query(selector).await;
            let other_content = serde_json::from_str(&String::from_utf8_lossy(&reply_content.unwrap().1.1.payload.contiguous())).unwrap_or(HashMap::new());
            // get subintervals diff
            diff_content.append(&mut this.get_full_content_diff(other_content));
        }
        diff_content
    }

    async fn perform_query(&self, selector: String) -> Option<(KeyExpr<'static>, (Timestamp, Value))> {
        debug!(
            "[ALIGNER]Sending Query for getting intervals of cold alignment '{}'...",
            selector
        );

        let mut replies = self.session.get(&selector).await.unwrap();
        while let Some(reply) = replies.next().await {
            debug!(
                "[ALIGNER]>> Received ('{}': '{}')",
                reply.sample.key_expr.as_str(),
                reply.sample.value.payload
            );
            // reply_content =
            // format!("{:?}", reply.sample.value.payload);
            return Some((reply.sample.key_expr, (reply.sample.timestamp.unwrap(), reply.sample.value)));
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

        let diff_subintervals = HashSet::new();
        for each_int in diff_intervals {
            // get subintervals for mismatching intervals from other_rep
            let selector = format!(
                "{}{}{}/{}/*/{}",
                ALIGN_PREFIX,
                self.key_expr,
                other_rep.to_string(),
                other.timestamp,
                each_int
            );
            let reply_content = self.perform_query(selector).await;
            let other_subintervals = serde_json::from_str(&String::from_utf8_lossy(&reply_content.unwrap().1.1.payload.contiguous())).unwrap_or(HashMap::new());
            // get intervals diff
            diff_subintervals.union(&this.get_subinterval_diff(other_subintervals));
        }

        let mut diff_content = Vec::new();
        for each_sub in diff_subintervals {
            //get content for mismatching intervals from other_rep
            let selector = format!(
                "{}{}{}/{}/*/*/{}",
                ALIGN_PREFIX,
                self.key_expr,
                other_rep.to_string(),
                other.timestamp,
                each_sub
            );
            let reply_content = self.perform_query(selector).await;
            let other_content = serde_json::from_str(&String::from_utf8_lossy(&reply_content.unwrap().1.1.payload.contiguous())).unwrap_or(HashMap::new());
            // get subintervals diff
            diff_content.append(&mut this.get_full_content_diff(other_content));
        }
        diff_content
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

        let mut diff_content = Vec::new();
        for each_sub in diff_subintervals {
            //get content for mismatching intervals from other_rep
            let selector = format!(
                "{}{}{}/{}/*/*/{}",
                ALIGN_PREFIX,
                self.key_expr,
                other_rep.to_string(),
                other.timestamp,
                each_sub
            );
            let reply_content = self.perform_query(selector).await;
            let other_content = serde_json::from_str(&String::from_utf8_lossy(&reply_content.unwrap().1.1.payload.contiguous())).unwrap_or(HashMap::new());
            // get subintervals diff
            diff_content.append(&mut this.get_full_content_diff(other_content));
        }
        diff_content
    }
}

// replying queries
impl Replica {
    fn parse_selector(
        &self,
        selector: &str,
    ) -> (
        Timestamp,
        String,
        Option<String>,
        Option<String>,
        Option<Timestamp>,
    ) {
        // filter out the redundant part => /digest<key_expr><name>/
        let filtering_out_len = ALIGN_PREFIX.len() + self.key_expr.len() + self.name.len() + 1;
        let (_, selector) = selector.split_at(filtering_out_len);
        let parts: Vec<&str> = selector.split("/").collect();
        // println!("[PARSING>>>>] {:?}", parts);
        // 0/1 -> timestamp
        let timestamp = format!("{}/{}", parts[0].to_uppercase(), parts[1].to_uppercase());
        // let timestamp = format!("{}", parts[0].to_uppercase());
        let timestamp = Timestamp::from_str(&timestamp).unwrap();
        // 2 -> era
        let era = String::from(parts[2]);
        // 3 -> interval
        let interval = if parts.len() > 3 {
            Some(String::from(parts[3]))
        } else {
            None
        };
        // 4 -> subinterval
        let subinterval = if parts.len() > 4 {
            Some(String::from(parts[4]))
        } else {
            None
        };
        // 5 -> content
        let content = if parts.len() > 5 {
            let content_str = format!("{}/{}", parts[5].to_uppercase(), parts[6].to_uppercase());
            Some(Timestamp::from_str(content_str.as_str()).unwrap())
        } else {
            None
        };
        (timestamp, era, interval, subinterval, content)
    }

    async fn get_value(
        &self,
        era: String,
        interval: Option<String>,
        subinterval: Option<String>,
        content: Option<Timestamp>,
    ) -> Option<String> {
        if era.as_str() != "*" {
            // println!("[ALIGN_EVAL] getting era content");
            let eratype = EraType::from_str(&era).unwrap();
            let intervals = self.get_intervals(eratype).await;
            return Some(serde_json::to_string(&intervals).unwrap());
        }

        if interval.is_some() {
            // println!("[ALIGN_EVAL] getting interval content");
            let i = interval.unwrap();
            if i != "*" {
                let int: u64 = i.parse().unwrap();
                let subintervals = self.get_subintervals(int).await;
                return Some(serde_json::to_string(&subintervals).unwrap());
            }
        }

        if subinterval.is_some() {
            // println!("[ALIGN_EVAL] getting subinterval content");
            let s = subinterval.unwrap();
            if s != "*" {
                let sub: u64 = s.parse().unwrap();
                let content = self.get_content(sub).await;
                return Some(serde_json::to_string(&content).unwrap());
            }
        }

        if content.is_some() {
            // println!("[ALIGN_EVAL] getting update from timestamp {}", ts);
            let ts = content.unwrap();
            return self.get_entry_with_ts(ts).await;
        }

        None
    }

    // TODO: replace this and directly read from storage calling storage infra
    async fn get_entry_with_ts(&self, timestamp: Timestamp) -> Option<String> {
        // TODO: query on /keyexpr/key?starttime=ts;stoptime=ts
        // get corresponding key from log
        // let mut key: Option<String> = None;
        let log = self.stable_log.read().await;
        for (k, ts) in &*log {
            if ts.clone() == timestamp {
                return Some(k.to_string());
            }
        }
        None
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
        let digest = self.digest.read().await;
        digest.as_ref().unwrap().get_era_content(era)
    }

    async fn get_subintervals(&self, interval: u64) -> HashMap<u64, u64> {
        let digest = self.digest.read().await;
        let mut intervals = HashSet::new();
        intervals.insert(interval);
        digest.as_ref().unwrap().get_interval_content(intervals)
    }

    async fn get_content(&self, subinterval: u64) -> HashMap<u64, Vec<zenoh::time::Timestamp>> {
        let digest = self.digest.read().await;
        let mut subintervals = HashSet::new();
        subintervals.insert(subinterval);
        digest
            .as_ref()
            .unwrap()
            .get_subinterval_content(subintervals)
    }
}

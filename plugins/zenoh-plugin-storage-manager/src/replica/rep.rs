//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use super::align_eval::AlignEval;
use super::aligner::Aligner;
use super::digest::Digest;
use crate::storages_mgt::StorageMessage;
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
use std::time::Duration;
use zenoh::net::protocol::io::SplitBuffer;
use zenoh::prelude::Sample;
use zenoh::prelude::*;
use zenoh::prelude::{KeyExpr, Value};
use zenoh::queryable;
use zenoh::time::Timestamp;
use zenoh::Session;
use zenoh_backend_traits::config::ReplicaConfig;
use zenoh_backend_traits::{Query, StorageInsertionResult};
use zenoh_core::Result as ZResult;
// #[path = "digest.rs"]
// pub mod digest;
// pub use digest::*;
// #[path = "aligner.rs"]
// pub mod aligner;
// pub use aligner::*;
// #[path = "align_eval.rs"]
// pub mod align_eval;
// pub use align_eval::*;

// const ERA: &str = "era";
// const INTERVALS: &str = "intervals";
// const SUBINTERVALS: &str = "subintervals";
// const CONTENTS: &str = "contents";

struct ReplicaData {
    stable_log: Arc<RwLock<HashMap<String, Timestamp>>>, // log entries until the snapshot time
    volatile_log: RwLock<HashMap<String, Timestamp>>,    // log entries after the snapshot time
    digests_published: RwLock<HashSet<u64>>, // checksum of all digests generated and published by this replica
    digests_processed: Arc<RwLock<HashSet<u64>>>, // checksum of all digests received by the replica
    last_snapshot_time: RwLock<Timestamp>,   // the latest snapshot time
    last_interval: RwLock<u64>,              // the latest interval
    digest: Arc<RwLock<Option<Digest>>>,     // the current stable digest
}

pub struct Replica {
    name: String, // name of replica  -- UUID(zenoh)-<storage_name>((-<storage_type>??))
    session: Arc<Session>, // zenoh session used by the replica
    key_expr: String, // key expression of the storage to be functioning as a replica
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
        log: Vec<(String, Timestamp)>,
    ) -> ZResult<Sender<crate::StorageMessage>> {
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
                    stable_log: Arc::new(RwLock::new(HashMap::<String, Timestamp>::new())),
                    volatile_log: RwLock::new(HashMap::<String, Timestamp>::new()),
                    digests_published: RwLock::new(HashSet::<u64>::new()),
                    digests_processed: Arc::new(RwLock::new(HashSet::<u64>::new())),
                    last_snapshot_time: RwLock::new(time),
                    last_interval: RwLock::new(interval),
                    digest: Arc::new(RwLock::new(None)),
                }),
            };

            replica.initialize_log(log).await;
            replica.initialize_digest().await;

            // channel to queue digests to be aligned
            let (tx_digest, rx_digest) = flume::unbounded();
            // channel for aaligner to send missing samples to storage
            let (tx_sample, rx_sample) = flume::unbounded();
            // digest sub
            let digest_sub = replica.start_digest_sub(tx_digest);
            // eval for align
            let digest_key = replica.get_digest_key();
            let replica_data = replica.replica_data.as_ref().unwrap();
            let align_eval = AlignEval::start_align_eval(
                replica.session.clone(),
                &digest_key,
                &replica.name,
                replica_data.stable_log.clone(),
                replica_data.digest.clone(),
            ); //replica.start_align_eval();
               // aligner
            let aligner = Aligner::start_aligner(
                replica.session.clone(),
                &digest_key,
                &replica.name,
                rx_digest,
                tx_sample,
                replica_data.digests_processed.clone(),
                replica_data.digest.clone(),
            ); //replica.start_aligner(rx_digest);
               // digest pub
            let digest_pub = replica.start_digest_pub();

            //updating snapshot time
            let snapshot_task = replica.update_snapshot_task();

            //actual storage
            // TODO: use the incoming sample from aligner. Those are the missing ones.
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
                sample.value
            );
            let digest: Digest =
                serde_json::from_str(&format!("{}", sample.value)).unwrap();
            let ts = digest.timestamp;
            let to_be_processed = self
                .processing_needed(from, digest.timestamp, digest.checksum, received.clone())
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

    // pub async fn start_align_eval(&self) {
    //     let digest_key = format!("{}{}/**", self.get_digest_key(), self.name);

    //     debug!("[ALIGN_EVAL]Creating Queryable on '{}'...", digest_key);
    //     let mut queryable = self
    //         .session
    //         .queryable(&digest_key)
    //         .kind(EVAL)
    //         .await
    //         .unwrap();

    //     loop {
    //         let query = queryable.receiver().next().await;
    //         let query = query.unwrap();
    //         debug!(
    //             "[ALIGN_EVAL]>> [Queryable ] Received Query '{}'",
    //             query.selector()
    //         );
    //         let selector = query.selector().clone();
    //         let (era, interval, subinterval, content) = self.parse_selector(selector.clone());
    //         debug!("[ALIGN_EVAL] Parsed selector era: {:?}, interval:{:?}, subinterval:{:?}, content:{:?}", era, interval, subinterval, content);
    //         let value = self.get_value(era, interval, subinterval, content).await;
    //         let value = match value {
    //             Some(v) => v,
    //             None => String::from(""),
    //         };
    //         debug!("[ALIGN_EVAL] value for the query is {}", value);
    //         query.reply(Sample::new(
    //             selector.key_selector.as_str().to_string(),
    //             value,
    //         ));
    //     }
    // }

    // pub async fn start_aligner(&self, rx: Receiver<(String, Digest)>) {
    //     while let Ok((from, incoming_digest)) = rx.recv_async().await {
    //         debug!(
    //             "[ALIGNER]Processing digest: {:?} from {}",
    //             incoming_digest, from
    //         );
    //         if self.in_processed(incoming_digest.checksum).await {
    //             debug!("[ALIGNER]Skipping already processed digest");
    //             continue;
    //         } else {
    //             // process this digest
    //             self.process_incoming_digest(incoming_digest, &from).await;
    //         }
    //     }
    // }

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
        if result.is_ok() && !matches!(result.unwrap(), StorageInsertionResult::Outdated) {
            self.update_log(
                sample.key_expr.as_str().to_string(),
                *sample.get_timestamp().unwrap(),
            )
            .await;
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
    async fn initialize_log(&self, log: Vec<(String, Timestamp)>) {
        let replica_data = self.replica_data.as_ref().unwrap();
        let last_snapshot_time = replica_data.last_snapshot_time.read().await;

        let mut stable_log = replica_data.stable_log.write().await;
        let mut volatile_log = replica_data.volatile_log.write().await;
        for (k, ts) in log {
            // depending on the associated timestamp, either to stable_log or volatile log
            // entries until last_snapshot_time goes to stable
            if ts > *last_snapshot_time {
                if volatile_log.contains_key(&k) {
                    if *volatile_log.get(&k).unwrap() < ts {
                        (*volatile_log).insert(k, ts);
                    }
                } else {
                    (*volatile_log).insert(k, ts);
                }
            } else {
                if stable_log.contains_key(&k) {
                    if *stable_log.get(&k).unwrap() < ts {
                        (*stable_log).insert(k, ts);
                    }
                } else {
                    (*stable_log).insert(k, ts);
                }
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
            super::DigestConfig {
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
            .duration_since(super::EPOCH_START)
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

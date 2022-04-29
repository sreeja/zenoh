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
use async_std::sync::Arc;
use async_std::sync::RwLock;
use async_std::task::sleep;
use flume::Sender;
use futures::join;
use futures::stream::StreamExt;
use log::{debug, info};
use std::collections::{HashMap, HashSet};
use std::str;
use zenoh::prelude::Sample;
use zenoh::prelude::*;
use zenoh::time::Timestamp;
use zenoh::Session;
use zenoh_backend_traits::config::ReplicaConfig;
use zenoh_core::Result as ZResult;
use super::{Digest, Aligner, AlignEval, StorageService, Snapshotter};

pub struct Replica {
    name: String, // name of replica  -- UUID(zenoh)-<storage_name>((-<storage_type>??))
    session: Arc<Session>, // zenoh session used by the replica
    key_expr: String,      // key expression of the storage to be functioning as a replica
    replica_config: ReplicaConfig, // replica configuration - if some, replica, if none, normal storage
    digests_published: RwLock<HashSet<u64>>, // checksum of all digests generated and published by this replica
}

impl Replica {
    pub async fn initialize_replica(
        config: ReplicaConfig,
        session: Arc<Session>,
        storage: Box<dyn zenoh_backend_traits::Storage>,
        in_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>,
        out_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>,
        key_expr: &str,
        name: &str,
        log: Vec<(String, Timestamp)>,
    ) -> ZResult<Sender<crate::StorageMessage>> {
        info!("[REPLICA]Opening session...");

        let replica = Replica {
            name: name.to_string(),
            session,
            key_expr: key_expr.to_string(),
            replica_config: config,
            digests_published: RwLock::new(HashSet::new())
        };

        // channel to queue digests to be aligned
        let (tx_digest, rx_digest) = flume::unbounded();
        // channel for aaligner to send missing samples to storage
        let (tx_sample, rx_sample) = flume::unbounded();
        // channel for storage to send loggin information back
        let (tx_log, rx_log) = flume::unbounded();
        let config = replica.replica_config.clone();
        // snapshotter
        let snapshotter = Arc::new(Snapshotter::new(rx_log, log, config.clone()).await);
        // digest sub
        let digest_sub = replica.start_digest_sub(tx_digest);
        // eval for align
        let digest_key = Replica::get_digest_key(key_expr.to_string(), config.align_prefix);
        let align_eval = AlignEval::start_align_eval(
            replica.session.clone(),
            &digest_key,
            &replica.name,
            snapshotter.clone()
        ); 
        // aligner
        let aligner = Aligner::start_aligner(
            replica.session.clone(),
            &digest_key,
            rx_digest,
            tx_sample,
            snapshotter.clone()
        ); 
        // digest pub
        let digest_pub = replica.start_digest_pub(snapshotter.clone());

        //updating snapshot time
        let snapshot_task = snapshotter.start();

        //actual storage
        let storage_task = StorageService::start(replica.session.clone(), &replica.key_expr, &replica.name, storage, in_interceptor, out_interceptor, Some(rx_sample), Some(tx_log));

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
        let mut received = HashMap::<String, Timestamp>::new();

        let digest_key = format!("{}**", Replica::get_digest_key(self.key_expr.to_string(), self.replica_config.align_prefix.to_string()));

        debug!(
            "[DIGEST_SUB]Creating Subscriber named {} on '{}'...",
            self.name, digest_key
        );
        let mut subscriber = self.session.subscribe(&digest_key).await.unwrap();

        loop {
            let sample = subscriber.receiver().next().await;
            let sample = sample.unwrap();
            let from = &sample.key_expr.as_str()[Replica::get_digest_key(self.key_expr.to_string(), self.replica_config.align_prefix.to_string()).len()..];
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

    pub async fn start_digest_pub(&self, snapshotter: Arc<Snapshotter>) {
        let digest_key = format!("{}{}", Replica::get_digest_key(self.key_expr.to_string(), self.replica_config.align_prefix.to_string()), self.name);

        debug!(
            "[DIGEST_PUB]Declaring digest on key expression '{}'...",
            digest_key
        );
        let expr_id = self.session.declare_expr(&digest_key).await.unwrap();
        debug!("[DIGEST_PUB] => ExprId {}", expr_id);

        debug!("[DIGEST_PUB]Declaring publication on '{}'...", expr_id);
        self.session.declare_publication(expr_id).await.unwrap();

        loop {
            sleep(self.replica_config.publication_interval).await;

            let digest = snapshotter.get_digest().await;
            let digest = digest.compress();
            let digest_json = serde_json::to_string(&digest).unwrap();
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

        return true;
    }


    fn get_digest_key(key_expr: String, align_prefix: String) -> String {
        if key_expr.ends_with("**") {
            format!("{}{}", align_prefix, key_expr.strip_suffix("**").unwrap())
        } else if key_expr.ends_with('/') {
            format!("{}{}", align_prefix, key_expr)
        } else {
            format!("{}{}/", align_prefix, key_expr)
        }
    }
}

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

use super::Snapshotter;
use async_std::sync::Arc;
use async_std::sync::RwLock;
use flume::{Receiver, Sender};
use log::debug;
use std::collections::{HashMap, HashSet};
use std::str;
use zenoh::prelude::Sample;
use zenoh::prelude::{KeyExpr, Value};
use zenoh::time::Timestamp;
use zenoh::Session;
use zenoh_core::AsyncResolve;

pub struct Aligner {
    session: Arc<Session>,
    digest_key: String,
    // replica_name: String,
    snapshotter: Arc<Snapshotter>,
    rx_digest: Receiver<(String, super::Digest)>,
    tx_sample: Sender<Sample>,
    digests_processed: RwLock<HashSet<u64>>,
}

impl Aligner {
    pub async fn start_aligner(
        session: Arc<Session>,
        digest_key: &str,
        // replica_name: &str,
        rx_digest: Receiver<(String, super::Digest)>,
        tx_sample: Sender<Sample>,
        snapshotter: Arc<Snapshotter>,
    ) {
        let aligner = Aligner {
            session,
            digest_key: digest_key.to_string(),
            // replica_name: replica_name.to_string(),
            snapshotter,
            rx_digest,
            tx_sample,
            digests_processed: RwLock::new(HashSet::new()),
        };
        aligner.start().await;
    }

    pub async fn start(&self) {
        while let Ok((from, incoming_digest)) = self.rx_digest.recv_async().await {
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

    async fn in_processed(&self, checksum: u64) -> bool {
        // let replica_data = self.replica_data.as_ref().unwrap();
        let processed_set = self.digests_processed.read().await;
        let processed = processed_set.contains(&checksum);
        drop(processed_set);
        if processed {
            debug!("[ALIGNER]Dropping {} since already processed", checksum);
            true
        } else {
            false
        }
    }

    //identify alignment requirements -> {hot => [subintervals], warm => [intervals], cold => []}
    async fn process_incoming_digest(&self, other: super::Digest, from: &str) {
        let checksum = other.checksum;
        let timestamp = other.timestamp;
        let missing_content = self.get_missing_content(other, from).await;
        debug!("[REPLICA] Missing content is {:?}", missing_content);

        if !missing_content.is_empty() {
            let missing_data = self
                .get_missing_data(&missing_content, timestamp, from)
                .await;

            debug!("[REPLICA] Missing data is {:?}", missing_data);

            for (key, (ts, value)) in missing_data {
                let sample = Sample::new(key, value).with_timestamp(ts);
                debug!("[REPLICA] Adding sample {:?}", sample);
                // TODO: change this to a storage pipeline
                // self.process_sample(sample).await;
                self.tx_sample.send_async(sample).await.unwrap();
            }

            // TODO: if missing content is not identified, should we still consider it as processed?

            // let replica_data = self.replica_data.as_ref().unwrap();
            let mut processed = self.digests_processed.write().await;
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
            super::CONTENTS,
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

    async fn get_missing_content(&self, other: super::Digest, from: &str) -> Vec<Timestamp> {
        // get my digest
        // let replica_data = self.replica_data.as_ref().unwrap();
        let this = &self.snapshotter.get_digest().await;

        // get first level diff of digest wrt other - subintervals, of HOT, intervals of WARM and COLD if misaligned
        let mis_eras = this.get_era_diff(other.eras.clone());
        let mut missing_content = Vec::new();
        if mis_eras.contains(&super::EraType::Cold) {
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
        if mis_eras.contains(&super::EraType::Warm) {
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
        if mis_eras.contains(&super::EraType::Hot) {
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
        this: &super::Digest,
        other_rep: String,
        timestamp: Timestamp,
    ) -> Vec<Timestamp> {
        let properties = format!("timestamp={};{}=cold", timestamp, super::ERA);
        let reply_content = self.perform_query(other_rep.to_string(), properties).await;
        let other_intervals: HashMap<u64, u64> =
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
                super::INTERVALS,
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
                    super::SUBINTERVALS,
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
        let selector = format!("{}{}?({})", self.digest_key, from, properties);
        debug!("[ALIGNER]Sending Query '{}'...", selector);
        let replies = self.session.get(&selector).res().await.unwrap();
        if let Ok(reply) = replies.recv_async().await {
            match reply.sample {
                Ok(sample) => {
                    debug!(
                        "[ALIGNER]>> Received ('{}': '{}')",
                        sample.key_expr.as_str(),
                        sample.value
                    );
                    return Some(format!("{:?}", sample.value));
                }
                Err(e) => println!("Error on receiving sample: {}", e),
            }
        }
        None
    }

    //peform warm alignment
    // if WARM misaligned, ask for subinterval hashes of misaligned intervals
    // for misaligned subintervals, ask content
    async fn perform_warm_alignment(
        &self,
        this: &super::Digest,
        other_rep: String,
        other: super::Digest,
    ) -> Vec<Timestamp> {
        // get interval hashes for WARM intervals from other
        let other_intervals = other.get_era_content(super::EraType::Warm);
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
                super::INTERVALS,
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
                    super::SUBINTERVALS,
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
        this: &super::Digest,
        other_rep: String,
        other: super::Digest,
    ) -> Vec<Timestamp> {
        // get interval hashes for HOT intervals from other
        let other_intervals = other.get_era_content(super::EraType::Hot);
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
                super::SUBINTERVALS,
                diff_string.join(",")
            );
            let reply_content = self.perform_query(other_rep.to_string(), properties).await;
            let other_content = serde_json::from_str(&reply_content.unwrap()).unwrap_or_default();
            // get subintervals diff
            return this.get_full_content_diff(other_content);
        }
        Vec::new()
    }
}

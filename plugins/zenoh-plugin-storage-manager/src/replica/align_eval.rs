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

use super::digest::*;
use super::Snapshotter;
use async_std::sync::Arc;
use futures::stream::StreamExt;
use log::{debug, error};
use std::collections::{HashMap, HashSet};
use std::str;
use std::str::FromStr;
use zenoh::prelude::Sample;
use zenoh::prelude::*;
use zenoh::queryable::EVAL;
use zenoh::time::Timestamp;
use zenoh::Session;
// #[path = "digest.rs"]
// mod digest;
// use digest::*;

// const ERA: &str = "era";
// const INTERVALS: &str = "intervals";
// const SUBINTERVALS: &str = "subintervals";
// const CONTENTS: &str = "contents";

pub struct AlignEval {
    session: Arc<Session>,
    digest_key: String,
    snapshotter: Arc<Snapshotter>,
}

impl AlignEval {
    pub async fn start_align_eval(
        session: Arc<Session>,
        digest_key: &str,
        replica_name: &str,
        snapshotter: Arc<Snapshotter>,
    ) -> Self {
        let digest_key = format!("{}{}/**", digest_key, replica_name);

        let align_eval = AlignEval {
            session,
            digest_key,
            snapshotter,
        };

        align_eval.start().await
    }

    async fn start(&self) -> Self {
        debug!("[ALIGN_EVAL] Creating Eval on '{}'...", self.digest_key);
        let mut queryable = self
            .session
            .queryable(&self.digest_key)
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
                // warn!(
                //     "*********************** entry for content {} is {:?}",
                //     each, entry
                // );
                if entry.is_some() {
                    let entry = entry.unwrap();
                    // warn!("*************** value is {:?}", entry.value);
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
        debug!(
            "[ALIGN QUERYABLE] Properties are ************** : {:?}",
            properties
        );
        let era = if properties.get(super::ERA).is_none() {
            None
        } else {
            Some(EraType::from_str(properties.get(super::ERA).unwrap()).unwrap())
        };
        let intervals = if properties.get(super::INTERVALS).is_none() {
            None
        } else {
            let mut intervals = properties.get(super::INTERVALS).unwrap().to_string();
            intervals.remove(0);
            intervals.pop();
            Some(
                intervals
                    .split(',')
                    .map(|x| x.parse::<u64>().unwrap())
                    .collect::<Vec<u64>>(),
            )
        };
        let subintervals = if properties.get(super::SUBINTERVALS).is_none() {
            None
        } else {
            let mut subintervals = properties.get(super::SUBINTERVALS).unwrap().to_string();
            subintervals.remove(0);
            subintervals.pop();
            Some(
                subintervals
                    .split(',')
                    .map(|x| x.parse::<u64>().unwrap())
                    .collect::<Vec<u64>>(),
            )
        };
        let contents = if properties.get(super::CONTENTS).is_none() {
            None
        } else {
            let contents = serde_json::from_str(properties.get(super::CONTENTS).unwrap()).unwrap();
            Some(contents)
        };
        (era, intervals, subintervals, contents)
    }
}

// replying queries
impl AlignEval {
    // TODO: replace this and directly read from storage calling storage infra
    // TODO: query on /keyexpr/key?starttime=ts;stoptime=ts
    async fn get_entry_with_ts(&self, timestamp: Timestamp) -> Option<Sample> {
        // get corresponding key from log
        // let mut key: Option<String> = None;
        let mut key = None;
        // let replica_data = self.replica_data.as_ref().unwrap();
        let log = self.snapshotter.get_stable_log().await;
        // debug!("**************** log is {:?}", *log);
        for (k, ts) in log {
            // debug!("************** searching for {} in log", timestamp);
            if ts == timestamp {
                // debug!("**************** got corresponding key {} ", k);
                key = Some(k.to_string());
            }
        }
        // None

        if key.is_some() {
            let mut replies = self.session.get(&key.unwrap()).await.unwrap();
            if let Some(reply) = replies.next().await {
                debug!(
                    "[ALIGN QUERYABLE]>> Received ('{}': '{}')",
                    reply.sample.key_expr.as_str(),
                    reply.sample.value
                );
                if reply.sample.timestamp.is_some() {
                    if reply.sample.timestamp.unwrap() > timestamp {
                        // data must have been already overridden
                        return None;
                    } else if reply.sample.timestamp.unwrap() < timestamp {
                        error!("[ALIGN QUERYABLE] Data in the storage is older than requested.");
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
        // let replica_data = self.replica_data.as_ref().unwrap();
        let digest = self.snapshotter.get_digest().await;
        digest.get_era_content(era)
    }

    async fn get_subintervals(&self, interval: u64) -> HashMap<u64, u64> {
        // let replica_data = self.replica_data.as_ref().unwrap();
        let digest = self.snapshotter.get_digest().await;
        let mut intervals = HashSet::new();
        intervals.insert(interval);
        digest.get_interval_content(intervals)
    }

    async fn get_content(&self, subinterval: u64) -> HashMap<u64, Vec<zenoh::time::Timestamp>> {
        // let replica_data = self.replica_data.as_ref().unwrap();
        let digest = self.snapshotter.get_digest().await;
        let mut subintervals = HashSet::new();
        subintervals.insert(subinterval);
        digest.get_subinterval_content(subintervals)
    }
}

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
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::str;
use std::str::FromStr;
use zenoh::prelude::Sample;
use zenoh::prelude::*;
use zenoh::queryable::EVAL;
use zenoh::time::Timestamp;
use zenoh::Session;

pub struct AlignEval {
    session: Arc<Session>,
    digest_key: String,
    snapshotter: Arc<Snapshotter>,
}

#[derive(Debug)]
enum AlignComponent {
    Era(EraType),
    Intervals(Vec<u64>),
    Subintervals(Vec<u64>),
    Contents(Vec<Timestamp>),
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
            let diff_required = self.parse_selector(selector.clone());
            debug!(
                "[ALIGN_EVAL] Parsed selector diff_required:{:?}",
                diff_required
            );
            let value = self.get_value(diff_required).await;
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

    async fn get_value(&self, diff_required: Option<AlignComponent>) -> Option<String> {
        // TODO: timestamp is useless????
        match diff_required {
            Some(AlignComponent::Era(era)) => {
                let intervals = self.get_intervals(era).await;
                Some(serde_json::to_string(&intervals).unwrap())
            }
            Some(AlignComponent::Intervals(intervals)) => {
                let mut subintervals = HashMap::new();
                for each in intervals {
                    subintervals.extend(self.get_subintervals(each).await);
                }
                Some(serde_json::to_string(&subintervals).unwrap())
            }
            Some(AlignComponent::Subintervals(subintervals)) => {
                let mut content = HashMap::new();
                for each in subintervals {
                    content.extend(self.get_content(each).await);
                }
                Some(serde_json::to_string(&content).unwrap())
            }
            Some(AlignComponent::Contents(contents)) => {
                let mut result = HashMap::new();
                for each in contents {
                    let entry = self.get_entry_with_ts(each).await;
                    if entry.is_some() {
                        let entry = entry.unwrap();
                        result.insert(
                            entry.key_expr.as_str().to_string(),
                            (entry.value.as_json().unwrap(), entry.timestamp),
                        );
                    }
                }
                Some(serde_json::to_string(&result).unwrap())
            }
            None => None,
        }
    }

    fn parse_selector(&self, selector: Selector) -> Option<AlignComponent> {
        let properties = selector.parse_value_selector().unwrap().properties; // note: this is a hashmap
        debug!(
            "[ALIGN QUERYABLE] Properties are ************** : {:?}",
            properties
        );
        if properties.get(super::ERA).is_some() {
            Some(AlignComponent::Era(
                EraType::from_str(properties.get(super::ERA).unwrap()).unwrap(),
            ))
        } else if properties.get(super::INTERVALS).is_some() {
            let mut intervals = properties.get(super::INTERVALS).unwrap().to_string();
            intervals.remove(0);
            intervals.pop();
            Some(AlignComponent::Intervals(
                intervals
                    .split(',')
                    .map(|x| x.parse::<u64>().unwrap())
                    .collect::<Vec<u64>>(),
            ))
        } else if properties.get(super::SUBINTERVALS).is_some() {
            let mut subintervals = properties.get(super::SUBINTERVALS).unwrap().to_string();
            subintervals.remove(0);
            subintervals.pop();
            Some(AlignComponent::Subintervals(
                subintervals
                    .split(',')
                    .map(|x| x.parse::<u64>().unwrap())
                    .collect::<Vec<u64>>(),
            ))
        } else if properties.get(super::CONTENTS).is_some() {
            let contents = serde_json::from_str(properties.get(super::CONTENTS).unwrap()).unwrap();
            Some(AlignComponent::Contents(contents))
        } else {
            None
        }
    }
}

// replying queries
impl AlignEval {
    async fn get_entry_with_ts(&self, timestamp: Timestamp) -> Option<Sample> {
        // get corresponding key from log
        let mut key = None;
        let log = self.snapshotter.get_stable_log().await;
        for (k, ts) in log {
            if ts == timestamp {
                key = Some(k.to_string());
            }
        }

        if key.is_some() {
            let mut replies = self.session.get(&key.unwrap()).await.unwrap();
            if let Some(reply) = replies.next().await {
                debug!(
                    "[ALIGN QUERYABLE]>> Received ('{}': '{}')",
                    reply.sample.key_expr.as_str(),
                    reply.sample.value
                );
                if reply.sample.timestamp.is_some() {
                    match reply.sample.timestamp.unwrap().cmp(&timestamp) {
                        Ordering::Greater => return None,
                        Ordering::Less => {
                            error!(
                                "[ALIGN QUERYABLE] Data in the storage is older than requested."
                            );
                            return None;
                        }
                        Ordering::Equal => return Some(reply.sample),
                    }
                }
            }
        }
        None
    }

    async fn get_intervals(&self, era: EraType) -> HashMap<u64, u64> {
        let digest = self.snapshotter.get_digest().await;
        digest.get_era_content(era)
    }

    async fn get_subintervals(&self, interval: u64) -> HashMap<u64, u64> {
        let digest = self.snapshotter.get_digest().await;
        let mut intervals = HashSet::new();
        intervals.insert(interval);
        digest.get_interval_content(intervals)
    }

    async fn get_content(&self, subinterval: u64) -> HashMap<u64, Vec<zenoh::time::Timestamp>> {
        let digest = self.snapshotter.get_digest().await;
        let mut subintervals = HashSet::new();
        subintervals.insert(subinterval);
        digest.get_subinterval_content(subintervals)
    }
}

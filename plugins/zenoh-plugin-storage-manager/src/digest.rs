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
use crc::{Crc, CRC_64_ECMA_182};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::str::FromStr;
use std::string::ParseError;
use std::time::{Duration, SystemTime};
use zenoh::time::Timestamp;
use derive_new::new;
// use std::error::Error;
// use std::fmt;

// define DELTA, EPOCH_START, etc
pub const EPOCH_START: SystemTime = SystemTime::UNIX_EPOCH;
pub const PROPAGATION_DELAY: Duration = Duration::from_millis(200);
pub const DELTA: Duration = Duration::from_millis(1000);
const SUB_INTERVALS: usize = 10;
const HOT: usize = 5;
const WARM: usize = 10;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Log {
    pub era: EraType,
    pub interval: u64,
    pub subinterval: u64,
    pub content: zenoh::time::Timestamp,
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Digest {
    pub timestamp: zenoh::time::Timestamp, // check this if can be made Timestamp
    pub checksum: u64,
    pub eras: HashMap<EraType, Interval>,
    pub intervals: HashMap<u64, Interval>,
    pub subintervals: HashMap<u64, SubInterval>,
}

#[derive(new, Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Interval {
    #[new(default)]
    pub checksum: u64,
    #[new(default)]
    pub content: Vec<u64>,
}

#[derive(new, Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct SubInterval {
    #[new(default)]
    pub checksum: u64,
    #[new(default)]
    pub content: Vec<zenoh::time::Timestamp>,
}

#[derive(PartialEq, Eq, Hash, Ord, PartialOrd, Debug, Clone, Deserialize, Serialize)]
pub enum EraType {
    Hot,
    Warm,
    Cold,
}

impl FromStr for EraType {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, std::convert::Infallible> {
        let s = s.to_lowercase();
        match s.as_str() {
            "cold" => Ok(EraType::Cold),
            "warm" => Ok(EraType::Warm),
            "hot" => Ok(EraType::Hot),
            _ => Ok(EraType::Cold), //TODO: fix this later
        }
    }
}

trait Checksum {
    fn format_content(&self) -> String;
}

impl Checksum for Timestamp {
    fn format_content(&self) -> String {
        format!("{}", self)
    }
}

impl Checksum for u64 {
    fn format_content(&self) -> String {
        self.to_string()
    }
}

// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct EraParseError();

// impl Error for EraParseError {
//     fn description(&self) -> &str {
//         "invalid era"
//     }
// }

// impl fmt::Display for EraParseError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "invalid era")
//     }
// }

//functions for digest creation and update
impl Digest {
    pub fn create_digest(
        timestamp: zenoh::time::Timestamp,
        raw_log: Vec<Timestamp>,
        latest_interval: u64,
        latest_snapshot_time: Timestamp,
    ) -> Digest {
        let processed_log = Digest::process_log(raw_log, latest_interval, latest_snapshot_time);
        let (subinterval_content, interval_content, era_content) =
            Digest::populate_content(processed_log);

        let mut subinterval_hash = HashMap::new();
        for (sub, content) in subinterval_content {
            let subinterval = SubInterval {
                checksum: Digest::get_subinterval_checksum(&content),
                content: content,
            };
            subinterval_hash.insert(sub, subinterval);
        }

        let mut interval_hash = HashMap::new();
        for (int, content) in interval_content {
            let interval = Interval {
                checksum: Digest::get_interval_checksum(&content, &subinterval_hash),
                content: content,
            };
            interval_hash.insert(int, interval);
        }

        let mut era_hash = HashMap::new();
        for (e, content) in era_content.clone() {
            let era = Interval {
                checksum: Digest::get_era_checksum(&content, &interval_hash),
                content: content,
            };
            era_hash.insert(e, era);
        }

        Digest {
            timestamp: timestamp,
            checksum: Digest::get_digest_checksum(&era_hash),
            eras: era_hash,
            intervals: interval_hash,
            subintervals: subinterval_hash,
        }
    }

    fn get_content_hash<T:Checksum>(content: &Vec<T>) -> u64 {
        let crc64 = Crc::<u64>::new(&CRC_64_ECMA_182);
        let mut hasher = crc64.digest();
        for s_cont in content {
            let formatted = s_cont.format_content();
            hasher.update(formatted.as_bytes());
        }
        hasher.finalize()
    }

    fn get_subinterval_checksum(content: &Vec<Timestamp>) -> u64 {
        Digest::get_content_hash(content)
    }

    fn get_interval_checksum(content: &Vec<u64>, info: &HashMap<u64, SubInterval>) -> u64 {
        let mut hashable_content = Vec::new();
        for i in content.clone() {
            let i_cont = info.get(&i).unwrap().checksum;
            hashable_content.push(i_cont);
        }
        Digest::get_content_hash(&hashable_content)
    }

    fn get_era_checksum(content: &Vec<u64>, info: &HashMap<u64, Interval>) -> u64 {
        let mut hashable_content = Vec::new();
        for i in content.clone() {
            let i_cont = info.get(&i).unwrap().checksum;
            hashable_content.push(i_cont);
        }
        Digest::get_content_hash(&hashable_content)
    }

    fn get_digest_checksum(content: &HashMap<EraType, Interval>) -> u64 {
        let mut hashable_content = Vec::new();
        for i_cont in content.values() {
            hashable_content.push(i_cont.checksum);
        }
        Digest::get_content_hash(&hashable_content)
    }

    fn populate_content(
        processed_log: Vec<Log>,
    ) -> (
        HashMap<u64, Vec<Timestamp>>,
        HashMap<u64, Vec<u64>>,
        HashMap<EraType, Vec<u64>>,
    ) {
        let mut subinterval_content = HashMap::new();
        let mut interval_content = HashMap::new();
        let mut era_content = HashMap::new();
        for log_entry in processed_log {
            if !subinterval_content.contains_key(&log_entry.subinterval) {
                subinterval_content.insert(log_entry.subinterval, Vec::new());
            }
            subinterval_content
                .get_mut(&log_entry.subinterval)
                .unwrap()
                .push(log_entry.content);
            if !interval_content.contains_key(&log_entry.interval) {
                interval_content.insert(log_entry.interval, Vec::new());
            }
            interval_content
                .get_mut(&log_entry.interval)
                .unwrap()
                .push(log_entry.subinterval);
            if !era_content.contains_key(&log_entry.era) {
                era_content.insert(log_entry.era.clone(), Vec::new());
            }
            era_content
                .get_mut(&log_entry.era)
                .unwrap()
                .push(log_entry.interval);
        }

        for content in subinterval_content.values_mut() {
            content.sort_unstable();
            content.dedup();
        }
        for content in interval_content.values_mut() {
            content.sort_unstable();
            content.dedup();
        }
        for content in era_content.values_mut() {
            content.sort_unstable();
            content.dedup();
        }
        (subinterval_content, interval_content, era_content)
    }

    pub async fn update_digest(
        current: Digest,
        latest_interval: u64,
        last_snapshot_time: Timestamp,
        content: HashSet<Timestamp>,
        redundant_content: HashSet<Timestamp>,
    ) -> Digest {
        // push content in correct places
        let (current, subintervals_to_update, intervals_to_update, eras_to_update) =
            Digest::update_content(&mut current.clone(), content, latest_interval);

        // remove redundant content from proper places
        let (current, further_subintervals, further_intervals, further_eras) =
            Digest::remove_redundant_content(
                &mut current.clone(),
                redundant_content,
                latest_interval,
            );

        let subintervals_to_update = subintervals_to_update.union(&further_subintervals);
        let intervals_to_update = intervals_to_update.union(&further_intervals);
        let eras_to_update = eras_to_update.union(&further_eras);

        let mut subintervals = current.subintervals.clone();
        let mut intervals = current.intervals.clone();
        let mut eras = current.eras.clone();

        // reconstruct updates parts
        for sub in subintervals_to_update {
            // order the content, hash them
            subintervals.get_mut(&sub).unwrap().content.sort_unstable();
            subintervals.get_mut(&sub).unwrap().content.dedup();
            let checksum =
                Digest::get_subinterval_checksum(&subintervals.get(&sub).unwrap().content);

            subintervals.get_mut(&sub).unwrap().checksum = checksum;
        }

        for int in intervals_to_update {
            // order the content, hash them
            intervals.get_mut(&int).unwrap().content.sort_unstable();
            intervals.get_mut(&int).unwrap().content.dedup();
            let checksum =
                Digest::get_interval_checksum(&intervals.get(&int).unwrap().content, &subintervals);

            intervals.get_mut(&int).unwrap().checksum = checksum;
        }

        for era in eras_to_update {
            // order the content, hash them
            eras.get_mut(&era).unwrap().content.sort_unstable();
            eras.get_mut(&era).unwrap().content.dedup();
            let checksum =
                Digest::get_era_checksum(&eras.get(&era).unwrap().content, &intervals);

            eras.get_mut(&era).unwrap().checksum = checksum;
        }

        // update the shared value
        Digest {
            timestamp: last_snapshot_time,
            checksum: Digest::get_digest_checksum(&eras),
            eras: eras,
            intervals: intervals,
            subintervals: subintervals,
        }
    }

    fn update_content(
        current: &mut Digest,
        content: HashSet<Timestamp>,
        latest_interval: u64,
    ) -> (Digest, HashSet<u64>, HashSet<u64>, HashSet<EraType>) {
        let mut eras_to_update = HashSet::new();
        let mut intervals_to_update = HashSet::new();
        let mut subintervals_to_update = HashSet::new();

        for entry in content {
            let (era, interval, subinterval) = Digest::get_bucket(latest_interval, entry);
            eras_to_update.insert(era.clone());
            intervals_to_update.insert(interval);
            subintervals_to_update.insert(subinterval);

            if current.subintervals.contains_key(&subinterval) {
                current
                    .subintervals
                    .get_mut(&subinterval)
                    .unwrap()
                    .content
                    .push(entry);
            } else {
                current.subintervals.insert(
                    subinterval,
                    SubInterval {
                        checksum: 0,
                        content: vec![entry],
                    },
                );
            }
            if current.intervals.contains_key(&interval) {
                current
                    .intervals
                    .get_mut(&interval)
                    .unwrap()
                    .content
                    .push(subinterval);
            } else {
                current.intervals.insert(
                    interval,
                    Interval {
                        checksum: 0,
                        content: vec![subinterval],
                    },
                );
            }
            if current.eras.contains_key(&era) {
                current.eras.get_mut(&era).unwrap().content.push(interval);
            } else {
                current.eras.insert(
                    era,
                    Interval {
                        checksum: 0,
                        content: vec![interval],
                    },
                );
            }
        }

        (
            current.clone(),
            subintervals_to_update,
            intervals_to_update,
            eras_to_update,
        )
    }

    fn remove_redundant_content(
        current: &mut Digest,
        redundant_content: HashSet<Timestamp>,
        latest_interval: u64,
    ) -> (Digest, HashSet<u64>, HashSet<u64>, HashSet<EraType>) {
        let mut eras_to_update = HashSet::new();
        let mut intervals_to_update = HashSet::new();
        let mut subintervals_to_update = HashSet::new();

        for entry in redundant_content {
            let (era, interval, subinterval) = Digest::get_bucket(latest_interval, entry);

            if current.subintervals.contains_key(&subinterval) {
                current
                    .subintervals
                    .get_mut(&subinterval)
                    .unwrap()
                    .content
                    .dedup();
                current
                    .subintervals
                    .get_mut(&subinterval)
                    .unwrap()
                    .content
                    .retain(|&x| x.get_time() != entry.get_time() && x.get_id() != entry.get_id());
                subintervals_to_update.insert(subinterval);
            }
            if current.intervals.contains_key(&interval) {
                current
                    .intervals
                    .get_mut(&interval)
                    .unwrap()
                    .content
                    .dedup();
                current
                    .intervals
                    .get_mut(&interval)
                    .unwrap()
                    .content
                    .retain(|&x| x != subinterval);
                intervals_to_update.insert(interval);
            }
            if current.eras.contains_key(&era) {
                current.eras.get_mut(&era).unwrap().content.dedup();
                current
                    .eras
                    .get_mut(&era)
                    .unwrap()
                    .content
                    .retain(|&x| x != interval);
                eras_to_update.insert(era.clone());
            }
        }
        (
            current.clone(),
            subintervals_to_update,
            intervals_to_update,
            eras_to_update,
        )
    }

    fn get_bucket(latest_interval: u64, ts: Timestamp) -> (EraType, u64, u64) {
        let hot_min = latest_interval - u64::try_from(HOT).unwrap() + 1;
        let warm_min =
            latest_interval - u64::try_from(HOT).unwrap() - u64::try_from(WARM).unwrap() + 1;

        let ts = u64::try_from(
            ts.get_time()
                .to_system_time()
                .duration_since(EPOCH_START)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let delta = u64::try_from(DELTA.as_millis()).unwrap();

        let interval = ts / delta;
        let subinterval = ts / (delta / u64::try_from(SUB_INTERVALS).unwrap());

        let era = if interval >= hot_min {
            EraType::Hot
        } else if interval >= warm_min {
            EraType::Warm
        } else {
            EraType::Cold
        };

        (era, interval, subinterval)
    }

    fn process_log(
        raw_log: Vec<zenoh::time::Timestamp>,
        latest_interval: u64,
        latest_snapshot_time: Timestamp,
    ) -> Vec<Log> {
        let mut log = Vec::new();

        for entry_ts in raw_log {
            if entry_ts <= latest_snapshot_time {
                let (era, interval, subinterval) = Digest::get_bucket(latest_interval, entry_ts);
                log.push(Log {
                    era: era,
                    interval: interval,
                    subinterval: subinterval,
                    content: entry_ts,
                });
            }
        }
        log
    }
}

//functions for digest compression
impl Digest {
    pub fn compress(&self) -> Digest {
        let mut compressed_intervals = HashMap::new();
        let mut compressed_subintervals = HashMap::new();
        if self.eras.contains_key(&EraType::Hot) {
            for int in &self.eras.get(&EraType::Hot).unwrap().content {
                compressed_intervals.insert(int.clone(), self.intervals.get(&int).unwrap().clone());
                for sub in &self.intervals.get(&int).unwrap().content {
                    let subinterval = self.subintervals.get(&sub).unwrap().clone();
                    let comp_sub = SubInterval {
                        checksum: subinterval.checksum,
                        content: Vec::new(),
                    };
                    compressed_subintervals.insert(sub.clone(), comp_sub);
                }
            }
        };
        if self.eras.contains_key(&EraType::Warm) {
            for int in &self.eras.get(&EraType::Warm).unwrap().content {
                let interval = self.intervals.get(&int).unwrap().clone();
                let comp_int = Interval {
                    checksum: interval.checksum,
                    content: Vec::new(),
                };
                compressed_intervals.insert(int.clone(), comp_int);
            }
        };
        let mut compressed_eras = HashMap::new();
        for era in self.eras.keys() {
            if era.clone() == EraType::Cold {
                compressed_eras.insert(
                    EraType::Cold,
                    Interval {
                        checksum: self.eras.get(era).unwrap().checksum,
                        content: Vec::new(),
                    },
                );
            } else {
                compressed_eras.insert(era.clone(), self.eras.get(era).unwrap().clone());
            }
        }
        Digest {
            timestamp: self.timestamp,
            checksum: self.checksum,
            eras: compressed_eras,
            intervals: compressed_intervals,
            subintervals: compressed_subintervals,
        }
    }

    pub fn get_era_content(&self, era: EraType) -> HashMap<u64, u64> {
        let mut result = HashMap::new();
        for int in self.eras.get(&era).unwrap().content.clone() {
            result.insert(int, self.intervals.get(&int).unwrap().checksum.clone());
        }
        result
    }

    pub fn get_interval_content(&self, intervals: HashSet<u64>) -> HashMap<u64, u64> {
        //return (subintervalid, checksum) for the set of intervals
        let mut result = HashMap::new();
        for each in intervals {
            for sub in self.intervals.get(&each).unwrap().content.clone() {
                result.insert(sub, self.subintervals.get(&sub).unwrap().checksum.clone());
            }
        }
        result
    }

    pub fn get_subinterval_content(
        &self,
        subintervals: HashSet<u64>,
    ) -> HashMap<u64, Vec<zenoh::time::Timestamp>> {
        let mut result = HashMap::new();
        for each in subintervals {
            result.insert(each, self.subintervals.get(&each).unwrap().content.clone());
        }
        result
    }
}

//functions for alignment
impl Digest {
    //return mismatching eras
    pub fn get_era_diff(&self, other: HashMap<EraType, Interval>) -> HashSet<EraType> {
        let mut result = HashSet::new();
        for era in vec![EraType::Hot, EraType::Warm, EraType::Cold] {
            if other.contains_key(&era) {
                if self.eras.contains_key(&era) {
                    if self.eras.get(&era).unwrap().checksum != other.get(&era).unwrap().checksum {
                        result.insert(era);
                    }
                } else {
                    result.insert(era);
                }
            } // else no need to check
        }
        result
    }

    //return mismatching intervals in an era
    pub fn get_interval_diff(&self, other_intervals: HashMap<u64, u64>) -> HashSet<u64> {
        let mut mis_int = HashSet::new();
        for int in other_intervals.keys() {
            if self.intervals.contains_key(&int) {
                if other_intervals.get(&int).unwrap().clone()
                    != self.intervals.get(&int).unwrap().checksum
                {
                    mis_int.insert(int.clone());
                }
            } else {
                mis_int.insert(int.clone());
            }
        }
        mis_int
    }

    //return mismatching subintervals in an interval
    pub fn get_subinterval_diff(&self, other_subintervals: HashMap<u64, u64>) -> HashSet<u64> {
        let mut mis_sub = HashSet::new();
        for sub in other_subintervals.keys() {
            if self.subintervals.contains_key(&sub) {
                if other_subintervals.get(&sub).unwrap().clone()
                    != self.subintervals.get(&sub).unwrap().checksum
                {
                    mis_sub.insert(sub.clone());
                }
            } else {
                mis_sub.insert(sub.clone());
            }
        }
        mis_sub
    }

    pub fn get_full_content_diff(
        &self,
        other_subintervals: HashMap<u64, Vec<zenoh::time::Timestamp>>,
    ) -> Vec<zenoh::time::Timestamp> {
        let mut result = Vec::new();
        for (sub, content) in other_subintervals {
            let mut other = self.get_content_diff(sub, content.clone());
            result.append(&mut other);
        }
        result
    }

    //return missing content in a subinterval
    pub fn get_content_diff(
        &self,
        subinterval: u64,
        content: Vec<zenoh::time::Timestamp>,
    ) -> Vec<zenoh::time::Timestamp> {
        if !self.subintervals.contains_key(&subinterval) {
            return content;
        }
        let mut mis_content = Vec::new();
        for c in content {
            if !self
                .subintervals
                .get(&subinterval)
                .unwrap()
                .content
                .contains(&c)
            {
                mis_content.push(c);
            }
        }
        mis_content
    }
}

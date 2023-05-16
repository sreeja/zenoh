//
// Copyright (c) 2023 ZettaScale Technology
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
use async_std::sync::RwLock;
use emseries::{And, DateTimeTz, EndTime, Recordable, Series, StartTime};
use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;
use zenoh::prelude::*;
use zenoh::{sample::Sample, time::Timestamp, Result as ZResult};
use zenoh::buffers::{SplitBuffer, ZBuf, ZSlice};

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub keyexpr: KeyExpr<'static>,
    pub kind: SampleKind,
    pub timestamp: Timestamp,
    pub encoding: Encoding,
    pub payload: ZBuf,
    pub snapshot: HashSet<KeyExpr<'static>>,
}

impl LogEntry {
    fn get_log_entry(sample: Sample, snapshot: HashSet<KeyExpr<'static>>) -> LogEntry {
        LogEntry {
            keyexpr: sample.key_expr,
            kind: sample.kind,
            timestamp: sample.timestamp.unwrap(),
            encoding: sample.value.encoding,
            payload: sample.value.payload,
            snapshot,
        }
    }

    fn get_sample(entry: &LogEntry) -> Sample {
        let value = Value::new(entry.payload.clone()).encoding(entry.encoding.clone());
        let mut sample = Sample::new(entry.keyexpr.clone(), value).with_timestamp(entry.timestamp);
        sample.kind = entry.kind;
        sample
    }
}

impl Recordable for LogEntry {
    fn timestamp(&self) -> DateTimeTz {
        DateTimeTz::from_str(&self.timestamp.get_time().to_string()).unwrap()
    }

    fn tags(&self) -> Vec<String> {
        Vec::new()
    }
}

impl Serialize for LogEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LogEntry", 5)?;
        state.serialize_field("keyexpr", &self.keyexpr.to_string())?;
        state.serialize_field("kind", &self.kind.to_string())?;
        state.serialize_field("timestamp", &self.timestamp.to_string())?;
        state.serialize_field("encoding", &self.encoding.to_string())?;
        state.serialize_field("payload", &self.payload.slices().collect::<Vec<&[u8]>>())?;
        state.serialize_field("snapshot", &serde_json::to_string(&self.snapshot).unwrap())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for LogEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            KeyExpr,
            Kind,
            Timestamp,
            Encoding,
            Payload,
            Snapshot,
        }

        struct LogEntryVisitor;

        impl<'de> Visitor<'de> for LogEntryVisitor {
            type Value = LogEntry;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("`LogEntry`")
            }

            fn visit_map<V>(self, mut map: V) -> Result<LogEntry, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut keyexpr = None;
                let mut kind = None;
                let mut timestamp = None;
                let mut encoding = None;
                let mut payload = None;
                let mut snapshot = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::KeyExpr => {
                            if keyexpr.is_some() {
                                return Err(de::Error::duplicate_field("keyexpr"));
                            }
                            keyexpr = Some(map.next_value()?);
                        }
                        Field::Kind => {
                            if kind.is_some() {
                                return Err(de::Error::duplicate_field("kind"));
                            }
                            let serialized_kind: String = map.next_value()?;
                            kind = if serialized_kind.eq(&(SampleKind::Put).to_string()) {
                                Some(SampleKind::Put)
                            } else {
                                Some(SampleKind::Delete)
                            };
                        }
                        Field::Timestamp => {
                            if timestamp.is_some() {
                                return Err(de::Error::duplicate_field("timestamp"));
                            }
                            let ts_str = map.next_value()?;
                            timestamp = Some(Timestamp::from_str(ts_str).unwrap());
                        }
                        Field::Encoding => {
                            if encoding.is_some() {
                                return Err(de::Error::duplicate_field("encoding"));
                            }
                            let serialized_encoding: String = map.next_value()?;
                            encoding = Some(Encoding::from(serialized_encoding));
                        }
                        Field::Payload => {
                            if payload.is_some() {
                                return Err(de::Error::duplicate_field("payload"));
                            }
                            let serialized_zbuf: Vec<Vec<u8>> = map.next_value()?;
                            let mut zbuf = ZBuf::default();
                            for each in serialized_zbuf {
                                let slice: ZSlice = each.to_vec().into();
                                zbuf.push_zslice(slice);
                            }
                            payload = Some(zbuf);
                        }
                        Field::Snapshot => {
                            if snapshot.is_some() {
                                return Err(de::Error::duplicate_field("snapshot"));
                            }
                            let serialized_snapshot: String = map.next_value()?;
                            snapshot = Some(serde_json::from_str(&serialized_snapshot).unwrap());
                        }
                    }
                }

                let keyexpr = keyexpr.ok_or_else(|| de::Error::missing_field("keyexpr"))?;
                let kind = kind.ok_or_else(|| de::Error::missing_field("kind"))?;
                let timestamp = timestamp.ok_or_else(|| de::Error::missing_field("timestamp"))?;
                let encoding = encoding.ok_or_else(|| de::Error::missing_field("encoding"))?;
                let payload = payload.ok_or_else(|| de::Error::missing_field("payload"))?;
                let snapshot = snapshot.ok_or_else(|| de::Error::missing_field("snapshot"))?;
                Ok(LogEntry {
                    keyexpr,
                    kind,
                    timestamp,
                    encoding,
                    payload,
                    snapshot,
                })
            }
        }
        const FIELDS: &[&str; 6] = &["keyexpr", "kind", "timestamp", "encoding", "payload", "snapshot"];
        deserializer.deserialize_struct("LogEntry", FIELDS, LogEntryVisitor)
    }
}

#[test]
fn test_serialize() {
    let ts = Timestamp::from_str("2023-05-05T10:12:33.448842000Z/01").unwrap();
    let sample =
        Sample::new(OwnedKeyExpr::new("demo/test").unwrap(), "testing value").with_timestamp(ts);

    let log = LogEntry {
        keyexpr: sample.clone().key_expr,
        kind: SampleKind::Put,
        timestamp: ts,
        encoding: sample.clone().value.encoding,
        payload: sample.clone().value.payload,
        snapshot: HashSet::from([KeyExpr::from_str("demo/snap").unwrap()]),
    };

    let serialized = serde_json::to_string(&log).unwrap();
    assert_eq!("{\"keyexpr\":\"demo/test\",\"kind\":\"PUT\",\"timestamp\":\"2023-05-05T10:12:33.448842000Z/01\",\"encoding\":\"text/plain\",\"payload\":[[116,101,115,116,105,110,103,32,118,97,108,117,101]],\"snapshot\":\"[\\\"demo/snap\\\"]\"}", serialized);
}

#[test]
fn test_deserialize() {
    let ts = Timestamp::from_str("2023-05-05T10:12:33.448842000Z/01").unwrap();
    let sample =
        Sample::new(OwnedKeyExpr::new("demo/test").unwrap(), "testing value").with_timestamp(ts);

    let d: Result<LogEntry, serde_json::Error> = serde_json::from_str("{\"keyexpr\":\"demo/test\",\"kind\":\"PUT\",\"timestamp\":\"2023-05-05T10:12:33.448842000Z/01\",\"encoding\":\"text/plain\",\"payload\":[[116,101,115,116,105,110,103,32,118,97,108,117,101]],\"snapshot\":\"[\\\"demo/snap\\\"]\"}");
    match d {
        Ok(deserialized) => {
            assert_eq!("demo/test", &deserialized.keyexpr.to_string());
            assert_eq!(sample.kind, deserialized.kind);
            assert_eq!(ts, deserialized.timestamp);
            assert_eq!(sample.value.encoding, deserialized.encoding);
            assert_eq!(sample.value.payload, deserialized.payload);
            assert_eq!(1, deserialized.snapshot.len());
            assert!(deserialized.snapshot.contains(&KeyExpr::from_str("demo/snap").unwrap()))
        }
        Err(e) => {
            assert_eq!(format!(""), format!("{e}"));
        }
    }
}

#[test]
fn test_serde() {
    let ts = Timestamp::from_str("2023-05-05T10:12:33.448842000Z/01").unwrap();
    let sample =
        Sample::new(OwnedKeyExpr::new("demo/test").unwrap(), "testing value").with_timestamp(ts);

    let log = LogEntry {
        keyexpr: sample.clone().key_expr,
        kind: SampleKind::Put,
        timestamp: ts,
        encoding: sample.clone().value.encoding,
        payload: sample.clone().value.payload,
        snapshot: HashSet::from([KeyExpr::from_str("demo/snap").unwrap()]),
    };

    let serialized = serde_json::to_string(&log).unwrap();

    let d: Result<LogEntry, serde_json::Error> = serde_json::from_str(&serialized);
    match d {
        Ok(deserialized) => {
            assert_eq!("demo/test", &deserialized.keyexpr.to_string());
            assert_eq!(sample.kind, deserialized.kind);
            assert_eq!(ts, deserialized.timestamp);
            assert_eq!(sample.value.encoding, deserialized.encoding);
            assert_eq!(sample.value.payload, deserialized.payload);
            assert_eq!(1, deserialized.snapshot.len());
            assert!(deserialized.snapshot.contains(&KeyExpr::from_str("demo/snap").unwrap()))
        }
        Err(e) => {
            assert_eq!(format!(""), format!("{e}"));
        }
    }
}

pub struct Logger {
    log: RwLock<Series<LogEntry>>,
}

impl Logger {
    pub async fn new(log_file: &str) -> Self {
        let ts: Series<LogEntry> =
            Series::open(log_file).expect("expect the time series to open correctly");
        Logger {
            log: RwLock::new(ts),
        }
    }

    pub async fn save(&self, mut sample: Sample, snapshot: Option<HashSet<KeyExpr<'static>>>) -> ZResult<()> {
        let mut log = self.log.write().await;
        sample.ensure_timestamp();
        log.put(LogEntry::get_log_entry(sample, snapshot.unwrap_or_else(|| HashSet::new())))?;
        drop(log);
        Ok(())
    }

    pub async fn get(&self, from: Option<Timestamp>, to: Option<Timestamp>) -> Vec<Sample> {
        if from.is_some() && to.is_some() {
            let criteria = And {
                lside: StartTime {
                    time: DateTimeTz::from_str(&from.unwrap().get_time().to_string()).unwrap(),
                    incl: true,
                },
                rside: EndTime {
                    time: DateTimeTz::from_str(&to.unwrap().get_time().to_string()).unwrap(),
                    incl: true,
                },
            };
            let series = self.log.read().await;
            match series.search_sorted(criteria, |a, b| a.1.timestamp.cmp(&b.1.timestamp)) {
                Ok(res) => {
                    let mut result = Vec::new();
                    for (_, each) in res {
                        result.push(LogEntry::get_sample(each));
                    }
                    result
                }
                Err(e) => {
                    log::error!("Error when querying : {}", e);
                    Vec::new()
                }
            }
        } else if from.is_some() {
            let criteria = StartTime {
                time: DateTimeTz::from_str(&from.unwrap().get_time().to_string()).unwrap(),
                incl: true,
            };
            let series = self.log.read().await;
            match series.search_sorted(criteria, |a, b| a.1.timestamp.cmp(&b.1.timestamp)) {
                Ok(res) => {
                    let mut result = Vec::new();
                    for (_, each) in res {
                        result.push(LogEntry::get_sample(each));
                    }
                    result
                }
                Err(e) => {
                    log::error!("Error when querying : {}", e);
                    Vec::new()
                }
            }
        } else if to.is_some() {
            let criteria = EndTime {
                time: DateTimeTz::from_str(&to.unwrap().get_time().to_string()).unwrap(),
                incl: true,
            };
            let series = self.log.read().await;
            match series.search_sorted(criteria, |a, b| a.1.timestamp.cmp(&b.1.timestamp)) {
                Ok(res) => {
                    let mut result = Vec::new();
                    for (_, each) in res {
                        result.push(LogEntry::get_sample(each));
                    }
                    result
                }
                Err(e) => {
                    log::error!("Error when querying : {}", e);
                    Vec::new()
                }
            }
        } else {
            let series = self.log.read().await;
            match series.all_records() {
                Ok(mut log) => {
                    let mut result = Vec::new();
                    log.sort_by(|a, b| a.1.timestamp.cmp(&b.1.timestamp));
                    for (_, each) in log {
                        result.push(LogEntry::get_sample(&each));
                    }
                    result
                }
                Err(e) => {
                    log::error!("Error when querying : {}", e);
                    Vec::new()
                }
            }
        }
    }
}

#[test]
fn test_logger() {
    async_std::task::block_on(async {
            // clear contents of log if it exits
            let log_file = std::fs::File::create("test").unwrap();
            drop(log_file);
            // start logger
            let logger = Logger::new("test").await;
            // save some samples
            for i in 10..60 {
                let key = OwnedKeyExpr::from_str(&format!("test/{i}")).unwrap();
                let value = format!("test {i}");
                let timestamp =
                    Timestamp::from_str(&format!("2023-05-05T10:12:{i}.448842000Z/01")).unwrap();
                logger
                    .save(Sample::new(key, value).with_timestamp(timestamp), None)
                    .await
                    .unwrap();
            }
            // query from start to end
            let log = logger.get(None, None).await;
            assert_eq!(log.len(), 50);
            // query from a given time to end
            let log = logger
                .get(
                    None,
                    Some(Timestamp::from_str("2023-05-05T10:12:25.448842000Z/01").unwrap()),
                )
                .await;
            assert_eq!(log.len(), 16);
            // query from start to a given time
            let log = logger
                .get(
                    Some(Timestamp::from_str("2023-05-05T10:12:25.448842000Z/01").unwrap()),
                    None,
                )
                .await;
            assert_eq!(log.len(), 35);
            // query between given times
            let log = logger
                .get(
                    Some(Timestamp::from_str("2023-05-05T10:12:12.448842000Z/01").unwrap()),
                    Some(Timestamp::from_str("2023-05-05T10:12:58.448842000Z/01").unwrap()),
                )
                .await;
            assert_eq!(log.len(), 47);
            let log = logger
                .get(
                    Some(Timestamp::from_str("2023-05-05T10:12:12.448842000Z/01").unwrap()),
                    Some(Timestamp::from_str("2023-05-05T10:12:59.448842000Z/01").unwrap()),
                )
                .await;
            assert_eq!(log.len(), 48);
            let log = logger
                .get(
                    Some(Timestamp::from_str("2023-05-05T10:12:12.448842000Z/01").unwrap()),
                    Some(Timestamp::from_str("2023-05-05T10:12:59.448842100Z/01").unwrap()),
                )
                .await;
            assert_eq!(log.len(), 48);
            let log = logger
                .get(
                    Some(Timestamp::from_str("2023-05-05T10:12:10.448842000Z/01").unwrap()),
                    Some(Timestamp::from_str("2023-05-05T10:12:59.448842000Z/01").unwrap()),
                )
                .await;
            assert_eq!(log.len(), 50);
            let log = logger
                .get(
                    Some(Timestamp::from_str("2023-05-05T10:12:09.548842000Z/01").unwrap()),
                    Some(Timestamp::from_str("2023-05-05T10:12:59.448842000Z/01").unwrap()),
                )
                .await;
            assert_eq!(log.len(), 50);
        })
}

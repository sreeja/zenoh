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
use std::collections::HashMap;
// use async_std::channel::{bounded, Sender};
use async_std::sync::Arc;
// use async_std::task;
// use futures::select;
// use futures::stream::StreamExt;
// use futures::FutureExt;
// use futures::join;
use log::debug; // }, error, trace, warn};
use zenoh::prelude::*;
// use zenoh::query::{QueryConsolidation, QueryTarget, Target};
// use zenoh::queryable;
use zenoh::Session;
// use zenoh_backend_traits::Query;
use zenoh_backend_traits::config::ReplicaConfig;
use zenoh_core::Result as ZResult;

pub use super::replica::Replica;

pub enum StorageMessage {
    Stop,
    GetStatus(async_std::channel::Sender<serde_json::Value>),
}

pub(crate) async fn start_storage(
    storage: Box<dyn zenoh_backend_traits::Storage>,
    config: Option<ReplicaConfig>,
    admin_key: String,
    key_expr: String,
    in_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>,
    out_interceptor: Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>>,
    zenoh: Arc<Session>,
) -> ZResult<flume::Sender<StorageMessage>> {
    debug!("Start storage {} on {}", admin_key, key_expr);

    // TODO: start storage + replica: digest_sub, digest_pub, aligner and align_eval
    // TODO: Key-value stores and time-series to be addressed
    // TODO: fix the name; to be read from the configuration file
    // let replica =
    Replica::initialize_replica(
        config,
        zenoh.clone(),
        storage,
        in_interceptor,
        out_interceptor,
        &key_expr,
        &admin_key,
        HashMap::new(),
    )
    .await
    // replica.start_replica().await
}

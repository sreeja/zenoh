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

use std::time::SystemTime;

pub mod align_eval;
pub mod aligner;
pub mod digest;
pub mod storage;
pub mod snapshotter;
pub mod rep;

pub use align_eval::AlignEval;
pub use aligner::Aligner;
pub use digest::{Digest, DigestConfig, EraType};
pub use storage::StorageService;
pub use snapshotter::{ReplicationInfo, Snapshotter};
pub use rep::Replica;

const ERA: &str = "era";
const INTERVALS: &str = "intervals";
const SUBINTERVALS: &str = "subintervals";
const CONTENTS: &str = "contents";
pub const EPOCH_START: SystemTime = SystemTime::UNIX_EPOCH;

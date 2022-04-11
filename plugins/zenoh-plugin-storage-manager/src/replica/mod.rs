use std::time::SystemTime;

pub mod rep;
pub mod aligner;
pub mod align_eval;
pub mod digest;

pub use rep::Replica;
pub use aligner::Aligner;
pub use align_eval::AlignEval;
pub use digest::{Digest, DigestConfig, EraType};

const ERA: &str = "era";
const INTERVALS: &str = "intervals";
const SUBINTERVALS: &str = "subintervals";
const CONTENTS: &str = "contents";
pub const EPOCH_START: SystemTime = SystemTime::UNIX_EPOCH;

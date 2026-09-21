// Codegen's inner lint attributes cannot be used by include!; scope its
// allowances here so handwritten modules retain their normal lint checks.
#[allow(unknown_lints, clippy::all, unused_mut, non_upper_case_globals)]
pub mod bundle {
    include!(concat!(env!("OUT_DIR"), "/proto/bundle.rs"));
}

pub mod cla_udp;
pub mod protobuf;

pub use cla_udp::{ClaError, UdpConvergenceLayer};

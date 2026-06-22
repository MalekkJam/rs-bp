pub mod bundle {
    include!(concat!(env!("OUT_DIR"), "/proto/bundle.rs"));
}

pub mod cla_udp;
pub mod protobuf;

pub use cla_udp::{ClaError, UdpConvergenceLayer};

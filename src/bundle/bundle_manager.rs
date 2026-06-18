use uuid::Uuid;
use chrono::{ Local, Duration};
use crate::bundle::model::{Bundle, BundleKind, MsgStatus};

const DEFAULT_TTL: Duration = Duration::weeks(3);

pub struct BundleManager;

impl BundleManager {

    pub fn new() -> Self {
        BundleManager
    }

    fn create_bundle(nodeId : Uuid, destination : Uuid, content : String, bundleKind : BundleKind) -> Bundle {
        Bundle {
            id : Uuid::new_v4(),
            source : nodeId,
            destination : destination,
            content : content,
            timestamp : Local::now(),
            ttl : DEFAULT_TTL,
            kind : bundleKind,
            shipment_status : MsgStatus::Pending
        }
    }
}
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::bundle::bundle_manager::BundleManager;
use crate::bundle::routing::RoutingEngine;
use crate::bundle::storage::Storage;

pub struct BundleLayer {
    pub bundle_manager: BundleManager,
    pub storage: Storage,
    pub routing_engine: RoutingEngine,
}

pub struct Bundle {
    pub id: Uuid,
    pub source: Uuid,
    pub destination: Uuid,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub payload: BundlePayload,
}

pub enum BundlePayload {
    Message(String),
    Ack { original_bundle_id: Uuid },
    RequestSummaryVector,
    SummaryVector(Vec<Uuid>),
}

pub struct StoredBundle {
    pub bundle: Bundle,
    pub status: MsgStatus,
}

pub enum MsgStatus {
    // the bundle is created but not yet sent
    Pending,

    // the bundle is on the way to the destination
    InTransit,

    // the bundle has been delivered to the destination
    /// For Data bundles: set when an Ack is received, then deleted from storage.
    /// For Ack bundles: set when the Ack reaches the original sender.
    Delivered,

    // the bundle has expired //TTL exceeded
    Expired,
}


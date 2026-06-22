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

#[derive(Clone)]
pub struct Bundle {
    pub id: Uuid,
    pub source: Uuid,
    pub destination: Uuid,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub payload: BundlePayload,
}

#[derive(Clone)]
pub enum BundlePayload {
    Message(String),
    Ack { original_bundle_id: Uuid },
    RequestSummaryVector,
    SummaryVector(Vec<Uuid>),
}


use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bundle {
    pub id: Uuid,
    pub source: Uuid,
    pub destination: Uuid,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub payload: BundlePayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BundlePayload {
    Message(String),
    Ack { original_bundle_id: Uuid },
    RequestSummaryVector,
    SummaryVector(Vec<Uuid>),
}
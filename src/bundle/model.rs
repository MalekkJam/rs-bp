use chrono::{DateTime, Utc};
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bundle {
    pub id: String, // ex: ipn:1:1 or ipn:1:2
    pub source: String,
    pub destination: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub payload: BundlePayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BundlePayload {
    Message(String),
    Ack { original_bundle_id: String },
    RequestSummaryVector,
    SummaryVector(Vec<String>),
}

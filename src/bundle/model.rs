use chrono::{DateTime, Utc};
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bundle {
    pub id: String, // UUID for new bundles; legacy IDs remain readable.
    pub source: String,
    pub destination: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub hop_count: Option<HopCount>,
    pub payload: BundlePayload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HopCount {
    pub limit: u32,
    pub count: u32,
}

impl Default for HopCount {
    fn default() -> Self {
        Self {
            limit: 16,
            count: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BundlePayload {
    Message(String),
    Ack { original_bundle_id: String },
    RequestSummaryVector,
    SummaryVector(Vec<String>),
}

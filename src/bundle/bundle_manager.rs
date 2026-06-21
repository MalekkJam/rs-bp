use crate::bundle::model::{Bundle, BundlePayload};
use chrono::{Duration, Utc};
use uuid::Uuid;

const DEFAULT_TTL: Duration = Duration::weeks(3);

pub struct BundleManager;

impl BundleManager {
    pub fn new() -> Self {
        BundleManager
    }

    fn create_bundle(
        node_id: Uuid,
        destination: Uuid,
        payload: BundlePayload,
    ) -> Bundle {
        let created_at = Utc::now();

        Bundle {
            id: Uuid::new_v4(),
            source: node_id,
            destination,
            created_at,
            expires_at: created_at + DEFAULT_TTL,
            payload,
        }
    }
}

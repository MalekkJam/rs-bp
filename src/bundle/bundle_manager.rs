use crate::bundle::model::{Bundle, BundlePayload};
use chrono::{Duration, Timelike, Utc};
use uuid::Uuid;

const DEFAULT_TTL: Duration = Duration::weeks(3);

pub struct BundleManager;

impl BundleManager {
    pub fn new() -> Self {
        BundleManager
    }

    pub fn create_bundle(
        &self,
        node_id: Uuid,
        destination: Uuid,
        payload: BundlePayload,
    ) -> Bundle {
        let created_at = Utc::now()
            .with_nanosecond(0)
            .expect("zero nanoseconds is always valid");

        Bundle {
            id: Uuid::new_v4(),
            source: node_id,
            destination,
            created_at,
            expires_at: created_at + DEFAULT_TTL,
            payload,
        }
    }

    pub fn bundle_expired(bundle: &Bundle) -> bool {
        Utc::now() >= bundle.expires_at
    }

    pub fn bundle_at_destination(bundle: &Bundle, node_id: Uuid) -> bool {
        bundle.destination == node_id
    }
}

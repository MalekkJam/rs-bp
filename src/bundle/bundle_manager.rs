use crate::bundle::model::{Bundle, BundlePayload};
use chrono::{Duration, Timelike, Utc};
use uuid::Uuid;

const DEFAULT_TTL: Duration = Duration::weeks(3);

#[derive(Default)]
pub struct BundleManager;

impl BundleManager {
    pub fn new() -> Self {
        Self
    }

    pub fn create_bundle(
        &self,
        node_id: impl Into<String>,
        destination: impl Into<String>,
        payload: BundlePayload,
    ) -> Bundle {
        let created_at = Utc::now()
            .with_nanosecond(0)
            .expect("zero nanoseconds is always valid");

        Bundle {
            id: Uuid::new_v4().to_string(),
            source: node_id.into(),
            destination: destination.into(),
            created_at,
            expires_at: created_at + DEFAULT_TTL,
            hop_count: Some(Default::default()),
            payload,
        }
    }

    pub fn bundle_expired(bundle: &Bundle) -> bool {
        Utc::now() >= bundle.expires_at
    }

    pub fn bundle_at_destination(bundle: &Bundle, node_id: &str) -> bool {
        bundle.destination == node_id
    }
}

#[cfg(test)]
mod tests {
    use super::BundleManager;
    use crate::bundle::BundlePayload;

    #[test]
    fn creates_distinct_ids_across_managers_and_restarts() {
        let manager = BundleManager::new();
        let node_id = "ipn:1:7001";
        let destination = "ipn:1:7002";

        let first = manager.create_bundle(
            node_id,
            destination,
            BundlePayload::Message("first".to_string()),
        );
        let second = BundleManager::new().create_bundle(
            node_id,
            destination,
            BundlePayload::Message("second".to_string()),
        );

        assert_ne!(first.id, second.id);
        assert!(uuid::Uuid::parse_str(&first.id).is_ok());
        assert!(uuid::Uuid::parse_str(&second.id).is_ok());
    }
}

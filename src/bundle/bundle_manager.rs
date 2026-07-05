use crate::bundle::model::{Bundle, BundlePayload};
use chrono::{Duration, Timelike, Utc};
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_TTL: Duration = Duration::weeks(3);

pub struct BundleManager {
    next_bundle_sequence: AtomicU64,
}

impl BundleManager {
    pub fn new() -> Self {
        BundleManager {
            next_bundle_sequence: AtomicU64::new(1),
        }
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
            id: self.next_bundle_id(),
            source: node_id.into(),
            destination: destination.into(),
            created_at,
            expires_at: created_at + DEFAULT_TTL,
            payload,
        }
    }

    pub fn bundle_expired(bundle: &Bundle) -> bool {
        Utc::now() >= bundle.expires_at
    }

    pub fn bundle_at_destination(bundle: &Bundle, node_id: &str) -> bool {
        bundle.destination == node_id
    }

    fn next_bundle_id(&self) -> String {
        let sequence = self.next_bundle_sequence.fetch_add(1, Ordering::Relaxed);
        format!("ipn:1:{sequence}")
    }
}

#[cfg(test)]
mod tests {
    use super::BundleManager;
    use crate::bundle::BundlePayload;

    #[test]
    fn creates_auto_incrementing_ipn_bundle_ids() {
        let manager = BundleManager::new();
        let node_id = "ipn:1:7001";
        let destination = "ipn:1:7002";

        let first = manager.create_bundle(
            node_id,
            destination,
            BundlePayload::Message("first".to_string()),
        );
        let second = manager.create_bundle(
            node_id,
            destination,
            BundlePayload::Message("second".to_string()),
        );

        assert_eq!(first.id, "ipn:1:1");
        assert_eq!(second.id, "ipn:1:2");
    }
}

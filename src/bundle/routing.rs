use crate::bundle::model::{Bundle, BundlePayload};
use std::collections::HashSet;

/// Prepare one outgoing hop without changing stored retry state or the
/// end-to-end identity, destination, payload, or lifetime of the bundle.
pub fn forwarding_copy(bundle: &Bundle) -> Option<Bundle> {
    let mut hops = bundle.hop_count.unwrap_or_default();
    if !(1..=255).contains(&hops.limit) || hops.count >= hops.limit {
        return None;
    }
    hops.count += 1;
    let mut forwarded = bundle.clone();
    forwarded.hop_count = Some(hops);
    Some(forwarded)
}

pub struct RoutingEngine {
    node_id: String,
    peers: Vec<String>,
    seen_ids: HashSet<String>,
}

/// The decision returned to BundleLayer after evaluating a bundle.
/// BundleLayer is responsible for executing the actual side effects.
pub enum EpidemicDecision {
    Ignore,
    StoreAndForward {
        peers: Vec<String>,
    },
    AckDelivered {
        original_bundle_id: String,
    },
    ForwardAckAndDelete {
        original_bundle_id: String,
        peers: Vec<String>,
    },
}

impl RoutingEngine {
    pub fn new(node_id: String, peers: Vec<String>) -> Self {
        RoutingEngine {
            node_id,
            peers,
            seen_ids: HashSet::new(),
        }
    }

    pub fn epidemic_propagation(&mut self, bundle: &Bundle) -> EpidemicDecision {
        match &bundle.payload {
            BundlePayload::Message(_) => self.handle_message(bundle),
            BundlePayload::Ack { original_bundle_id } => {
                self.handle_ack(bundle, original_bundle_id.clone())
            }
            BundlePayload::RequestSummaryVector | BundlePayload::SummaryVector(_) => {
                EpidemicDecision::Ignore
            }
        }
    }

    fn handle_message(&mut self, bundle: &Bundle) -> EpidemicDecision {
        if self.seen_ids.contains(&bundle.id) {
            return EpidemicDecision::Ignore;
        }

        self.seen_ids.insert(bundle.id.clone());
        EpidemicDecision::StoreAndForward {
            peers: self.peers.clone(),
        }
    }

    fn handle_ack(&mut self, bundle: &Bundle, original_bundle_id: String) -> EpidemicDecision {
        if self.seen_ids.contains(&bundle.id) {
            return EpidemicDecision::Ignore;
        }
        self.seen_ids.insert(bundle.id.clone());

        if bundle.destination == self.node_id {
            return EpidemicDecision::AckDelivered { original_bundle_id };
        }

        EpidemicDecision::ForwardAckAndDelete {
            original_bundle_id,
            peers: self.peers.clone(),
        }
    }

    pub fn forward_bundle(&self, bundle: Bundle, peers: Vec<String>) {
        for peer in peers {
            println!("Forwarding bundle {} to peer {}", bundle.id, peer);
        }
    }
}

#[cfg(test)]
mod forwarding_tests {
    use super::*;
    use crate::bundle::bundle_manager::BundleManager;
    use crate::bundle::model::HopCount;

    #[test]
    fn forwarding_preserves_end_to_end_fields_and_retries_use_the_same_count() {
        let original =
            BundleManager::new().create_bundle("A", "C", BundlePayload::Message("hello".into()));
        let first = forwarding_copy(&original).unwrap();
        assert_eq!(first, forwarding_copy(&original).unwrap());
        assert_eq!(first.hop_count.unwrap().count, 1);
        assert_eq!(original.hop_count.unwrap().count, 0);
        let mut without_hop_change = first.clone();
        without_hop_change.hop_count = original.hop_count;
        assert_eq!(without_hop_change, original);
        assert_eq!(forwarding_copy(&first).unwrap().hop_count.unwrap().count, 2);
    }

    #[test]
    fn hop_limit_stops_forwarding_and_legacy_bundles_get_a_limit() {
        let mut bundle =
            BundleManager::new().create_bundle("A", "C", BundlePayload::Message("hello".into()));
        bundle.hop_count = Some(HopCount { limit: 2, count: 2 });
        assert!(forwarding_copy(&bundle).is_none());
        bundle.hop_count = None;
        assert_eq!(
            forwarding_copy(&bundle).unwrap().hop_count,
            Some(HopCount {
                limit: 16,
                count: 1
            })
        );
    }
}

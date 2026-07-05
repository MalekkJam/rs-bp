use crate::bundle::model::{Bundle, BundlePayload};
use std::collections::HashSet;

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

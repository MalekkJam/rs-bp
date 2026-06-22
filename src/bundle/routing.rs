use crate::bundle::model::{Bundle, BundlePayload};
use std::collections::HashSet;
use uuid::Uuid;

pub struct RoutingEngine {
    node_id: Uuid,
    peers: Vec<Uuid>,        // known neighbor node IDs
    seen_ids: HashSet<Uuid>, // dedup tracker — bundles already processed
}

/// The decision returned to BundleLayer after evaluating a bundle.
/// BundleLayer is responsible for executing the actual side effects.
pub enum EpidemicDecision {
    // Case 2: bundle already seen — nothing to do.
    Ignore,

    // Case 1: new message bundle — store it and flood to every peer.
    StoreAndForward {
        peers: Vec<Uuid>,
    },

    // Case 3a: ack reached its original sender (us).
    // BundleLayer must delete the data bundle we were holding.
    AckDelivered {
        original_bundle_id: Uuid,
    },

    // Case 3b: we are an intermediate node that received an ack.
    // BundleLayer already holds the ack bundle — forward it to peers and delete our copy of the data bundle.
    ForwardAckAndDelete {
        original_bundle_id: Uuid,
        peers: Vec<Uuid>,
    },
}

impl RoutingEngine {
    pub fn new(node_id: Uuid, peers: Vec<Uuid>) -> Self {
        RoutingEngine {
            node_id,
            peers,
            seen_ids: HashSet::new(),
        }
    }

    /// Called by BundleLayer whenever a bundle is created locally or received
    /// from a peer. Returns a decision; BundleLayer performs the actual side effects.
    pub fn epidemic_propagation(&mut self, bundle: &Bundle) -> EpidemicDecision {
        match &bundle.payload {
            BundlePayload::Message(_) => self.handle_message(bundle),
            BundlePayload::Ack { original_bundle_id } => {
                self.handle_ack(bundle, *original_bundle_id)
            }
            BundlePayload::RequestSummaryVector | BundlePayload::SummaryVector(_) => {
                EpidemicDecision::Ignore
            }
        }
    }

    fn handle_message(&mut self, bundle: &Bundle) -> EpidemicDecision {
        // Case 2: already seen — ignore to prevent re-flooding
        if self.seen_ids.contains(&bundle.id) {
            return EpidemicDecision::Ignore;
        }

        // Case 1: new bundle — mark as seen, store and flood to all peers
        self.seen_ids.insert(bundle.id);
        EpidemicDecision::StoreAndForward {
            peers: self.peers.clone(),
        }
    }

    fn handle_ack(&mut self, bundle: &Bundle, original_bundle_id: Uuid) -> EpidemicDecision {
        // Dedup: ignore if we already processed this ack
        if self.seen_ids.contains(&bundle.id) {
            return EpidemicDecision::Ignore;
        }
        self.seen_ids.insert(bundle.id);

        // Case 3a: we are the original sender — the ack has reached home
        if bundle.destination == self.node_id {
            return EpidemicDecision::AckDelivered { original_bundle_id };
        }

        // Case 3b: intermediate node — forward the ack and drop our data bundle copy
        EpidemicDecision::ForwardAckAndDelete {
            original_bundle_id,
            peers: self.peers.clone(),
        }
    }

    pub fn forward_bundle(&self, bundle: Bundle, peers: Vec<Uuid>) {
        for peer in peers {
            // Here you would implement the actual sending logic, e.g., via network sockets
            println!("Forwarding bundle {} to peer {}", bundle.id, peer);
        }
    }
}

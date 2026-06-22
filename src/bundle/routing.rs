use std::collections::HashSet;
use uuid::Uuid;
use crate::bundle::model::{Bundle, BundleKind};

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
    StoreAndForward { peers: Vec<Uuid> },

    // Case 3a: ack reached its original sender (us).
    // BundleLayer must delete the data bundle we were holding.
    AckDelivered { original_bundle_id: Uuid },

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
        match bundle.kind {
            BundleKind::Message => self.handle_message(bundle),
            BundleKind::Ack     => self.handle_ack(bundle),
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

    fn handle_ack(&mut self, bundle: &Bundle) -> EpidemicDecision {
        // The ack's content field carries the original data bundle's ID.
        // e.g. when NodeB creates the ack, it sets content = original_bundle.id.to_string()
        let original_bundle_id = match Uuid::parse_str(&bundle.content) {
            Ok(id) => id,
            Err(_) => return EpidemicDecision::Ignore, // malformed ack
        };

        // Dedup: ignore if we already processed this ack
        if self.seen_ids.contains(&bundle.id) {
            return EpidemicDecision::Ignore;
        }
        self.seen_ids.insert(bundle.id);

        // Case 3a: we are the original sender — the ack has reached home
        if bundle.destination.id == self.node_id {
            return EpidemicDecision::AckDelivered { original_bundle_id };
        }

        // Case 3b: intermediate node — forward the ack and drop our data bundle copy
        EpidemicDecision::ForwardAckAndDelete {
            original_bundle_id,
            peers: self.peers.clone(),
        }
    }

    pub fn forward_bundle(&self,bundle : Bundle, peers: Vec<Uuid>) {

        for peer in peers {
            // Here you would implement the actual sending logic, e.g., via network sockets
            println!("Forwarding bundle {} to peer {}", bundle.id, peer);
        }
    }

}
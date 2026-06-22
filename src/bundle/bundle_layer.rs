impl BundleLayer {
    pub fn new() -> Self {
        BundleLayer {
            bundle_manager : BundleManager::new(),
            storage : Storage::new(),
            routing_engine: RoutingEngine::new()
        }
    }

    pub fn create_bundle(&self, nodeId : Uuid, destination : Uuid, content : String, bundleKind : BundleKind) -> Bundle {
        self.bundle_manager.create_bundle(nodeId, destination, content, bundleKind)
    }

    pub fn handle_bundle(&self, bundle : Bundle) {
        let decision = self.routing_engine.epidemic_propagation(&bundle);
        match decision {
            EpidemicDecision::Ignore => {
                // do nothing
            },
            EpidemicDecision::StoreAndForward { peers } => {
                self.store_bundle(bundle.clone());
                self.forward_bundle(bundle, peers);
            },
            EpidemicDecision::AckDelivered { original_bundle_id } => {
                self.delete_bundle(original_bundle_id);
            },
            EpidemicDecision::ForwardAckAndDelete { original_bundle_id, peers } => {
                self.delete_bundle(original_bundle_id);
                self.forward_bundle(bundle, peers);
            }
        }
    }

    fn store_bundle(&self, bundle : Bundle) {
        self.storage.store_bundle(bundle);
    }

    fn delete_bundle(&self, bundle_id : Uuid) {
        self.storage.delete_bundle(bundle_id);
    }

    fn forward_bundle(&self, bundle: Bundle, peers: Vec<Uuid>) {
        self.routing_engine.forward_bundle(bundle, peers);
    }
}
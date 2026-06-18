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

    pub fn store_bundle(&self, bundle : Bundle) {
        self.storage.store_bundle(bundle);
    }
}
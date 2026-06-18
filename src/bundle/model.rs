use std::string::String;

pub struct BundleLayer {
    bundle_manager: BundleManager,
    storage: Storage,
    routing_engine: RoutingEngine,
}

pub struct Bundle {
    pub id: Uuid,                   
    pub source: Node,               
    pub destination: Node,        
    pub content : String,
    pub timestamp: DateTime<Utc>,   
    pub ttl: DateTime<Utc>, 
    pub kind: BundleKind, 
    pub shipment_status: MsgStatus,
}

pub enum MsgStatus {
    // the bundle is created but not yet sent
    Pending,

    // the bundle is on the way to the destination
    InTransit,

    // the bundle has been delivered to the destination
    /// For Data bundles: set when an Ack is received, then deleted from storage.
    /// For Ack bundles: set when the Ack reaches the original sender.
    Delivered,

    // the bundle has expired //TTL exceeded
    Expired,
}

pub enum BundleKind {
    Message, 
    Ack,
}
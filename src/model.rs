use uuid::Uuid;

use crate::bundle::BundleLayer;
use crate::cla::ConvergenceLayer;

pub struct Node {
    pub id: Uuid,
    pub name: String,
    pub endpoint: NodeEndpoint,
    pub peers: Vec<Uuid>,
    pub bundle_layer: BundleLayer,
    pub cla: Box<dyn ConvergenceLayer>,
}

pub struct NodeEndpoint {
    pub address: String,
    pub port: u16,
}

pub struct Node {
    id: Uuid,
    bundle : BundleLayer, // handle bundle creation, ttl, ack,
    cla : ConvergenceLayer, // serialize bundle, decouple bundle with a specific transport type (udp)
    transport : TransportLayer // send data with each type 
}
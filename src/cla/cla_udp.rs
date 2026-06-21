pub struct UdpConvergenceLayer {
    transport: UdpTransport,
}

impl UdpConvergenceLayer {
    pub fn new(transport: UdpTransport) -> Self {
        Self { transport }
    }

    pub async fn send_bundle(&self, bundle: &Bundle, peer: SocketAddr) -> Result<(), ClaError> {
        let bytes = self.serialize(bundle)?;
        self.transport.send_to(&bytes, peer).await?;
        Ok(())
    }

    pub async fn receive_bundle(&self) -> Result<Bundle, ClaError> {
        let mut buffer = vec![0; 65_535];
        let (size, _peer) = self.transport.recv_from(&mut buffer).await?;
        self.deserialize(&buffer[..size])
    }
}

use std::net::SocketAddr;
use tokio::net::UdpSocket;

pub struct UdpTransport {
    socket: UdpSocket,
}

impl UdpTransport {
    pub async fn bind(local_addr: SocketAddr) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(local_addr).await?;
        Ok(Self { socket })
    }

    pub async fn send_to(&self, bytes: &[u8], peer_addr: SocketAddr) -> std::io::Result<usize> {
        self.socket.send_to(bytes, peer_addr).await
    }

    pub async fn recv_from(&self, buffer: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        self.socket.recv_from(buffer).await
    }
}

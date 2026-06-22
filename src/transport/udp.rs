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

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use super::UdpTransport;
    use std::time::Duration;

    #[tokio::test]
    async fn binds_to_an_ephemeral_local_address() {
        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let local_addr = transport.local_addr().unwrap();

        assert_eq!(local_addr.ip().to_string(), "127.0.0.1");
        assert_ne!(local_addr.port(), 0);
    }

    #[tokio::test]
    async fn sends_and_receives_a_datagram() {
        let receiver = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let sender = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let receiver_addr = receiver.local_addr().unwrap();
        let sender_addr = sender.local_addr().unwrap();
        let payload = b"bundle payload";

        let sent = sender.send_to(payload, receiver_addr).await.unwrap();
        assert_eq!(sent, payload.len());

        let mut buffer = [0_u8; 64];
        let (size, peer_addr) = tokio::time::timeout(
            Duration::from_secs(1),
            receiver.recv_from(&mut buffer),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(&buffer[..size], payload);
        assert_eq!(peer_addr, sender_addr);
    }
}

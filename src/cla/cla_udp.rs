use super::bundle::ProtobufBundle;
use super::protobuf;
use crate::bundle::Bundle;
use crate::transport::UdpTransport;
use std::fmt;
use std::net::SocketAddr;

pub struct UdpConvergenceLayer {
    transport: UdpTransport,
}

#[derive(Debug)]
pub enum ClaError {
    Io(std::io::Error),
    Serialize,
    Deserialize,
}

impl fmt::Display for ClaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClaError::Io(error) => write!(f, "udp io error: {}", error),
            ClaError::Serialize => write!(f, "failed to serialize bundle"),
            ClaError::Deserialize => write!(f, "failed to deserialize bundle"),
        }
    }
}

impl std::error::Error for ClaError {}

impl From<std::io::Error> for ClaError {
    fn from(error: std::io::Error) -> Self {
        ClaError::Io(error)
    }
}

impl UdpConvergenceLayer {
    pub fn new(transport: UdpTransport) -> Self {
        Self { transport }
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ClaError> {
        self.transport.local_addr().map_err(ClaError::Io)
    }

    pub async fn send_bundle(&self, bundle: &Bundle, peer: SocketAddr) -> Result<(), ClaError> {
        let bytes = self.serialize(bundle)?;
        self.transport.send_to(&bytes, peer).await?;
        Ok(())
    }

    pub async fn receive_bundle(&self) -> Result<Bundle, ClaError> {
        self.receive_bundle_from()
            .await
            .map(|(bundle, _peer)| bundle)
    }

    pub async fn receive_bundle_from(&self) -> Result<(Bundle, SocketAddr), ClaError> {
        let mut buffer = vec![0; 65_535];
        let (size, peer) = self.transport.recv_from(&mut buffer).await?;
        let bundle = self.deserialize(&buffer[..size])?;
        Ok((bundle, peer))
    }

    fn serialize(&self, bundle: &Bundle) -> Result<Vec<u8>, ClaError> {
        let protobuf_bundle = ProtobufBundle::from(bundle);
        protobuf::serialize(&protobuf_bundle).ok_or(ClaError::Serialize)
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<Bundle, ClaError> {
        let protobuf_bundle = protobuf::deserialize(bytes).ok_or(ClaError::Deserialize)?;
        Ok(Bundle::from(protobuf_bundle))
    }
}

#[cfg(test)]
mod tests {
    use super::{ClaError, UdpConvergenceLayer};
    use crate::bundle::{Bundle, BundlePayload};
    use crate::transport::UdpTransport;
    use chrono::{Duration as ChronoDuration, Timelike, Utc};
    use std::time::Duration;
    use uuid::Uuid;

    fn make_bundle(payload: BundlePayload) -> Bundle {
        let created_at = Utc::now().with_nanosecond(0).unwrap();
        Bundle {
            id: Uuid::new_v4(),
            source: Uuid::new_v4(),
            destination: Uuid::new_v4(),
            created_at,
            expires_at: created_at + ChronoDuration::minutes(5),
            payload,
        }
    }

    #[tokio::test]
    async fn serializes_and_deserializes_ack_payload() {
        let cla = UdpConvergenceLayer::new(
            UdpTransport::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let bundle = make_bundle(BundlePayload::Ack {
            original_bundle_id: Uuid::new_v4(),
        });

        let bytes = cla.serialize(&bundle).unwrap();
        let decoded = cla.deserialize(&bytes).unwrap();

        assert_eq!(decoded, bundle);
    }

    #[tokio::test]
    async fn sends_and_receives_bundle_over_udp() {
        let receiver = UdpConvergenceLayer::new(
            UdpTransport::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let sender = UdpConvergenceLayer::new(
            UdpTransport::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let bundle = make_bundle(BundlePayload::Message("hello from cla".to_string()));

        sender
            .send_bundle(&bundle, receiver.local_addr().unwrap())
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(1), receiver.receive_bundle())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(received, bundle);
    }

    #[tokio::test]
    async fn rejects_invalid_wire_bytes() {
        let cla = UdpConvergenceLayer::new(
            UdpTransport::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );

        let error = cla.deserialize(b"not a protobuf bundle").unwrap_err();

        assert!(matches!(error, ClaError::Deserialize));
    }
}

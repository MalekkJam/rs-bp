use super::bundle::{
    protobuf_bundle, Ack, ProtobufBundle, RequestSummaryVector, SummaryVector,
};
use crate::bundle::{Bundle, BundlePayload};
use chrono::{DateTime, Utc};
use protobuf::Message;
use uuid::Uuid;

impl From<&Bundle> for ProtobufBundle {
    fn from(b: &Bundle) -> Self {
        let payload = match &b.payload {
            BundlePayload::Message(message) => {
                Some(protobuf_bundle::Payload::Message(message.clone()))
            }
            BundlePayload::Ack { original_bundle_id } => {
                let mut ack = Ack::new();
                ack.original_bundle_id = original_bundle_id.to_string();
                Some(protobuf_bundle::Payload::Ack(ack))
            }
            BundlePayload::RequestSummaryVector => Some(
                protobuf_bundle::Payload::RequestSummaryVector(RequestSummaryVector::new()),
            ),
            BundlePayload::SummaryVector(bundle_ids) => {
                let mut summary_vector = SummaryVector::new();
                summary_vector.bundle_ids =
                    bundle_ids.iter().map(Uuid::to_string).collect();
                Some(protobuf_bundle::Payload::SummaryVector(summary_vector))
            }
        };

        ProtobufBundle {
            id: b.id.to_string(),
            source_id: b.source.to_string(),
            destination_id: b.destination.to_string(),
            created_at: b.created_at.timestamp(),
            expires_at: b.expires_at.timestamp(),
            payload,
            ..ProtobufBundle::new()
        }
    }
}

impl From<ProtobufBundle> for Bundle {
    fn from(p: ProtobufBundle) -> Self {
        let payload = match p.payload {
            Some(protobuf_bundle::Payload::Message(message)) => {
                BundlePayload::Message(message)
            }
            Some(protobuf_bundle::Payload::Ack(ack)) => BundlePayload::Ack {
                original_bundle_id: Uuid::parse_str(&ack.original_bundle_id)
                    .unwrap_or_default(),
            },
            Some(protobuf_bundle::Payload::RequestSummaryVector(_)) => {
                BundlePayload::RequestSummaryVector
            }
            Some(protobuf_bundle::Payload::SummaryVector(summary_vector)) => {
                BundlePayload::SummaryVector(
                    summary_vector
                        .bundle_ids
                        .into_iter()
                        .map(|bundle_id| {
                            Uuid::parse_str(&bundle_id).unwrap_or_default()
                        })
                        .collect(),
                )
            }
            None => BundlePayload::Message(String::new()),
        };

        Bundle {
            id: Uuid::parse_str(&p.id).unwrap_or_default(),
            source: Uuid::parse_str(&p.source_id).unwrap_or_default(),
            destination: Uuid::parse_str(&p.destination_id).unwrap_or_default(),
            created_at: DateTime::<Utc>::from_timestamp(p.created_at, 0)
                .unwrap_or_default(),
            expires_at: DateTime::<Utc>::from_timestamp(p.expires_at, 0)
                .unwrap_or_default(),
            payload,
        }
    }
}

pub fn serialize(bundle: &ProtobufBundle) -> Option<Vec<u8>> {
    match bundle.write_to_bytes() {
        Ok(bytes) => Some(bytes),
        Err(e) => {
            eprintln!("failed to serialize bundle: {}", e);
            None
        }
    }
}

pub fn deserialize(data: &[u8]) -> Option<ProtobufBundle> {
    match ProtobufBundle::parse_from_bytes(data) {
        Ok(bundle) => Some(bundle),
        Err(e) => {
            eprintln!("failed to deserialize bundle: {}", e);
            None
        }
    }
}

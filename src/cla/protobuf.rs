use super::bundle::{
    protobuf_bundle, Ack, ProtobufBundle, RequestSummaryVector, SummaryVector,
};
use crate::bundle::{Bundle, BundlePayload};
use chrono::{DateTime, Utc};
use protobuf::Message;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtobufConversionError {
    InvalidId(&'static str),
    InvalidTimestamp(&'static str),
    MissingPayload,
}

impl fmt::Display for ProtobufConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtobufConversionError::InvalidId(field) => {
                write!(f, "invalid ID in protobuf field '{field}'")
            }
            ProtobufConversionError::InvalidTimestamp(field) => {
                write!(f, "invalid timestamp in protobuf field '{field}'")
            }
            ProtobufConversionError::MissingPayload => write!(f, "protobuf bundle has no payload"),
        }
    }
}

impl std::error::Error for ProtobufConversionError {}

impl From<&Bundle> for ProtobufBundle {
    fn from(b: &Bundle) -> Self {
        let payload = match &b.payload {
            BundlePayload::Message(message) => {
                Some(protobuf_bundle::Payload::Message(message.clone()))
            }
            BundlePayload::Ack { original_bundle_id } => {
                let mut ack = Ack::new();
                ack.original_bundle_id = original_bundle_id.clone();
                Some(protobuf_bundle::Payload::Ack(ack))
            }
            BundlePayload::RequestSummaryVector => Some(
                protobuf_bundle::Payload::RequestSummaryVector(RequestSummaryVector::new()),
            ),
            BundlePayload::SummaryVector(bundle_ids) => {
                let mut summary_vector = SummaryVector::new();
                summary_vector.bundle_ids = bundle_ids.clone();
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

impl TryFrom<ProtobufBundle> for Bundle {
    type Error = ProtobufConversionError;

    fn try_from(p: ProtobufBundle) -> Result<Self, Self::Error> {
        let payload = match p.payload {
            Some(protobuf_bundle::Payload::Message(message)) => {
                BundlePayload::Message(message)
            }
            Some(protobuf_bundle::Payload::Ack(ack)) => BundlePayload::Ack {
                original_bundle_id: non_empty_string(
                    ack.original_bundle_id,
                    "ack.original_bundle_id",
                )?,
            },
            Some(protobuf_bundle::Payload::RequestSummaryVector(_)) => {
                BundlePayload::RequestSummaryVector
            }
            Some(protobuf_bundle::Payload::SummaryVector(summary_vector)) => {
                BundlePayload::SummaryVector(
                    summary_vector
                        .bundle_ids
                        .into_iter()
                        .map(|bundle_id| non_empty_string(bundle_id, "summary_vector.bundle_ids"))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            None => return Err(ProtobufConversionError::MissingPayload),
        };

        Ok(Bundle {
            id: non_empty_string(p.id, "id")?,
            source: non_empty_string(p.source_id, "source_id")?,
            destination: non_empty_string(p.destination_id, "destination_id")?,
            created_at: DateTime::<Utc>::from_timestamp(p.created_at, 0)
                .ok_or(ProtobufConversionError::InvalidTimestamp("created_at"))?,
            expires_at: DateTime::<Utc>::from_timestamp(p.expires_at, 0)
                .ok_or(ProtobufConversionError::InvalidTimestamp("expires_at"))?,
            payload,
        })
    }
}

fn non_empty_string(
    value: String,
    field: &'static str,
) -> Result<String, ProtobufConversionError> {
    if value.trim().is_empty() {
        return Err(ProtobufConversionError::InvalidId(field));
    }

    Ok(value)
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

use ::protobuf::Message;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use rs_bp::bundle::{Bundle, BundlePayload};
use rs_bp::cla::bundle::{PendingBundleRecord, ProtobufBundle};
use rs_bp::cla::protobuf;

const RECORD_MAGIC: &[u8] = b"RSBPQ1\0";

pub(crate) fn pending_directory(node_id: &str) -> PathBuf {
    PathBuf::from("storage")
        .join(safe_path_component(node_id))
        .join("pending")
}

pub(crate) async fn save_pending(directory: &Path, bundle: &Bundle) -> io::Result<()> {
    let directory = directory.to_owned();
    let bundle = bundle.clone();
    tokio::task::spawn_blocking(move || save_pending_sync(&directory, &bundle))
        .await
        .map_err(io::Error::other)?
}

fn save_pending_sync(directory: &Path, bundle: &Bundle) -> io::Result<()> {
    save_record_sync(directory, bundle, None)
}

pub(crate) async fn save_forwarded_pending(
    directory: &Path,
    bundle: &Bundle,
    previous_peer: SocketAddr,
) -> io::Result<()> {
    let directory = directory.to_owned();
    let bundle = bundle.clone();
    tokio::task::spawn_blocking(move || save_record_sync(&directory, &bundle, Some(previous_peer)))
        .await
        .map_err(io::Error::other)?
}

fn save_record_sync(
    directory: &Path,
    bundle: &Bundle,
    previous_peer: Option<SocketAddr>,
) -> io::Result<()> {
    let destination = pending_path(directory, &bundle.id)?;
    if !matches!(bundle.payload, BundlePayload::Message(_)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only messages may be queued",
        ));
    }
    Bundle::try_from(ProtobufBundle::from(bundle))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    fs::create_dir_all(directory)?;
    let protobuf_bundle = ProtobufBundle::from(bundle);
    let bytes = if let Some(previous_peer) = previous_peer {
        let record = PendingBundleRecord {
            bundle: Some(protobuf_bundle).into(),
            previous_peer: previous_peer.to_string(),
            ..Default::default()
        };
        let mut bytes = RECORD_MAGIC.to_vec();
        bytes.extend(record.write_to_bytes().map_err(io::Error::other)?);
        bytes
    } else {
        protobuf::serialize(&protobuf_bundle).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "could not serialize pending bundle",
            )
        })?
    };
    let temporary = directory.join(format!(".pending-{}.tmp", uuid::Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let result = (|| {
        file.write_all(&bytes)?;
        file.sync_all()?;
        // Publish complete bytes without replacing another bundle with this ID.
        // Requires a filesystem supporting hard links; no unsafe fallback.
        fs::hard_link(&temporary, &destination)
    })();
    drop(file);
    if let Err(error) = fs::remove_file(&temporary) {
        eprintln!(
            "could not remove temporary file {}: {error}",
            temporary.display()
        );
    }
    result
}

pub(crate) async fn load_pending(directory: &Path) -> io::Result<HashMap<String, Bundle>> {
    let directory = directory.to_owned();
    tokio::task::spawn_blocking(move || load_pending_sync(&directory))
        .await
        .map_err(io::Error::other)?
}

fn load_pending_sync(directory: &Path) -> io::Result<HashMap<String, Bundle>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error),
    };
    let mut pending = HashMap::new();

    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("bundle") {
            continue;
        }

        // An unreadable queue is an I/O failure, not an empty or corrupt queue.
        let bytes = fs::read(&path)?;
        let result = decode_record(&bytes)
            .ok()
            .map(|(bundle, _)| bundle)
            .filter(|bundle| matches!(bundle.payload, BundlePayload::Message(_)))
            .filter(|bundle| {
                pending_path(directory, &bundle.id).is_ok_and(|expected| expected == path)
            });
        match result {
            Some(bundle) => {
                pending.insert(bundle.id.clone(), bundle);
            }
            None => eprintln!("ignored invalid pending file {}", path.display()),
        }
    }

    Ok(pending)
}

fn decode_record(bytes: &[u8]) -> io::Result<(Bundle, Option<SocketAddr>)> {
    let (wire, previous_peer) = if let Some(bytes) = bytes.strip_prefix(RECORD_MAGIC) {
        let record = PendingBundleRecord::parse_from_bytes(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let wire = record
            .bundle
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing stored bundle"))?
            .clone();
        let previous_peer = record
            .previous_peer
            .parse::<SocketAddr>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        (wire, Some(previous_peer))
    } else {
        (
            ProtobufBundle::parse_from_bytes(bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
            None,
        )
    };
    let bundle = Bundle::try_from(wire)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok((bundle, previous_peer))
}

/// Read the relay's local reverse path; it is never inferred from an EID or
/// taken from a network-supplied field.
pub(crate) async fn previous_peer(
    directory: &Path,
    bundle_id: &str,
) -> io::Result<Option<SocketAddr>> {
    let path = pending_path(directory, bundle_id)?;
    let bundle_id = bundle_id.to_owned();
    tokio::task::spawn_blocking(move || {
        let (bundle, previous_peer) = decode_record(&fs::read(path)?)?;
        if bundle.id != bundle_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stored bundle ID mismatch",
            ));
        }
        Ok(previous_peer)
    })
    .await
    .map_err(io::Error::other)?
}

pub(crate) async fn remove_pending(directory: &Path, bundle_id: &str) -> io::Result<()> {
    let directory = directory.to_owned();
    let bundle_id = bundle_id.to_owned();
    tokio::task::spawn_blocking(move || remove_pending_sync(&directory, &bundle_id))
        .await
        .map_err(io::Error::other)?
}

fn remove_pending_sync(directory: &Path, bundle_id: &str) -> io::Result<()> {
    match fs::remove_file(pending_path(directory, bundle_id)?) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn pending_path(directory: &Path, bundle_id: &str) -> io::Result<PathBuf> {
    let is_uuid = uuid::Uuid::parse_str(bundle_id).is_ok_and(|id| id.to_string() == bundle_id);
    let is_legacy = bundle_id.strip_prefix("ipn:1:").is_some_and(|sequence| {
        sequence
            .parse::<u64>()
            .is_ok_and(|value| value.to_string() == sequence)
    });
    if !is_uuid && !is_legacy {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported pending bundle ID",
        ));
    }
    Ok(directory.join(format!("{}.bundle", safe_path_component(bundle_id))))
}

fn safe_path_component(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            character if character.is_control() => '_',
            character => character,
        })
        .collect()
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use super::{
        load_pending_sync as load_pending, remove_pending_sync as remove_pending,
        save_pending_sync as save_pending,
    };
    use rs_bp::bundle::{bundle_manager::BundleManager, BundlePayload};

    pub(crate) struct TestDirectory(pub(crate) PathBuf);

    impl TestDirectory {
        pub(crate) fn new() -> Self {
            let path = std::env::temp_dir().join(format!("rs-bp-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            // This directory was uniquely created and is owned by this test.
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn message() -> Bundle {
        BundleManager::new().create_bundle(
            "ipn:1:7001",
            "ipn:1:7002",
            BundlePayload::Message("original".into()),
        )
    }

    #[test]
    fn refuses_to_overwrite_a_pending_bundle() {
        let dir = TestDirectory::new();
        let bundle = message();
        save_pending(&dir.0, &bundle).unwrap();
        let mut replacement = bundle.clone();
        replacement.payload = BundlePayload::Message("replacement".into());
        assert!(save_pending(&dir.0, &replacement).is_err());
        assert_eq!(load_pending(&dir.0).unwrap()[&bundle.id], bundle);
    }

    #[test]
    fn ignores_wrong_filenames_corrupt_files_and_partial_writes() {
        let dir = TestDirectory::new();
        let bundle = message();
        let bytes = protobuf::serialize(&ProtobufBundle::from(&bundle)).unwrap();
        fs::write(dir.0.join("wrong.bundle"), &bytes).unwrap();
        fs::write(dir.0.join("corrupt.bundle"), b"broken").unwrap();
        fs::write(dir.0.join("interrupted.tmp"), &bytes[..4]).unwrap();
        assert!(load_pending(&dir.0).unwrap().is_empty());
    }

    #[test]
    fn restores_and_removes_legacy_pending_files() {
        let dir = TestDirectory::new();
        let mut bundle = message();
        bundle.id = "ipn:1:42".into();
        let bytes = protobuf::serialize(&ProtobufBundle::from(&bundle)).unwrap();
        fs::write(dir.0.join("ipn_1_42.bundle"), bytes).unwrap();
        assert_eq!(load_pending(&dir.0).unwrap()[&bundle.id], bundle);
        remove_pending(&dir.0, &bundle.id).unwrap();
        remove_pending(&dir.0, &bundle.id).unwrap();
        assert!(load_pending(&dir.0).unwrap().is_empty());
    }

    #[test]
    fn rejects_unsafe_or_ambiguous_ids_without_creating_files() {
        let dir = TestDirectory::new();
        for id in [
            "../outside",
            "..\\outside",
            "ipn_1_1",
            "ipn:1:01",
            "CON",
            "",
            "a/b",
            "a_b",
        ] {
            let mut bundle = message();
            bundle.id = id.into();
            assert!(save_pending(&dir.0, &bundle).is_err());
            assert!(remove_pending(&dir.0, id).is_err());
        }
        assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 0);
    }

    #[test]
    fn loads_only_messages_and_leaves_invalid_files_untouched() {
        let dir = TestDirectory::new();
        let mut bundle = message();
        bundle.payload = BundlePayload::Ack {
            original_bundle_id: "ipn:1:1".into(),
        };
        let path = pending_path(&dir.0, &bundle.id).unwrap();
        fs::write(
            &path,
            protobuf::serialize(&ProtobufBundle::from(&bundle)).unwrap(),
        )
        .unwrap();
        assert!(load_pending(&dir.0).unwrap().is_empty());
        assert!(path.exists());
    }

    #[test]
    fn failed_publication_cleans_up_its_temporary_file() {
        let dir = TestDirectory::new();
        let bundle = message();
        fs::create_dir(pending_path(&dir.0, &bundle.id).unwrap()).unwrap();
        assert!(save_pending(&dir.0, &bundle).is_err());
        assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn relay_record_restores_previous_peer_and_rejects_corrupted_metadata() {
        let dir = TestDirectory::new();
        let bundle = message();
        let upstream: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        save_forwarded_pending(&dir.0, &bundle, upstream)
            .await
            .unwrap();
        assert_eq!(load_pending(&dir.0).unwrap()[&bundle.id], bundle);
        assert_eq!(
            previous_peer(&dir.0, &bundle.id).await.unwrap(),
            Some(upstream)
        );
        assert!(
            save_forwarded_pending(&dir.0, &bundle, "127.0.0.1:9999".parse().unwrap())
                .await
                .is_err()
        );
        assert_eq!(
            previous_peer(&dir.0, &bundle.id).await.unwrap(),
            Some(upstream)
        );
        let record = PendingBundleRecord {
            bundle: Some(ProtobufBundle::from(&bundle)).into(),
            previous_peer: "invalid-peer".into(),
            ..Default::default()
        };
        let mut bytes = RECORD_MAGIC.to_vec();
        bytes.extend(record.write_to_bytes().unwrap());
        fs::write(pending_path(&dir.0, &bundle.id).unwrap(), bytes).unwrap();
        assert!(load_pending(&dir.0).unwrap().is_empty());
        assert!(previous_peer(&dir.0, &bundle.id).await.is_err());
    }
}

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use rs_bp::bundle::Bundle;
use rs_bp::cla::bundle::ProtobufBundle;
use rs_bp::cla::protobuf;

use super::AppResult;

pub(crate) fn pending_directory(node_id: &str) -> PathBuf {
    PathBuf::from("storage")
        .join(safe_path_component(node_id))
        .join("pending")
}

pub(crate) fn save_pending(directory: &Path, bundle: &Bundle) -> AppResult<()> {
    fs::create_dir_all(directory)?;
    let protobuf_bundle = ProtobufBundle::from(bundle);
    let bytes = protobuf::serialize(&protobuf_bundle).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "could not serialize pending bundle",
        )
    })?;
    fs::write(pending_path(directory, &bundle.id), bytes)?;
    Ok(())
}

pub(crate) fn load_pending(directory: &Path) -> AppResult<HashMap<String, Bundle>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error.into()),
    };
    let mut pending = HashMap::new();

    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("bundle") {
            continue;
        }

        let result = fs::read(&path)
            .ok()
            .and_then(|bytes| protobuf::deserialize(&bytes))
            .and_then(|bundle| Bundle::try_from(bundle).ok());
        match result {
            Some(bundle) => {
                pending.insert(bundle.id.clone(), bundle);
            }
            None => eprintln!("ignored invalid pending file {}", path.display()),
        }
    }

    Ok(pending)
}

pub(crate) fn remove_pending(directory: &Path, bundle_id: &str) -> AppResult<()> {
    match fs::remove_file(pending_path(directory, bundle_id)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn pending_path(directory: &Path, bundle_id: &str) -> PathBuf {
    directory.join(format!("{}.bundle", safe_path_component(bundle_id)))
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

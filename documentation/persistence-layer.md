# Persistence layer

## Functional documentation

The persistence layer keeps outbound bundles available while they are waiting
for acknowledgement. This allows a sender node to restart and continue retrying
previously queued bundles.

In the current MVP, persistence is used for pending outbound bundles only.
Received messages and duplicate-suppression state are not persisted.

Pending bundles are stored under:

```text
storage/<safe-node-id>/pending/<safe-bundle-id>.bundle
```

Each `.bundle` file contains protobuf-encoded bundle bytes.

## Technical documentation

Current runtime implementation: `src/main.rs`

Planned/refactoring implementation: `src/bundle/storage.rs`

### Runtime persistence functions

The CLI runtime currently owns these functions:

| Function | Purpose |
| --- | --- |
| `pending_directory(node_id)` | Builds the pending directory path for a node. |
| `save_pending(directory, bundle)` | Serializes one bundle to protobuf bytes and writes it to disk. |
| `load_pending(directory)` | Reads `.bundle` files, decodes valid bundles, and ignores invalid files. |
| `remove_pending(directory, bundle_id)` | Deletes the pending file for an acknowledged or expired bundle. |
| `pending_path(directory, bundle_id)` | Builds the path for one pending bundle file. |
| `safe_path_component(value)` | Replaces characters that are unsafe in file names. |

### Save flow

When a user sends a message:

1. `BundleManager` creates the bundle.
2. `save_pending` creates the pending directory if needed.
3. The bundle is converted into a generated `ProtobufBundle`.
4. The protobuf bundle is serialized to bytes.
5. The bytes are written to `<bundle-id>.bundle`.

The bundle is saved before the first UDP send attempt. This means the bundle is
not lost if the process stops after queueing but before acknowledgement.

### Load flow

On node startup, `load_pending`:

1. Reads the node pending directory.
2. Skips files whose extension is not `.bundle`.
3. Reads each candidate file as bytes.
4. Parses protobuf bytes into `ProtobufBundle`.
5. Converts the protobuf bundle into a domain `Bundle`.
6. Inserts valid bundles into an in-memory `HashMap<String, Bundle>`.
7. Logs and ignores invalid pending files.

### Delete flow

Pending files are deleted when:

- an ACK for the original bundle arrives;
- the pending bundle expires during retry scanning.

Missing files are treated as already removed, not as fatal errors.

### Planned storage abstraction

`src/bundle/storage.rs` contains an older or planned `Storage` abstraction. It
is not currently exported by `src/bundle/mod.rs` and is not used by `src/main.rs`.

Its intended role is to encapsulate capacity-limited bundle storage. However,
the file currently references older domain types that do not match the active
`Bundle` model, so it should be treated as refactoring work rather than current
runtime behavior.

## Current limitations

- Only pending outbound bundles are persisted.
- Duplicate suppression state is not persisted.
- Storage is not yet encapsulated behind a stable bundle-layer API.
- Pending files are protobuf bytes, not human-readable JSON.
- There is no atomic write/rename sequence for pending files yet.


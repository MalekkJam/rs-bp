# Persistence layer

## Functional documentation

The persistence layer keeps outbound bundles available while they are waiting
for acknowledgement. This allows a sender node to restart and continue retrying
previously queued bundles.

Persistence covers locally originated and relayed pending messages. Relays also
persist their previous UDP peer. Final-delivery duplicate state is not persisted.

Pending bundles are stored under:

```text
storage/<safe-node-id>/pending/<safe-bundle-id>.bundle
```

Locally originated files contain raw `ProtobufBundle` bytes, as before. Relay
files start with the bytes `RSBPQ1\0`, followed by a `PendingBundleRecord` containing
the bundle and previous UDP peer. The wrapper is local metadata, never a datagram.

## Technical documentation

Current implementation: `src/app/persistence.rs`, called by `src/app/runtime.rs`.
Save, load, and remove are async wrappers around synchronous helpers running in
`tokio::task::spawn_blocking`. The event loop awaits each operation in order.

Planned/refactoring implementation: `src/bundle/storage.rs`

### Runtime persistence functions

The persistence module owns these functions:

| Function | Purpose |
| --- | --- |
| `pending_directory(node_id)` | Builds the pending directory path for a node. |
| `save_pending(directory, bundle)` | Serializes one bundle to protobuf bytes and writes it to disk. |
| `save_forwarded_pending(directory, bundle, previous_peer)` | Atomically saves a relay bundle with its reverse path. |
| `previous_peer(directory, bundle_id)` | Reads and validates a stored reverse path for returning an ACK. |
| `load_pending(directory)` | Reads `.bundle` files, decodes valid bundles, and ignores invalid files. |
| `remove_pending(directory, bundle_id)` | Deletes the pending file for an acknowledged or expired bundle. |
| `pending_path(directory, bundle_id)` | Validates a canonical UUID or legacy numeric ID and builds its path. |
| `safe_path_component(value)` | Replaces characters that are unsafe in file names. |

### Save flow

When a user sends a message:

1. `BundleManager` creates the bundle.
2. `save_pending` creates the pending directory if needed.
3. The bundle is converted into a generated `ProtobufBundle`.
4. The protobuf bundle is serialized to bytes.
5. Bytes are written to a uniquely created `.tmp` file in the same directory.
6. `sync_all` flushes the file contents before publication.
7. A hard link publishes the complete file at `<safe-bundle-id>.bundle` without
   replacing an existing file. The temporary link is then removed.

The bundle is saved before insertion into memory or the first UDP send attempt.
Existing IDs cause an error instead of an overwrite. Publication requires a
filesystem supporting hard links; failures are reported to the user. A stopped
process can leave a temporary file, which startup ignores. Directory metadata
is not explicitly synced, so this is not a full power-loss durability guarantee.

The no-overwrite publication uses the documented existing-destination error of
[`std::fs::hard_link`](https://doc.rust-lang.org/std/fs/fn.hard_link.html).

### Load flow

On node startup, `load_pending`:

1. Reads the node pending directory.
2. Skips non-regular files and files whose extension is not `.bundle`.
3. Reads each candidate file as bytes.
4. Detects the relay record marker or reads a legacy raw `ProtobufBundle`.
   Relay records require a parseable previous peer address and embedded bundle.
5. Converts the protobuf bundle into a domain `Bundle`.
6. Requires a message payload and a filename matching the validated bundle ID.
7. Inserts valid bundles into an in-memory `HashMap<String, Bundle>`.
8. Logs and ignores invalid files without deleting them. Actual read errors
   abort restoration rather than silently pretending the queue is empty.

Old IDs such as `ipn:1:42` still map to `ipn_1_42.bundle`. Newly created IDs are
canonical lowercase UUIDs and use their UUID string as the filename stem.

### Delete flow

Pending files are deleted when:

- an unexpired ACK for the original bundle passes recipient/source/peer checks;
- the pending bundle expires or exhausts its hop limit during retry scanning.

Relays must successfully attempt to return the application ACK to the saved
previous peer before deleting their retained message. If that datagram is lost,
an upstream retry can recreate the relay entry and elicit another final ACK.

Missing files are treated as already removed. The runtime removes in-memory
state only after successful file deletion. On failure it logs the error and
keeps the entry, allowing another ACK or expiry pass to retry deletion.

### Planned storage abstraction

`src/bundle/storage.rs` contains an older or planned `Storage` abstraction. It
is not currently exported by `src/bundle/mod.rs` and is not used by `src/app/runtime.rs`.

Its intended role is to encapsulate capacity-limited bundle storage. However,
the file currently references older domain types that do not match the active
`Bundle` model, so it should be treated as refactoring work rather than current
runtime behavior.

## Current limitations

- Final delivery history is not persisted; pending relay messages are persisted.
- Duplicate suppression state is not persisted.
- Storage is not yet encapsulated behind a stable bundle-layer API.
- Pending files are protobuf bytes, not human-readable JSON.
- Full power-loss durability and automatic cleanup of abandoned temporary files
  are not implemented. Invalid files are retained for inspection.
- Tests exercise conflicting publication, partial-file restoration, invalid
  paths, and deletion errors; they do not simulate filesystem or power failure.
- Older executables cannot load versioned relay records. Do not downgrade while
  relay queues are pending; new executables still restore old raw pending files.

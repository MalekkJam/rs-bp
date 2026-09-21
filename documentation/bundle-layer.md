# Bundle layer

## Functional documentation

The bundle layer defines the messages exchanged by nodes. It is responsible for
representing bundle data, creating new bundles, checking expiration, and
describing routing decisions.

A bundle is the application's unit of delivery. In the current MVP, bundles can
carry:

- a text message;
- an acknowledgement for a previously sent bundle;
- a request for a summary vector;
- a summary vector containing known bundle IDs.

The runtime currently uses message and acknowledgement payloads. Summary-vector
payloads exist in the domain model and protobuf schema, but the synchronization
workflow is not implemented yet.

## Technical documentation

Main files:

- `src/bundle/model.rs`
- `src/bundle/bundle_manager.rs`
- `src/bundle/routing.rs`
- `src/bundle/mod.rs`

### Domain model

`src/bundle/model.rs` defines:

```rust
pub struct Bundle {
    pub id: String,
    pub source: String,
    pub destination: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub hop_count: Option<HopCount>,
    pub payload: BundlePayload,
}
```

`BundlePayload` is an enum:

```rust
pub enum BundlePayload {
    Message(String),
    Ack { original_bundle_id: String },
    RequestSummaryVector,
    SummaryVector(Vec<String>),
}
```

### Bundle manager

`BundleManager` creates bundles and provides simple bundle predicates.

Important methods:

| Method | Purpose |
| --- | --- |
| `new()` / `default()` | Creates a stateless manager using UUID v4 bundle IDs. |
| `create_bundle(...)` | Builds a bundle with source, destination, payload, creation time, expiry time, and generated ID. |
| `bundle_expired(&Bundle)` | Returns true when current UTC time is at or after the bundle expiry time. |
| `bundle_at_destination(&Bundle, node_id)` | Returns true when the bundle destination equals the supplied node ID. |

New bundle IDs are canonical UUID v4 strings generated with the existing `uuid`
dependency. Recreating a manager does not reset a counter. Legacy
`ipn:1:<sequence>` IDs remain readable in protobuf and pending queue files.

The default time-to-live is three weeks:

```rust
const DEFAULT_TTL: Duration = Duration::weeks(3);
```

Creation timestamps are rounded to whole seconds by setting nanoseconds to zero.
That matches the protobuf wire model, which stores timestamps as integer Unix
seconds.

### Routing engine

The active `forwarding_copy(&Bundle)` helper prepares one hop by incrementing a
copy of the optional hop counter. New bundles default to a 16-hop limit. Legacy
bundles without counters acquire that default on forwarding. The original queue
entry stays unchanged across retries, and exhausted limits prevent forwarding.
Source, final destination, timestamps, payload, and ID remain unchanged.

`src/bundle/routing.rs` contains a planned epidemic-routing decision engine.
It tracks:

- the local node ID;
- known peers;
- bundle IDs already seen during the process.

The routing engine returns an `EpidemicDecision` instead of performing every
side effect directly:

| Decision | Meaning |
| --- | --- |
| `Ignore` | The bundle is duplicate or unsupported. |
| `StoreAndForward { peers }` | Store the message and forward it to peers. |
| `AckDelivered { original_bundle_id }` | An ACK reached the node that needed it; delete the original pending bundle. |
| `ForwardAckAndDelete { original_bundle_id, peers }` | Delete the original pending bundle and forward the ACK. |

In the current runtime, `src/app/runtime.rs` handles message delivery, ACKs, and
retries, delegating file operations to `src/app/persistence.rs`. The routing engine exists as a separate domain
component but is not currently wired into the CLI node loop.

### Module exports

`src/bundle/mod.rs` currently exports:

```rust
pub mod bundle_manager;
pub mod model;
pub mod routing;

pub use model::{Bundle, BundlePayload};
```

`bundle_layer.rs` and `storage.rs` are present in the source tree but are not
exported from this module.

## Current limitations

- Legacy sequence IDs may already collide. New UUID generation does not repair
  collisions in existing queued data. Node IDs still depend only on UDP ports.
- The older epidemic engine is not integrated; the runtime uses configured
  next-hop forwarding with `forwarding_copy` and durable reverse ACK paths.
- Summary-vector behavior is represented but inactive.
- The planned `bundle_layer.rs` file references older types and is not part of the current module tree.

# Node model

## Functional documentation

The node model describes the intended shape of a complete node: identity,
endpoint, peers, bundle layer, and convergence layer.

This model is useful as architecture documentation for where the project is
going, but it is not the structure currently used by the CLI runtime. The
current executable builds its node state directly in `src/main.rs`.

## Technical documentation

Main file:

- `src/model.rs`

The file defines:

```rust
pub struct Node {
    pub id: Uuid,
    pub name: String,
    pub endpoint: NodeEndpoint,
    pub peers: Vec<Uuid>,
    pub bundle_layer: BundleLayer,
    pub cla: Box<dyn ConvergenceLayer>,
}
```

and:

```rust
pub struct NodeEndpoint {
    pub address: String,
    pub port: u16,
}
```

The intended responsibilities are:

| Field | Intended purpose |
| --- | --- |
| `id` | Stable node identity. |
| `name` | Human-readable node name. |
| `endpoint` | Network address and port. |
| `peers` | Known peer node IDs. |
| `bundle_layer` | Bundle creation, storage, and routing behavior. |
| `cla` | Transport-specific convergence layer implementation. |

## Current limitations

- `src/model.rs` is not exported from `src/lib.rs`.
- It references abstractions that are not currently wired into the runtime.
- The active runtime uses string node IDs derived from UDP ports, not UUID node IDs.
- `ConvergenceLayer` is referenced conceptually, but the active exported CLA type is `UdpConvergenceLayer`.


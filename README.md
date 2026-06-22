# rs-bp

`rs-bp` is an experimental Delay-Tolerant Networking (DTN) node written in
Rust. It implements a small store-and-forward messaging workflow over UDP and
uses Protocol Buffers as its wire format.

The project is inspired by the architectural principles of
[RFC 9171](https://www.rfc-editor.org/rfc/rfc9171), but it is not currently a
Bundle Protocol v7 compliant implementation. Its immediate purpose is to
provide a clear, testable foundation for intermittent-connectivity experiments.

## Current Status

The current MVP supports:

- Independent node processes bound to configurable UDP addresses.
- Interactive text-message submission to a configured next-hop node.
- Typed bundles with UUIDs, source and destination IDs, creation time, expiry,
  and payload data.
- Protocol Buffer serialization through a UDP convergence layer.
- Delivery acknowledgements referencing the original bundle ID.
- A durable pending queue under `storage/`.
- Automatic retry every two seconds until an acknowledgement is received.
- Queue restoration after the sender restarts.
- Duplicate message suppression during a node process lifetime.
- Expiration of pending bundles after their configured TTL.

Summary-vector payloads are represented in the domain and wire models, but
peer inventory synchronization is not implemented in the runtime yet.

## Architecture

The codebase separates transferable bundle data from network transport:

```text
Interactive CLI
      |
      v
Bundle manager and pending queue
      |
      v
UDP convergence layer
      |
      +-- Bundle <-> Protocol Buffer conversion
      |
      v
Tokio UDP transport
```

The main components are:

- `bundle`: domain models, bundle creation, expiration checks, and routing
  decisions.
- `cla`: Protocol Buffer conversion and the UDP convergence layer.
- `transport`: raw asynchronous UDP send and receive operations.
- `main.rs`: node configuration, command processing, retry scheduling,
  acknowledgement handling, and MVP queue persistence.

Mutable node state is currently owned by one Tokio task and multiplexed with
`tokio::select!`. This keeps the MVP free of shared-state locks. Persistence
will eventually move out of `main.rs` into the dedicated bundle storage layer.

## Bundle Model

Every bundle contains:

| Field | Purpose |
| --- | --- |
| `id` | Unique bundle identifier |
| `source` | Originating node identifier |
| `destination` | Intended next-hop node identifier |
| `created_at` | Bundle creation time in UTC |
| `expires_at` | Expiration time in UTC |
| `payload` | Message, ACK, summary request, or summary vector |

Node IDs are deterministically derived from the configured socket address in
the current MVP. Pending bundles are stored as protobuf-encoded files under:

```text
storage/<node-id>/pending/<bundle-id>.bundle
```

The `storage/` directory is excluded from version control.

## Requirements

- A recent stable Rust toolchain.
- Two available local UDP ports for the two-node example.

Verify the toolchain with:

```bash
rustc --version
cargo --version
```

## Running Two Nodes

Open two terminals in the repository.

Terminal A:

```bash
cargo run -- node 127.0.0.1:7001 127.0.0.1:7002
```

Terminal B:

```bash
cargo run -- node 127.0.0.1:7002 127.0.0.1:7001
```

At either prompt, send a message with:

```text
send hello from node A
```

The receiving node prints the message and sends an ACK to the UDP source. The
sending node removes the bundle from its pending queue only after that ACK is
received.

Running `cargo run` without arguments starts the same node mode and prompts for
the local and next-hop addresses.

## Offline Delivery

The next-hop node does not need to be running when a message is submitted:

1. Start node A with node B's address as its next hop.
2. Leave node B offline and enter `send <text>` on node A.
3. Check node A with `pending`; the bundle remains queued on disk.
4. Start node B.
5. Node A retries the bundle, node B displays it, and node B returns an ACK.
6. Node A removes the acknowledged bundle from memory and disk.

Because UDP does not establish a connection, delivery is determined by the
application-level ACK rather than by a successful socket send.

## Commands

| Command | Description |
| --- | --- |
| `send <text>` | Queue and immediately attempt to send a text bundle |
| `pending` | List bundles waiting for acknowledgement |
| `status` | Show the node ID, next hop, and pending count |
| `help` | Display available commands |
| `quit` or `exit` | Stop the node |

Run the self-contained transport demonstration with:

```bash
cargo run -- demo
```

## Testing

Run all current tests with:

```bash
cargo test --all-targets
```

The test suite currently covers UDP binding and datagram exchange, protobuf
round trips, malformed datagram rejection, and end-to-end bundle transfer
through the UDP convergence layer.

## Project Layout

```text
rs-bp/
|-- build.rs
|-- Cargo.toml
|-- src/
|   |-- main.rs
|   |-- lib.rs
|   |-- bundle/
|   |   |-- model.rs
|   |   |-- bundle_manager.rs
|   |   |-- routing.rs
|   |   |-- bundle_layer.rs       # under refactoring
|   |   `-- storage.rs            # under refactoring
|   |-- cla/
|   |   |-- bundle.proto
|   |   |-- protobuf.rs
|   |   `-- cla_udp.rs
|   `-- transport/
|       `-- udp.rs
`-- storage/                       # runtime data, ignored by Git
```

## Known Limitations

- The runtime supports one configured next hop per node.
- There is no peer discovery, multi-hop forwarding, or contact scheduling.
- Summary-vector synchronization is not active.
- Persistence is implemented in the MVP entry point rather than the final
  bundle storage abstraction.
- Duplicate suppression is not persisted across receiver restarts.
- UDP traffic is unauthenticated and unencrypted.
- Incoming protobuf fields still require stricter UUID and timestamp
  validation.
- The current implementation is RFC-inspired, not RFC 9171 interoperable.

## Roadmap

1. Make protobuf-to-domain conversion fallible and reject malformed fields.
2. Complete the storage and bundle-layer abstractions.
3. Move pending-queue and ACK orchestration out of `main.rs`.
4. Add peer tables and summary-vector exchange.
5. Implement multi-hop store-carry-forward routing.
6. Add restart, ACK-loss, and multi-node integration tests.

## License

This project is distributed under the terms of the repository's
[LICENSE](LICENSE) file.

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
- Explicit final destinations with `send-to`, durable relay forwarding, and
  delivery ACKs returned through the saved reverse path.
- Typed bundles with UUIDs, source and destination IDs, creation time, expiry,
  and payload data.
- Protocol Buffer serialization through a UDP convergence layer.
- Delivery acknowledgements referencing the original bundle ID.
- A persistent pending queue under `storage/`, with complete-file publication
  that refuses to overwrite an existing bundle.
- Automatic retry every two seconds until an acknowledgement is received.
- Queue restoration after the sender restarts.
- Duplicate message suppression during a node process lifetime.
- Expiration of pending bundles after their configured TTL.
- A default 16-hop limit; retransmissions do not consume extra stored hops.

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
- `app/cli.rs`: node configuration and command-line dispatch.
- `app/runtime.rs`: commands, retry scheduling, and acknowledgement handling.
- `app/persistence.rs`: pending queue files, accessed through blocking tasks.
- `main.rs`: thin Tokio entry point and fatal-error reporting.

Mutable node state is currently owned by one Tokio task and multiplexed with
`tokio::select!`. This keeps the MVP free of shared-state locks. Filesystem work
runs through `spawn_blocking`; queue operations remain sequentially awaited.

## Bundle Model

Every bundle contains:

| Field | Purpose |
| --- | --- |
| `id` | Unique bundle identifier |
| `source` | Originating node identifier |
| `destination` | Final delivery endpoint, independent of the next-hop address |
| `created_at` | Bundle creation time in UTC |
| `expires_at` | Expiration time in UTC |
| `hop_count` | Optional hop limit and count; initialized for new bundles |
| `payload` | Message, ACK, summary request, or summary vector |

New bundle IDs are UUID v4 strings. Existing `ipn:1:<sequence>` bundle IDs remain
readable, including their original pending files and ACK references. Node IDs
still use `ipn:1:<UDP-port>`; IP addresses do not contribute to node identity.
Pending bundles are stored as protobuf-encoded files under:

```text
storage/<safe-node-id>/pending/<safe-bundle-id>.bundle
```

For example, node `ipn:1:7001` uses `storage/ipn_1_7001/pending/`.
The `storage/` directory is excluded from version control. Keep the same local
port and next-hop address when restarting a node with queued bundles.

## Requirements

- A recent stable Rust toolchain.
- Two available local UDP ports for the two-node example.
- A storage filesystem supporting hard links (for example, NTFS). Queue writes
  fail explicitly if this operation is unavailable.

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

The receiving node checks the destination and expiry, prints each message once
per process, and ACKs every valid copy back to the UDP source. The sender checks
the ACK's destination, source, expiry, and actual peer address before deleting
the referenced pending file. Memory is removed only after disk deletion succeeds.
These checks do not authenticate UDP traffic.

Running `cargo run` without arguments starts the same node mode and prompts for
the local and next-hop addresses.

## Sending A to C through B

Restart any old node processes with the updated executable. Open three terminals
in the repository and run one command in each:

```powershell
# Terminal A: forward through B
cargo run -- node 127.0.0.1:7001 127.0.0.1:7002
```

```powershell
# Terminal B: forward through C
cargo run -- node 127.0.0.1:7002 127.0.0.1:7003
```

```powershell
# Terminal C: default forward route through A
cargo run -- node 127.0.0.1:7003 127.0.0.1:7001
```

At A's `rs-bp>` prompt:

```text
send-to ipn:1:7003 Hello C, through B!
```

B stores and forwards the bundle without displaying its message. C displays
the message with A as its source. C's application delivery ACK travels C → B → A
using B's persisted previous-peer address. The configured next hop is the default
forwarding route, not a replacement for the bundle's destination.

To test offline delivery, leave C stopped when sending. `pending` on A and B
shows their retained copies. Restart B with the same addresses if desired, then
start C: B restores its queue and reverse path and continues delivery. If A is
offline when the ACK returns, A's restored retries can recover a lost receipt.
`send <text>` still addresses the configured next-hop endpoint. A destination
equal to the local node is delivered locally without sending it around the ring.

The legacy `ipn:1:<port>` labels shown by `status` remain application identifiers;
they are not standard IPN URI syntax. See the
[multi-hop architecture and compatibility notes](documentation/multi-hop-design.md).

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
| `send-to <destination> <text>` | Send to a final endpoint through the configured next hop |
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

The tests cover UDP transfer, all payload round trips, malformed fields and
datagram size, UUID generation, legacy queue restoration, conflicting/invalid
files, destination and ACK validation, failed sends/deletions, expiration, and
queue restoration with lost-ACK recovery through the runtime handlers.
`tests/multi_hop.rs` also runs three real node processes, with C initially offline
and both A and B restarted before final delivery.

Also run `cargo fmt --all -- --check`, `cargo check --all-targets`, and
`cargo clippy --all-targets -- -D warnings` before submitting code changes.

## Project Layout

```text
rs-bp/
|-- build.rs
|-- Cargo.toml
|-- src/
|   |-- main.rs
|   |-- lib.rs
|   |-- app/
|   |   |-- cli.rs
|   |   |-- runtime.rs
|   |   `-- persistence.rs
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
- There is no peer discovery, destination-specific route table, or contact scheduling.
- Summary-vector synchronization is not active.
- Persistence lives in `app/persistence.rs`; the older bundle storage abstraction
  is inactive. File contents are synced before publication, but directory entries
  are not explicitly synced. Full power-loss durability is not guaranteed.
- Duplicate suppression is not persisted across receiver restarts.
- UDP traffic is unauthenticated and unencrypted.
- Node IDs still collide for hosts using the same UDP port. Legacy bundle IDs
  may already collide; generating new UUIDs cannot repair existing collisions.
- Wire IDs remain opaque nonblank strings for compatibility. Pending filenames
  accept canonical UUIDs or legacy numeric `ipn:1:<sequence>` IDs.
- Encoded bundles are limited to 65,507 bytes; fragmentation is not implemented.
- Restored bundles are retried to the configured next hop; changing that peer
  while a queue exists is not a supported migration.
- Return paths are fixed to the previous UDP peer at first acceptance. Route
  changes, multiple upstream paths, and dynamic peer addresses are unsupported.
- Relay queue files contain versioned local records. Older executables cannot
  read them; use upgraded nodes together for multi-hop tests. Old raw pending
  files remain readable by this version.
- ACKs are custom application delivery receipts, not BPv7 status reports or
  custody-transfer signals. Protobuf is not the RFC's CBOR wire encoding.
- The current implementation is RFC-inspired, not RFC 9171 interoperable.

## Roadmap

1. Introduce stable node identities with an explicit queue-migration strategy.
2. Strengthen crash recovery and persist bounded duplicate-suppression state.
3. Reconcile the inactive storage and bundle-layer abstractions with the runtime.
4. Add peer tables and summary-vector exchange.
5. Extend configured multi-hop forwarding with destination-specific routing.
6. Expand process-level, crash-injection, and multi-node integration tests.

## License

This project is distributed under the terms of the repository's
[LICENSE](LICENSE) file.

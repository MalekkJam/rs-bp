# Application runtime layer

## Functional documentation

The application runtime is the executable node process. It provides the user
interface, starts the UDP convergence layer, manages the current node state, and
coordinates bundle delivery.

Users can run the node in two ways:

```text
cargo run
cargo run -- node <local-address> <next-node-address>
```

The runtime supports these interactive commands:

| Command | Function |
| --- | --- |
| `send <text>` | Creates a message bundle, stores it as pending, and sends it to the configured next hop. |
| `pending` | Lists bundles still waiting for acknowledgement. |
| `status` | Displays local node ID, next-hop node ID, next-hop address, and pending count. |
| `help` | Prints available commands. |
| `quit` / `exit` | Stops the node. |

The runtime also has a demo mode:

```text
cargo run -- demo
```

Demo mode creates two local UDP convergence layers, sends one bundle between
them, verifies the received bundle, and exits.

## Technical documentation

Main file: `src/main.rs`

The async entry point is:

```rust
#[tokio::main(flavor = "current_thread")]
async fn main()
```

The runtime uses a single-thread Tokio event loop. `run()` parses CLI arguments
and dispatches to either:

- `run_node(bind_addr, next_addr)`;
- `run_demo()`;
- usage output.

### Node startup

`run_node` performs the following setup:

1. Creates a `UdpConvergenceLayer` bound to the local UDP address.
2. Derives a node ID from the local UDP port using `node_id_for_address`.
3. Derives the next-hop node ID from the next-hop UDP port.
4. Creates a `BundleManager`.
5. Computes the pending queue directory:

   ```text
   storage/<safe-node-id>/pending/
   ```

6. Loads pending `.bundle` files from disk.
7. Starts the main event loop.

### Main event loop

The node loop uses `tokio::select!` to multiplex three sources of work:

| Event | Handler |
| --- | --- |
| User input from stdin | `handle_command` |
| Incoming UDP bundle | `handle_incoming` |
| Retry timer tick | `retry_pending` |

This means the MVP keeps state simple: pending bundles and received IDs live in
normal in-memory collections inside one async task.

### Sending a message

When the user enters `send <text>`:

1. `BundleManager::create_bundle` creates a `BundlePayload::Message`.
2. `save_pending` serializes the bundle to protobuf bytes and writes it to disk.
3. The bundle is inserted into the in-memory `pending` map.
4. `UdpConvergenceLayer::send_bundle` sends the bundle to the next-hop address.

Because UDP send does not prove delivery, the bundle stays pending until an ACK
arrives.

### Receiving a message

Incoming UDP datagrams are decoded by:

```rust
cla.receive_bundle_from().await
```

The returned sender address is used when sending an acknowledgement back to the
peer that actually sent the datagram.

For `BundlePayload::Message`, `handle_incoming`:

1. Drops expired bundles.
2. Prints the message if it has not already been seen during this process.
3. Creates an ACK bundle referencing the original bundle ID.
4. Sends the ACK back to the UDP peer address.

For `BundlePayload::Ack`, `handle_incoming`:

1. Looks up the referenced original bundle ID in `pending`.
2. Removes the bundle from memory if present.
3. Removes the matching `.bundle` file from disk.

Summary-vector payloads are recognized but not implemented by the runtime yet.

### Retry behavior

Every two seconds, `retry_pending` scans the pending map:

- expired bundles are removed from memory and disk;
- non-expired bundles are sent again to the configured next-hop address.

This implements simple store-and-retry behavior for offline peers.

## Current limitations

- One configured next hop per node.
- No peer discovery.
- No multi-hop routing in the runtime.
- Duplicate suppression is in memory only and is lost on restart.
- Persistence logic currently lives in `src/main.rs` rather than the planned storage abstraction.
- UDP traffic is not authenticated or encrypted.


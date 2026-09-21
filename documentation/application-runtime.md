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
| `send-to <destination> <text>` | Uses the specified final endpoint while forwarding via the configured next hop. |
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

Main files: `src/main.rs` (entry point), `src/app/cli.rs` (configuration),
`src/app/runtime.rs` (node loop), and `src/app/persistence.rs` (queue files).

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

When the user enters `send <text>` or `send-to <destination> <text>`:

1. `BundleManager::create_bundle` creates a message with the selected final
   destination. Local destinations are displayed immediately without forwarding.
2. `forwarding_copy` prepares a copy with one outgoing hop added. The adapter
   checks its encoded datagram size. Oversized messages are rejected.
3. `save_pending` publishes a complete protobuf file without overwriting an
   existing bundle; a failed save is reported and does not queue the message.
4. The bundle is inserted into the in-memory `pending` map.
5. `UdpConvergenceLayer::send_bundle` attempts delivery to the next-hop address.
   Send failures are logged and the bundle remains queued for retry.

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
2. For a nonlocal destination, checks the hop limit, saves the bundle and actual
   previous UDP peer in one relay queue record, and forwards an incremented copy.
   Only the hop count changes. Duplicate pending IDs preserve the original
   reverse path and leave retransmission to the retry timer.
3. For a local destination, prints the message once per process and creates a
   custom application delivery ACK referencing the original bundle ID.
4. Sends that ACK back to the UDP peer address, including for duplicates. ACK
   send failures are logged; another copy can trigger another attempt.

For `BundlePayload::Ack`, `handle_incoming`:

1. Requires an unexpired ACK matching a retained original message.
2. Looks up the referenced original bundle ID in `pending` and verifies that its
   destination matches the ACK source and its source matches the ACK destination.
3. Requires the actual UDP peer address to equal the configured next hop.
4. Reads the original message's previous peer from its queue record. A relay
   forwards the ACK to that peer without changing the ACK's source or destination.
   A locally originated record requires the ACK destination to match this node.
5. After a successful ACK relay attempt (or local receipt), removes the pending
   file and then its memory state. Failed sends or file operations retain it.

Unknown or duplicate ACKs are harmless. Deletion failures keep the pending entry
for another attempt. These checks do not provide cryptographic authentication.

Summary-vector payloads are recognized but not implemented by the runtime yet.

### Retry behavior

Every two seconds, `retry_pending` scans the pending map:

- expired or hop-exhausted bundles are removed from disk, then memory;
- other bundles are sent to the configured next hop using an incremented copy.
  Stored counters are unchanged, so a retry does not consume another logical hop.

This implements simple store-and-retry behavior for offline peers.

## Current limitations

- One configured next hop per node.
- No peer discovery.
- Multi-hop forwarding uses one default next hop; no route discovery or route table.
- Duplicate suppression is in memory only and is lost on restart.
- Persistence lives in `src/app/persistence.rs` and runs in blocking tasks;
  handlers await each queue operation before processing the next event.
- UDP traffic is not authenticated or encrypted.
- Relay receipts follow fixed reverse paths, and final-delivery deduplication is
  process-local. See [multi-hop-design.md](multi-hop-design.md) for RFC boundaries.

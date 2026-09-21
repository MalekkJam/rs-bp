# Coding instructions for rs-bp

Use this guide when correcting or enhancing this repository. Follow applicable
`AGENTS.md` instructions and the user's current task. Keep changes focused on
the requested outcome; the improvement priorities below are context, not an
instruction to implement the entire roadmap.

## Project purpose

`rs-bp` is an experimental Rust 2021 Delay-Tolerant Networking node using Tokio,
UDP, and Protocol Buffers. Preserve its simple, testable store-and-forward
workflow. It is RFC 9171-inspired; do not describe it as BPv7 compliant or
interoperable without a separate implementation and verification effort.

Read [README.md](../README.md), the relevant [layer documentation](../documentation/README.md),
and the active code before making changes. Documentation describes intent but
can lag behind implementation. Report discrepancies and update affected docs
when the task touches them; do not reproduce stale assumptions in new code.

## Actual architecture

Paths below are relative to the repository root.

| Location | Responsibility |
| --- | --- |
| `src/main.rs` | Thin entry point; starts the current-thread Tokio runtime and reports fatal errors. |
| `src/app/cli.rs` | Argument parsing, address prompts, command help, and dispatch. |
| `src/app/runtime.rs` | Node state, interactive commands, receive handling, ACKs, retries, and demo. |
| `src/app/persistence.rs` | Pending files and versioned relay records with local reverse-path metadata. |
| `src/bundle/model.rs` | Transport-independent `Bundle` and `BundlePayload` data. |
| `src/bundle/bundle_manager.rs` | Bundle creation, ID generation, TTL, and destination/expiry predicates. |
| `src/bundle/routing.rs` | Active forwarding-copy/hop-limit policy; older epidemic engine remains inactive. |
| `src/cla/protobuf.rs` | Domain/wire conversion and protobuf encoding/decoding. |
| `src/cla/cla_udp.rs` | Sending and receiving bundles through the UDP adapter. |
| `src/transport/udp.rs` | Raw asynchronous datagram I/O; no delivery or routing policy. |
| `src/cla/bundle.proto`, `build.rs` | Wire schema and build-time Rust generation. |

`src/lib.rs`, `src/bundle/mod.rs`, and `src/app/mod.rs` define which modules are
active. `src/model.rs`, `src/bundle/storage.rs`, and
`src/bundle/bundle_layer.rs` are unfinished files outside the compiled module
tree. Their old types and tests are not evidence of working functionality.
Do not wire them in or copy their patterns without reconciling them with the
active model and adding relevant tests.

Keep domain rules independent of sockets and CLI output. Keep serialization in
the convergence layer and raw bytes in transport. Keep `main.rs` small. Introduce
traits or additional layers only when a concrete use case requires them.

## Verified baseline and known gaps

Updated after reliability and multi-hop corrections on 2026-09-21. Recheck this
snapshot after subsequent changes.

- **Evidence:** New bundle IDs are UUID v4 strings. Legacy numeric
  `ipn:1:<sequence>` IDs and their pending filenames remain readable. Node IDs
  still use `ipn:1:<port>`, ignoring the IP address. **Inference, high confidence:**
  hosts sharing a port still share node identity; existing legacy ID collisions
  are not repaired. Changing node identity requires an explicit queue migration.
- **Evidence:** Fallible protobuf conversion rejects blank IDs, missing payloads,
  out-of-range timestamps, and non-positive lifetimes. Wire IDs otherwise remain
  opaque strings for compatibility. Pending paths only accept canonical UUIDs
  or canonical numeric legacy IDs.
- **Evidence:** The runtime separates local delivery from relay forwarding while
  preserving source/final destination/lifetime. ACK source, destination, expiry,
  and actual next-hop address are checked against the retained message. Initial
  sends and ACK send failures are
  logged without terminating the node. Encoded bundles over 65,507 bytes are
  rejected before new messages enter the queue.
- **Evidence:** Queue files are written to unique temporary files, synced, and
  published using a non-overwriting hard link. Filesystem work runs through
  `spawn_blocking`. Disk deletion precedes memory removal; failure retains the
  entry. Restoration validates payload and filename and preserves legacy paths.
  Relay records also atomically save the previous UDP peer for reverse ACKs.
- **Unknown / limitation:** Hard-link support is required. Directory entries
  are not explicitly synced, so full power-loss durability is not established.
  Interrupted temporary files are ignored, not automatically deleted. Tests
  cover simulated partial files and I/O failures, not actual power loss.
- **Evidence:** `send-to <destination> <text>` supports configured multi-hop
  forwarding; `send <text>` preserves direct-neighbor behavior. Local destinations
  are delivered without forwarding. New bundles use a 16-hop limit, incremented
  on outgoing copies without mutating retry state. Peer synchronization remains
  inactive, and final-delivery deduplication is process-local. Queues require the same local
  port and configured next-hop address; peer reconfiguration is not a migration.
- **Evidence:** Active tests cover conversion, persistence, runtime ACKs,
  failed sends/deletions, expiry, forwarding limits, reverse-path restoration,
  and lost-ACK recovery. A three-process test restarts both A and B with C initially
  offline, then verifies final delivery and queue cleanup. Formatting
  and strict Clippy pass; generated-code allowances are scoped to its module.

Next priorities are node-identity migration, persistent/bounded deduplication,
and destination-specific routing. Do not execute the entire
roadmap unless the current user task authorizes it.

## Behavior to preserve and strengthen

These are requirements for future changes; the baseline above identifies where
the implementation does not yet establish them.

- Persist an outbound message before its first send. A successful UDP send is
  not proof of delivery. Keep it pending until an accepted ACK or expiration.
- ACKs reference the original bundle ID, not the ACK's own ID. Duplicate or
  unknown ACKs must be harmless. Validate the intended recipient and expected
  peer before allowing an ACK to delete pending data; do not claim this provides
  cryptographic authentication.
- Distinguish final destination from configured transport next hop. Only the
  final destination displays a message or generates a delivery ACK. Relays
  persist before forwarding and return validated ACKs on stored reverse paths.
  Never rewrite end-to-end fields or report receipt at a relay as final delivery.
- Keep the RFC 9171 architecture and the experimental protocol distinct. The
  custom ACK is an application receipt, not a BPv7 status report or custody
  transfer. See `documentation/multi-hop-design.md` for references and limits.
- Display a duplicate message only once within the supported deduplication
  scope, but ACK each valid, unexpired copy so lost ACKs can be recovered.
  Reply to the actual UDP sender address.
- Do not deliver or retry expired messages. Preserve the `now >= expires_at`
  boundary. Current defaults are a three-week TTL and a two-second retry period.
- Preserve queue restoration across sender restarts. Identity changes must
  account for existing files, ACK references, and duplicate tracking.
- Keep malformed network input from panicking or terminating the node. Isolate
  corrupt pending files and report useful diagnostics. Handle transient network
  failures without discarding queued data, including Windows UDP
  `ConnectionReset` when an offline port produces an ICMP response.
- Keep in-memory and on-disk queue transitions recoverable on failure. When
  strengthening persistence, test interrupted writes and failed deletions;
  define atomic replacement and flushing behavior before claiming durability.
- Keep file paths inside the intended storage directory. Test sanitization and
  collisions; never use an unvalidated received ID as a path component.

## Rust and async coding standards

- Use descriptive `snake_case` functions and variables, `PascalCase` types, and
  `SCREAMING_SNAKE_CASE` constants. Prefer small functions with one clear purpose.
- Prefer existing utilities, standard-library types, and installed dependencies.
  Add dependencies only when requested. Avoid speculative frameworks, generic
  abstractions, unrelated rewrites, and unnecessary configuration.
- Borrow with `&str`, slices, or references when ownership is unnecessary.
  Clone when required by ownership, not merely to avoid understanding it.
- Use enums and exhaustive matching for payloads and state transitions. Keep
  visibility narrow; expose APIs deliberately rather than making helpers public
  solely for testing.
- Return `Result` for recoverable failures and propagate with `?`. Prefer typed
  errors at library boundaries; the existing boxed `AppResult` is suitable at
  the application boundary. Preserve error causes and context when touching
  error paths, and avoid logging the same failure at every layer.
- Do not use `unwrap`, `expect`, or panics for user input, packets, or runtime
  filesystem failures. They are acceptable in tests and for documented internal
  invariants. Avoid `unsafe` unless a concrete need and safety argument exist.
- Preserve single-task ownership of pending and received-ID collections unless
  a demonstrated need justifies concurrency. Avoid unnecessary `Arc<Mutex<_>>`
  and locks held across `.await`.
- Do not add blocking I/O or long CPU loops to the Tokio event loop. Persistence
  already uses `spawn_blocking`; preserve sequential ordering and propagate
  operation/join failures when changing its async wrappers.
- Document public contracts and non-obvious decisions with concise Rustdoc or
  comments. Do not retain misleading comments or narrate obvious statements.

## Wire format and compatibility

- Edit `src/cla/bundle.proto` and handwritten conversion code, not generated
  Rust under `target/`. `build.rs` generates and prepares that code for inclusion.
- Preserve existing protobuf field numbers and meanings. Do not reuse removed
  tags; reserve them. Account for both network peers and persisted bundles when
  changing the schema, ID format, or validation rules.
- Hop counters are optional for legacy decoding; forwarding adds the default
  when absent. Relay records use the `RSBPQ1\0` disk marker and must never be sent
  as datagrams. Preserve reads of old raw queue files; older executables cannot
  read new relay records. Do not silently downgrade or migrate pending queues.
- Use fallible conversion at incoming boundaries. Never silently replace invalid
  IDs, timestamps, or missing payloads with plausible defaults.
- Keep timestamps consistent with the current integer Unix-second wire format.
  Test TTL boundaries and invalid timestamp relationships when adding validation.
- Account for encoded datagram size before sending. The 65,535-byte receive
  allocation is not a validated application payload limit or fragmentation scheme.
- Do not disable warnings across handwritten code to hide generated-code issues.
  Any necessary generated-code lint allowance should be narrowly scoped and
  explained at the generation/include boundary.

## Change workflow and verification

1. Read the affected implementation, callers, tests, and documentation. Check
   `git status --short` and preserve unrelated work.
2. State the intended behavior and smallest useful change. Before refactoring,
   write a brief cleanup plan and add missing behavior tests. For bug fixes,
   reproduce the bug with a regression test where practical.
3. Implement one coherent change. Keep protocol, persisted-data, and CLI
   compatibility explicit; do not accidentally turn cleanup into new behavior.
4. Run targeted tests, then the applicable checks below. Inspect failures and
   distinguish new regressions from the recorded baseline. Do not claim a clean
   build or lint result when checks fail.
5. Update affected README/layer documentation and this guide when facts change.
   Report changed files, user-visible behavior, validation results, and remaining
   gaps. Stop once the requested change is complete and verified.

Run commands from the repository root:

```text
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo run -- demo
```

Use `--offline` for Cargo dependency resolution when dependencies are cached
and network access is unavailable. Run the demo when changing transport or
conversion. Documentation-only edits need content/link/diff review rather than
new tests. Check final diffs for unrelated edits and whitespace problems.

Tests should assert observable behavior rather than mirror the implementation:

- Use loopback and ephemeral ports (`127.0.0.1:0`) for socket tests. Bound network
  waits with timeouts; avoid fixed ports and arbitrary sleeps.
- Use isolated temporary directories for persistence tests, clean up only their
  own artifacts, and never modify the user's `storage/` queue.
- Cover each payload's round trip and invalid fields when changing conversion.
- Cover TTL boundaries, duplicate messages with repeated ACKs, unknown/wrong-peer
  ACKs, lost ACK recovery, offline delivery, restart restoration, and IDs across
  nodes/restarts when changing runtime reliability.
- Cover malformed files, interrupted writes, and deletion failures when changing
  persistence. Prefer controlled time inputs for expiry tests.
- Test private app helpers within their modules; use integration tests for
  externally observable CLI/node workflows. Tests in unexported legacy files do
  not count as executed coverage.

Keep `target/` and runtime `storage/` data out of changes. `Cargo.lock` is
currently ignored; changing that policy is a separate, explicit repository change.

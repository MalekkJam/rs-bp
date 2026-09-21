# Multi-hop implementation contract

Implement `send-to <destination> <text>` while retaining `send <text>` for the
configured next hop. The default route is the existing configured UDP next hop;
the destination endpoint remains independent of that transport address.

Architecture references: RFC 9171 sections
[5.3](https://www.rfc-editor.org/rfc/rfc9171.html#section-5.3),
[5.4](https://www.rfc-editor.org/rfc/rfc9171.html#section-5.4),
[5.7](https://www.rfc-editor.org/rfc/rfc9171.html#section-5.7), and
[4.4.3](https://www.rfc-editor.org/rfc/rfc9171.html#section-4.4.3).

## Decisions and invariants

- Separate local application delivery from forwarding. Preserve the original
  bundle ID, source, final destination, payload, creation time and expiration.
- Retain a pending message at each relay before attempting transmission. Keep
  it until a validated final-destination application receipt or expiration.
- Use the existing custom ACK as an application delivery receipt, never as an
  RFC status report, hop-transfer acknowledgement, or custody-transfer signal.
  Only the final destination generates it. Relays preserve it and send it along
  the saved reverse path. UDP traffic is still unauthenticated.
- Persist the previous UDP peer together with the relay's bundle in one atomic
  record. Local metadata does not enter the transferable bundle. Load original
  raw protobuf pending files as locally originated records without a predecessor.
- When an ACK is lost, upstream retries trigger downstream retries and another
  final-destination ACK. A relay can release its copy after successfully sending
  the ACK upstream; an upstream retry can recreate the forwarding record.
- Duplicate pending IDs must not overwrite the saved previous peer. Reject
  conflicting content with the same ID. Final-delivery deduplication remains
  process-local, as before this feature.
- Add an optional hop-count structure to the experimental wire/domain model.
  New messages start at zero with a limit of 16; each outgoing hop uses a copy
  with an incremented count. Retries do not mutate the stored count. Legacy
  bundles without the structure acquire the default on forwarding.
- Keep queue I/O in persistence, forwarding policy in the bundle layer, and
  socket/encoding operations in their existing transport/convergence layers.

## Alternatives considered

Directly rewriting the destination at B would lose the final delivery endpoint.
ACKing at B on receipt would falsely report delivery at C. Sending C's ACK
straight to A would require direct connectivity and bypass the requested relay.
Dynamic discovery and general routing tables are unnecessary for the configured
A-to-B-to-C path. Durable reverse-path metadata keeps this increment bounded.

## Verification

Keep the prior reliability suite. Add tests for explicit destination parsing,
durable nonlocal reception, local-only delivery, immutable end-to-end fields,
reverse ACK validation, relay restart, C offline, duplicate/lost ACK recovery,
failed relay persistence, loop/hop exhaustion, and legacy queue/wire decoding.
Run a real three-process integration test in isolated storage directories,
followed by formatting, all-target compilation/tests, strict Clippy, and demo.

## Compatibility boundaries

This is an RFC-inspired architecture on an experimental protobuf/UDP protocol.
It does not implement BPv7 CBOR blocks, standard status reports, registrations,
fragmentation, full retention constraints, or standardized endpoint encoding.
The existing `ipn:1:<port>` labels are legacy application IDs, not standard IPN
URIs. Use upgraded nodes together for multi-hop operation; older executables do
not understand relay queue records or enforce the new hop limit. No automatic
identity migration, route discovery, or full power-loss guarantee is introduced.

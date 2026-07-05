# Convergence layer adapter

## Functional documentation

The convergence layer adapter is the bridge between application bundles and a
specific network transport. In this project, the implemented convergence layer
uses UDP and protobuf.

Its responsibilities are:

- convert a domain `Bundle` into bytes before sending;
- send those bytes to a UDP peer;
- receive bytes from UDP;
- convert received bytes back into a domain `Bundle`;
- expose the sender address when the caller needs to reply.

The main runtime uses this layer for both message bundles and ACK bundles.

## Technical documentation

Main files:

- `src/cla/cla_udp.rs`
- `src/cla/protobuf.rs`
- `src/cla/bundle.proto`
- `src/cla/mod.rs`

### Public type

`UdpConvergenceLayer` wraps a `UdpTransport`:

```rust
pub struct UdpConvergenceLayer {
    transport: UdpTransport,
}
```

It is created with:

```rust
pub fn new(transport: UdpTransport) -> Self
```

### Sending bundles

```rust
pub async fn send_bundle(&self, bundle: &Bundle, peer: SocketAddr) -> Result<(), ClaError>
```

Send flow:

1. `serialize(bundle)` converts `Bundle` into generated `ProtobufBundle`.
2. `protobuf::serialize` encodes the protobuf object into bytes.
3. `transport.send_to(&bytes, peer).await` sends one UDP datagram.

If serialization fails, the method returns `ClaError::Serialize`.
If UDP send fails, the `std::io::Error` is converted into `ClaError::Io`.

### Receiving bundles

```rust
pub async fn receive_bundle_from(&self) -> Result<(Bundle, SocketAddr), ClaError>
```

Receive flow:

1. Allocate a UDP receive buffer:

   ```rust
   let mut buffer = vec![0; 65_535];
   ```

2. Wait for one datagram:

   ```rust
   let (size, peer) = self.transport.recv_from(&mut buffer).await?;
   ```

3. Decode only the received bytes:

   ```rust
   let bundle = self.deserialize(&buffer[..size])?;
   ```

4. Return the decoded bundle and the UDP sender address:

   ```rust
   Ok((bundle, peer))
   ```

`receive_bundle()` is a convenience method that calls `receive_bundle_from()`
and discards the sender address.

### Protobuf schema

`src/cla/bundle.proto` defines the wire format:

```proto
message ProtobufBundle {
    string id = 1;
    string source_id = 2;
    string destination_id = 3;
    int64 created_at = 4;
    int64 expires_at = 5;

    oneof payload {
        string message = 6;
        Ack ack = 7;
        RequestSummaryVector request_summary_vector = 8;
        SummaryVector summary_vector = 9;
    }
}
```

The generated Rust code is produced by `build.rs` and included by
`src/cla/mod.rs`:

```rust
include!(concat!(env!("OUT_DIR"), "/proto/bundle.rs"));
```

### Conversion rules

`src/cla/protobuf.rs` implements:

- `From<&Bundle> for ProtobufBundle`;
- `TryFrom<ProtobufBundle> for Bundle`.

During deserialization, the conversion rejects:

- empty bundle IDs;
- empty source IDs;
- empty destination IDs;
- empty ACK original bundle IDs;
- empty summary-vector bundle IDs;
- invalid timestamps;
- missing payloads.

### Error model

`ClaError` has three variants:

| Error | Meaning |
| --- | --- |
| `Io(std::io::Error)` | UDP bind/send/receive or address lookup failed. |
| `Serialize` | Domain bundle could not be encoded as protobuf bytes. |
| `Deserialize` | Received bytes could not be parsed or converted into a valid domain bundle. |

## Current limitations

- The UDP buffer is allocated for every receive call.
- A bundle larger than one UDP datagram is not fragmented or reassembled.
- Protobuf parse failures are logged to stderr by the helper and then returned as `ClaError::Deserialize`.
- The layer does not authenticate, encrypt, or validate peer identity.


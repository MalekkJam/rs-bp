# Transport layer

## Functional documentation

The transport layer is the lowest network layer in the project. It sends and
receives raw bytes over UDP. It does not know what a bundle is and does not
perform serialization, acknowledgement handling, retry, routing, or validation.

From the application's point of view, this layer provides:

- binding to a local UDP address;
- sending bytes to a peer address;
- receiving bytes and the sender address;
- reading the local socket address.

## Technical documentation

Main files:

- `src/transport/udp.rs`
- `src/transport/mod.rs`

### Public type

`UdpTransport` wraps Tokio's async UDP socket:

```rust
pub struct UdpTransport {
    socket: UdpSocket,
}
```

`src/transport/mod.rs` exposes it as:

```rust
pub mod udp;
pub use udp::UdpTransport;
```

### Binding

```rust
pub async fn bind(local_addr: SocketAddr) -> std::io::Result<Self>
```

This calls:

```rust
UdpSocket::bind(local_addr).await
```

If the caller passes port `0`, the operating system chooses an available
ephemeral port.

### Sending

```rust
pub async fn send_to(&self, bytes: &[u8], peer_addr: SocketAddr) -> std::io::Result<usize>
```

This sends one UDP datagram and returns the number of bytes accepted by the OS
socket layer.

Important UDP behavior: a successful `send_to` does not mean the peer received
or processed the datagram. The application-level ACK workflow is responsible
for delivery confirmation.

### Receiving

```rust
pub async fn recv_from(&self, buffer: &mut [u8]) -> std::io::Result<(usize, SocketAddr)>
```

This waits asynchronously for one UDP datagram and writes it into the supplied
buffer. It returns:

- the number of bytes written;
- the sender's socket address.

The convergence layer decides how to interpret the received bytes.

### Local address

```rust
pub fn local_addr(&self) -> std::io::Result<SocketAddr>
```

This exposes the actual bound address. It is useful when binding with port `0`
because the caller can discover the assigned port.

## Current limitations

- UDP is unreliable: datagrams can be lost, duplicated, reordered, or truncated.
- There is no connection lifecycle.
- There is no backpressure protocol at this layer.
- The layer does not enforce a maximum application bundle size.


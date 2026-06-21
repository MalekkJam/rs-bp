# whatspace

Whatspace is a minimal Delay-Tolerant Networking (DTN) middleware implemented in Rust.
It simulates distributed DTN nodes based on the Store-Carry-Forward model.

The project focuses on modular system design, persistent storage, asynchronous networking, and distributed coordination between independent nodes.

---

## Overview

The main goal of this project is to build a Rust implementation inspired by [RFC 9171](https://datatracker.ietf.org/doc/rfc9171/).

Each instance of the program represents an independent DTN node capable of:

- Generating and receiving bundles
- Persisting bundles locally
- Forwarding bundles opportunistically
- Handling bundle expiration
- Recovering from process restarts
- Operating under intermittent connectivity

Multiple nodes can be executed simultaneously in separate terminals or containers.

---

## Architecture

### Layered Node Architecture

<img width="2720" height="2320" alt="whatspace_dtn_architecture" src="https://github.com/user-attachments/assets/b4f1c5f8-c2e0-43de-a677-df6c4f8fd3be" />

Each node is structured into the following modules:

- **Application Layer**: handles user commands and display only. It has no knowledge of routing or network internals.
- **Bundle Layer**: the core DTN logic. It manages bundle lifecycle, expiration, ACK handling, persistent storage, deduplication, and routing decisions. It never touches transport directly.
- **Convergence Layer**: the transport adapter. It serializes bundles for sending and deserializes incoming bytes back into bundles. It decouples the bundle layer from any specific transport.
- **Transport Layer**: raw network I/O, currently intended as UDP via Tokio.

Runtime ownership follows the same layering: a `Node` owns its `BundleLayer` and convergence layer, while a convergence-layer implementation owns or uses the transport below it.

---

### Distributed Architecture

Each node maintains:

- Its own process
- Its own local storage
- Its own routing logic

Nodes communicate through convergence-layer connections.
There is no shared database between nodes, preserving the distributed nature of the system.

---

## Features

### Bundle Management

Each network bundle contains:

- Unique identifier
- Source node ID
- Destination node ID
- Creation timestamp
- Expiration timestamp
- Typed payload

Bundles are transferable network data. Local lifecycle state is stored separately through `StoredBundle`, so one node can track a bundle as pending, in transit, delivered, or expired without putting that local state into the network bundle itself.

The current payload model supports:

- User messages
- ACKs referencing the original bundle ID
- Summary-vector requests
- Summary vectors containing known bundle IDs

---

### Persistent Storage

- Local structured storage
- Duplicate detection
- Automatic removal of expired bundles
- Local status tracking through `StoredBundle`
- State recovery after node restart

Each node maintains independent persistent storage.

---

### Routing Logic

- Store-Carry-Forward mechanism
- Epidemic routing (simplified)
- Peer inventory synchronization
- Duplicate forwarding prevention
- Delivery confirmation handling through ACK payloads

---

### Convergence Layer

- CLA trait abstracts transport concerns
- Current intended implementation: UDP via Tokio
- New transports can be added without touching bundle or routing logic

### Command Line Interface

Available commands:

- `send`: create and send a bundle
- `list`: list locally stored bundles
- `peers`: display configured peers
- `status`: display node state

---

## Project Structure

The project follows a modular architecture where each feature is isolated in its own module.

```text
WhatSpace/
├── src/
│   ├── main.rs
│   ├── model.rs              <- Node and endpoint structs
│   ├── bundle/
│   │   ├── model.rs          <- Bundle, payload, stored bundle, bundle layer
│   │   ├── bundle_layer.rs   <- bundle layer orchestration
│   │   ├── bundle_manager.rs <- lifecycle: create, expiration, ACK
│   │   ├── routing.rs        <- epidemic routing decisions
│   │   └── storage.rs        <- persist, dedup, expiry, local status
│   └── cla/
│       └── bundle.proto      <- bundle wire schema
├── scripts/
│   └── test_ack_flow.sh
├── Cargo.toml
└── README.md
```

---

## Running the Project

### 1. Build

```bash
cargo build
```

### 2. Run

```bash
cargo run
```

---

## License

This project is licensed under the MIT License.

See the [LICENSE](LICENSE) file for details.

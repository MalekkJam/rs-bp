# whatspace

Whatspace is a minimal Delay-Tolerant Networking (DTN) middleware implemented in Rust.  
It simulates distributed DTN nodes based on the Store–Carry–Forward model.

The project focuses on modular system design, persistent storage, asynchronous networking, and distributed coordination between independent nodes.

---

## Overview

Each instance of the program represents an independent DTN node capable of:

- Generating and receiving bundles
- Persisting bundles locally
- Forwarding bundles opportunistically
- Handling TTL expiration
- Recovering from process restarts
- Operating under intermittent connectivity

Multiple nodes can be executed simultaneously in separate terminals or containers.

---

## Architecture

### Layered Node Architecture

<img width="2720" height="2320" alt="whatspace_dtn_architecture" src="https://github.com/user-attachments/assets/b4f1c5f8-c2e0-43de-a677-df6c4f8fd3be" />

Each node is structured into the following modules:

- **Application Layer** : Handles user commands and display only. Has no knowledge of routing or network internals.

- **Bundle Layer** : the core DTN logic. Manages bundle lifecycle (creation, TTL, ACK), persistent storage, deduplication, and routing decisions. Never touches TCP directly.

- **Convergence Layer** : the transport adapter. Serializes bundles for sending and deserializes incoming bytes back into bundles. Decouples the bundle layer from any specific transport. Adding a new transport (Bluetooth, UDP) only requires a new CLA implementation.

- **Transport Layer** : raw TCP via Tokio.

---

### Distributed Architecture

<img width="810" height="300" alt="Blank diagram (2)" src="https://github.com/user-attachments/assets/a27de688-217d-46df-b56f-47515d6e6101" />

Each node maintains:

- Its own process
- Its own local storage
- Its own routing logic

Nodes communicate exclusively via TCP connections.  
There is no shared database between nodes, preserving the distributed nature of the system.

---

## Features

### Bundle Management

Each bundle contains:

- Unique identifier
- Source node
- Destination node
- Timestamp
- TTL (Time To Live)
- Payload

Bundles are serialized using Serde + Protobuf and stored locally per node.

---

### Persistent Storage

- Local structured storage
- Duplicate detection
- Automatic removal of expired bundles
- State recovery after node restart

Each node maintains independent persistent storage.

---

### Routing Logic

- Store–Carry–Forward mechanism
- Epidemic routing (simplified)
- Peer inventory synchronization
- Duplicate forwarding prevention
- Delivery confirmation handling (ACK)

---

### Convergence Layer

- CLA trait abstracts all transport concerns
- Current implementation: TCP via Tokio
- New transports can be added without touching bundle or routing logic

### Command Line Interface

Available commands:

- `send` – create and send a bundle
- `list` – list locally stored bundles
- `peers` – display configured peers
- `status` – display node state

---

## Project Structure

The project follows a modular architecture where each feature is isolated in its own module.

```
WhatSpace/
├── src/
│   ├── main.rs
│   ├── cli/
│   │   ├── mod.rs
│   │   ├── cli.rs
│   │   └── handlers.rs
│   ├── bundle/
│   │   ├── mod.rs
│   │   ├── model.rs          ← Bundle struct, fields, serialization
│   │   ├── bundle_manager.rs ← lifecycle: create, TTL, ACK
│   │   └── storage.rs        ← persist, dedup, expiry
│   ├── routing/
│   │   ├── mod.rs
│   │   ├── engine.rs         ← orchestrates routing decisions
│   │   ├── epidemic.rs       ← epidemic routing strategy
│   │   └── scf.rs            ← store-carry-forward strategy
│   └── cla/
│       ├── mod.rs            ← CLA trait definition
│       └── tcp.rs            ← TCP implementation of the CLA
├── scripts/
│   └── test_ack_flow.sh
├── tests/
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

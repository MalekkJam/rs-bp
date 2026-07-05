# rs-bp documentation

This folder contains layer-by-layer documentation for the current `rs-bp` MVP.
Each layer document is split into:

- Functional documentation: what the layer does from the application's point of view.
- Technical documentation: how the layer is implemented in the Rust codebase.

## Layers

| Layer | Documentation | Main source files |
| --- | --- | --- |
| Application runtime | [application-runtime.md](application-runtime.md) | `src/main.rs` |
| Bundle layer | [bundle-layer.md](bundle-layer.md) | `src/bundle/model.rs`, `src/bundle/bundle_manager.rs`, `src/bundle/routing.rs` |
| Convergence layer adapter | [convergence-layer.md](convergence-layer.md) | `src/cla/cla_udp.rs`, `src/cla/protobuf.rs`, `src/cla/bundle.proto` |
| Transport layer | [transport-layer.md](transport-layer.md) | `src/transport/udp.rs`, `src/transport/mod.rs` |
| Persistence layer | [persistence-layer.md](persistence-layer.md) | `src/main.rs`, `src/bundle/storage.rs` |
| Node model | [node-model.md](node-model.md) | `src/model.rs` |

## High-level flow

```text
User command
    |
    v
Application runtime
    |
    v
Bundle layer
    |
    v
Convergence layer adapter
    |
    v
Transport layer
    |
    v
UDP network
```

For the current MVP, the application runtime owns most orchestration:

- reading CLI commands;
- creating bundles;
- saving pending bundles;
- sending bundles through UDP;
- receiving bundles;
- sending acknowledgements;
- retrying pending bundles;
- deleting acknowledged or expired pending bundles.

Some planned abstractions already exist under `src/bundle/storage.rs`,
`src/bundle/bundle_layer.rs`, and `src/model.rs`, but they are not part of the
exported `lib.rs` module tree used by the current runtime.


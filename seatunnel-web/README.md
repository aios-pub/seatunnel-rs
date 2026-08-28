# seatunnel-web

Web management console for SeaTunnel clusters, split into two independent
crates — which is why this directory contains two `src/` folders:

```
seatunnel-web/
├── server/           crate `seatunnel-web`: axum REST server + binary.
│   └── src/          Compiles natively; embeds ../ui/dist via rust-embed
│                     and talks to the cluster master over gRPC.
└── ui/               crate `seatunnel-web-ui`: Leptos 0.8 CSR frontend.
    ├── src/          Compiles to WebAssembly via trunk (standalone crate,
    ├── dist/         deliberately NOT a workspace member; dist/ is the
    └── style.css     committed trunk build output).
```

The split is required, not stylistic: the frontend targets
`wasm32-unknown-unknown` with different dependencies (leptos, gloo) and its
own lockfile, while the server targets the host platform. Keeping the UI
crate out of the Cargo workspace means plain `cargo build` never needs the
wasm toolchain.

Common commands (from the repository root):

```bash
cargo build -p seatunnel-web      # server binary, embeds ui/dist as-is
cargo test -p seatunnel-web       # server unit tests
trunk serve                       # UI hot-reload dev loop (run in ui/)
trunk build --release             # regenerate ui/dist, then rebuild the server
```

# SeaTunnel Rust Quick Start

## Installation

```bash
cargo build --release
cp target/release/seatunnel /usr/local/bin/
```

## Run Local Mode

```bash
seatunnel run -c config/v2.stream.template.conf -m local
```

## Run Cluster Mode

```bash
cargo run --bin seatunnel server master --addr 0.0.0.0:5000
seatunnel run -c config/v2.stream.template.conf -m cluster -a 127.0.0.1:5000
```

## Development

```bash
cargo test --workspace
cargo bench
```

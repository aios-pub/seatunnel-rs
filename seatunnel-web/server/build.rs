/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Guards the embedded web console bundle (`../ui/dist`, the committed
//! trunk output that rust-embed packs into this crate).
//!
//! A dist produced by a bare `trunk build` carries a ~33 MB debug-profile
//! wasm; embedding it silently makes every console page load ship tens of
//! megabytes. Release builds therefore REFUSE to compile against a bundle
//! that looks like a debug build — regenerate it with
//! `scripts/build-web-ui.sh` and commit `seatunnel-web/ui/dist` first.
//! Debug builds only warn: rust-embed reads the files from disk at runtime
//! there, so nothing gets embedded anyway.

use std::path::PathBuf;

/// A bundle at or above this size cannot be a release+wasm-opt build.
const SUSPICIOUS_WASM_MB: u64 = 10;

fn main() {
    let dist = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("../ui/dist");
    // Explicit invalidation: rebuild this crate whenever the committed
    // bundle changes, so the assets are always re-embedded.
    println!("cargo:rerun-if-changed={}", dist.display());

    let Ok(files) = std::fs::read_dir(&dist) else {
        // rust-embed reports the missing folder with its own error.
        return;
    };
    // Check EVERY wasm in dist, not just the first one readdir happens to
    // yield — a stray debug artifact next to the real bundle must still be
    // caught.
    let oversized = files
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "wasm"))
        .filter_map(|path| {
            let mb = std::fs::metadata(&path).ok()?.len() / 1024 / 1024;
            (mb >= SUSPICIOUS_WASM_MB).then_some((path, mb))
        })
        .next();
    let Some((wasm, mb)) = oversized else {
        return;
    };
    let name = wasm
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("<unnamed>");
    let message = format!(
        "embedded ui bundle {name} is {mb} MB — looks like a debug-profile \
         trunk build. Regenerate it with scripts/build-web-ui.sh (trunk \
         build --release) and commit seatunnel-web/ui/dist first",
    );
    if std::env::var("PROFILE").as_deref() == Ok("release") {
        println!("cargo:error={message}");
    } else {
        println!("cargo:warning={message}");
    }
}

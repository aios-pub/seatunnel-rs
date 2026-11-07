# Agent Task Context
Project: Rust implementation of SeaTunnel, located at `/Volumes/PSSD/CodeProjects/seatunnel`
Note: This is the Rust‑based re‑implementation of the original Java Apache SeaTunnel project.

Requirements:
1. All code comments must be written in English.
2. Git commit messages must be written in English, follow conventional‑commits specification.
3. Follow Rust idioms, keep code consistent with existing project style.

## String Truncation (iron rule)

Rust `str` is UTF-8: `len()` counts **bytes**, and `&s[..n]` / `s.truncate(n)` **panic** when `n` lands inside a multi-byte character. CDC rows routinely carry Chinese text, and byte-index truncation crashed a production task exactly this way (byte 200 fell inside '刻', bytes 198..201).

1. Never slice or truncate a `str` by a byte count. Truncation must go through `seatunnel_common::truncate_chars(s, n)` — char-boundary based (`chars().take(n)`), appends `…` when it cuts, cannot panic on any input.
2. Any code that must index into a `str` by position first proves the index is a char boundary (`is_char_boundary`) or is provably operating on ASCII-only content.
3. Slicing `&[u8]` buffers or `Vec` elements is unaffected — this rule is about `str`/`String` only.

## Error Observability

Error logs must answer three questions: which `file:line` produced the error, what the underlying cause chain is, and what the stack was on a panic. The shared infrastructure lives in the `seatunnel-common` crate; follow these rules:

1. Preserve the error chain. Error types use `thiserror` and keep the underlying error reachable via `#[source]` (reference: `EngineError::Rpc`/`RpcStatus` in `seatunnel-web/server/src/engine.rs`). Do not fold a structured error into a `String` (`format!("{:?}")`, `.to_string()`) when the original can be carried; folding is acceptable only when no error object exists (failover loops, client-side message building).
2. Stamp the production site. At error propagation points (`?` conversions, `map_err`), prefer `result.located()?` from `seatunnel_common::Locate` — it captures the caller's `file:line` via `#[track_caller]`. Existing `?` points migrate incrementally; new error-producing code should stamp.
3. Log the full chain. When logging an error, print `{:?}` of the located error (rich `Debug` + location + `caused by:` chain) or `seatunnel_common::error_chain(&e)`. A bare top-level `Display` is reserved for user-facing text (HTTP bodies, API responses) — never leak locations, chains or debug dumps into those.
4. Panics go through the logger. Every binary's `main` installs `seatunnel_common::install_panic_hook()` immediately after the tracing subscriber is initialized; it logs the panic message, site and a forced backtrace to `tracing::error!`. Do not add alternative panic reporters, and do not gate backtrace capture on `RUST_BACKTRACE`.

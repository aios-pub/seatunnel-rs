/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Shared process-level observability infrastructure.
//!
//! Small, dependency-light helpers every SeaTunnel binary wires in, so a
//! failure in the logs can answer three questions: where it was produced
//! (file:line, via [`Located`]/[`Locate::located`]), what the underlying
//! cause chain is ([`error_chain`]), and — for panics — the full stack
//! trace ([`install_panic_hook`], routed through the structured logger so
//! it lands in the rolling log files like any other error).

use std::error::Error as StdError;
use std::fmt;
use std::panic::Location;

/// Route panic reports through `tracing` instead of the default stderr
/// hook: every panic is logged at error level with the panic message, the
/// panic site and a forced backtrace, so the rolling log files carry full
/// stack traces.
///
/// Purely observability — panics still unwind (or abort under
/// `panic = "abort"`) exactly as before. Nested `catch_unwind` guards
/// (e.g. the worker's per-task pipeline guard) keep working: the hook runs
/// before unwinding and preserves the backtrace a caught payload would
/// otherwise lose. Install once, after the tracing subscriber is
/// initialized.
pub fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "non-string panic payload".to_string()
        };
        let location = info
            .location()
            .map(|l| l.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        // force_capture: the log is the only trace that survives — env
        // vars (RUST_BACKTRACE) must not gate it.
        let backtrace = std::backtrace::Backtrace::force_capture();
        tracing::error!(
            "panic: {} at {}\nbacktrace:\n{}",
            message,
            location,
            backtrace
        );
    }));
}

/// Format an error with its full `source()` chain, one line per cause:
///
/// ```text
/// rpc failed: get_job_status
/// caused by: transport error
/// caused by: connection refused (os error 111)
/// ```
///
/// Printing only the top-level error (or `format!("{:?}")` of a folded
/// string) hides the underlying raw error; walking `source()` keeps every
/// cause visible in the log line.
pub fn error_chain(err: &(impl StdError + ?Sized)) -> String {
    let mut out = err.to_string();
    let mut cause = err.source();
    while let Some(e) = cause {
        out.push_str("\ncaused by: ");
        out.push_str(&e.to_string());
        cause = e.source();
    }
    out
}

/// An error stamped with the location of the code that produced it — the
/// `?`/`map_err` site captured via `#[track_caller]`.
///
/// Display stays the inner error's, so user-facing strings (HTTP error
/// bodies) are unchanged; the `Debug` impl — the `err:{:?}` log form —
/// carries the production site and the full cause chain.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Located<E> {
    location: &'static Location<'static>,
    error: E,
}

impl<E> Located<E> {
    /// Wrap `error` with the caller's location.
    #[track_caller]
    pub fn new(error: E) -> Self {
        Located {
            location: Location::caller(),
            error,
        }
    }

    /// Where this error was produced (file, line, column).
    pub fn location(&self) -> &'static Location<'static> {
        self.location
    }

    /// The wrapped error.
    pub fn inner(&self) -> &E {
        &self.error
    }

    /// Unwrap back into the original error.
    pub fn into_inner(self) -> E {
        self.error
    }
}

impl<E: fmt::Display> fmt::Display for Located<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl<E: StdError + 'static> StdError for Located<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.error.source()
    }
}

impl<E: StdError + 'static> fmt::Debug for Located<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} at {}", self.error, self.location)?;
        let mut cause = self.error.source();
        while let Some(e) = cause {
            write!(f, "\ncaused by: {e}")?;
            cause = e.source();
        }
        Ok(())
    }
}

/// `#[track_caller]` conversion helper for `Result`: `result.located()?`
/// stamps the call site onto the error before it propagates, so the log
/// shows the file:line that produced it rather than only where it was
/// finally printed. Existing `?` points keep working unchanged and can
/// migrate to `.located()?` incrementally.
///
/// ```no_run
/// # use seatunnel_common::Locate;
/// # fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let text = std::fs::read_to_string("job.conf").located()?; // carries this line's location
/// # let _ = text;
/// # Ok(())
/// # }
/// ```
pub trait Locate<T, E>: Sized {
    #[track_caller]
    fn located(self) -> Result<T, Located<E>>;
}

impl<T, E> Locate<T, E> for Result<T, E> {
    #[track_caller]
    fn located(self) -> Result<T, Located<E>> {
        match self {
            Ok(value) => Ok(value),
            Err(error) => Err(Located::new(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Leaf(&'static str);

    impl fmt::Display for Leaf {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl StdError for Leaf {}

    #[derive(Debug)]
    struct Middle {
        cause: Leaf,
    }

    impl fmt::Display for Middle {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "middle failed")
        }
    }

    impl StdError for Middle {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(&self.cause)
        }
    }

    #[test]
    fn error_chain_walks_every_cause() {
        let err = Middle {
            cause: Leaf("disk full"),
        };
        let chain = error_chain(&err);
        assert_eq!(chain, "middle failed\ncaused by: disk full");

        let chain = error_chain(&Leaf("only"));
        assert_eq!(chain, "only");
    }

    #[test]
    fn located_stamps_the_call_site() {
        let err = Err::<(), Leaf>(Leaf("boom")).located().unwrap_err();
        assert_eq!(err.location().file(), file!());
        assert_eq!(err.location().line(), line!() - 2);
        assert_eq!(err.to_string(), "boom");
    }

    #[test]
    fn located_debug_shows_error_location_and_cause_chain() {
        let err = Located::new(Middle {
            cause: Leaf("full"),
        });
        let debug = format!("{:?}", err);
        // Inner Debug (rich detail) + location, then the Display cause chain.
        assert!(debug.starts_with("Middle {"), "debug: {debug}");
        assert!(debug.contains(" at "), "debug: {debug}");
        assert!(debug.contains("lib.rs"), "debug: {debug}");
        assert!(debug.contains("caused by: full"), "debug: {debug}");
    }

    #[test]
    fn located_delegates_source_to_inner() {
        let err = Located::new(Middle {
            cause: Leaf("full"),
        });
        let source = std::error::Error::source(&err).expect("delegates source");
        assert_eq!(source.to_string(), "full");
    }

    #[test]
    fn panic_hook_is_installable() {
        // Idempotent global install; the hook only logs, so a panic later
        // in the test process still unwinds through the harness normally.
        install_panic_hook();
    }
}

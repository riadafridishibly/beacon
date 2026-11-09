//! beacon sends telemetry events to a beacon server. Events wait in an on-disk
//! queue and go out in gzipped batches.

// Apps may build with panic = "abort", where a panic here ends the process.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]
#![warn(missing_docs)]

use std::sync::{Mutex, MutexGuard, PoisonError};

mod client;
mod event;
mod queue;
mod send;

#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod tests;

pub use client::{Client, Config, Error, LogFn};
pub use event::Props;
pub use send::{Request, Response, Transport, TransportError};

/// A poisoned lock still guards usable data, and telemetry must not panic.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

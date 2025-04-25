// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use thiserror::Error;
use tokio::sync::Notify;

/// Activates a background task
///
/// For more on what this means, see the documentation at
/// `nexus/src/app/background/mod.rs`.
///
/// Activators are created with [`Activator::new()`] and then wired up to
/// specific background tasks using Nexus's `Driver::register()`.  If you call
/// `Activator::activate()` before the activator is wired up to a background
/// task, then once the Activator _is_ wired up to a task, that task will
/// immediately be activated.
///
/// Activators are designed specifically so they can be created before the
/// corresponding task has been created and then wired up with just an
/// `&Activator` (not a `&mut Activator`).  See the
/// `nexus/src/app/background/mod.rs` documentation for more on why.
#[derive(Clone)]
pub struct Activator(Arc<ActivatorInner>);

/// Shared state for an `Activator`.
struct ActivatorInner {
    pub(super) notify: Notify,
    pub(super) wired_up: AtomicBool,
}

impl Activator {
    /// Create an activator that is not yet wired up to any background task
    pub fn new() -> Activator {
        Self(Arc::new(ActivatorInner {
            notify: Notify::new(),
            wired_up: AtomicBool::new(false),
        }))
    }

    /// Activate the background task that this Activator has been wired up to
    ///
    /// If this Activator has not yet been wired up, then whenever it _is_ wired
    /// up, that task will be immediately activated.
    pub fn activate(&self) {
        self.0.notify.notify_one();
    }

    /// Sets the task as wired up.
    ///
    /// Returns an error if the task was already wired up.
    pub fn mark_wired_up(&self) -> Result<(), AlreadyWiredUpError> {
        match self.0.wired_up.compare_exchange(
            false,
            true,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(false) => Ok(()),
            Ok(true) => unreachable!(
                "on success, the return value is always \
                 the previous value (false)"
            ),
            Err(true) => Err(AlreadyWiredUpError {}),
            Err(false) => unreachable!(
                "on failure, the return value is always \
                 the previous and current value (true)"
            ),
        }
    }

    /// Blocks until the background task that this Activator has been wired up
    /// to is activated.
    ///
    /// If this Activator has not yet been wired up, then whenever it _is_ wired
    /// up, that task will be immediately activated.
    pub async fn activated(&self) {
        debug_assert!(
            self.0.wired_up.load(Ordering::SeqCst),
            "nothing should await activation from an activator that hasn't \
             been wired up"
        );
        self.0.notify.notified().await
    }
}

/// Indicates that an activator was wired up more than once.
#[derive(Debug, Error)]
#[error("activator was already wired up")]
pub struct AlreadyWiredUpError {}

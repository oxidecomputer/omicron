use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::Notify;

/// Activates a background task
///
/// See [`crate::app::background`] module-level documentation for more on what
/// that means.
///
/// Activators are created with [`Activator::new()`] and then wired up to
/// specific background tasks using [`Driver::register()`].  If you call
/// `Activator::activate()` before the activator is wired up to a background
/// task, then once the Activator _is_ wired up to a task, that task will
/// immediately be activated.
///
/// Activators are designed specifically so they can be created before the
/// corresponding task has been created and then wired up with just an
/// `&Activator` (not a `&mut Activator`).  See the [`super::init`] module-level
/// documentation for more on why.
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
    /// If this Activator has not yet been wired up with [`Driver::register()`],
    /// then whenever it _is_ wired up, that task will be immediately activated.
    pub fn activate(&self) {
        self.0.notify.notify_one();
    }

    /// Sets the task as wired up.
    ///
    /// Returns an error if the task was already wired up.
    // XXX return a proper error type here
    pub fn mark_wired_up(&self) -> Result<bool, bool> {
        self.0.wired_up.compare_exchange(
            false,
            true,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
    }

    /// Blocks until the background task that this Activator has been wired up to
    /// is activated.
    ///
    /// If this Activator has not yet been wired up with [`Driver::register()`],
    /// then whenever it _is_ wired up, that task will be immediately activated.
    pub async fn activated(&self) {
        debug_assert!(
            self.0.wired_up.load(Ordering::SeqCst),
            "nothing should await activation from an activator that hasn't \
             been wired up"
        );
        self.0.notify.notified().await
    }
}

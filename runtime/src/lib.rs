// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common Tokio runtime initialization for the Oxide control plane.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
pub use tokio::runtime::Builder;

/// Runs the provided `main` future, constructing a multi-threaded Tokio runtime
/// with the default settings.
pub fn run<T>(main: impl Future<Output = T>) -> T {
    run_builder(&mut Builder::new_multi_thread(), main)
}

pub fn run_builder<T>(
    builder: &mut Builder,
    main: impl Future<Output = T>,
) -> T {
    #[cfg(target_os = "illumos")]
    if let Err(e) = tokio_dtrace::register_hooks(&mut builder) {
        panic!("Failed to register tokio-dtrace hooks: {e}");
    }

    let rt = builder
        .enable_all()
        // Tokio's "LIFO slot optimization" will place the last task notified by
        // another task on a worker thread in a special slot that is polled
        // before any other tasks from that worker's run queue. This is intended
        // to reduce latency in message-passing systems. However, the LIFO slot
        // currently does not participate in work-stealing, meaning that it can
        // actually *increase* latency substantially when the task that caused
        // the wakeup goes CPU-bound for a long period of time. Therefore, we
        // disable this optimization until the LIFO slot is made stealable.
        //
        //  See: https://github.com/tokio-rs/tokio/issues/4941
        .disable_lifo_slot()
        // May as well include a number in worker thread names...
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let n = ATOMIC_ID.fetch_add(1, Ordering::AcqRel);
            format!("tokio-runtime-worker-{n}")
        })
        .build();
    // If we can't construct the runtime, this is invariably fatal and there
    // is no way to recover. So, let's just panic here instead of making
    // the `main` function handle both the error returned by the main future
    // *and* errors from initializing the runtime.
    match rt {
        Ok(rt) => rt.block_on(main),
        Err(e) => panic!("Failed to initialize Tokio runtime: {e}"),
    }
}

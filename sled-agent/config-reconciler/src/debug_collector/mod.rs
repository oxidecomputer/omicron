// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The Debug Collector is responsible for collecting, archiving, and managing
//! long-term storage of various debug data on the system.  For details on how
//! this works, see the [`worker`] module docs.  The docs here describe the
//! components involved in this subsystem.
//!
//! The consumer (sled agent) interacts with this subsystem in basically two
//! ways:
//!
//! - As the set of internal and external disks change, the consumer updates a
//!   pair of watch channels so that this subsystem can respond appropriately.
//!
//! - When needed, the consumer requests immediate archival of debug data on
//!   a former zone root filesystem and waits for this to finish.
//!
//! For historical reasons, the flow is a bit more complicated than you'd think.
//! It's easiest to describe it from the bottom up.
//!
//! 1. The `DebugCollectorWorker` (in worker.rs) is an actor-style struct
//!    running in its own tokio task that does virtually all of the work of this
//!    subsystem: deciding which datasets to use for what purposes, configuring
//!    dumpadm and coreadm, running savecore, archiving log files, cleaning up
//!    debug datasets, etc.
//!
//! 2. The `DebugCollector` is a client-side handle to the
//!    `DebugCollectorWorker`.  It provides a function-oriented interface to the
//!    worker that just sends messages over an `mpsc` channel to the worker and
//!    then waits for responses.  These commands do the two things mentioned
//!    above (updating the worker with new disk information and requesting
//!    archival of debug data from former zone roots).
//!
//! The consumer sets up the debug collector by invoking [`spawn()`] with the
//! watch channels that store the information about internal and external disks.
//! `spawn()` returns a [`FormerZoneRootArchiver`] that the consumer uses when
//! it wants to archive former zone root filesystems.  Internally, `spawn()`
//! creates:
//!
//!   - a `DebugCollectorTask`.  This runs in its own tokio task.  Its sole job
//!     is to notice when the watch channels containing the set of current
//!     internal and external disks change and propagate that information to the
//!     `DebugCollectorWorker` (via the `DebugCollector`).  (In the future,
//!     these watch channels could probably be plumbed directly to the worker,
//!     eliminating the `DebugCollectorTask` and its tokio task altogether.)
//!
//!   - the `DebugCollector` (see above), which itself creates the
//!     `DebugCollectorWorker` (see above).
//!
//! So the flow ends up being:
//!
//! ```text
//!   +---------------------+
//!   | sled agent at-large |
//!   +---------------------+
//!        |
//!        | either:
//!        |
//!        | 1. change to the set of internal/external disks available
//!        |    (stored in watch channels)
//!        |
//!        | 2. request to archive debug data from former zone root filesystem
//!        |
//!        v
//!   +---------------------+
//!   | DebugCollectorTask  | (running in its own tokio task)
//!   | (see task.rs)       |
//!   +---------------------+
//!        |
//!        | calls into
//!        |
//!        v
//!   +---------------------+
//!   | DebugCollector      |
//!   | (see handle.rs)     |
//!   +---------------------+
//!        |
//!        | sends message on `mpsc` channel
//!        v
//!   +----------------------+
//!   | DebugCollectorWorker | (running in its own tokio task)
//!   | (see worker.rs)      |
//!   +----------------------+
//! ```

mod file_archiver;
mod handle;
mod helpers;
mod task;
mod worker;

pub use task::FormerZoneRootArchiver;
pub(crate) use task::spawn;

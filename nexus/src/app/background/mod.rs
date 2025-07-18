// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Nexus Background Tasks
//!
//! A **background task** in Nexus is any operation that can be activated both
//! periodically and by an explicit signal.  This is aimed at RFD 373-style
//! "reliable persistent workflows", also called "reconcilers" or "controllers".
//! These are a kind of automation that examines some _current_ state, compares
//! it to some _intended_ state, and potentially takes action to try to bring
//! the current state in sync with the intended state.  Our canonical example is
//! that we want to have Nexus monitor the intended DNS configuration.  When it
//! changes, we want to propagate the new configuration to all DNS servers.  We
//! implement this with three different background tasks:
//!
//! 1. `DnsConfigWatcher` reads the DNS configuration from the database, stores
//!    it in memory, and makes it available via a `tokio::sync::watch` channel.
//! 2. `DnsServersWatcher` reads the list of DNS servers from the database,
//!    stores it in memory, and makes it available via a `tokio::sync::watch`
//!    channel.
//! 3. `DnsPropagator` uses the the watch channels provided by the other two
//!    background tasks to notice when either the DNS configuration or the list
//!    of DNS servers has changed.  It uses the latest values to make a request
//!    to each server to update its configuration.
//!
//! When Nexus changes the DNS configuration, it will update the database with
//! the new configuration and then explicitly activate the `DnsConfigWatcher`.
//! When it reads the new config, it will send it to its watch channel, and that
//! will activate the `DnsPropagator`.  If any of this fails, or if Nexus
//! crashes at any point, then the periodic activation of every background task
//! will eventually cause the latest config to be propagated to all of the
//! current servers.
//!
//! The background task framework here is pretty minimal: essentially what it
//! gives you is that you just write an idempotent function that you want to
//! happen periodically or on-demand, wrap it in an impl of `BackgroundTask`,
//! register that with the `Driver`, and you're done.  The framework will take
//! care of:
//!
//! * providing a way for Nexus at-large to activate your task
//! * activating your task periodically
//! * ensuring that the task is activated only once at a time in this Nexus
//!   (but note that it may always be running concurrently in other Nexus
//!   instances)
//! * providing basic visibility into whether the task is running, when the task
//!   last ran, etc.
//!
//! We may well want to extend the framework as we build more tasks in general
//! and reconcilers specifically.  But we should be mindful not to create
//! footguns for ourselves!  See "Design notes" below.
//!
//! ## Notes for background task implementors
//!
//! Background tasks are not necessarily just for reconcilers.  That's just the
//! design center.  The first two DNS background tasks above aren't reconcilers
//! in any non-trivial sense.
//!
//! Background task activations do not accept input, by design.  See "Design
//! notes" below.
//!
//! Generally, you probably don't want to have your background task do retries.
//! If things fail, you rely on the periodic reactivation to try again.
//!
//! ## Design notes
//!
//! The underlying design for RFD 373-style reconcilers is inspired by a few
//! related principles:
//!
//! * the principle in distributed systems of having exactly one code path to
//!   achieve a thing, and then always using that path to do that thing (as
//!   opposed to having separate paths for, say, the happy path vs. failover,
//!   and having one of those paths rarely used)
//! * the [constant-work pattern][1], which basically suggests that a system can
//!   be more robust and scalable if it's constructed in a way that always does
//!   the same amount of work.  Imagine if we made requests to the DNS servers
//!   to incrementally update their config every time the DNS data changed.
//!   This system does more work as users make more requests.  During overloads,
//!   things can fall over.  Compare with a system whose frontend merely updates
//!   the DNS configuration that _should_ exist and whose backend periodically
//!   scans the complete intended state and then sets its own state accordingly.
//!   The backend does the same amount of work no matter how many requests were
//!   made, making it more resistant to overload.  A big downside of this
//!   approach is increased latency from the user making a request to seeing it
//!   applied.  This can be mitigated (sacrificing some, but not all, of the
//!   "constant work" property) by triggering a backend scan operation when user
//!   requests complete.
//! * the design pattern in distributed systems of keeping two copies of data in
//!   sync using both event notifications (like a changelog) _and_ periodic full
//!   scans.  The hope is that a full scan never finds a change that wasn't
//!   correctly sync'd, but incorporating an occasional full scan into the
//!   design ensures that such bugs are found and their impact repaired
//!   automatically.
//!
//! [1]: https://aws.amazon.com/builders-library/reliability-and-constant-work/
//!
//! Combining these, we get a design pattern for a "reconciler" where:
//!
//! * The reconciler is activated by explicit request (when we know it has work
//!   to do) _and_ periodically (to deal with all manner of transient failures)
//! * The reconciler's activity is idempotent: given the same underlying state
//!   (e.g., database state), it always attempts to do the same thing.
//! * Each activation of the reconciler accepts no input.  That is, even when we
//!   think we know what changed, we do not use that information.  This ensures
//!   that the reconciler really is idempotent and its actions are based solely
//!   on the state that it's watching.  Put differently: having reconcilers
//!   accept an explicit hint about what changed (and then doing something
//!   differently based on that) bifurcates the code: there's the common case
//!   where that hint is available and the rarely-exercised case when it's not
//!   (e.g., because Nexus crashed and it's the subsequent periodic activation
//!   that's propagating this change).  This is what we're trying to avoid.
//! * We do allow reconcilers to be triggered by a `tokio::sync::watch` channel
//!   -- but again, not using the _data_ from that channel.  There are two big
//!   advantages here: (1) reduced latency from when a change is made to when
//!   the reconciler applies it, and (2) (arguably another way to say the same
//!   thing) we can space out the periodic activations much further, knowing
//!   that most of the time we're not increasing latency by doing this.  This
//!   compromises the "constant-work" pattern a bit: we might wind up running
//!   the reconciler more often during busy times than during idle times, and we
//!   could find that overloads something.  However, the _operation_ of the
//!   reconciler can still be constant work, and there's no more than that
//!   amount of work going on at any given time.
//!
//!   `watch` channels are a convenient primitive here because they only store
//!   one value.  With a little care, we can ensure that the writer never blocks
//!   and the readers can all see the latest value.  (By design, reconcilers
//!   generally only care about the latest state of something, not any
//!   intermediate states.)  We don't have to worry about an unbounded queue, or
//!   handling a full queue, or other forms of backpressure.

mod driver;
mod init;
mod status;
mod tasks;

pub use driver::Driver;
pub use init::BackgroundTasksData;
pub use init::BackgroundTasksInitializer;
pub(crate) use init::BackgroundTasksInternal;
pub use nexus_background_task_interface::Activator;
pub use tasks::saga_recovery::SagaRecoveryHelpers;

use futures::future::BoxFuture;
use nexus_auth::context::OpContext;

/// An operation activated both periodically and by an explicit signal
///
/// See module-level documentation for details.
pub trait BackgroundTask: Send + Sync {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value>;
}

/// Identifies a background task
///
/// This is returned by [`Driver::register()`] to identify the corresponding
/// background task.  It's then accepted by functions like
/// [`Driver::activate()`] and [`Driver::task_status()`] to identify the task.
#[derive(Clone, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct TaskName(String);

impl TaskName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[usdt::provider(provider = "nexus")]
mod probes {
    /// Fires just before explicitly activating the named background task.
    fn background__task__activate(task_name: &str) {}

    /// Fires just before running the activate method of the named task.
    fn background__task__activate__start(
        task_name: &str,
        iteration: u64,
        reason: &str,
    ) {
    }

    /// Fires just after completing the task, with the result as JSON.
    fn background__task__activate__done(
        task_name: &str,
        iteration: u64,
        details: &str,
    ) {
    }
}

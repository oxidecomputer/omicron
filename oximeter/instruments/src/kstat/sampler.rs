// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generate oximeter samples from kernel statistics.

use super::Timestamp;
use super::now;
use crate::kstat::Error;
use crate::kstat::Expiration;
use crate::kstat::ExpirationReason;
use crate::kstat::KstatTarget;
use crate::kstat::hrtime_to_utc;
use chrono::DateTime;
use chrono::Utc;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use kstat_rs::Ctl;
use kstat_rs::Kstat;
use oximeter::Metric;
use oximeter::MetricsError;
use oximeter::Sample;
use oximeter::types::Cumulative;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::o;
use slog::trace;
use slog::warn;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map::Entry;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Sleep;
use tokio::time::interval;
use tokio::time::sleep;

// The `KstatSampler` generates some statistics about its own operation, mostly
// for surfacing failures to collect and dropped samples.
mod self_stats {
    oximeter::use_timeseries!("kstat-sampler.toml");
    use super::BTreeMap;
    use super::Cumulative;
    use super::TargetId;
    pub use kstat_sampler::ExpiredTargets;
    pub use kstat_sampler::KstatSampler;
    pub use kstat_sampler::SamplesDropped;

    #[derive(Debug)]
    pub struct SelfStats {
        pub target: KstatSampler,
        // We'll store it this way for quick lookups, and build the type as we
        // need it when publishing the samples, from the key and value.
        pub drops: BTreeMap<(TargetId, String), Cumulative<u64>>,
        pub expired: ExpiredTargets,
    }

    impl SelfStats {
        pub fn new(hostname: String) -> Self {
            Self {
                target: KstatSampler { hostname: hostname.into() },
                drops: BTreeMap::new(),
                expired: ExpiredTargets { datum: Cumulative::new(0) },
            }
        }
    }
}

/// An identifier for a single tracked kstat target.
///
/// This opaque identifier can be used to unregister targets from the sampler.
/// If not removed, data from the targets will be produced according to the
/// [`ExpirationBehavior`] configured for the target.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TargetId(u64);

impl fmt::Debug for TargetId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for TargetId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// When to expire kstat which can no longer be collected from.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ExpirationBehavior {
    /// Never stop attempting to produce data from this target.
    Never,
    /// Expire after a number of sequential failed collections.
    ///
    /// If the payload is 0, expire after the first failure.
    Attempts(usize),
    /// Expire after a specified period of failing to collect.
    Duration(Duration),
}

/// Details about the collection and expiration intervals for a target.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CollectionDetails {
    /// The interval on which data from the target is collected.
    pub interval: Duration,
    /// The expiration behavior, specifying how to handle situations in which we
    /// cannot collect kstat samples.
    ///
    /// Note that this includes both errors during an attempt to collect, and
    /// situations in which the target doesn't signal interest in any kstats.
    /// The latter can occur if the physical resource (such as a datalink) that
    /// underlies the kstat has disappeared, for example.
    pub expiration: ExpirationBehavior,
}

impl CollectionDetails {
    /// Return collection details with no expiration.
    pub fn never(interval: Duration) -> Self {
        Self { interval, expiration: ExpirationBehavior::Never }
    }

    /// Return collection details that expires after a number of attempts.
    pub fn attempts(interval: Duration, count: usize) -> Self {
        Self { interval, expiration: ExpirationBehavior::Attempts(count) }
    }

    /// Return collection details that expires after a duration has elapsed.
    pub fn duration(interval: Duration, duration: Duration) -> Self {
        Self { interval, expiration: ExpirationBehavior::Duration(duration) }
    }
}

/// The status of a sampled kstat-based target.
#[derive(Clone, Debug)]
pub enum TargetStatus {
    /// The target is currently being collected from normally.
    ///
    /// The timestamp of the last collection is included.
    Ok { last_collection: Option<Timestamp> },
    /// The target has been expired.
    ///
    /// The details about the expiration are included.
    Expired {
        reason: ExpirationReason,
        // NOTE: The error is a string, because it's not cloneable.
        error: String,
        expired_at: Timestamp,
    },
}

/// A request sent from `KstatSampler` to the worker task.
// NOTE: The docstrings here are public for ease of consumption by IDEs and
// other tooling.
#[derive(Debug)]
enum Request {
    /// Add a new target for sampling.
    AddTarget {
        target: Box<dyn KstatTarget>,
        details: CollectionDetails,
        reply_tx: oneshot::Sender<Result<TargetId, Error>>,
    },
    /// Request the status for a target
    TargetStatus {
        id: TargetId,
        reply_tx: oneshot::Sender<Result<TargetStatus, Error>>,
    },
    /// Update a target.
    UpdateTarget {
        target: Box<dyn KstatTarget>,
        details: CollectionDetails,
        reply_tx: oneshot::Sender<Result<TargetId, Error>>,
    },
    /// Remove a target.
    RemoveTarget { id: TargetId, reply_tx: oneshot::Sender<Result<(), Error>> },
    /// Return the creation times of all tracked / extant kstats.
    #[cfg(all(test, target_os = "illumos"))]
    CreationTimes {
        reply_tx: oneshot::Sender<BTreeMap<KstatPath, DateTime<Utc>>>,
    },
}

/// Data about a single kstat target.
#[derive(Debug)]
struct SampledKstat {
    /// The target from which to collect.
    target: Box<dyn KstatTarget>,
    /// The details around collection and expiration behavior.
    details: CollectionDetails,
    /// The time at which we _added_ this target to the sampler.
    time_added: Timestamp,
    /// The last time we successfully collected from the target.
    time_of_last_collection: Option<Timestamp>,
    /// Attempts since we last successfully collected from the target.
    attempts_since_last_collection: usize,
}

/// Represents the current state of a registered target.
///
/// We use this to report the status of a kstat target, such as reporting if its
/// been expired.
#[derive(Debug)]
enum SampledObject {
    Kstat(SampledKstat),
    Expired(Expiration),
}

/// Helper to hash a target, used for creating unique IDs for them.
fn hash_target(t: &dyn KstatTarget) -> TargetId {
    use std::hash::Hash;
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    t.name().hash(&mut hasher);
    for f in t.fields() {
        f.hash(&mut hasher);
    }
    TargetId(hasher.finish())
}

/// Small future that yields an ID for a target to be sampled after its
/// predefined interval expires.
struct YieldIdAfter {
    /// Future to which we delegate to awake us after our interval.
    sleep: Pin<Box<Sleep>>,
    /// The next interval to yield when we complete.
    interval: Duration,
    /// The ID of the target to yield when we complete.
    id: TargetId,
}

impl std::fmt::Debug for YieldIdAfter {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("YieldIdAfter")
            .field("sleep", &"_")
            .field("interval", &self.interval)
            .field("id", &self.id)
            .finish()
    }
}

impl YieldIdAfter {
    fn new(id: TargetId, interval: Duration) -> Self {
        Self { sleep: Box::pin(sleep(interval)), interval, id }
    }
}

impl core::future::Future for YieldIdAfter {
    type Output = (TargetId, Duration);

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        match self.sleep.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready((self.id, self.interval)),
        }
    }
}

// The operation we want to take on a future in our set, after handling an inbox
// message.
enum Operation {
    // We want to add a new future.
    Add(YieldIdAfter),
    // Remove a future with the existing ID.
    Remove(TargetId),
    // We want to update an existing future.
    Update((TargetId, Duration)),
}

/// An owned type used to keep track of the creation time for each kstat in
/// which interest has been signaled.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct KstatPath {
    pub module: String,
    pub instance: i32,
    pub name: String,
}

impl<'a> From<Kstat<'a>> for KstatPath {
    fn from(k: Kstat<'a>) -> Self {
        Self {
            module: k.ks_module.to_string(),
            instance: k.ks_instance,
            name: k.ks_name.to_string(),
        }
    }
}

/// The interval on which we prune creation times.
///
/// As targets are added and kstats are tracked, we store their creation times
/// in the `KstatSamplerWorker::creation_times` mapping. This lets us keep a
/// consistent start time, which is important for cumulative metrics. In order
/// to keep these consistent, we don't necessarily prune them as a target is
/// removed or interest in the kstat changes, since the target may be added
/// again. Instead, we prune the creation times only when the _kstat itself_ is
/// removed from the kstat chain.
pub(crate) const CREATION_TIME_PRUNE_INTERVAL: Duration =
    Duration::from_secs(60);

/// Type which owns the `kstat` chain and samples each target on an interval.
///
/// This type runs in a separate tokio task. As targets are added, it schedules
/// futures which expire after the target's sampling interval, yielding the
/// target's ID. (This is the `YieldIdAfter` type.) When those futures complete,
/// this type then samples the kstats they indicate, and push those onto a
/// per-target queue of samples.
#[derive(Debug)]
struct KstatSamplerWorker {
    log: Logger,

    /// The kstat chain.
    ctl: Option<Ctl>,

    /// The set of registered targets to collect kstats from, ordered by their
    /// IDs.
    targets: BTreeMap<TargetId, SampledObject>,

    /// The set of creation times for all tracked kstats.
    ///
    /// As interest in kstats is noted, we add a creation time for those kstats
    /// here. It is removed only when the kstat itself no longer appears on the
    /// kstat chain, even if a caller removes the target or no longer signals
    /// interest in it.
    creation_times: BTreeMap<KstatPath, DateTime<Utc>>,

    /// The per-target queue of samples, pulled by the main `KstatSampler` type
    /// when producing metrics.
    samples: Arc<Mutex<BTreeMap<TargetId, Vec<Sample>>>>,

    /// Inbox channel on which the `KstatSampler` sends messages.
    inbox: mpsc::Receiver<Request>,

    /// The maximum number of samples we allow in each per-target buffer, to
    /// avoid huge allocations when we produce data faster than it's collected.
    sample_limit: usize,

    /// Outbound queue on which to publish self statistics, which are expected to
    /// be low-volume.
    self_stat_queue: broadcast::Sender<Sample>,

    /// The statistics we maintain about ourselves.
    ///
    /// This is an option, since it's possible we fail to extract the hostname
    /// at construction time. In that case, we'll try again the next time we
    /// need it.
    self_stats: Option<self_stats::SelfStats>,
}

fn hostname() -> Option<String> {
    let out =
        std::process::Command::new("hostname").env_clear().output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Stores the number of samples taken, used for testing.
#[cfg(all(test, target_os = "illumos"))]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SampleCounts {
    pub total: usize,
    pub overflow: usize,
}

#[cfg(all(test, target_os = "illumos"))]
impl std::ops::AddAssign for SampleCounts {
    fn add_assign(&mut self, rhs: Self) {
        self.total += rhs.total;
        self.overflow += rhs.overflow;
    }
}

impl KstatSamplerWorker {
    /// Create a new sampler worker.
    fn new(
        log: Logger,
        inbox: mpsc::Receiver<Request>,
        self_stat_queue: broadcast::Sender<Sample>,
        samples: Arc<Mutex<BTreeMap<TargetId, Vec<Sample>>>>,
        sample_limit: usize,
    ) -> Result<Self, Error> {
        let ctl = Some(Ctl::new().map_err(Error::Kstat)?);
        let self_stats = hostname().map(self_stats::SelfStats::new);
        Ok(Self {
            ctl,
            log,
            targets: BTreeMap::new(),
            creation_times: BTreeMap::new(),
            samples,
            inbox,
            sample_limit,
            self_stat_queue,
            self_stats,
        })
    }

    /// Consume self and run its main polling loop.
    ///
    /// This will accept messages on its inbox, and also sample the registered
    /// kstats at their intervals. Samples will be pushed onto the queue.
    async fn run(
        mut self,
        #[cfg(all(test, target_os = "illumos"))]
        sample_count_tx: mpsc::UnboundedSender<SampleCounts>,
    ) {
        let mut sample_timeouts = FuturesUnordered::new();
        let mut creation_prune_interval =
            interval(CREATION_TIME_PRUNE_INTERVAL);
        creation_prune_interval.tick().await; // Completes immediately.
        loop {
            tokio::select! {
                _ = creation_prune_interval.tick() => {
                    if let Err(e) = self.prune_creation_times() {
                        error!(
                            self.log,
                            "failed to prune creation times";
                            "error" => ?e,
                        );
                    }
                }
                maybe_id = sample_timeouts.next(), if !sample_timeouts.is_empty() => {
                    let Some((id, interval)) = maybe_id else {
                        unreachable!();
                    };
                    if let Some(next_timeout) = self.sample_next_target(
                        id,
                        interval,
                        #[cfg(all(test, target_os = "illumos"))]
                        &sample_count_tx,
                    ) {
                        sample_timeouts.push(next_timeout);
                    }
                }
                maybe_request = self.inbox.recv() => {
                    let Some(request) = maybe_request else {
                        debug!(self.log, "inbox returned None, exiting");
                        return;
                    };
                    trace!(
                        self.log,
                        "received request on inbox";
                        "request" => ?request,
                    );
                    if let Some(next_op) = self.handle_inbox_request(request) {
                        self.update_sample_timeouts(&mut sample_timeouts, next_op);
                    }
                }
            }
        }
    }

    fn update_sample_timeouts(
        &self,
        sample_timeouts: &mut FuturesUnordered<YieldIdAfter>,
        next_op: Operation,
    ) {
        match next_op {
            Operation::Add(fut) => sample_timeouts.push(fut),
            Operation::Remove(id) => {
                // Swap out all futures, and then filter out the one we're now
                // removing.
                let old = std::mem::take(sample_timeouts);
                sample_timeouts
                    .extend(old.into_iter().filter(|fut| fut.id != id));
            }
            Operation::Update((new_id, new_interval)) => {
                // Update just the one future, if it exists, or insert one.
                //
                // NOTE: we update the _interval_, not the sleep object itself,
                // which means this won't take effect until the next tick.
                match sample_timeouts.iter_mut().find(|fut| fut.id == new_id) {
                    Some(old) => old.interval = new_interval,
                    None => {
                        warn!(
                            &self.log,
                            "attempting to update the samping future \
                            for a target, but no active future found \
                            in the set, it will be added directly";
                            "id" => %&new_id,
                        );
                        sample_timeouts
                            .push(YieldIdAfter::new(new_id, new_interval));
                    }
                }
            }
        }
    }

    // Handle a message on the worker's inbox.
    fn handle_inbox_request(&mut self, request: Request) -> Option<Operation> {
        match request {
            Request::AddTarget { target, details, reply_tx } => {
                match self.add_target(target, details) {
                    Ok(id) => {
                        trace!(
                            self.log,
                            "added target with timeout";
                            "id" => ?id,
                            "details" => ?details,
                        );
                        match reply_tx.send(Ok(id)) {
                            Ok(_) => trace!(self.log, "sent reply"),
                            Err(e) => error!(
                                self.log,
                                "failed to send reply";
                                "id" => ?id,
                                "error" => ?e,
                            ),
                        }
                        Some(Operation::Add(YieldIdAfter::new(
                            id,
                            details.interval,
                        )))
                    }
                    Err(e) => {
                        error!(
                            self.log,
                            "failed to add target";
                            "error" => ?e,
                        );
                        match reply_tx.send(Err(e)) {
                            Ok(_) => trace!(self.log, "sent reply"),
                            Err(e) => error!(
                                self.log,
                                "failed to send reply";
                                "error" => ?e,
                            ),
                        }
                        None
                    }
                }
            }
            Request::UpdateTarget { target, details, reply_tx } => {
                match self.update_target(target, details) {
                    Ok(id) => {
                        trace!(
                            self.log,
                            "updated target with timeout";
                            "id" => ?id,
                            "details" => ?details,
                        );
                        match reply_tx.send(Ok(id)) {
                            Ok(_) => trace!(self.log, "sent reply"),
                            Err(e) => error!(
                                self.log,
                                "failed to send reply";
                                "id" => ?id,
                                "error" => ?e,
                            ),
                        }
                        Some(Operation::Update((id, details.interval)))
                    }
                    Err(e) => {
                        error!(
                            self.log,
                            "failed to update target";
                            "error" => ?e,
                        );
                        match reply_tx.send(Err(e)) {
                            Ok(_) => trace!(self.log, "sent reply"),
                            Err(e) => error!(
                                self.log,
                                "failed to send reply";
                                "error" => ?e,
                            ),
                        }
                        None
                    }
                }
            }
            Request::RemoveTarget { id, reply_tx } => {
                let do_remove = self.targets.remove(&id).is_some();
                if let Some(remaining_samples) =
                    self.samples.lock().unwrap().remove(&id)
                {
                    if !remaining_samples.is_empty() {
                        warn!(
                            self.log,
                            "target removed with queued samples";
                            "id" => ?id,
                            "n_samples" => remaining_samples.len(),
                        );
                    }
                }
                match reply_tx.send(Ok(())) {
                    Ok(_) => trace!(self.log, "sent reply"),
                    Err(e) => error!(
                        self.log,
                        "failed to send reply";
                        "error" => ?e,
                    ),
                }
                if do_remove { Some(Operation::Remove(id)) } else { None }
            }
            Request::TargetStatus { id, reply_tx } => {
                trace!(
                    self.log,
                    "request for target status";
                    "id" => ?id,
                );
                let response = match self.targets.get(&id) {
                    None => Err(Error::NoSuchTarget),
                    Some(SampledObject::Kstat(k)) => Ok(TargetStatus::Ok {
                        last_collection: k.time_of_last_collection,
                    }),
                    Some(SampledObject::Expired(e)) => {
                        Ok(TargetStatus::Expired {
                            reason: e.reason,
                            error: e.error.to_string(),
                            expired_at: e.expired_at,
                        })
                    }
                };
                match reply_tx.send(response) {
                    Ok(_) => trace!(self.log, "sent reply"),
                    Err(e) => error!(
                        self.log,
                        "failed to send reply";
                        "id" => ?id,
                        "error" => ?e,
                    ),
                }
                None
            }
            #[cfg(all(test, target_os = "illumos"))]
            Request::CreationTimes { reply_tx } => {
                debug!(self.log, "request for creation times");
                reply_tx.send(self.creation_times.clone()).unwrap();
                debug!(self.log, "sent reply for creation times");
                None
            }
        }
    }

    fn sample_next_target(
        &mut self,
        id: TargetId,
        interval: Duration,
        #[cfg(all(test, target_os = "illumos"))]
        sample_count_tx: &mpsc::UnboundedSender<SampleCounts>,
    ) -> Option<YieldIdAfter> {
        match self.sample_one(id) {
            Ok(Some(samples)) => {
                if samples.is_empty() {
                    debug!(
                        self.log,
                        "no new samples from target, requeueing";
                        "id" => ?id,
                    );
                    return Some(YieldIdAfter::new(id, interval));
                }
                let n_samples = samples.len();
                debug!(
                    self.log,
                    "pulled samples from target";
                    "id" => ?id,
                    "n_samples" => n_samples,
                );

                // Append any samples to the per-target queues.
                //
                // This returns None if there was no queue, and logs
                // the error internally. We'll just go round the
                // loop again in that case.
                let Some(n_overflow_samples) =
                    self.append_per_target_samples(id, samples)
                else {
                    return None;
                };

                // Safety: We only get here if the `sample_one()`
                // method works, which means we have a record of the
                // target, and it's not expired.
                if n_overflow_samples > 0 {
                    let SampledObject::Kstat(ks) =
                        self.targets.get(&id).unwrap()
                    else {
                        unreachable!();
                    };
                    self.increment_dropped_sample_counter(
                        id,
                        ks.target.name().to_string(),
                        n_overflow_samples,
                    );
                }

                // Send the total number of samples we've actually
                // taken and the number we've appended over to any
                // testing code which might be listening.
                #[cfg(all(test, target_os = "illumos"))]
                sample_count_tx
                    .send(SampleCounts {
                        total: n_samples,
                        overflow: n_overflow_samples,
                    })
                    .unwrap();

                trace!(
                    self.log,
                    "re-queueing target for sampling";
                    "id" => ?id,
                    "interval" => ?interval,
                );
                Some(YieldIdAfter::new(id, interval))
            }
            Ok(None) => {
                debug!(
                    self.log,
                    "sample timeout triggered for non-existent target";
                    "id" => ?id,
                );
                None
            }
            Err(Error::Expired(expiration)) => {
                error!(
                    self.log,
                    "expiring kstat after too many failures";
                    "id" => ?id,
                    "reason" => ?expiration.reason,
                    "error" => ?expiration.error,
                );
                let _ =
                    self.targets.insert(id, SampledObject::Expired(expiration));
                self.increment_expired_target_counter();
                None
            }
            Err(e) => {
                error!(
                    self.log,
                    "failed to sample kstat target, requeueing";
                    "id" => ?id,
                    "error" => ?e,
                );
                Some(YieldIdAfter::new(id, interval))
            }
        }
    }

    fn append_per_target_samples(
        &self,
        id: TargetId,
        mut samples: Vec<Sample>,
    ) -> Option<usize> {
        // Limit the number of samples we actually contain
        // in the sample queue. This is to avoid huge
        // allocations when we produce lots of data, but
        // we're not polled quickly enough by oximeter.
        //
        // Note that this is a _per-target_ queue.
        let mut all_samples = self.samples.lock().unwrap();
        let Some(current_samples) = all_samples.get_mut(&id) else {
            error!(
                self.log,
                "per-target sample queue not found!";
                "id" => ?id,
            );
            return None;
        };
        let n_new_samples = samples.len();
        let n_current_samples = current_samples.len();
        let n_total_samples = n_new_samples + n_current_samples;
        let n_overflow_samples =
            n_total_samples.saturating_sub(self.sample_limit);
        if n_overflow_samples > 0 {
            warn!(
                self.log,
                "sample queue is too full, dropping oldest samples";
                "n_new_samples" => n_new_samples,
                "n_current_samples" => n_current_samples,
                "n_overflow_samples" => n_overflow_samples,
            );
            // It's possible that the number of new samples
            // is big enough to overflow the current
            // capacity, and also require removing new
            // samples.
            if n_overflow_samples < n_current_samples {
                let _ = current_samples.drain(..n_overflow_samples);
            } else {
                // Clear all the current samples, and some
                // of the new ones. The subtraction below
                // cannot panic, because
                // `n_overflow_samples` is computed above by
                // adding `n_current_samples`.
                current_samples.clear();
                let _ =
                    samples.drain(..(n_overflow_samples - n_current_samples));
            }
        }
        current_samples.extend(samples);
        Some(n_overflow_samples)
    }

    /// Add samples for one target to the internal queue.
    ///
    /// Note that this updates the kstat chain.
    fn sample_one(
        &mut self,
        id: TargetId,
    ) -> Result<Option<Vec<Sample>>, Error> {
        self.update_chain()?;
        let ctl = self.ctl.as_ref().unwrap();
        let Some(maybe_kstat) = self.targets.get_mut(&id) else {
            return Ok(None);
        };
        let SampledObject::Kstat(sampled_kstat) = maybe_kstat else {
            panic!("Should not be sampling an expired kstat");
        };

        // Fetch each interested kstat, and include the data and creation times
        // for each of them.
        let kstats = ctl
            .iter()
            .filter(|kstat| sampled_kstat.target.interested(kstat))
            .map(|mut kstat| {
                let data = ctl.read(&mut kstat).map_err(Error::Kstat)?;
                let creation_time = Self::ensure_kstat_creation_time(
                    &self.log,
                    kstat,
                    &mut self.creation_times,
                )?;
                Ok((creation_time, kstat, data))
            })
            .collect::<Result<Vec<_>, _>>();
        match kstats {
            Ok(k) if !k.is_empty() => {
                sampled_kstat.time_of_last_collection = Some(now());
                sampled_kstat.attempts_since_last_collection = 0;
                sampled_kstat.target.to_samples(&k).map(Option::Some)
            }
            other => {
                // Convert a list of zero interested kstats into an error.
                let e = match other {
                    Err(e) => e,
                    Ok(k) if k.is_empty() => {
                        trace!(
                            self.log,
                            "no matching samples for target, converting \
                            to sampling error";
                            "id" => ?id,
                        );
                        Error::NoSuchKstat
                    }
                    _ => unreachable!(),
                };
                sampled_kstat.attempts_since_last_collection += 1;

                // Check if this kstat should be expired, based on the attempts
                // we've previously made and the expiration policy.
                match sampled_kstat.details.expiration {
                    ExpirationBehavior::Never => {}
                    ExpirationBehavior::Attempts(n_attempts) => {
                        if sampled_kstat.attempts_since_last_collection
                            >= n_attempts
                        {
                            return Err(Error::Expired(Expiration {
                                reason: ExpirationReason::Attempts(n_attempts),
                                error: Box::new(e),
                                expired_at: now(),
                            }));
                        }
                    }
                    ExpirationBehavior::Duration(duration) => {
                        // Use the time of the last collection, if one exists,
                        // or the time we added the kstat if not.
                        let start = sampled_kstat
                            .time_of_last_collection
                            .unwrap_or(sampled_kstat.time_added);
                        let expire_at = start + duration;
                        let now_ = now();
                        if now_ >= expire_at {
                            return Err(Error::Expired(Expiration {
                                reason: ExpirationReason::Duration(duration),
                                error: Box::new(e),
                                expired_at: now_,
                            }));
                        }
                    }
                }

                // Do not expire the kstat, simply fail this collection.
                Err(e)
            }
        }
    }

    fn increment_dropped_sample_counter(
        &mut self,
        target_id: TargetId,
        target_name: String,
        n_overflow_samples: usize,
    ) {
        assert!(n_overflow_samples > 0);
        if let Some(stats) = self.get_or_try_build_self_stats() {
            // Get the entry for this target, or build a counter starting a 0.
            // We'll always add the number of overflow samples afterwards.
            let drops = stats
                .drops
                .entry((target_id, target_name.clone()))
                .or_default();
            *drops += n_overflow_samples as u64;
            let metric = self_stats::SamplesDropped {
                target_id: target_id.0,
                target_name: target_name.into(),
                datum: *drops,
            };
            let sample = match Sample::new(&stats.target, &metric) {
                Ok(s) => s,
                Err(e) => {
                    error!(
                        self.log,
                        "could not generate sample for dropped sample counter";
                        "error" => ?e,
                    );
                    return;
                }
            };
            match self.self_stat_queue.send(sample) {
                Ok(_) => trace!(self.log, "sent dropped sample counter stat"),
                Err(_) => error!(
                    self.log,
                    "failed to send dropped sample counter to self stat queue"
                ),
            }
        } else {
            warn!(
                self.log,
                "cannot record dropped sample statistic, failed to get hostname"
            );
        }
    }

    fn increment_expired_target_counter(&mut self) {
        if let Some(stats) = self.get_or_try_build_self_stats() {
            stats.expired.datum_mut().increment();
            let sample = match Sample::new(&stats.target, &stats.expired) {
                Ok(s) => s,
                Err(e) => {
                    error!(
                        self.log,
                        "could not generate sample for expired target counter";
                        "error" => ?e,
                    );
                    return;
                }
            };
            match self.self_stat_queue.send(sample) {
                Ok(_) => trace!(self.log, "sent expired target counter stat"),
                Err(e) => error!(
                    self.log,
                    "failed to send target counter to self stat queue";
                    "error" => ?e,
                ),
            }
        } else {
            warn!(
                self.log,
                "cannot record expiration statistic, failed to get hostname"
            );
        }
    }

    /// If we have an actual `SelfStats` struct, return it, or try to create one.
    /// We'll still return `None` in that latter case, and we fail to make one.
    fn get_or_try_build_self_stats(
        &mut self,
    ) -> Option<&mut self_stats::SelfStats> {
        if self.self_stats.is_none() {
            self.self_stats = hostname().map(self_stats::SelfStats::new);
        }
        self.self_stats.as_mut()
    }

    /// Ensure that we have recorded the creation time for all interested kstats
    /// for a new target.
    fn ensure_creation_times_for_target(
        &mut self,
        target: &dyn KstatTarget,
    ) -> Result<(), Error> {
        self.update_chain()?;
        let ctl = self.ctl.as_ref().unwrap();
        for kstat in ctl.iter().filter(|k| target.interested(k)) {
            Self::ensure_kstat_creation_time(
                &self.log,
                kstat,
                &mut self.creation_times,
            )?;
        }
        Ok(())
    }

    /// Ensure that we store the creation time for the provided kstat.
    fn ensure_kstat_creation_time(
        log: &Logger,
        kstat: Kstat,
        creation_times: &mut BTreeMap<KstatPath, DateTime<Utc>>,
    ) -> Result<DateTime<Utc>, Error> {
        let path = KstatPath::from(kstat);
        match creation_times.entry(path.clone()) {
            Entry::Occupied(entry) => {
                trace!(
                    log,
                    "creation time already exists for tracked target";
                    "path" => ?path,
                );
                Ok(*entry.get())
            }
            Entry::Vacant(entry) => {
                let creation_time = hrtime_to_utc(kstat.ks_crtime)?;
                debug!(
                    log,
                    "storing new creation time for tracked target";
                    "path" => ?path,
                    "creation_time" => ?creation_time,
                );
                entry.insert(creation_time);
                Ok(creation_time)
            }
        }
    }

    /// Prune the stored creation times, removing any that no longer have
    /// corresponding kstats on the chain.
    fn prune_creation_times(&mut self) -> Result<(), Error> {
        if self.creation_times.is_empty() {
            trace!(self.log, "no creation times to prune");
            return Ok(());
        }
        // We'll create a list of all the creation times to prune, by
        // progressively removing any _extant_ kstats from the set of keys we
        // currently have. If something is _not_ on the chain, it'll remain in
        // this map at the end of the loop below, and thus we know we need to
        // remove it.
        let mut to_remove: BTreeSet<_> =
            self.creation_times.keys().cloned().collect();

        // Iterate the chain, and remove any current keys that do _not_ appear
        // on the chain.
        self.update_chain()?;
        let ctl = self.ctl.as_ref().unwrap();
        for kstat in ctl.iter() {
            let path = KstatPath::from(kstat);
            let _ = to_remove.remove(&path);
        }

        if to_remove.is_empty() {
            trace!(self.log, "kstat creation times is already pruned");
        } else {
            debug!(
                self.log,
                "pruning creation times for kstats that are gone";
                "to_remove" => ?to_remove,
                "n_to_remove" => to_remove.len(),
            );
            self.creation_times.retain(|key, _value| !to_remove.contains(key));
        }
        Ok(())
    }

    /// Start tracking a single KstatTarget object.
    fn add_target(
        &mut self,
        target: Box<dyn KstatTarget>,
        details: CollectionDetails,
    ) -> Result<TargetId, Error> {
        let id = hash_target(&*target);
        match self.targets.get(&id) {
            Some(SampledObject::Kstat(_)) => {
                return Err(Error::DuplicateTarget {
                    target_name: target.name().to_string(),
                    fields: target
                        .field_names()
                        .iter()
                        .map(ToString::to_string)
                        .zip(target.field_values())
                        .collect(),
                });
            }
            Some(SampledObject::Expired(e)) => {
                warn!(
                    self.log,
                    "replacing expired kstat target";
                    "id" => ?id,
                    "expiration_reason" => ?e.reason,
                    "error" => ?e.error,
                    "expired_at" => ?e.expired_at,
                );
            }
            None => {}
        }

        self.insert_target(id, target, details)
    }

    fn update_target(
        &mut self,
        target: Box<dyn KstatTarget>,
        details: CollectionDetails,
    ) -> Result<TargetId, Error> {
        let id = hash_target(&*target);
        match self.targets.get(&id) {
            // If the target is already expired, we'll replace it with the new
            // target and start sampling it again.
            Some(SampledObject::Expired(e)) => {
                warn!(
                    self.log,
                    "replacing expired kstat target";
                    "id" => ?id,
                    "expiration_reason" => ?e.reason,
                    "error" => ?e.error,
                    "expired_at" => ?e.expired_at,
                );
            }
            Some(_) => {}
            None => return Err(Error::NoSuchTarget),
        }

        self.insert_target(id, target, details)
    }

    fn update_chain(&mut self) -> Result<(), Error> {
        let new_ctl = match self.ctl.take() {
            None => Ctl::new(),
            Some(old) => old.update(),
        }
        .map_err(Error::Kstat)?;
        let _ = self.ctl.insert(new_ctl);
        Ok(())
    }

    fn insert_target(
        &mut self,
        id: TargetId,
        target: Box<dyn KstatTarget>,
        details: CollectionDetails,
    ) -> Result<TargetId, Error> {
        self.ensure_creation_times_for_target(&*target)?;
        let item = SampledKstat {
            target,
            details,
            time_added: now(),
            time_of_last_collection: None,
            attempts_since_last_collection: 0,
        };
        let _ = self.targets.insert(id, SampledObject::Kstat(item));

        // Add to the per-target queues, making sure to keep any samples that
        // were already there previously. This would be a bit odd, since it
        // means that the target expired, but we hadn't been polled by oximeter.
        // Nonetheless keep these samples anyway.
        let n_samples =
            self.samples.lock().unwrap().entry(id).or_default().len();
        match n_samples {
            0 => debug!(
                self.log,
                "inserted empty per-target sample queue";
                "id" => ?id,
            ),
            n => debug!(
                self.log,
                "per-target queue appears to have old samples";
                "id" => ?id,
                "n_samples" => n,
            ),
        }

        Ok(id)
    }
}

/// A type for reporting kernel statistics as oximeter samples.
#[derive(Clone, Debug)]
pub struct KstatSampler {
    log: Logger,
    samples: Arc<Mutex<BTreeMap<TargetId, Vec<Sample>>>>,
    outbox: mpsc::Sender<Request>,
    // NOTE: We're using a broadcast MPMC channel here intentionally, even
    // though there is only one receiver. That's to make use of its ring-buffer
    // properties, where old samples are evicted if `oximeter` can't collect
    // from us quickly enough. See the discussion in
    // https://github.com/oxidecomputer/omicron/issues/7983 for more.
    self_stat_rx: Arc<Mutex<broadcast::Receiver<Sample>>>,
    _worker_task: Arc<tokio::task::JoinHandle<()>>,
    #[cfg(all(test, target_os = "illumos"))]
    sample_count_rx: Arc<Mutex<mpsc::UnboundedReceiver<SampleCounts>>>,
}

// Size of the queue used to report self-stats to oximeter on collection.
const SELF_STAT_QUEUE_SIZE: usize = 4096;

impl KstatSampler {
    /// The maximum number of samples allowed in the internal buffer, before
    /// oldest samples are dropped.
    ///
    /// This is to avoid unbounded allocations in situations where data is
    /// produced faster than it is collected.
    ///
    /// Note that this is a _per-target_ sample limit!
    pub const DEFAULT_SAMPLE_LIMIT: usize = 500;

    /// Create a new sampler.
    pub fn new(log: &Logger) -> Result<Self, Error> {
        Self::with_sample_limit(log, Self::DEFAULT_SAMPLE_LIMIT)
    }

    /// Create a new sampler with a sample limit.
    pub fn with_sample_limit(
        log: &Logger,
        limit: usize,
    ) -> Result<Self, Error> {
        let samples = Arc::new(Mutex::new(BTreeMap::new()));
        let (self_stat_tx, self_stat_rx) =
            broadcast::channel(SELF_STAT_QUEUE_SIZE);
        let (outbox, inbox) = mpsc::channel(1);
        let worker = KstatSamplerWorker::new(
            log.new(o!("component" => "kstat-sampler-worker")),
            inbox,
            self_stat_tx,
            samples.clone(),
            limit,
        )?;
        #[cfg(all(test, target_os = "illumos"))]
        let (sample_count_rx, _worker_task) = {
            let (sample_count_tx, sample_count_rx) = mpsc::unbounded_channel();
            (
                Arc::new(Mutex::new(sample_count_rx)),
                Arc::new(tokio::task::spawn(worker.run(sample_count_tx))),
            )
        };
        #[cfg(not(all(test, target_os = "illumos")))]
        let _worker_task = Arc::new(tokio::task::spawn(worker.run()));
        Ok(Self {
            log: log.new(o!("component" => "kstat-sampler")),
            samples,
            outbox,
            self_stat_rx: Arc::new(Mutex::new(self_stat_rx)),
            _worker_task,
            #[cfg(all(test, target_os = "illumos"))]
            sample_count_rx,
        })
    }

    /// Add a target, which can be used to produce zero or more samples.
    ///
    /// Note that adding a target which has previously expired is _not_ an
    /// error, and instead replaces the expired target.
    pub async fn add_target(
        &self,
        target: impl KstatTarget,
        details: CollectionDetails,
    ) -> Result<TargetId, Error> {
        let name = target.name();
        debug!(self.log, "adding new target"; "name" => %name);
        let (reply_tx, reply_rx) = oneshot::channel();
        let request =
            Request::AddTarget { target: Box::new(target), details, reply_tx };
        trace!(self.log, "sending add_target request to worker"; "name" => %name);
        self.outbox.send(request).await.map_err(|_| Error::SendError)?;
        trace!(self.log, "sent add_target request to worker"; "name" => %name);
        reply_rx.await.map_err(|_| Error::RecvError)?
    }

    /// Remove a tracked target.
    pub async fn remove_target(&self, id: TargetId) -> Result<(), Error> {
        debug!(self.log, "removing target"; "id" => %id);
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = Request::RemoveTarget { id, reply_tx };
        trace!(self.log, "sending remove_target request to worker"; "id" => %id);
        self.outbox.send(request).await.map_err(|_| Error::SendError)?;
        trace!(self.log, "sent remove_target request to worker"; "id" => %id);
        reply_rx.await.map_err(|_| Error::RecvError)?
    }

    /// Update the details for a target.
    pub async fn update_target(
        &self,
        target: impl KstatTarget,
        details: CollectionDetails,
    ) -> Result<TargetId, Error> {
        let name = target.name();
        debug!(self.log, "updating target"; "name" => %name);
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = Request::UpdateTarget {
            target: Box::new(target),
            details,
            reply_tx,
        };
        trace!(self.log, "sending update_target request to worker"; "name" => %name);
        self.outbox.send(request).await.map_err(|_| Error::SendError)?;
        trace!(self.log, "sent update_target request to worker"; "name" => %name);
        reply_rx.await.map_err(|_| Error::RecvError)?
    }

    /// Fetch the status for a target.
    ///
    /// If the target is being collected normally, then `TargetStatus::Ok` is
    /// returned, which contains the time of the last collection, if any.
    ///
    /// If the target exists, but has been expired, then the details about the
    /// expiration are returned in `TargetStatus::Expired`.
    ///
    /// If the target doesn't exist at all, then an error is returned.
    pub async fn target_status(
        &self,
        id: TargetId,
    ) -> Result<TargetStatus, Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = Request::TargetStatus { id, reply_tx };
        trace!(self.log, "sending target_request to worker"; "id" => %id);
        self.outbox.send(request).await.map_err(|_| Error::SendError)?;
        trace!(self.log, "sent target_request to worker"; "id" => %id);
        reply_rx.await.map_err(|_| Error::RecvError)?
    }

    /// Return the number of samples pushed by the sampling task, if any.
    #[cfg(all(test, target_os = "illumos"))]
    pub(crate) fn sample_counts(&self) -> Option<SampleCounts> {
        match self.sample_count_rx.lock().unwrap().try_recv() {
            Ok(c) => Some(c),
            Err(mpsc::error::TryRecvError::Empty) => None,
            _ => panic!("sample_tx disconnected"),
        }
    }

    /// Return the creation times for all tracked kstats.
    #[cfg(all(test, target_os = "illumos"))]
    pub(crate) async fn creation_times(
        &self,
    ) -> BTreeMap<KstatPath, DateTime<Utc>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = Request::CreationTimes { reply_tx };
        self.outbox.send(request).await.map_err(|_| Error::SendError).unwrap();
        reply_rx.await.map_err(|_| Error::RecvError).unwrap()
    }
}

impl oximeter::Producer for KstatSampler {
    fn produce(
        &mut self,
    ) -> Result<Box<(dyn Iterator<Item = Sample>)>, MetricsError> {
        // Swap the _entries_ of all the existing per-target sample queues, but
        // we need to leave empty queues in their place. I.e., we can't remove
        // keys.
        let mut samples = Vec::new();
        for (_id, queue) in self.samples.lock().unwrap().iter_mut() {
            samples.append(queue);
        }

        // Append any self-stat samples as well.
        let mut rx = self.self_stat_rx.lock().unwrap();
        loop {
            match rx.try_recv() {
                Ok(sample) => samples.push(sample),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(n_missed)) => {
                    warn!(
                        self.log,
                        "producer missed some samples because they're \
                        being produced faster than `oximeter` is collecting";
                        "n_missed" => %n_missed,
                    );
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    debug!(
                        self.log,
                        "kstat stampler self-stat queue tx disconnected"
                    );
                    break;
                }
            }
        }
        drop(rx);

        Ok(Box::new(samples.into_iter()))
    }
}

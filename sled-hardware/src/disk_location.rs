// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Chassis locations of disks, learned from the platform hardware topology.
//!
//! The illumos fault management topology (libtopo) knows which bay or M.2
//! socket each NVMe controller sits in and labels it the way the chassis
//! does: "N5" for a U.2 bay, "M.2 East" for a boot device. This module holds
//! the platform-independent parts of asking for that label: the key that
//! joins devinfo's view of a controller to topo's, and a cache that decides
//! when topo needs to be consulted at all.

use slog::{Logger, debug, warn};
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

/// Instance number of an `nvme` driver node, the `N` in `nvme<N>`.
///
/// devinfo reports it on the `nvme` node and topo reports it as the
/// `io/instance` property of its `nvme` node, so it is the key that joins the
/// two views of a controller. It is unrelated to the PCIe physical slot
/// number and to NVMe firmware slots.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct NvmeInstance(i32);

/// An instance number outside the range devinfo and libnvme use, which is
/// zero through `i32::MAX`.
#[derive(Debug, thiserror::Error)]
#[error("invalid nvme driver instance number: {0}")]
pub struct InvalidNvmeInstance(pub i64);

impl NvmeInstance {
    /// The instance number as devinfo and libnvme represent it.
    pub fn as_i32(self) -> i32 {
        self.0
    }
}

impl TryFrom<i32> for NvmeInstance {
    type Error = InvalidNvmeInstance;

    /// From a devinfo instance number, which is signed but never negative.
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < 0 {
            Err(InvalidNvmeInstance(value.into()))
        } else {
            Ok(Self(value))
        }
    }
}

impl TryFrom<u32> for NvmeInstance {
    type Error = InvalidNvmeInstance;

    /// From a topo `io/instance` property, which is unsigned.
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        i32::try_from(value)
            .map(Self)
            .map_err(|_| InvalidNvmeInstance(value.into()))
    }
}

impl fmt::Display for NvmeInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "nvme{}", self.0)
    }
}

/// Remembers what the platform topology said about each NVMe controller, so
/// that topo, whose snapshot takes hundreds of milliseconds on a sled, is
/// consulted only when a controller appears that it has not been asked about.
///
/// A controller's location cannot change while the system is up. Its instance
/// number is fixed by its devinfo path, and its label comes from the
/// platform's static topology map. A label, once learned, is therefore kept
/// for the life of the process.
#[derive(Debug, Default)]
pub struct DiskLocationCache {
    /// What topo said about each controller the last time it was asked.
    /// `None` means it was asked and had no label, or topo could not be read.
    /// A controller absent from the map has never been asked about.
    labels: HashMap<NvmeInstance, Option<String>>,
    /// When topo was last consulted. Throttles re-asking about `None`
    /// entries; never delays the first question about a new controller.
    last_attempt: Option<Instant>,
}

impl DiskLocationCache {
    /// Minimum time between repeated attempts to resolve a controller that
    /// topo previously had no label for.
    pub const RETRY_INTERVAL: Duration = Duration::from_secs(60);

    pub fn new() -> Self {
        Self::default()
    }

    /// Consults topo, through `read_labels`, if any controller in `present`
    /// calls for it: immediately for a controller never asked about, and at
    /// most once per [`Self::RETRY_INTERVAL`] for controllers topo previously
    /// had no label for. Afterwards every controller in `present` has an
    /// entry, so the steady state of repeated polls with the same controllers
    /// never touches topo.
    ///
    /// `read_labels` returns the label of every controller topo has one for.
    /// On success it replaces the cache wholesale. On failure the labels
    /// already known are kept and the failure is logged.
    pub fn refresh_if_needed<F, E>(
        &mut self,
        log: &Logger,
        present: &[NvmeInstance],
        now: Instant,
        read_labels: F,
    ) where
        F: FnOnce() -> Result<HashMap<NvmeInstance, String>, E>,
        E: fmt::Display,
    {
        if !self.should_refresh(present, now) {
            return;
        }
        self.last_attempt = Some(now);

        let read_ok = match read_labels() {
            Ok(labels) => {
                debug!(
                    log,
                    "read disk locations from hardware topology";
                    "count" => labels.len(),
                );
                self.labels =
                    labels.into_iter().map(|(i, l)| (i, Some(l))).collect();
                true
            }
            Err(err) => {
                warn!(
                    log,
                    "failed to read disk locations from hardware topology";
                    "err" => %err,
                );
                false
            }
        };

        for instance in present {
            let entry = self.labels.entry(*instance).or_insert(None);
            if entry.is_none() && read_ok {
                warn!(
                    log,
                    "hardware topology has no location label for disk \
                     controller";
                    "nvme_instance" => %instance,
                );
            }
        }
    }

    fn should_refresh(&self, present: &[NvmeInstance], now: Instant) -> bool {
        let mut retry_unlabelled = false;
        for instance in present {
            match self.labels.get(instance) {
                None => return true,
                Some(None) => retry_unlabelled = true,
                Some(Some(_)) => {}
            }
        }
        retry_unlabelled
            && self.last_attempt.is_none_or(|last| {
                now.duration_since(last) >= Self::RETRY_INTERVAL
            })
    }

    /// The chassis location label of a controller, if topo had one.
    pub fn location(&self, instance: NvmeInstance) -> Option<&str> {
        self.labels.get(&instance).and_then(|label| label.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_test_utils::dev::test_setup_log;
    use std::cell::Cell;

    fn inst(n: i32) -> NvmeInstance {
        NvmeInstance::try_from(n).unwrap()
    }

    fn labels(pairs: &[(i32, &str)]) -> HashMap<NvmeInstance, String> {
        pairs.iter().map(|(n, l)| (inst(*n), l.to_string())).collect()
    }

    #[test]
    fn nvme_instance_conversions() {
        assert_eq!(inst(0).as_i32(), 0);
        assert_eq!(inst(17).as_i32(), 17);
        assert!(NvmeInstance::try_from(-1i32).is_err());
        assert_eq!(NvmeInstance::try_from(3u32).unwrap(), inst(3));
        assert!(NvmeInstance::try_from(u32::MAX).is_err());
        assert_eq!(inst(5).to_string(), "nvme5");
    }

    #[test]
    fn first_sight_consults_topo_then_steady_state_does_not() {
        let logctx = test_setup_log(
            "first_sight_consults_topo_then_steady_state_does_not",
        );
        let mut cache = DiskLocationCache::new();
        let calls = Cell::new(0);
        let t0 = Instant::now();
        let present = [inst(0), inst(1)];

        cache.refresh_if_needed(&logctx.log, &present, t0, || {
            calls.set(calls.get() + 1);
            Ok::<_, String>(labels(&[(0, "N0"), (1, "N1")]))
        });
        assert_eq!(calls.get(), 1);
        assert_eq!(cache.location(inst(0)), Some("N0"));
        assert_eq!(cache.location(inst(1)), Some("N1"));

        // Same controllers, an hour later: nothing to ask.
        let later = t0 + Duration::from_secs(3600);
        cache.refresh_if_needed(&logctx.log, &present, later, || {
            calls.set(calls.get() + 1);
            Ok::<_, String>(HashMap::new())
        });
        assert_eq!(calls.get(), 1);
        assert_eq!(cache.location(inst(0)), Some("N0"));
        logctx.cleanup_successful();
    }

    #[test]
    fn unlabelled_controller_is_retried_with_throttle() {
        let logctx =
            test_setup_log("unlabelled_controller_is_retried_with_throttle");
        let mut cache = DiskLocationCache::new();
        let calls = Cell::new(0);
        let t0 = Instant::now();
        let present = [inst(0)];

        // Topo knows nothing about this controller.
        cache.refresh_if_needed(&logctx.log, &present, t0, || {
            calls.set(calls.get() + 1);
            Ok::<_, String>(HashMap::new())
        });
        assert_eq!(calls.get(), 1);
        assert_eq!(cache.location(inst(0)), None);

        // Too soon to ask again.
        let soon = t0 + Duration::from_secs(30);
        cache.refresh_if_needed(&logctx.log, &present, soon, || {
            calls.set(calls.get() + 1);
            Ok::<_, String>(labels(&[(0, "N0")]))
        });
        assert_eq!(calls.get(), 1);
        assert_eq!(cache.location(inst(0)), None);

        // After the retry interval the question is asked again.
        let retry = t0 + DiskLocationCache::RETRY_INTERVAL;
        cache.refresh_if_needed(&logctx.log, &present, retry, || {
            calls.set(calls.get() + 1);
            Ok::<_, String>(labels(&[(0, "N0")]))
        });
        assert_eq!(calls.get(), 2);
        assert_eq!(cache.location(inst(0)), Some("N0"));
        logctx.cleanup_successful();
    }

    #[test]
    fn new_controller_bypasses_throttle() {
        let logctx = test_setup_log("new_controller_bypasses_throttle");
        let mut cache = DiskLocationCache::new();
        let calls = Cell::new(0);
        let t0 = Instant::now();

        cache.refresh_if_needed(&logctx.log, &[inst(0)], t0, || {
            calls.set(calls.get() + 1);
            Ok::<_, String>(HashMap::new())
        });
        assert_eq!(calls.get(), 1);

        // A controller never seen before shows up well inside the retry
        // interval; it is asked about right away.
        let soon = t0 + Duration::from_secs(5);
        cache.refresh_if_needed(&logctx.log, &[inst(0), inst(1)], soon, || {
            calls.set(calls.get() + 1);
            Ok::<_, String>(labels(&[(1, "N1")]))
        });
        assert_eq!(calls.get(), 2);
        assert_eq!(cache.location(inst(0)), None);
        assert_eq!(cache.location(inst(1)), Some("N1"));
        logctx.cleanup_successful();
    }

    #[test]
    fn topo_failure_keeps_known_labels_and_is_throttled() {
        let logctx =
            test_setup_log("topo_failure_keeps_known_labels_and_is_throttled");
        let mut cache = DiskLocationCache::new();
        let calls = Cell::new(0);
        let t0 = Instant::now();

        cache.refresh_if_needed(&logctx.log, &[inst(0)], t0, || {
            calls.set(calls.get() + 1);
            Ok::<_, String>(labels(&[(0, "N0")]))
        });
        assert_eq!(calls.get(), 1);

        // A new controller appears but topo is broken.
        let t1 = t0 + Duration::from_secs(1);
        let present = [inst(0), inst(1)];
        cache.refresh_if_needed(&logctx.log, &present, t1, || {
            calls.set(calls.get() + 1);
            Err::<HashMap<NvmeInstance, String>, _>("topo exploded")
        });
        assert_eq!(calls.get(), 2);
        assert_eq!(cache.location(inst(0)), Some("N0"));
        assert_eq!(cache.location(inst(1)), None);

        // The failure is throttled like any other unresolved controller.
        let t2 = t1 + Duration::from_secs(2);
        cache.refresh_if_needed(&logctx.log, &present, t2, || {
            calls.set(calls.get() + 1);
            Ok::<_, String>(labels(&[(0, "N0"), (1, "N1")]))
        });
        assert_eq!(calls.get(), 2);
        assert_eq!(cache.location(inst(1)), None);

        let t3 = t1 + DiskLocationCache::RETRY_INTERVAL;
        cache.refresh_if_needed(&logctx.log, &present, t3, || {
            calls.set(calls.get() + 1);
            Ok::<_, String>(labels(&[(0, "N0"), (1, "N1")]))
        });
        assert_eq!(calls.get(), 3);
        assert_eq!(cache.location(inst(1)), Some("N1"));
        logctx.cleanup_successful();
    }
}

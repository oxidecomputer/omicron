// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

//! Types for publishing kernel statistics via oximeter.
//!
//! # illumos kernel statistics
//!
//! illumos defines a generic framework for tracking statistics throughout the
//! OS, called `kstat`. These statistics are generally defined by the kernel or
//! device drivers, and made available to userspace for reading through
//! `libkstat`.
//!
//! Kernel statistics are defined by a 4-tuple of names, canonically separated
//! with a ":". For example, the `cpu:0:vm:pgin` kstat tracks the number of
//! memory pages paged in for CPU 0. In this case, the kstat is tracked by a
//! u64, though several data types are supported.
//!
//! This module uses the `kstat-rs` crate, which is a fairly low-level wrapper
//! around `libkstat`. For the purposes of this module, most folks will be
//! interested in the types [`kstat_rs::Kstat`], [`kstat_rs::Data`], and
//! [`kstat_rs::NamedData`].
//!
//! # Oximeter
//!
//! Oximeter is the Oxide control plane component which collects telemetry from
//! other parts of the system, called _producers_. Statistics are defined using
//! a _target_, which is an object about which statistics are collected, and
//! _metrics_, which are the actual measurements about a target. As an example,
//! a target might be an NVMe drive on the rack, and a metric might be its
//! temperature, or the estimated remaining drive lifetime.
//!
//! Targets and metrics are encapsulated by traits of the same name. Using
//! these, producers can generate timestamped [`Sample`]s, which the `oximeter`
//! collector program pulls at regular intervals for storage in the timeseries
//! database.
//!
//! # What does this mod do?
//!
//! This module is intended to connect illumos kstats with oximeter. Developers
//! can use this to define a mapping betweeen one or more kstats, and an
//! oximeter `Target` and `Metric`. This mapping is encapsulated in the
//! [`KstatTarget`] trait, which extends the `Target` trait itself.
//!
//! To implement the trait, developers register their interest in a particular
//! [`Kstat`] through the [`KstatTarget::interested()`] method. They then
//! describe how to generate any number of [`Sample`]s from that set of kstats,
//! through the [`KstatTarget::to_samples()`] method.
//!
//! # The [`KstatSampler`]
//!
//! Most folks will instantiate a [`KstatSampler`], which manages any number of
//! tracked `KstatTarget`s. Users can register their implementation of
//! `KstatTarget` with the sampler, and it will periodically generate samples
//! from it, converting the "interesting" kstats into `Sample`s.
//!
//! # Intervals and expiration
//!
//! When users register a target for sampling, they are required to include
//! details about how often their target should be sampled, and what to do if we
//! cannot produce samples due to an error, or if there are _no kstats_ that the
//! target is interested in. These details are captured in the
//! [`CollectionDetails`] type.
//!
//! After a configurable period of errors (expressed in either consecutive error
//! counts or a duration of concecutive errors), a target is _expired_, and will
//! no longer be collected from. A target's status may be queried with the
//! [`KstatSampler::target_status()`] method, which will inform the caller if
//! the target has expired. In this case, users can re-register a target, which
//! will replace the expired one, generating new samples (assuming the error
//! condition has been resolved).

use chrono::DateTime;
use chrono::Utc;
use kstat_rs::Data;
use kstat_rs::Error as KstatError;
use kstat_rs::Kstat;
use kstat_rs::NamedData;
use kstat_rs::NamedType;
use oximeter::FieldValue;
use oximeter::MetricsError;
use oximeter::Sample;
use oximeter::Target;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::time::Duration;

#[cfg(any(feature = "datalink", test))]
pub mod link;
mod sampler;

pub use sampler::CollectionDetails;
pub use sampler::ExpirationBehavior;
pub use sampler::KstatSampler;
pub use sampler::KstatSemaphore;
pub use sampler::TargetId;
pub use sampler::TargetStatus;

cfg_if::cfg_if! {
    if #[cfg(all(test, target_os = "illumos"))] {
        type Timestamp = tokio::time::Instant;
        #[inline(always)]
        fn now() -> Timestamp {
            tokio::time::Instant::now()
        }
    } else {
        type Timestamp = chrono::DateTime<chrono::Utc>;
        #[inline(always)]
        fn now() -> Timestamp {
            chrono::Utc::now()
        }
    }
}

/// The reason a kstat target was expired and removed from a sampler.
#[derive(Clone, Copy, Debug)]
pub enum ExpirationReason {
    /// Expired after too many failed attempts.
    Attempts(usize),
    /// Expired after a defined interval of consistent failures.
    Duration(Duration),
}

/// An error describing why a kstat target was expired.
#[derive(Debug)]
pub struct Expiration {
    /// The reason for expiration.
    pub reason: ExpirationReason,
    /// The last error before expiration.
    pub error: Box<Error>,
    /// The time at which the expiration occurred.
    pub expired_at: Timestamp,
}

/// Errors resulting from reporting kernel statistics.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Could not find kstat with the expected name")]
    NoSuchKstat,

    #[error("Kstat does not have the expected data type")]
    UnexpectedDataType { expected: NamedType, found: NamedType },

    #[error("Expected a named kstat")]
    ExpectedNamedKstat,

    #[error("Duplicate target instance")]
    DuplicateTarget {
        target_name: String,
        fields: BTreeMap<String, FieldValue>,
    },

    #[error("No such kstat target exists")]
    NoSuchTarget,

    #[error("Error generating sample")]
    Sample(#[from] MetricsError),

    #[error("Kstat library error")]
    Kstat(#[from] KstatError),

    #[error("Kstat control handle is not available")]
    NoKstatCtl,

    #[error("Overflow computing kstat creation or snapshot time")]
    TimestampOverflow,

    #[error("Failed to send message to background sampling task")]
    SendError,

    #[error("Failed to receive message from background sampling task")]
    RecvError,

    #[error("Expired following too many unsuccessful collection attempts")]
    Expired(Expiration),

    #[error("Expired after unsucessfull collections for {duration:?}")]
    ExpiredAfterDuration { duration: Duration, error: Box<Error> },
}

/// Type alias for a list of kstats.
///
/// This includes the kstat's creation time, the kstat itself, and its data.
pub type KstatList<'a, 'k> = &'a [(DateTime<Utc>, Kstat<'k>, Data<'k>)];

/// A trait for generating oximeter samples from a kstat.
///
/// This trait is used to describe the kernel statistics that are relevant for
/// an `oximeter::Target`, and how to generate samples from them.
pub trait KstatTarget:
    Target + Send + Sync + 'static + std::fmt::Debug
{
    /// Return true for any kstat you're interested in.
    fn interested(&self, kstat: &Kstat<'_>) -> bool;

    /// Convert from a kstat and its data to a list of samples.
    fn to_samples(
        &self,
        kstats: KstatList<'_, '_>,
    ) -> Result<Vec<Sample>, Error>;
}

/// Convert from a high-res timestamp into UTC, if possible.
pub fn hrtime_to_utc(hrtime: i64) -> Result<DateTime<Utc>, Error> {
    let utc_now = Utc::now();
    let hrtime_now = get_hires_time();
    match hrtime_now.cmp(&hrtime) {
        Ordering::Equal => Ok(utc_now),
        Ordering::Less => {
            let offset = u64::try_from(hrtime - hrtime_now)
                .map_err(|_| Error::TimestampOverflow)?;
            Ok(utc_now + Duration::from_nanos(offset))
        }
        Ordering::Greater => {
            let offset = u64::try_from(hrtime_now - hrtime)
                .map_err(|_| Error::TimestampOverflow)?;
            Ok(utc_now - Duration::from_nanos(offset))
        }
    }
}

/// Helper trait for converting a `NamedData` item into a specific contained data
/// type, if possible.
pub trait ConvertNamedData {
    fn as_i32(&self) -> Result<i32, Error>;
    fn as_u32(&self) -> Result<u32, Error>;
    fn as_i64(&self) -> Result<i64, Error>;
    fn as_u64(&self) -> Result<u64, Error>;
}

impl ConvertNamedData for NamedData<'_> {
    fn as_i32(&self) -> Result<i32, Error> {
        if let NamedData::Int32(x) = self {
            Ok(*x)
        } else {
            Err(Error::UnexpectedDataType {
                expected: NamedType::Int32,
                found: self.data_type(),
            })
        }
    }

    fn as_u32(&self) -> Result<u32, Error> {
        if let NamedData::UInt32(x) = self {
            Ok(*x)
        } else {
            Err(Error::UnexpectedDataType {
                expected: NamedType::UInt32,
                found: self.data_type(),
            })
        }
    }

    fn as_i64(&self) -> Result<i64, Error> {
        if let NamedData::Int64(x) = self {
            Ok(*x)
        } else {
            Err(Error::UnexpectedDataType {
                expected: NamedType::Int64,
                found: self.data_type(),
            })
        }
    }

    fn as_u64(&self) -> Result<u64, Error> {
        if let NamedData::UInt64(x) = self {
            Ok(*x)
        } else {
            Err(Error::UnexpectedDataType {
                expected: NamedType::UInt64,
                found: self.data_type(),
            })
        }
    }
}

/// Return a high-resolution monotonic timestamp, in nanoseconds since an
/// arbitrary point in the past.
///
/// This is equivalent to `gethrtime(3C)` on illumos, and `clock_gettime()` with
/// an equivalent clock source on other platforms.
pub fn get_hires_time() -> i64 {
    // NOTE: See `man clock_gettime`, but this is an alias for `CLOCK_HIGHRES`,
    // and is the same source that underlies `gethrtime()`, which this API is
    // intended to emulate on other platforms.
    #[cfg(target_os = "illumos")]
    const SOURCE: libc::clockid_t = libc::CLOCK_MONOTONIC;
    #[cfg(not(target_os = "illumos"))]
    const SOURCE: libc::clockid_t = libc::CLOCK_MONOTONIC_RAW;
    let mut tp = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    if unsafe { libc::clock_gettime(SOURCE, &mut tp as *mut _) } == 0 {
        const NANOS_PER_SEC: i64 = 1_000_000_000;
        tp.tv_sec
            .checked_mul(NANOS_PER_SEC)
            .and_then(|nsec| nsec.checked_add(tp.tv_nsec))
            .unwrap_or(0)
    } else {
        0
    }
}

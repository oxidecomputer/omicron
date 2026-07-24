// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Report metrics about CPU cores on the host system

use crate::kstat::ConvertNamedData;
use crate::kstat::Error;
use crate::kstat::KstatList;
use crate::kstat::KstatTarget;
use crate::kstat::hrtime_to_utc;
use crate::kstat::n_processors;
use chrono::DateTime;
use chrono::Utc;
use kstat_rs::Data;
use kstat_rs::Kstat;
use kstat_rs::Named;
use oximeter::FieldType;
use oximeter::FieldValue;
use oximeter::Sample;
use oximeter::Target;
use oximeter::types::Cumulative;
use uuid::Uuid;

oximeter::use_timeseries!("sled-cpu.toml");
pub use self::sled_cpu::SledCpu as SledCpuTarget;

/// The prefix for CPU microstate kstat fields.
const CPU_NSEC_PREFIX: &str = "cpu_nsec_";

/// The CPU microstates we track from kstats.
///
/// These correspond to the `cpu_nsec_*` fields in the `cpu::sys` kstat.
const CPU_MICROSTATES: &[&str] = &["idle", "user", "kernel", "dtrace", "intr"];

/// The number of statistics we track from each `cpufreq` kstat: the effective
/// and average frequencies, and the cumulative APERF / MPERF counters.
const N_CPUFREQ_STATS: usize = 4;

/// The maximum cardinality of the data we produce, per sampling interval.
pub fn max_cardinality() -> usize {
    (CPU_MICROSTATES.len() + N_CPUFREQ_STATS) * n_processors().unwrap_or(1024)
}

/// CPU metrics for a sled, tracking microstate and frequency statistics
/// across all cores.
#[derive(Clone, Debug)]
pub struct SledCpu {
    /// The target for this sled's CPUs.
    pub target: SledCpuTarget,
    /// Flag indicating whether the sled is synced with NTP.
    pub time_synced: bool,
}

impl SledCpu {
    /// Create a new `SledCpu` with the given target and synchronization flag.
    pub fn new(target: SledCpuTarget, time_synced: bool) -> Self {
        Self { target, time_synced }
    }

    /// Return the sled ID.
    pub fn sled_id(&self) -> Uuid {
        self.target.sled_id
    }

    /// Generate samples from a `cpu::sys` kstat's microstate counters.
    fn microstate_samples(
        &self,
        creation_time: &DateTime<Utc>,
        cpu_id: u32,
        snapshot_time: DateTime<Utc>,
        named: &[Named<'_>],
        samples: &mut Vec<Sample>,
    ) -> Result<(), Error> {
        for named_data in named.iter() {
            let Named { name, value } = named_data;

            // Check if this is a cpu_nsec_* field we care about
            let Some(state) = name.strip_prefix(CPU_NSEC_PREFIX) else {
                continue;
            };

            // Only process states we know about
            if !CPU_MICROSTATES.contains(&state) {
                continue;
            }

            let datum = value.as_u64()?;
            let metric = sled_cpu::CpuNsec {
                cpu_id,
                state: state.to_string().into(),
                datum: Cumulative::with_start_time(*creation_time, datum),
            };
            let sample = Sample::new_with_timestamp(
                snapshot_time,
                &self.target,
                &metric,
            )
            .map_err(Error::Sample)?;
            samples.push(sample);
        }
        Ok(())
    }

    /// Generate samples from a `cpufreq` kstat's frequency statistics.
    fn cpufreq_samples(
        &self,
        creation_time: &DateTime<Utc>,
        cpu_id: u32,
        snapshot_time: DateTime<Utc>,
        named: &[Named<'_>],
        samples: &mut Vec<Sample>,
    ) -> Result<(), Error> {
        // The frequency data is computed by the kernel over a window ending
        // at `snapshot_hrtime`, up to about a second before the kstat was
        // actually read. Prefer that as the sample timestamp, where present.
        let timestamp = named
            .iter()
            .find(|nd| nd.name == "snapshot_hrtime")
            .and_then(|nd| nd.value.as_u64().ok())
            .map(|hrtime| hrtime_to_utc(hrtime as i64))
            .transpose()?
            .unwrap_or(snapshot_time);

        for named_data in named.iter() {
            let Named { name, value } = named_data;
            let sample = match *name {
                "effective_Hz" => Sample::new_with_timestamp(
                    timestamp,
                    &self.target,
                    &sled_cpu::EffectiveFrequency {
                        cpu_id,
                        datum: value.as_u64()?,
                    },
                ),
                "average_Hz" => Sample::new_with_timestamp(
                    timestamp,
                    &self.target,
                    &sled_cpu::AverageFrequency {
                        cpu_id,
                        datum: value.as_u64()?,
                    },
                ),
                "aperf_total" => Sample::new_with_timestamp(
                    timestamp,
                    &self.target,
                    &sled_cpu::AperfTotal {
                        cpu_id,
                        datum: Cumulative::with_start_time(
                            *creation_time,
                            value.as_u64()?,
                        ),
                    },
                ),
                "mperf_total" => Sample::new_with_timestamp(
                    timestamp,
                    &self.target,
                    &sled_cpu::MperfTotal {
                        cpu_id,
                        datum: Cumulative::with_start_time(
                            *creation_time,
                            value.as_u64()?,
                        ),
                    },
                ),
                _ => continue,
            }
            .map_err(Error::Sample)?;
            samples.push(sample);
        }
        Ok(())
    }
}

impl KstatTarget for SledCpu {
    fn interested(&self, kstat: &Kstat<'_>) -> bool {
        self.time_synced
            && ((kstat.ks_module == "cpu" && kstat.ks_name == "sys")
                || (kstat.ks_module == "cpufreq" && kstat.ks_name == "cpufreq"))
    }

    fn to_samples(
        &self,
        kstats: KstatList<'_, '_>,
    ) -> Result<Vec<Sample>, Error> {
        let mut samples = Vec::new();

        for (creation_time, kstat, data) in kstats.iter() {
            let snapshot_time = hrtime_to_utc(kstat.ks_snaptime)?;
            let cpu_id = u32::try_from(kstat.ks_instance)
                .expect("CPU instance ID should fit in u32");

            let Data::Named(named) = data else {
                return Err(Error::ExpectedNamedKstat);
            };

            if kstat.ks_module == "cpufreq" {
                self.cpufreq_samples(
                    creation_time,
                    cpu_id,
                    snapshot_time,
                    named,
                    &mut samples,
                )?;
            } else {
                self.microstate_samples(
                    creation_time,
                    cpu_id,
                    snapshot_time,
                    named,
                    &mut samples,
                )?;
            }
        }

        Ok(samples)
    }
}

// NOTE: Delegate to the inner target type for this implementation.
impl Target for SledCpu {
    fn name(&self) -> &'static str {
        self.target.name()
    }

    fn field_names(&self) -> &'static [&'static str] {
        self.target.field_names()
    }

    fn field_types(&self) -> Vec<FieldType> {
        self.target.field_types()
    }

    fn field_values(&self) -> Vec<FieldValue> {
        self.target.field_values()
    }
}

#[cfg(all(test, target_os = "illumos"))]
mod tests {
    use super::*;
    use crate::kstat::CollectionDetails;
    use crate::kstat::KstatSampler;
    use crate::kstat::TargetStatus;
    use kstat_rs::Ctl;
    use oximeter::Producer;
    use slog::Drain;
    use slog::Logger;
    use std::time::Duration;
    use tokio::time::Instant;
    use uuid::Uuid;
    use uuid::uuid;

    fn test_logger() -> Logger {
        let dec =
            slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(dec).build().fuse();
        let log =
            Logger::root(drain, slog::o!("component" => "fake-cleanup-task"));
        log
    }

    const RACK_ID: Uuid = uuid!("de784702-cafb-41a9-b3e5-93af189def29");
    const SLED_ID: Uuid = uuid!("88240343-5262-45f4-86f1-3c82fe383f2a");
    const SLED_MODEL: &str = "fake-gimlet";
    const SLED_REVISION: u32 = 1;
    const SLED_SERIAL: &str = "fake-serial";

    #[test]
    fn test_kstat_interested() {
        let target = SledCpuTarget {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
        };
        let mut cpu = SledCpu::new(target, false);

        let ctl = Ctl::new().unwrap();
        let ctl = ctl.update().unwrap();
        let kstat =
            ctl.filter(Some("cpu"), Some(0), Some("sys")).next().unwrap();

        // Not interested when not time synced
        assert!(!cpu.interested(&kstat));

        // Interested when time synced
        cpu.time_synced = true;
        assert!(cpu.interested(&kstat));

        // Not interested in other cpu kstats (e.g., cpu:0:vm)
        if let Some(vm_kstat) =
            ctl.filter(Some("cpu"), Some(0), Some("vm")).next()
        {
            assert!(!cpu.interested(&vm_kstat));
        }

        // Not interested in non-cpu kstats
        if let Some(other_kstat) = ctl.filter(Some("link"), None, None).next() {
            assert!(!cpu.interested(&other_kstat));
        }

        // Interested in cpufreq kstats, where the host provides them.
        if let Some(freq_kstat) =
            ctl.filter(Some("cpufreq"), Some(0), Some("cpufreq")).next()
        {
            assert!(cpu.interested(&freq_kstat));
        }
    }

    #[test]
    fn test_sled_cpu_cpufreq_samples() {
        let target = SledCpuTarget {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
        };
        let cpu = SledCpu::new(target, true);
        let ctl = Ctl::new().unwrap();
        let ctl = ctl.update().unwrap();

        // Collect the cpufreq kstat for CPU 0, if the host provides it.
        let Some(mut kstat) =
            ctl.filter(Some("cpufreq"), Some(0), Some("cpufreq")).next()
        else {
            eprintln!("host does not provide cpufreq kstats, skipping");
            return;
        };
        let creation_time = hrtime_to_utc(kstat.ks_crtime).unwrap();
        let data = ctl.read(&mut kstat).unwrap();
        let samples = cpu.to_samples(&[(creation_time, kstat, data)]).unwrap();
        println!("{samples:#?}");

        // We should have the two frequency gauges, plus the cumulative
        // APERF/MPERF counters on hosts new enough to provide them.
        let mut names: Vec<_> =
            samples.iter().map(|s| s.timeseries_name.to_string()).collect();
        names.sort();
        assert!(
            names.contains(&"sled_cpu:effective_frequency".to_string()),
            "expected an effective_frequency sample, found {names:?}"
        );
        assert!(
            names.contains(&"sled_cpu:average_frequency".to_string()),
            "expected an average_frequency sample, found {names:?}"
        );
    }

    #[test]
    fn test_sled_cpu_samples() {
        let target = SledCpuTarget {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
        };
        let cpu = SledCpu::new(target, true);
        let ctl = Ctl::new().unwrap();
        let ctl = ctl.update().unwrap();

        // Collect kstats for CPU 0
        let mut kstat =
            ctl.filter(Some("cpu"), Some(0), Some("sys")).next().unwrap();
        let creation_time = hrtime_to_utc(kstat.ks_crtime).unwrap();
        let data = ctl.read(&mut kstat).unwrap();
        let samples = cpu.to_samples(&[(creation_time, kstat, data)]).unwrap();
        println!("{samples:#?}");

        // Extract the state from each sample
        let mut states: Vec<_> = samples
            .iter()
            .filter_map(|s| {
                s.sorted_metric_fields().get("state").and_then(|f| {
                    match &f.value {
                        oximeter::FieldValue::String(s) => {
                            Some(s.as_ref().to_string())
                        }
                        _ => None,
                    }
                })
            })
            .collect();
        states.sort();

        // Verify we got exactly one sample for each expected microstate
        let mut expected: Vec<_> =
            CPU_MICROSTATES.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(states, expected);
    }

    #[tokio::test]
    async fn test_kstat_sampler() {
        let mut sampler = KstatSampler::new(&test_logger()).unwrap();
        let target = SledCpuTarget {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
        };
        let cpu = SledCpu::new(target, true);
        let details = CollectionDetails::never(Duration::from_secs(1), 512);
        let id = sampler.add_target(cpu, details).await.unwrap();
        let samples: Vec<_> = sampler.produce().unwrap().collect();
        assert!(samples.is_empty());

        // Pause time, and advance until we're notified of new samples.
        tokio::time::pause();
        const MAX_DURATION: Duration = Duration::from_secs(3);
        const STEP_DURATION: Duration = Duration::from_secs(1);
        let now = Instant::now();
        let expected_counts = loop {
            tokio::time::advance(STEP_DURATION).await;
            if now.elapsed() > MAX_DURATION {
                panic!("Waited too long for samples");
            }
            if let Some(counts) = sampler.sample_counts() {
                break counts;
            }
        };
        let samples: Vec<_> = sampler.produce().unwrap().collect();
        println!("{samples:#?}");
        assert_eq!(samples.len(), expected_counts.total);
        assert_eq!(expected_counts.overflow, 0);

        // Test status and remove behavior.
        tokio::time::resume();
        assert!(matches!(
            sampler.target_status(id).await.unwrap(),
            TargetStatus::Ok { .. },
        ));
        sampler.remove_target(id).await.unwrap();
        assert!(sampler.target_status(id).await.is_err());
    }
}

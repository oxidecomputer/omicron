// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Report each disk's NVMe SMART / Health Information log page as metrics.

use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::internal::shared::SledIdentifiers;
use oximeter::MetricsError;
use oximeter::Sample;
use oximeter::queue::BoundedQueue;
use sled_agent_types::disk::DiskIdentity;
use sled_hardware::DiskHealthMap;
use sled_hardware::NvmeHealthLogPage;
use slog::Logger;
use slog::warn;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::interval;

oximeter::use_timeseries!("physical-disk.toml");

/// How often each disk's latest reading is turned into samples.
///
/// The hardware poll reads the log page more often than this; the readings
/// in between are not reported.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

/// Enough for a full sled's disks to go uncollected for several
/// `SAMPLE_INTERVAL`s.
const MAX_QUEUE_LENGTH: usize = 4096;

#[derive(Clone, Debug)]
pub(super) struct DiskHealthProducer {
    samples: Arc<Mutex<BoundedQueue<Sample>>>,
    _worker: Arc<JoinHandle<()>>,
}

impl DiskHealthProducer {
    pub(super) fn new(
        log: Logger,
        sled: SledIdentifiers,
        disk_health: watch::Receiver<Arc<DiskHealthMap>>,
    ) -> Self {
        let samples = Arc::new(Mutex::new(BoundedQueue::new(MAX_QUEUE_LENGTH)));
        let worker =
            tokio::spawn(worker(samples.clone(), sled, disk_health, log));
        Self { samples, _worker: Arc::new(worker) }
    }
}

impl oximeter::Producer for DiskHealthProducer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample>>, MetricsError> {
        let samples = self.samples.lock().unwrap().drain();
        Ok(Box::new(samples.into_iter()))
    }
}

async fn worker(
    samples: Arc<Mutex<BoundedQueue<Sample>>>,
    sled: SledIdentifiers,
    disk_health: watch::Receiver<Arc<DiskHealthMap>>,
    log: Logger,
) {
    // Samples carry the time their reading was collected. This producer is
    // started once the sled's clock is synchronized; a reading from before
    // then may be timestamped by an unsynchronized clock.
    let started = Utc::now();
    let mut tick = interval(SAMPLE_INTERVAL);
    let mut last_sampled = HashMap::new();
    loop {
        tick.tick().await;

        let disk_health = Arc::clone(&*disk_health.borrow());
        let new_samples =
            sample_disks(&log, &sled, &disk_health, started, &mut last_sampled);

        let n_dropped = samples.lock().unwrap().extend(new_samples);
        if n_dropped > 0 {
            warn!(
                log,
                "disk health sample queue overflow; dropping samples";
                "dropped" => n_dropped,
            );
        }
    }
}

/// Returns samples for every disk in `disk_health` with a reading newer than
/// the one last sampled, as recorded in `last_sampled`.
///
/// A disk with an error in place of a reading produces no samples, nor does
/// one whose reading was collected before `not_before`.
fn sample_disks(
    log: &Logger,
    sled: &SledIdentifiers,
    disk_health: &DiskHealthMap,
    not_before: DateTime<Utc>,
    last_sampled: &mut HashMap<DiskIdentity, DateTime<Utc>>,
) -> Vec<Sample> {
    last_sampled.retain(|identity, _| disk_health.contains_key(identity));

    let mut samples = Vec::new();
    for (identity, health) in disk_health {
        let Ok(page) = health else {
            continue;
        };
        if page.time_collected < not_before {
            continue;
        }
        // If the hardware poll has stalled, the reading is one we already
        // reported.
        if last_sampled.get(identity) == Some(&page.time_collected) {
            continue;
        }
        match disk_samples(sled, identity, page) {
            Ok(disk_samples) => {
                samples.extend(disk_samples);
                last_sampled.insert(identity.clone(), page.time_collected);
            }
            Err(err) => warn!(
                log,
                "failed to build disk health samples";
                "disk" => ?identity,
                "err" => %err,
            ),
        }
    }
    samples
}

fn disk_samples(
    sled: &SledIdentifiers,
    identity: &DiskIdentity,
    page: &NvmeHealthLogPage,
) -> Result<Vec<Sample>, MetricsError> {
    let target = physical_disk::PhysicalDisk {
        disk_vendor: identity.vendor.clone().into(),
        disk_model: identity.model.clone().into(),
        disk_serial: identity.serial.clone().into(),
        rack_id: sled.rack_id,
        sled_id: sled.sled_id,
        sled_model: sled.model.clone().into(),
        sled_revision: sled.revision,
        sled_serial: sled.serial.clone().into(),
    };

    let NvmeHealthLogPage {
        time_collected,
        critical_warnings,
        endurance_group_warnings,
        temperature_kelvin,
        available_spare_percent,
        available_spare_threshold_percent,
        life_used_percent,
        data_read_bytes,
        data_written_bytes,
        host_read_commands,
        host_write_commands,
        controller_busy_minutes,
        power_cycles,
        power_on_hours,
        unsafe_shutdowns,
        media_errors,
        error_log_entries,
        warning_temperature_minutes,
        critical_temperature_minutes,
        temperature_sensors_kelvin,
        thermal_management,
    } = page;

    let mut samples = Vec::new();
    // Pushes a sample of one metric, timestamped with the reading.
    macro_rules! sample {
        ($metric:expr) => {
            samples.push(Sample::new_with_timestamp(
                *time_collected,
                &target,
                &$metric,
            )?)
        };
    }

    if let Some(kelvin) = temperature_kelvin {
        sample!(physical_disk::Temperature { datum: celsius(*kelvin) });
    }
    for (sensor_number, kelvin) in (1..).zip(temperature_sensors_kelvin) {
        if let Some(kelvin) = kelvin {
            sample!(physical_disk::SensorTemperature {
                sensor_number,
                datum: celsius(*kelvin),
            });
        }
    }

    sample!(physical_disk::AvailableSpare { datum: *available_spare_percent });
    sample!(physical_disk::AvailableSpareThreshold {
        datum: *available_spare_threshold_percent,
    });
    sample!(physical_disk::LifeUsed { datum: *life_used_percent });

    sample!(physical_disk::DataRead { datum: *data_read_bytes });
    sample!(physical_disk::DataWritten { datum: *data_written_bytes });
    sample!(physical_disk::HostReadCommands { datum: *host_read_commands });
    sample!(physical_disk::HostWriteCommands { datum: *host_write_commands });
    sample!(physical_disk::ControllerBusyTime {
        datum: controller_busy_minutes.saturating_mul(SECONDS_PER_MINUTE),
    });
    sample!(physical_disk::PowerCycles { datum: *power_cycles });
    sample!(physical_disk::PowerOnTime {
        datum: power_on_hours.saturating_mul(SECONDS_PER_HOUR),
    });
    sample!(physical_disk::UnsafeShutdowns { datum: *unsafe_shutdowns });
    sample!(physical_disk::MediaErrors { datum: *media_errors });
    sample!(physical_disk::ErrorLogEntries { datum: *error_log_entries });

    if let Some(minutes) = warning_temperature_minutes {
        sample!(physical_disk::WarningTemperatureTime {
            datum: u64::from(*minutes) * SECONDS_PER_MINUTE,
        });
    }
    if let Some(minutes) = critical_temperature_minutes {
        sample!(physical_disk::CriticalTemperatureTime {
            datum: u64::from(*minutes) * SECONDS_PER_MINUTE,
        });
    }
    if let Some(thermal) = thermal_management {
        for (level, transitions, seconds) in [
            (1, thermal.temp1_transitions, thermal.temp1_seconds),
            (2, thermal.temp2_transitions, thermal.temp2_seconds),
        ] {
            sample!(physical_disk::ThermalManagementTransitions {
                level,
                datum: u64::from(transitions),
            });
            sample!(physical_disk::ThermalManagementTime {
                level,
                datum: u64::from(seconds),
            });
        }
    }

    // A warning the controller's NVMe version does not define is `None`, and
    // is not reported.
    for (warning, raised) in [
        ("available_spare_low", Some(critical_warnings.available_spare_low)),
        ("temperature", Some(critical_warnings.temperature)),
        ("reliability_degraded", Some(critical_warnings.reliability_degraded)),
        ("read_only", Some(critical_warnings.read_only)),
        (
            "volatile_memory_backup_failed",
            critical_warnings.volatile_memory_backup_failed,
        ),
        (
            "persistent_memory_region_read_only",
            critical_warnings.persistent_memory_region_read_only,
        ),
        (
            "indeterminate_personality_state",
            critical_warnings.indeterminate_personality_state,
        ),
    ] {
        if let Some(raised) = raised {
            sample!(physical_disk::CriticalWarning {
                warning: warning.into(),
                datum: raised,
            });
        }
    }
    if let Some(warnings) = endurance_group_warnings {
        for (warning, raised) in [
            ("available_spare_low", warnings.available_spare_low),
            ("reliability_degraded", warnings.reliability_degraded),
            ("read_only", warnings.read_only),
        ] {
            sample!(physical_disk::EnduranceGroupCriticalWarning {
                warning: warning.into(),
                datum: raised,
            });
        }
    }

    Ok(samples)
}

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;

/// Converts as `nvmeadm` does: the controller reports whole kelvins.
fn celsius(kelvin: u16) -> f32 {
    f32::from(kelvin) - 273.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_test_utils::dev::test_setup_log;
    use oximeter::Datum;
    use oximeter::FieldValue;
    use sled_agent_types::inventory::NvmeHealthError;
    use sled_agent_types::inventory::NvmeHealthErrorKind;
    use sled_hardware::NvmeCriticalWarnings;
    use sled_hardware::NvmeEnduranceGroupWarnings;
    use sled_hardware::NvmeThermalManagement;
    use uuid::Uuid;

    fn sled() -> SledIdentifiers {
        SledIdentifiers {
            rack_id: Uuid::new_v4(),
            sled_id: Uuid::new_v4(),
            model: "sled-model".to_string(),
            revision: 6,
            serial: "sled-serial".to_string(),
        }
    }

    fn identity(serial: &str) -> DiskIdentity {
        DiskIdentity {
            vendor: "disk-vendor".to_string(),
            model: "disk-model".to_string(),
            serial: serial.to_string(),
        }
    }

    /// A page with every optional part present.
    fn full_page() -> NvmeHealthLogPage {
        let mut temperature_sensors_kelvin = [None; 8];
        temperature_sensors_kelvin[0] = Some(321);
        temperature_sensors_kelvin[7] = Some(328);
        NvmeHealthLogPage {
            time_collected: Utc::now(),
            critical_warnings: NvmeCriticalWarnings {
                available_spare_low: false,
                temperature: false,
                reliability_degraded: true,
                read_only: false,
                volatile_memory_backup_failed: Some(false),
                persistent_memory_region_read_only: Some(false),
                indeterminate_personality_state: Some(false),
            },
            endurance_group_warnings: Some(NvmeEnduranceGroupWarnings {
                available_spare_low: false,
                reliability_degraded: true,
                read_only: false,
            }),
            temperature_kelvin: Some(320),
            available_spare_percent: 99,
            available_spare_threshold_percent: 10,
            life_used_percent: 3,
            data_read_bytes: 512_000,
            data_written_bytes: 1_024_000,
            host_read_commands: 1,
            host_write_commands: 2,
            controller_busy_minutes: 3,
            power_cycles: 4,
            power_on_hours: 5,
            unsafe_shutdowns: 6,
            media_errors: 7,
            error_log_entries: 8,
            warning_temperature_minutes: Some(9),
            critical_temperature_minutes: Some(10),
            temperature_sensors_kelvin,
            thermal_management: Some(NvmeThermalManagement {
                temp1_transitions: 11,
                temp2_transitions: 12,
                temp1_seconds: 13,
                temp2_seconds: 14,
            }),
        }
    }

    /// The datum of the one sample of `metric` whose metric fields are
    /// `fields`. Panics unless there is exactly one.
    #[track_caller]
    fn datum_of(
        samples: &[Sample],
        metric: &str,
        fields: &[(&str, FieldValue)],
    ) -> Datum {
        let name = format!("physical_disk:{metric}");
        let matching = samples
            .iter()
            .filter(|sample| sample.timeseries_name == name.as_str())
            .filter(|sample| {
                let sample_fields = sample.sorted_metric_fields();
                sample_fields.len() == fields.len()
                    && fields.iter().all(|(field, value)| {
                        sample_fields
                            .get(*field)
                            .is_some_and(|f| &f.value == value)
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "samples of {name} with {fields:?}");
        matching[0].measurement.datum().clone()
    }

    #[test]
    fn disk_samples_report_every_statistic() {
        let sled = sled();
        let identity = identity("disk-serial");
        let page = full_page();
        let samples = disk_samples(&sled, &identity, &page).unwrap();

        for sample in &samples {
            assert_eq!(sample.measurement.timestamp(), page.time_collected);
            let target = sample.sorted_target_fields();
            let string = |value: &'static str| FieldValue::String(value.into());
            for (field, value) in [
                ("disk_vendor", string("disk-vendor")),
                ("disk_model", string("disk-model")),
                ("disk_serial", string("disk-serial")),
                ("rack_id", FieldValue::Uuid(sled.rack_id)),
                ("sled_id", FieldValue::Uuid(sled.sled_id)),
                ("sled_model", string("sled-model")),
                ("sled_revision", FieldValue::U32(6)),
                ("sled_serial", string("sled-serial")),
            ] {
                assert_eq!(target[field].value, value, "target field {field}");
            }
        }

        let datum = |metric| datum_of(&samples, metric, &[]);
        let numbered = |metric, field, n| {
            datum_of(&samples, metric, &[(field, FieldValue::U8(n))])
        };
        let warning = |metric, name: &'static str| {
            datum_of(
                &samples,
                metric,
                &[("warning", FieldValue::String(name.into()))],
            )
        };

        assert_eq!(datum("temperature"), Datum::F32(47.0));
        assert_eq!(
            numbered("sensor_temperature", "sensor_number", 1),
            Datum::F32(48.0)
        );
        assert_eq!(
            numbered("sensor_temperature", "sensor_number", 8),
            Datum::F32(55.0)
        );
        assert_eq!(datum("available_spare"), Datum::U8(99));
        assert_eq!(datum("available_spare_threshold"), Datum::U8(10));
        assert_eq!(datum("life_used"), Datum::U8(3));
        assert_eq!(datum("data_read"), Datum::U64(512_000));
        assert_eq!(datum("data_written"), Datum::U64(1_024_000));
        assert_eq!(datum("host_read_commands"), Datum::U64(1));
        assert_eq!(datum("host_write_commands"), Datum::U64(2));
        assert_eq!(datum("controller_busy_time"), Datum::U64(3 * 60));
        assert_eq!(datum("power_cycles"), Datum::U64(4));
        assert_eq!(datum("power_on_time"), Datum::U64(5 * 3600));
        assert_eq!(datum("unsafe_shutdowns"), Datum::U64(6));
        assert_eq!(datum("media_errors"), Datum::U64(7));
        assert_eq!(datum("error_log_entries"), Datum::U64(8));
        assert_eq!(datum("warning_temperature_time"), Datum::U64(9 * 60));
        assert_eq!(datum("critical_temperature_time"), Datum::U64(10 * 60));
        assert_eq!(
            numbered("thermal_management_transitions", "level", 1),
            Datum::U64(11)
        );
        assert_eq!(
            numbered("thermal_management_transitions", "level", 2),
            Datum::U64(12)
        );
        assert_eq!(
            numbered("thermal_management_time", "level", 1),
            Datum::U64(13)
        );
        assert_eq!(
            numbered("thermal_management_time", "level", 2),
            Datum::U64(14)
        );
        for (name, raised) in [
            ("available_spare_low", false),
            ("temperature", false),
            ("reliability_degraded", true),
            ("read_only", false),
            ("volatile_memory_backup_failed", false),
            ("persistent_memory_region_read_only", false),
            ("indeterminate_personality_state", false),
        ] {
            assert_eq!(
                warning("critical_warning", name),
                Datum::Bool(raised),
                "critical warning {name}"
            );
        }
        for (name, raised) in [
            ("available_spare_low", false),
            ("reliability_degraded", true),
            ("read_only", false),
        ] {
            assert_eq!(
                warning("endurance_group_critical_warning", name),
                Datum::Bool(raised),
                "endurance group warning {name}"
            );
        }

        // Every sample was checked above: 2 of the 8 sensors are present.
        assert_eq!(samples.len(), 32);
    }

    #[test]
    fn disk_samples_leave_out_what_the_controller_does_not_report() {
        let page = NvmeHealthLogPage::new_fake(Utc::now());
        let samples =
            disk_samples(&sled(), &identity("serial"), &page).unwrap();

        let names = samples
            .iter()
            .map(|sample| sample.timeseries_name.to_string())
            .collect::<Vec<_>>();
        for absent in [
            "physical_disk:temperature",
            "physical_disk:sensor_temperature",
            "physical_disk:warning_temperature_time",
            "physical_disk:critical_temperature_time",
            "physical_disk:thermal_management_transitions",
            "physical_disk:thermal_management_time",
            "physical_disk:endurance_group_critical_warning",
        ] {
            assert!(
                !names.iter().any(|name| name == absent),
                "{absent} should not be reported"
            );
        }
        // Only the four warnings every NVMe version defines.
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == "physical_disk:critical_warning")
                .count(),
            4
        );
        assert_eq!(samples.len(), 17);
    }

    #[test]
    fn sample_disks_reports_each_reading_once() {
        let logctx = test_setup_log("sample_disks_reports_each_reading_once");
        let sled = sled();
        let read = identity("read");
        let unreadable = identity("unreadable");
        let page = NvmeHealthLogPage::new_fake(Utc::now());
        let samples_per_disk = disk_samples(&sled, &read, &page).unwrap().len();
        let mut disk_health = DiskHealthMap::from([
            (read.clone(), Ok(page.clone())),
            (
                unreadable.clone(),
                Err(NvmeHealthError {
                    kind: NvmeHealthErrorKind::LogPageReadFailed,
                    message: "test read failure".to_string(),
                }),
            ),
        ]);
        let mut last_sampled = HashMap::new();
        let mut sample_since = |disk_health: &DiskHealthMap, not_before| {
            sample_disks(
                &logctx.log,
                &sled,
                disk_health,
                not_before,
                &mut last_sampled,
            )
        };

        // A reading from before the cutoff is not sampled.
        let after_reading = page.time_collected + chrono::Duration::seconds(1);
        assert!(sample_since(&disk_health, after_reading).is_empty());

        let mut sample = |disk_health: &DiskHealthMap| {
            sample_since(disk_health, page.time_collected)
        };

        // Only the disk with a reading is sampled.
        let samples = sample(&disk_health);
        assert_eq!(samples.len(), samples_per_disk);
        assert!(samples.iter().all(|sample| {
            sample.sorted_target_fields()["disk_serial"].value
                == FieldValue::String("read".into())
        }));

        // The same reading is not sampled again.
        assert!(sample(&disk_health).is_empty());

        // A newer reading is, as is one for a disk that now has a reading.
        let newer = NvmeHealthLogPage::new_fake(
            page.time_collected + chrono::Duration::seconds(5),
        );
        disk_health.insert(read.clone(), Ok(newer.clone()));
        disk_health.insert(unreadable, Ok(newer.clone()));
        assert_eq!(sample(&disk_health).len(), 2 * samples_per_disk);

        // A disk that left and came back is sampled again, even if its
        // reading has the same timestamp.
        disk_health.remove(&read);
        assert!(sample(&disk_health).is_empty());
        disk_health.insert(read, Ok(newer));
        assert_eq!(sample(&disk_health).len(), samples_per_disk);

        logctx.cleanup_successful();
    }
}

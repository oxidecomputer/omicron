// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Report metrics about zones on the host system

use crate::kstat::ConvertNamedData;
use crate::kstat::Error;
use crate::kstat::KstatList;
use crate::kstat::KstatTarget;
use crate::kstat::hrtime_to_utc;
use kstat_rs::Data;
use kstat_rs::Kstat;
use kstat_rs::Named;
use oximeter::FieldType;
use oximeter::FieldValue;
use oximeter::Sample;
use oximeter::Target;
use oximeter::types::Cumulative;
use uuid::Uuid;

/// The prefix used for Omicron zone names.
const ZONE_PREFIX: &str = "oxz_";

/// Parsed zone metadata from a zone name like "oxz_cockroachdb_UUID".
struct ZoneMetadata {
    zone_type: String,
    zone_id: Uuid,
}

/// Parse a zone name into its service type and UUID.
///
/// TODO: Consider passing typed zone metadata from sled-agent instead of
/// parsing zone names. As of this writing, zone names are easy to parse,
/// and we can avoid the complexity of per-zone tracking or maintaining a
/// shared mapping of zone metadata.
fn parse_zone_name(zone_name: &str) -> Option<ZoneMetadata> {
    let rest = zone_name.strip_prefix(ZONE_PREFIX)?;
    match rest.rsplit_once('_') {
        Some((zone_type, uuid_str)) => match uuid_str.parse() {
            Ok(zone_id) => {
                Some(ZoneMetadata { zone_type: zone_type.to_string(), zone_id })
            }
            Err(_) => Some(ZoneMetadata {
                zone_type: rest.to_string(),
                zone_id: Uuid::nil(),
            }),
        },
        None => Some(ZoneMetadata {
            zone_type: rest.to_string(),
            zone_id: Uuid::nil(),
        }),
    }
}

oximeter::use_timeseries!("zone.toml");
pub use self::zone::Zone as ZoneTarget;

/// CPU metrics for all zones on a sled.
#[derive(Clone, Debug)]
pub struct Zone {
    /// The target for this sled's CPUs.
    pub target: ZoneTarget,
    /// Flag indicating whether the sled is synced with NTP.
    pub time_synced: bool,
}

impl Zone {
    /// Create a new `Zone` with the given target and synchronization flag.
    pub fn new(target: ZoneTarget, time_synced: bool) -> Self {
        Self { target, time_synced }
    }

    /// Return the sled ID.
    pub fn sled_id(&self) -> Uuid {
        self.target.sled_id
    }
}

impl KstatTarget for Zone {
    fn interested(&self, kstat: &Kstat<'_>) -> bool {
        self.time_synced && kstat.ks_module == "zones"
    }

    fn to_samples(
        &self,
        kstats: KstatList<'_, '_>,
    ) -> Result<Vec<Sample>, Error> {
        let mut samples = Vec::new();

        for (creation_time, kstat, data) in kstats.iter() {
            let snapshot_time = hrtime_to_utc(kstat.ks_snaptime)?;

            let Data::Named(named) = data else {
                return Err(Error::ExpectedNamedKstat);
            };

            let mut zone_name: Option<&str> = None;
            let mut nsec_user: Option<u64> = None;
            let mut nsec_sys: Option<u64> = None;
            let mut nsec_waitrq: Option<u64> = None;

            for named_data in named.iter() {
                let Named { name, value } = named_data;
                match *name {
                    "zonename" => zone_name = Some(value.as_str()?),
                    "nsec_user" => nsec_user = Some(value.as_u64()?),
                    "nsec_sys" => nsec_sys = Some(value.as_u64()?),
                    "nsec_waitrq" => nsec_waitrq = Some(value.as_u64()?),
                    _ => {}
                }
            }

            let zone_name = zone_name.ok_or(Error::NoSuchKstat)?.to_string();
            let (zone_type, zone_id) = match parse_zone_name(&zone_name) {
                Some(m) => (m.zone_type, m.zone_id),
                None => (String::new(), Uuid::nil()),
            };

            let user_metric = zone::CpuNsecUser {
                zone_name: zone_name.clone().into(),
                zone_type: zone_type.clone().into(),
                zone_id,
                datum: Cumulative::with_start_time(
                    *creation_time,
                    nsec_user.ok_or(Error::NoSuchKstat)?,
                ),
            };
            let user_sample = Sample::new_with_timestamp(
                snapshot_time,
                &self.target,
                &user_metric,
            )
            .map_err(Error::Sample)?;
            samples.push(user_sample);

            let sys_metric = zone::CpuNsecSys {
                zone_name: zone_name.clone().into(),
                zone_type: zone_type.clone().into(),
                zone_id,
                datum: Cumulative::with_start_time(
                    *creation_time,
                    nsec_sys.ok_or(Error::NoSuchKstat)?,
                ),
            };
            let sys_sample = Sample::new_with_timestamp(
                snapshot_time,
                &self.target,
                &sys_metric,
            )
            .map_err(Error::Sample)?;
            samples.push(sys_sample);

            let waitrq_metric = zone::CpuNsecWaitrq {
                zone_name: zone_name.into(),
                zone_type: zone_type.into(),
                zone_id,
                datum: Cumulative::with_start_time(
                    *creation_time,
                    nsec_waitrq.ok_or(Error::NoSuchKstat)?,
                ),
            };
            let waitrq_sample = Sample::new_with_timestamp(
                snapshot_time,
                &self.target,
                &waitrq_metric,
            )
            .map_err(Error::Sample)?;
            samples.push(waitrq_sample);
        }

        Ok(samples)
    }
}

// NOTE: Delegate to the inner target type for this implementation.
impl Target for Zone {
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

#[cfg(test)]
mod parse_tests {
    use super::*;

    #[test]
    fn test_parse_zone_name_omicron_zone() {
        let metadata = parse_zone_name(
            "oxz_cockroachdb_2be512e2-e127-40f0-95a4-67763ac02185",
        )
        .unwrap();
        assert_eq!(metadata.zone_type, "cockroachdb");
        assert_eq!(
            metadata.zone_id,
            "2be512e2-e127-40f0-95a4-67763ac02185".parse::<Uuid>().unwrap()
        );
    }

    #[test]
    fn test_parse_zone_name_no_prefix() {
        assert!(parse_zone_name("global").is_none());
    }

    #[test]
    fn test_parse_zone_name_no_uuid() {
        let metadata = parse_zone_name("oxz_switch").unwrap();
        assert_eq!(metadata.zone_type, "switch");
        assert_eq!(metadata.zone_id, Uuid::nil());
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

    /// The metric names we expect to produce for each zone.
    const ZONE_METRICS: &[&str] =
        &["cpu_nsec_user", "cpu_nsec_sys", "cpu_nsec_waitrq"];

    fn test_logger() -> Logger {
        let dec =
            slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(dec).build().fuse();
        Logger::root(drain, slog::o!("component" => "fake-cleanup-task"))
    }

    const RACK_ID: Uuid = uuid!("de784702-cafb-41a9-b3e5-93af189def29");
    const SLED_ID: Uuid = uuid!("88240343-5262-45f4-86f1-3c82fe383f2a");
    const SLED_MODEL: &str = "fake-gimlet";
    const SLED_REVISION: u32 = 1;
    const SLED_SERIAL: &str = "fake-serial";

    fn test_target() -> ZoneTarget {
        ZoneTarget {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
            sled_serial: SLED_SERIAL.into(),
        }
    }

    #[test]
    fn test_kstat_interested() {
        let mut zone = Zone::new(test_target(), false);

        let ctl = Ctl::new().unwrap();
        let ctl = ctl.update().unwrap();

        // There should be at least the global zone kstat.
        let kstat = ctl
            .filter(Some("zones"), None, None)
            .next()
            .expect("should have at least one zones kstat");

        // Not interested when not time synced.
        assert!(!zone.interested(&kstat));

        // Interested when time synced.
        zone.time_synced = true;
        assert!(zone.interested(&kstat));

        // Not interested in non-zone kstats.
        if let Some(cpu_kstat) =
            ctl.filter(Some("cpu"), Some(0), Some("sys")).next()
        {
            assert!(!zone.interested(&cpu_kstat));
        }
    }

    #[test]
    fn test_zone_cpu_samples() {
        let zone = Zone::new(test_target(), true);
        let ctl = Ctl::new().unwrap();
        let ctl = ctl.update().unwrap();

        // Collect kstats for the first zone we find.
        let mut kstat = ctl
            .filter(Some("zones"), None, None)
            .next()
            .expect("should have at least one zones kstat");
        let creation_time = hrtime_to_utc(kstat.ks_crtime).unwrap();
        let data = ctl.read(&mut kstat).unwrap();
        let samples = zone.to_samples(&[(creation_time, kstat, data)]).unwrap();

        // We should get exactly 3 samples (one per metric) for one zone.
        assert_eq!(samples.len(), 3);

        // Verify we got one sample for each expected metric.
        let mut metric_names: Vec<_> =
            samples.iter().map(|s| s.timeseries_name.to_string()).collect();
        metric_names.sort();

        let mut expected: Vec<_> =
            ZONE_METRICS.iter().map(|m| format!("zone:{m}")).collect();
        expected.sort();
        assert_eq!(metric_names, expected);

        // All samples should have the same zone_name field.
        let zone_names: Vec<_> = samples
            .iter()
            .filter_map(|s| {
                s.sorted_metric_fields().get("zone_name").and_then(|f| match &f
                    .value
                {
                    oximeter::FieldValue::String(s) => {
                        Some(s.as_ref().to_string())
                    }
                    _ => None,
                })
            })
            .collect();
        assert_eq!(zone_names.len(), 3);
        assert!(
            zone_names.windows(2).all(|w| w[0] == w[1]),
            "all samples should have the same zone_name"
        );
    }

    #[tokio::test]
    async fn test_kstat_sampler() {
        let mut sampler = KstatSampler::new(&test_logger()).unwrap();
        let zone = Zone::new(test_target(), true);
        let details = CollectionDetails::never(Duration::from_secs(1));
        let id = sampler.add_target(zone, details).await.unwrap();
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

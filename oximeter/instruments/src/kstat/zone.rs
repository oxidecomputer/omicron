// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Report metrics about zones on the host system

use crate::kstat::ConvertNamedData;
use crate::kstat::Error;
use crate::kstat::KstatList;
use crate::kstat::KstatTarget;
use crate::kstat::hrtime_to_utc;
use crate::kstat::n_processors;
use kstat_rs::Data;
use kstat_rs::Kstat;
use kstat_rs::Named;
use oximeter::FieldType;
use oximeter::FieldValue;
use oximeter::Sample;
use oximeter::Target;
use oximeter::types::Cumulative;
use uuid::Uuid;

/// The prefix for zone CPU kstat fields.
const CPU_NSEC_PREFIX: &str = "nsec_";

/// The CPU states we track from zone kstats.
const CPU_STATES: &[&str] = &["user", "sys", "waitrq"];

/// The prefix used for Omicron zone names.
const ZONE_PREFIX: &str = "oxz_";

/// The maximum cardinality of the data we produce, per sampling interval.
pub fn max_cardinality() -> usize {
    CPU_STATES.len() * n_processors().unwrap_or(1024)
}

/// Parsed zone metadata from a zone name.
struct ZoneMetadata {
    zone_type: String,
    zone_id: Uuid,
}

/// Extract zone metadata from a zone by name. If formatted as oxz_TYPE_UUID,
/// extract the type and UUID. For zones not formatted following this
/// convention, such as "global" or "oxz_switch", strip the "oxz_" prefix, and
/// treat the result as the zone type.
fn parse_zone_name(zone_name: &str) -> ZoneMetadata {
    if let Some(metadata) = parse_zone_name_prefix_uuid(zone_name) {
        metadata
    } else if let Some(rest) = zone_name.strip_prefix(ZONE_PREFIX) {
        ZoneMetadata { zone_type: rest.to_string(), zone_id: Uuid::nil() }
    } else {
        ZoneMetadata { zone_type: zone_name.into(), zone_id: Uuid::nil() }
    }
}

// Parse a zone name formatted as oxz_TYPE_UUID.
fn parse_zone_name_prefix_uuid(zone_name: &str) -> Option<ZoneMetadata> {
    let rest = zone_name.strip_prefix(ZONE_PREFIX)?;
    let (zone_type, uuid_str) = rest.rsplit_once('_')?;
    let zone_id = uuid_str.parse().ok()?;
    Some(ZoneMetadata { zone_type: zone_type.to_string(), zone_id })
}

oximeter::use_timeseries!("zone.toml");
pub use self::zone::Zone as ZoneTarget;

/// CPU metrics for all zones on a sled.
#[derive(Clone, Debug)]
pub struct Zone {
    /// The oximeter target for this zone's metrics.
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

            /* Parse zone kstats into cpu samples.

            Stats for the zone module look like this (stats we don't use elided):

            ...
            zones:26:oxz_cockroachdb_8bbea076-ff60-:nsec_sys        112675830670973
            zones:26:oxz_cockroachdb_8bbea076-ff60-:nsec_user       550830053620923
            zones:26:oxz_cockroachdb_8bbea076-ff60-:nsec_waitrq     9211749392692
            zones:26:oxz_cockroachdb_8bbea076-ff60-:zonename        oxz_cockroachdb_8bbea076-ff60-4330-8302-383e18140ef3

            The zone name in the identifier is truncated, so use the
            zonename statistic instead. Then parse cpu-related
            statistics into a cpu_nsec metric labeled by state.
            */

            // Must have exactly one statistic called "zonename".
            let zone_name = named
                .iter()
                .find(|n| n.name == "zonename")
                .ok_or(Error::NoSuchKstat)
                .and_then(|n| n.value.as_str())?
                .to_string();
            let zone_metadata = parse_zone_name(&zone_name);

            for named_data in named.iter() {
                let Named { name, value } = named_data;

                let Some(state) = name.strip_prefix(CPU_NSEC_PREFIX) else {
                    continue;
                };
                if !CPU_STATES.contains(&state) {
                    continue;
                }

                let datum = value.as_u64()?;
                let metric = zone::CpuNsec {
                    zone_name: zone_name.clone().into(),
                    zone_type: zone_metadata.zone_type.clone().into(),
                    zone_id: zone_metadata.zone_id,
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
        );
        assert_eq!(metadata.zone_type, "cockroachdb");
        assert_eq!(
            metadata.zone_id,
            "2be512e2-e127-40f0-95a4-67763ac02185".parse::<Uuid>().unwrap()
        );
    }

    #[test]
    fn test_parse_zone_name_no_prefix() {
        let metadata = parse_zone_name("global");
        assert_eq!(metadata.zone_type, "global");
        assert_eq!(metadata.zone_id, Uuid::nil());
    }

    #[test]
    fn test_parse_zone_name_no_uuid() {
        let metadata = parse_zone_name("oxz_switch");
        assert_eq!(metadata.zone_type, "switch");
        assert_eq!(metadata.zone_id, Uuid::nil());
    }

    #[test]
    fn test_parse_zone_name_invalid_uuid() {
        let metadata = parse_zone_name("oxz_foo_bar");
        assert_eq!(metadata.zone_type, "foo_bar");
        assert_eq!(metadata.zone_id, Uuid::nil());
    }
}

#[cfg(all(test, target_os = "illumos"))]
mod tests {
    use super::*;
    use kstat_rs::Ctl;
    use uuid::Uuid;
    use uuid::uuid;

    /// The metric name we expect to produce for each zone.
    const ZONE_METRIC: &str = "cpu_nsec";

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
    fn test_zone_samples() {
        let zone = Zone::new(test_target(), true);
        let ctl = Ctl::new().unwrap();
        let ctl = ctl.update().unwrap();

        // Collect kstats for the first reported zone.
        let mut kstat = ctl
            .filter(Some("zones"), None, None)
            .next()
            .expect("should have at least one zones kstat");
        let creation_time = hrtime_to_utc(kstat.ks_crtime).unwrap();
        let data = ctl.read(&mut kstat).unwrap();
        let samples = zone.to_samples(&[(creation_time, kstat, data)]).unwrap();

        // Assert that all metrics have the expected timeseries name.
        assert!(
            samples
                .iter()
                .all(|s| s.timeseries_name == format!("zone:{ZONE_METRIC}"))
        );

        // Extract the state from each sample.
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

        // Assert that we found all expected cpu states.
        let mut expected: Vec<_> =
            CPU_STATES.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(states, expected);
    }
}

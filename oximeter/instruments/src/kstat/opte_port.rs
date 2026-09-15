// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Report metrics about OPTE ports on the host system.

use crate::kstat::{
    ConvertNamedData, Error, KstatList, KstatTarget, hrtime_to_utc,
};
use chrono::{DateTime, Utc};
use kstat_rs::{Data, Kstat, Named};
use oximeter::{FieldType, FieldValue, Sample, Target, types::Cumulative};

oximeter::use_timeseries!("opte-port.toml");
pub use self::opte_port::OptePort as OptePortTarget;

#[derive(Clone, Debug)]
pub struct OptePort {
    /// The target for this port.
    pub target: OptePortTarget,
    /// Flag indicating whether the sled associated with this link is synced with
    /// NTP.
    pub time_synced: bool,
}

impl OptePort {
    pub fn new(target: OptePortTarget, time_synced: bool) -> Self {
        Self { target, time_synced }
    }

    pub fn name(&self) -> &str {
        &self.target.port_name
    }
}

// OPTE includes multiple kstat providers:
//
// * An `XdeStats` provider, which uses a fixed ks_name of "xde".
// * A `PortStats` provider, which uses the port name (e.g. opte0) as its
//   ks_name.
// * A `RouteCacheStats` provider, which uses <PORT_NAME>_route_cache as its
//   ks_name.
// * A `LayerStats` provider, which uses <PORT_NAME>_<LAYER_NAME> as its
//   ks_name.
//
// We capture metrics from the `PortStats` and `RouteCacheStats` providers as of
// this writing, and may add others in the future.

fn is_port_kstat(port_name: &str, ks_name: &str) -> bool {
    ks_name == port_name
}

fn is_route_cache_kstat(port_name: &str, ks_name: &str) -> bool {
    let Some(suffix) = ks_name.strip_prefix(port_name) else {
        return false;
    };
    suffix == "_route_cache"
}

impl KstatTarget for OptePort {
    fn interested(&self, kstat: &Kstat<'_>) -> bool {
        self.time_synced
            && kstat.ks_module == "xde"
            && kstat.ks_instance == 0
            && (is_port_kstat(self.name(), kstat.ks_name)
                || is_route_cache_kstat(self.name(), kstat.ks_name))
    }

    fn to_samples(
        &self,
        kstats: KstatList<'_, '_>,
    ) -> Result<Vec<Sample>, Error> {
        let mut samples: Vec<Sample> = vec![];
        for (creation_time, kstat, data) in kstats.iter() {
            let snapshot_time = hrtime_to_utc(kstat.ks_snaptime)?;
            let Data::Named(named) = data else {
                return Err(Error::ExpectedNamedKstat);
            };
            let extract = if is_port_kstat(self.name(), kstat.ks_name) {
                extract_port_kstat
            } else if is_route_cache_kstat(self.name(), kstat.ks_name) {
                extract_route_cache_kstat
            } else {
                continue;
            };
            let result = named
                .iter()
                .filter_map(|nd| {
                    extract(self, nd, *creation_time, snapshot_time)
                })
                .collect::<Result<Vec<_>, _>>()?;
            samples.extend(result);
        }
        Ok(samples)
    }
}

// NOTE: Delegate to the inner target type for this implementation.
impl Target for OptePort {
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

// Note: As of this writing, OPTE exposes 141 distinct kstat values per port. We
// expose a limited subset as oximeter metrics to limit cardinality. Because the
// cardinality of OPTE port metrics is proportional to the number of NICs
// attached to a running instance, and because we allow one instance per CPU
// thread and eight NICs per instance, the worst-case cardinality of these
// metrics would be very high if we included all OPTE metrics.

/// Extract a subset of values from a port kstat.
///
/// Note: OPTE exposes `in_uft_capacity` and `out_uft_capacity` kstat values,
/// but because they're constant values of 524288, we don't expose them as
/// oximeter metrics.
fn extract_port_kstat(
    target: &OptePort,
    named_data: &Named,
    creation_time: DateTime<Utc>,
    snapshot_time: DateTime<Utc>,
) -> Option<Result<Sample, Error>> {
    let Named { name, value } = named_data;
    if *name == "in_uft_hit" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::InUftHits {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "in_uft_miss" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::InUftMisses {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "in_uft_flows" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::InUftFlows { datum: x };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "in_uft_evictions" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::InUftEvictions {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "out_uft_hit" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::OutUftHits {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "out_uft_miss" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::OutUftMisses {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "out_uft_flows" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::OutUftFlows { datum: x };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "out_uft_evictions" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::OutUftEvictions {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else {
        None
    }
}

/// Extract a subset of values from a route cache kstat.
///
/// Note: OPTE exposes a `capacity` kstat value for the route cache, but
/// because it's a constant (8192), we don't expose it as an oximeter metric.
fn extract_route_cache_kstat(
    target: &OptePort,
    named_data: &Named,
    creation_time: DateTime<Utc>,
    snapshot_time: DateTime<Utc>,
) -> Option<Result<Sample, Error>> {
    let Named { name, value } = named_data;
    if *name == "hit" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::RouteCacheHits {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "miss" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::RouteCacheMisses {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "occupancy" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::RouteCacheOccupancy { datum: x };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "table_full" {
        Some(value.as_u64().and_then(|x| {
            let metric = opte_port::RouteCacheTableFull {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kstat_rs::NamedData;
    use oximeter::Datum;
    use uuid::Uuid;
    use uuid::uuid;

    const NIC_ID: Uuid = uuid!("de784702-cafb-41a9-b3e5-93af189def29");
    const PARENT_ID: Uuid = uuid!("de784702-cafb-41a9-b3e5-93af189def29");

    const RACK_ID: Uuid = uuid!("de784702-cafb-41a9-b3e5-93af189def29");
    const SLED_ID: Uuid = uuid!("88240343-5262-45f4-86f1-3c82fe383f2a");
    const SLED_MODEL: &str = "fake-gimlet";
    const SLED_REVISION: u32 = 1;
    const SLED_SERIAL: &str = "fake-serial";
    const ZONE_NAME: &str = "global";

    fn opte_port(name: &'static str) -> OptePort {
        OptePort::new(
            OptePortTarget {
                port_name: name.into(),
                interface_kind: "instance".into(),
                interface_id: NIC_ID,
                parent_id: PARENT_ID,
                zone_name: ZONE_NAME.into(),

                rack_id: RACK_ID,
                sled_id: SLED_ID,
                sled_serial: SLED_SERIAL.into(),
                sled_model: SLED_MODEL.into(),
                sled_revision: SLED_REVISION,
            },
            true,
        )
    }

    #[test]
    fn test_interested_positive() {
        let port = opte_port("opte1");
        assert!(port.interested(&Kstat::with_null_kstat("xde", 0, "opte1")));
        assert!(port.interested(&Kstat::with_null_kstat(
            "xde",
            0,
            "opte1_route_cache"
        )));
    }

    #[test]
    fn test_interested_negative() {
        let mut port = opte_port("opte1");
        assert!(!port.interested(&Kstat::with_null_kstat("xde", 0, "opte0")));
        assert!(!port.interested(&Kstat::with_null_kstat("link", 0, "opte1")));
        assert!(!port.interested(&Kstat::with_null_kstat("xde", 0, "opte10")));
        assert!(!port.interested(&Kstat::with_null_kstat(
            "xde",
            0,
            "opte0_route_cache"
        )));

        port.time_synced = false;
        assert!(!port.interested(&Kstat::with_null_kstat("xde", 0, "opte1")));
    }

    fn datum_value(sample: &Sample) -> u64 {
        match sample.measurement.datum() {
            Datum::CumulativeU64(value) => value.value(),
            Datum::U64(value) => *value,
            other => panic!("expected a CumulativeU64, found {other:?}"),
        }
    }

    #[test]
    fn test_to_samples() {
        let port = opte_port("opte1");

        let port_data = Data::Named(vec![
            Named { name: "in_uft_hit", value: NamedData::UInt64(1) },
            Named { name: "in_uft_miss", value: NamedData::UInt64(2) },
            Named { name: "in_uft_flows", value: NamedData::UInt64(3) },
            Named { name: "in_uft_evictions", value: NamedData::UInt64(4) },
            Named { name: "out_uft_hit", value: NamedData::UInt64(5) },
            Named { name: "out_uft_miss", value: NamedData::UInt64(6) },
            Named { name: "out_uft_flows", value: NamedData::UInt64(7) },
            Named { name: "out_uft_evictions", value: NamedData::UInt64(8) },
            Named { name: "unused", value: NamedData::UInt64(9) },
        ]);
        let cache_data = Data::Named(vec![
            Named { name: "hit", value: NamedData::UInt64(10) },
            Named { name: "miss", value: NamedData::UInt64(11) },
            Named { name: "occupancy", value: NamedData::UInt64(12) },
            Named { name: "table_full", value: NamedData::UInt64(13) },
            Named { name: "unused", value: NamedData::UInt64(14) },
        ]);
        let port_kstat = Kstat::with_null_kstat("xde", 0, "opte1");
        let cache_kstat = Kstat::with_null_kstat("xde", 0, "opte1_route_cache");

        let samples = port
            .to_samples(&[
                (Utc::now(), port_kstat, port_data),
                (Utc::now(), cache_kstat, cache_data),
            ])
            .unwrap();
        let values = samples
            .iter()
            .map(|s| (s.timeseries_name.to_string(), datum_value(s)))
            .collect::<Vec<_>>();

        assert_eq!(
            values,
            [
                ("opte_port:in_uft_hits".to_string(), 1),
                ("opte_port:in_uft_misses".to_string(), 2),
                ("opte_port:in_uft_flows".to_string(), 3),
                ("opte_port:in_uft_evictions".to_string(), 4),
                ("opte_port:out_uft_hits".to_string(), 5),
                ("opte_port:out_uft_misses".to_string(), 6),
                ("opte_port:out_uft_flows".to_string(), 7),
                ("opte_port:out_uft_evictions".to_string(), 8),
                ("opte_port:route_cache_hits".to_string(), 10),
                ("opte_port:route_cache_misses".to_string(), 11),
                ("opte_port:route_cache_occupancy".to_string(), 12),
                ("opte_port:route_cache_table_full".to_string(), 13),
            ]
        )
    }
}

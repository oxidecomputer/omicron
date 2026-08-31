// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle types.

use crate::fm::ereport::EreportFilters;
use crate::support_bundle::BundleDataSelection;
use crate::support_bundle::BundleTimeRange;
use crate::support_bundle::BundleZoneType;
use crate::support_bundle::SledSelection;
use crate::support_bundle::ZoneSelection;
use omicron_common::api::external::Error;

pub use nexus_types_versions::latest::support_bundle::*;

impl From<SupportBundleZoneType> for BundleZoneType {
    fn from(api: SupportBundleZoneType) -> Self {
        match api {
            SupportBundleZoneType::Global => Self::Global,
            SupportBundleZoneType::Switch => Self::Switch,
            SupportBundleZoneType::Propolis => Self::Propolis,
            SupportBundleZoneType::Ntp => Self::Ntp,
            SupportBundleZoneType::Clickhouse => Self::Clickhouse,
            SupportBundleZoneType::ClickhouseKeeper => Self::ClickhouseKeeper,
            SupportBundleZoneType::ClickhouseServer => Self::ClickhouseServer,
            SupportBundleZoneType::CockroachDb => Self::CockroachDb,
            SupportBundleZoneType::Crucible => Self::Crucible,
            SupportBundleZoneType::CruciblePantry => Self::CruciblePantry,
            SupportBundleZoneType::ExternalDns => Self::ExternalDns,
            SupportBundleZoneType::InternalDns => Self::InternalDns,
            SupportBundleZoneType::Nexus => Self::Nexus,
            SupportBundleZoneType::Oximeter => Self::Oximeter,
        }
    }
}

impl From<BundleZoneType> for SupportBundleZoneType {
    fn from(zone_type: BundleZoneType) -> Self {
        match zone_type {
            BundleZoneType::Global => Self::Global,
            BundleZoneType::Switch => Self::Switch,
            BundleZoneType::Propolis => Self::Propolis,
            BundleZoneType::Ntp => Self::Ntp,
            BundleZoneType::Clickhouse => Self::Clickhouse,
            BundleZoneType::ClickhouseKeeper => Self::ClickhouseKeeper,
            BundleZoneType::ClickhouseServer => Self::ClickhouseServer,
            BundleZoneType::CockroachDb => Self::CockroachDb,
            BundleZoneType::Crucible => Self::Crucible,
            BundleZoneType::CruciblePantry => Self::CruciblePantry,
            BundleZoneType::ExternalDns => Self::ExternalDns,
            BundleZoneType::InternalDns => Self::InternalDns,
            BundleZoneType::Nexus => Self::Nexus,
            BundleZoneType::Oximeter => Self::Oximeter,
        }
    }
}

impl TryFrom<SupportBundleDataSelection> for BundleDataSelection {
    type Error = Error;

    fn try_from(api: SupportBundleDataSelection) -> Result<Self, Error> {
        let SupportBundleDataSelection { data, start_time, end_time } = api;

        let selection = match data {
            SupportBundleData::All => BundleDataSelection::all(),
            SupportBundleData::Explicit {
                reconfigurator,
                sled_cubby_info,
                sp_dumps,
                host_info,
                ereports,
            } => {
                let mut selection = BundleDataSelection::new();
                if reconfigurator {
                    selection = selection.with_reconfigurator();
                }
                if sled_cubby_info {
                    selection = selection.with_sled_cubby_info();
                }
                if sp_dumps {
                    selection = selection.with_sp_dumps();
                }
                if let Some(host_info) = host_info {
                    selection = match host_info.sleds {
                        SupportBundleSledSelection::All => {
                            selection.with_all_sleds()
                        }
                        SupportBundleSledSelection::Specific { sleds } => {
                            selection.with_specific_sleds(sleds)
                        }
                    };
                    // The sled builders default the zone selection to All,
                    // so only a specific selection needs to be applied.
                    if let SupportBundleZoneSelection::Specific { types } =
                        host_info.zones
                    {
                        selection = selection.with_zone_types(
                            types.into_iter().map(BundleZoneType::from),
                        );
                    }
                }
                if let Some(ereports) = ereports {
                    selection = selection.with_ereports(
                        EreportFilters::new()
                            .with_serials(ereports.only_serials)
                            .with_classes(ereports.only_classes),
                    );
                }
                selection
            }
        };

        let time_range = BundleTimeRange::new(start_time, end_time)
            .map_err(|e| Error::invalid_request(e.to_string()))?;
        Ok(selection.with_time_range(time_range))
    }
}

impl From<&BundleDataSelection> for SupportBundleDataSelection {
    fn from(selection: &BundleDataSelection) -> Self {
        // The stored selection is always a concrete set of categories, so
        // this is always the explicit form.
        let data = SupportBundleData::Explicit {
            reconfigurator: selection.contains_reconfigurator(),
            sled_cubby_info: selection.contains_sled_cubby_info(),
            sp_dumps: selection.contains_sp_dumps(),
            host_info: selection.sled_selection().map(|sleds| {
                SupportBundleHostInfo {
                    sleds: match sleds {
                        SledSelection::All => SupportBundleSledSelection::All,
                        SledSelection::Specific(sleds) => {
                            SupportBundleSledSelection::Specific {
                                sleds: sleds.iter().copied().collect(),
                            }
                        }
                    },
                    // The zone selection lives on the same host-info entry
                    // as the sled selection, so it is present here; None
                    // would only mean host info is absent entirely.
                    zones: match selection.zone_selection() {
                        None | Some(ZoneSelection::All) => {
                            SupportBundleZoneSelection::All
                        }
                        Some(ZoneSelection::Types(types)) => {
                            SupportBundleZoneSelection::Specific {
                                types: types
                                    .iter()
                                    .copied()
                                    .map(Into::into)
                                    .collect(),
                            }
                        }
                    },
                }
            }),
            ereports: selection.ereport_filters().map(|filters| {
                SupportBundleEreports {
                    only_serials: filters.only_serials().to_vec(),
                    only_classes: filters.only_classes().to_vec(),
                }
            }),
        };

        let range = selection.time_range();
        Self { data, start_time: range.start(), end_time: range.end() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use chrono::Utc;
    use omicron_uuid_kinds::SledUuid;
    use proptest::prelude::*;
    use test_strategy::proptest;

    /// Every stored selection survives a trip through the API type.
    #[proptest]
    fn api_type_round_trip(selection: BundleDataSelection) {
        let api = SupportBundleDataSelection::from(&selection);
        let back = BundleDataSelection::try_from(api)
            .expect("a stored selection has an ordered time range");
        prop_assert_eq!(selection, back);
    }

    #[test]
    fn all_selects_every_category() {
        let api = SupportBundleDataSelection {
            data: SupportBundleData::All,
            start_time: None,
            end_time: None,
        };
        let selection = BundleDataSelection::try_from(api).unwrap();
        assert_eq!(selection, BundleDataSelection::all());
    }

    #[test]
    fn an_empty_explicit_selection_selects_nothing() {
        let api = SupportBundleDataSelection {
            data: SupportBundleData::Explicit {
                reconfigurator: false,
                sled_cubby_info: false,
                sp_dumps: false,
                host_info: None,
                ereports: None,
            },
            start_time: None,
            end_time: None,
        };
        let selection = BundleDataSelection::try_from(api).unwrap();
        assert_eq!(selection, BundleDataSelection::new());
    }

    #[test]
    fn explicit_selection_carries_per_category_settings() {
        let sled = SledUuid::new_v4();
        let api = SupportBundleDataSelection {
            data: SupportBundleData::Explicit {
                reconfigurator: true,
                sled_cubby_info: false,
                sp_dumps: false,
                host_info: Some(SupportBundleHostInfo {
                    sleds: SupportBundleSledSelection::Specific {
                        sleds: vec![sled],
                    },
                    zones: SupportBundleZoneSelection::Specific {
                        types: vec![
                            SupportBundleZoneType::Nexus,
                            SupportBundleZoneType::Global,
                        ],
                    },
                }),
                ereports: Some(SupportBundleEreports {
                    only_serials: vec!["BRM-FAKE-0".to_string()],
                    only_classes: vec!["fake.class".to_string()],
                }),
            },
            start_time: None,
            end_time: None,
        };
        let selection = BundleDataSelection::try_from(api).unwrap();

        assert!(selection.contains_reconfigurator());
        assert!(!selection.contains_sled_cubby_info());
        assert!(!selection.contains_sp_dumps());
        assert_eq!(
            selection.sled_selection(),
            Some(&SledSelection::Specific([sled].into_iter().collect()))
        );
        assert_eq!(
            selection.zone_selection(),
            Some(&ZoneSelection::Types(
                [BundleZoneType::Nexus, BundleZoneType::Global]
                    .into_iter()
                    .collect()
            ))
        );
        let filters = selection.ereport_filters().unwrap();
        assert_eq!(filters.only_serials(), ["BRM-FAKE-0"]);
        assert_eq!(filters.only_classes(), ["fake.class"]);
    }

    #[test]
    fn an_inverted_time_range_is_a_bad_request() {
        let ts = |secs| DateTime::<Utc>::from_timestamp(secs, 0).unwrap();
        let api = SupportBundleDataSelection {
            data: SupportBundleData::All,
            start_time: Some(ts(200)),
            end_time: Some(ts(100)),
        };
        let err = BundleDataSelection::try_from(api).unwrap_err();
        assert!(
            matches!(err, Error::InvalidRequest { .. }),
            "unexpected error: {err:?}"
        );
    }
}

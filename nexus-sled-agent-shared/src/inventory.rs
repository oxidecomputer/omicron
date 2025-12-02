// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inventory types shared between Nexus and sled-agent.

use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use daft::Diffable;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use indent_write::fmt::IndentWriter;
use omicron_common::disk::{DatasetKind, DatasetName, M2Slot};
use omicron_common::ledger::Ledgerable;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_common::update::OmicronZoneManifestSource;
use omicron_common::{
    api::{
        external::{ByteCount, Generation},
        internal::shared::{NetworkInterface, SourceNatConfig},
    },
    disk::{DatasetConfig, DiskVariant, OmicronPhysicalDiskConfig},
    update::ArtifactId,
    zpool_name::ZpoolName,
};
use omicron_uuid_kinds::{
    DatasetUuid, InternalZpoolUuid, MupdateUuid, OmicronZoneUuid,
};
use omicron_uuid_kinds::{MupdateOverrideUuid, PhysicalDiskUuid};
use omicron_uuid_kinds::{SledUuid, ZpoolUuid};
use schemars::schema::{Schema, SchemaObject};
use schemars::{JsonSchema, SchemaGenerator};
use serde::{Deserialize, Serialize};
// Export these types for convenience -- this way, dependents don't have to
// depend on sled-hardware-types.
pub use sled_hardware_types::{Baseboard, SledCpuFamily};
use strum::EnumIter;
use tufaceous_artifact::{ArtifactHash, KnownArtifactKind};

/// Re-export The latest versions of each type that has older versions
pub use nexus_sled_agent_shared_migrations::latest::*;

fn default_nexus_lockstep_port() -> u16 {
    omicron_common::address::NEXUS_LOCKSTEP_PORT
}

/// Like [`OmicronZoneType`], but without any associated data.
///
/// This enum is meant to correspond exactly 1:1 with `OmicronZoneType`.
///
/// # String representations of this type
///
/// There are no fewer than six string representations for this type, all
/// slightly different from each other.
///
/// 1. [`Self::zone_prefix`]: Used to construct zone names.
/// 2. [`Self::service_prefix`]: Used to construct SMF service names.
/// 3. [`Self::name_prefix`]: Used to construct `Name` instances.
/// 4. [`Self::report_str`]: Used for reporting and testing.
/// 5. [`Self::artifact_id_name`]: Used to match TUF artifact IDs.
/// 6. [`Self::artifact_in_install_dataset`]: Used to match zone image tarballs
///    in the install dataset. (This method is equivalent to appending `.tar.gz`
///    to the result of [`Self::zone_prefix`].)
///
/// There is no `Display` impl to ensure that users explicitly choose the
/// representation they want. (Please play close attention to this! The
/// functions are all similar but different, and we don't currently have great
/// type safety around the choice.)
///
/// ## Adding new representations
///
/// If you have a new use case for a string representation, please reuse one of
/// the six representations if at all possible. If you must add a new one,
/// please add it here rather than doing something ad-hoc in the calling code
/// so it's more legible.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Diffable,
    EnumIter,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub enum ZoneKind {
    BoundaryNtp,
    Clickhouse,
    ClickhouseKeeper,
    ClickhouseServer,
    CockroachDb,
    Crucible,
    CruciblePantry,
    ExternalDns,
    InternalDns,
    InternalNtp,
    Nexus,
    Oximeter,
}

impl ZoneKind {
    /// The NTP prefix used for both BoundaryNtp and InternalNtp zones and
    /// services.
    pub const NTP_PREFIX: &'static str = "ntp";

    /// Return a string that is used to construct **zone names**. This string
    /// is guaranteed to be stable over time.
    pub fn zone_prefix(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp".
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => Self::NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::ClickhouseServer => "clickhouse_server",
            // Note "cockroachdb" for historical reasons.
            ZoneKind::CockroachDb => "cockroachdb",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible_pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string that identifies **zone image filenames** in the install
    /// dataset.
    ///
    /// This method is exactly equivalent to `format!("{}.tar.gz",
    /// self.zone_prefix())`, but returns `&'static str`s. A unit test ensures
    /// they stay consistent.
    pub fn artifact_in_install_dataset(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp".
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => "ntp.tar.gz",
            ZoneKind::Clickhouse => "clickhouse.tar.gz",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper.tar.gz",
            ZoneKind::ClickhouseServer => "clickhouse_server.tar.gz",
            // Note "cockroachdb" for historical reasons.
            ZoneKind::CockroachDb => "cockroachdb.tar.gz",
            ZoneKind::Crucible => "crucible.tar.gz",
            ZoneKind::CruciblePantry => "crucible_pantry.tar.gz",
            ZoneKind::ExternalDns => "external_dns.tar.gz",
            ZoneKind::InternalDns => "internal_dns.tar.gz",
            ZoneKind::Nexus => "nexus.tar.gz",
            ZoneKind::Oximeter => "oximeter.tar.gz",
        }
    }

    /// Return a string that is used to construct **SMF service names**. This
    /// string is guaranteed to be stable over time.
    pub fn service_prefix(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp".
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => Self::NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::ClickhouseServer => "clickhouse_server",
            // Note "cockroachdb" for historical reasons.
            ZoneKind::CockroachDb => "cockroachdb",
            ZoneKind::Crucible => "crucible",
            // Note "crucible/pantry" for historical reasons.
            ZoneKind::CruciblePantry => "crucible/pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string suitable for use **in `Name` instances**. This string
    /// is guaranteed to be stable over time.
    ///
    /// This string uses dashes rather than underscores, as required by `Name`.
    pub fn name_prefix(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp" here.
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => Self::NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse-keeper",
            ZoneKind::ClickhouseServer => "clickhouse-server",
            // Note "cockroach" for historical reasons.
            ZoneKind::CockroachDb => "cockroach",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible-pantry",
            ZoneKind::ExternalDns => "external-dns",
            ZoneKind::InternalDns => "internal-dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string that is used for reporting and error messages. This is
    /// **not guaranteed** to be stable.
    ///
    /// If you're displaying a user-friendly message, prefer this method.
    pub fn report_str(self) -> &'static str {
        match self {
            ZoneKind::BoundaryNtp => "boundary_ntp",
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::ClickhouseServer => "clickhouse_server",
            ZoneKind::CockroachDb => "cockroach_db",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible_pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::InternalNtp => "internal_ntp",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string used as an artifact name for control-plane zones.
    /// This is **not guaranteed** to be stable.
    ///
    /// These strings match the `ArtifactId::name`s Nexus constructs when
    /// unpacking the composite control-plane artifact in a TUF repo. Currently,
    /// these are chosen by reading the `pkg` value of the `oxide.json` object
    /// inside each zone image tarball.
    pub fn artifact_id_name(self) -> &'static str {
        match self {
            ZoneKind::BoundaryNtp => "ntp",
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::ClickhouseServer => "clickhouse_server",
            ZoneKind::CockroachDb => "cockroachdb",
            ZoneKind::Crucible => "crucible-zone",
            ZoneKind::CruciblePantry => "crucible-pantry-zone",
            ZoneKind::ExternalDns => "external-dns",
            ZoneKind::InternalDns => "internal-dns",
            ZoneKind::InternalNtp => "ntp",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Map an artifact ID name to the corresponding file name in the install
    /// dataset.
    ///
    /// We don't allow mapping artifact ID names to `ZoneKind` because the map
    /// isn't bijective -- both internal and boundary NTP zones use the same
    /// `ntp` artifact. But the artifact ID name and the name in the install
    /// dataset do form a bijective map.
    pub fn artifact_id_name_to_install_dataset_file(
        artifact_id_name: &str,
    ) -> Option<&'static str> {
        let zone_kind = match artifact_id_name {
            // We arbitrarily select BoundaryNtp to perform the mapping with.
            "ntp" => ZoneKind::BoundaryNtp,
            "clickhouse" => ZoneKind::Clickhouse,
            "clickhouse_keeper" => ZoneKind::ClickhouseKeeper,
            "clickhouse_server" => ZoneKind::ClickhouseServer,
            "cockroachdb" => ZoneKind::CockroachDb,
            "crucible-zone" => ZoneKind::Crucible,
            "crucible-pantry-zone" => ZoneKind::CruciblePantry,
            "external-dns" => ZoneKind::ExternalDns,
            "internal-dns" => ZoneKind::InternalDns,
            "nexus" => ZoneKind::Nexus,
            "oximeter" => ZoneKind::Oximeter,
            _ => return None,
        };

        Some(zone_kind.artifact_in_install_dataset())
    }

    /// Return true if an artifact represents a control plane zone image
    /// of this kind.
    pub fn is_control_plane_zone_artifact(
        self,
        artifact_id: &ArtifactId,
    ) -> bool {
        artifact_id
            .kind
            .to_known()
            .map(|kind| matches!(kind, KnownArtifactKind::Zone))
            .unwrap_or(false)
            && artifact_id.name == self.artifact_id_name()
    }
}

#[cfg(test)]
mod tests {
    use omicron_common::api::external::Name;
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn test_name_prefixes() {
        for zone_kind in ZoneKind::iter() {
            let name_prefix = zone_kind.name_prefix();
            name_prefix.parse::<Name>().unwrap_or_else(|e| {
                panic!(
                    "failed to parse name prefix {:?} for zone kind {:?}: {}",
                    name_prefix, zone_kind, e
                );
            });
        }
    }

    #[test]
    fn test_zone_prefix_matches_artifact_in_install_dataset() {
        for zone_kind in ZoneKind::iter() {
            let zone_prefix = zone_kind.zone_prefix();
            let expected_artifact = format!("{zone_prefix}.tar.gz");
            assert_eq!(
                expected_artifact,
                zone_kind.artifact_in_install_dataset()
            );
        }
    }

    #[test]
    fn test_artifact_id_to_install_dataset_file() {
        for zone_kind in ZoneKind::iter() {
            let artifact_id_name = zone_kind.artifact_id_name();
            let expected_file = zone_kind.artifact_in_install_dataset();
            assert_eq!(
                Some(expected_file),
                ZoneKind::artifact_id_name_to_install_dataset_file(
                    artifact_id_name
                )
            );
        }
    }
}

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
fn path_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}

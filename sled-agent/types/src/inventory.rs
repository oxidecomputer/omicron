// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled-agent types for inventory.

use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::net::{IpAddr, Ipv6Addr};

use camino::Utf8PathBuf;
use iddqd::IdOrdMap;
use indent_write::fmt::IndentWriter;
use omicron_common::api::external::Generation;
use omicron_common::disk::{DatasetKind, DatasetName, M2Slot};
use omicron_common::update::{ArtifactId, OmicronZoneManifestSource};
use omicron_uuid_kinds::MupdateUuid;
use tufaceous_artifact::{ArtifactHash, KnownArtifactKind};

pub use sled_agent_types_versions::latest::inventory::*;

/// Generation 1 of `OmicronZonesConfig` is always the set of no zones.
pub const ZONES_CONFIG_INITIAL_GENERATION: Generation = Generation::from_u32(1);

/// The NTP prefix used for both BoundaryNtp and InternalNtp zones and services.
pub const NTP_PREFIX: &str = "ntp";

pub trait ZoneKindExt {
    /// Return a string that is used to construct **zone names**. This string
    /// is guaranteed to be stable over time.
    fn zone_prefix(self) -> &'static str;

    /// Return a string that identifies **zone image filenames** in the install
    /// dataset.
    ///
    /// This method is exactly equivalent to `format!("{}.tar.gz",
    /// self.zone_prefix())`, but returns `&'static str`s. A unit test ensures
    /// they stay consistent.
    fn artifact_in_install_dataset(self) -> &'static str;

    /// Return a string that is used to construct **SMF service names**. This
    /// string is guaranteed to be stable over time.
    fn service_prefix(self) -> &'static str;

    /// Return a string suitable for use **in `Name` instances**. This string
    /// is guaranteed to be stable over time.
    ///
    /// This string uses dashes rather than underscores, as required by `Name`.
    fn name_prefix(self) -> &'static str;

    /// Return a string that is used for reporting and error messages. This is
    /// **not guaranteed** to be stable.
    ///
    /// If you're displaying a user-friendly message, prefer this method.
    fn report_str(self) -> &'static str;

    /// Return a string used as an artifact name for control-plane zones.
    /// This is **not guaranteed** to be stable.
    ///
    /// These strings match the `ArtifactId::name`s Nexus constructs when
    /// unpacking the composite control-plane artifact in a TUF repo. Currently,
    /// these are chosen by reading the `pkg` value of the `oxide.json` object
    /// inside each zone image tarball.
    fn artifact_id_name(self) -> &'static str;

    /// Return true if an artifact represents a control plane zone image
    /// of this kind.
    fn is_control_plane_zone_artifact(self, artifact_id: &ArtifactId) -> bool;
}

impl ZoneKindExt for ZoneKind {
    fn zone_prefix(self) -> &'static str {
        match self {
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::ClickhouseServer => "clickhouse_server",
            ZoneKind::CockroachDb => "cockroachdb",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible_pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    fn artifact_in_install_dataset(self) -> &'static str {
        match self {
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => "ntp.tar.gz",
            ZoneKind::Clickhouse => "clickhouse.tar.gz",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper.tar.gz",
            ZoneKind::ClickhouseServer => "clickhouse_server.tar.gz",
            ZoneKind::CockroachDb => "cockroachdb.tar.gz",
            ZoneKind::Crucible => "crucible.tar.gz",
            ZoneKind::CruciblePantry => "crucible_pantry.tar.gz",
            ZoneKind::ExternalDns => "external_dns.tar.gz",
            ZoneKind::InternalDns => "internal_dns.tar.gz",
            ZoneKind::Nexus => "nexus.tar.gz",
            ZoneKind::Oximeter => "oximeter.tar.gz",
        }
    }

    fn service_prefix(self) -> &'static str {
        match self {
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::ClickhouseServer => "clickhouse_server",
            ZoneKind::CockroachDb => "cockroachdb",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible/pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    fn name_prefix(self) -> &'static str {
        match self {
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse-keeper",
            ZoneKind::ClickhouseServer => "clickhouse-server",
            ZoneKind::CockroachDb => "cockroach",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible-pantry",
            ZoneKind::ExternalDns => "external-dns",
            ZoneKind::InternalDns => "internal-dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    fn report_str(self) -> &'static str {
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

    fn artifact_id_name(self) -> &'static str {
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

    fn is_control_plane_zone_artifact(self, artifact_id: &ArtifactId) -> bool {
        artifact_id
            .kind
            .to_known()
            .map(|kind| matches!(kind, KnownArtifactKind::Zone))
            .unwrap_or(false)
            && artifact_id.name == self.artifact_id_name()
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

pub trait OmicronZoneTypeExt {
    /// Returns the [`ZoneKind`] corresponding to this variant.
    fn kind(&self) -> ZoneKind;

    /// Does this zone require time synchronization before it is initialized?"
    ///
    /// This function is somewhat conservative - the set of services
    /// that can be launched before timesync has completed is intentionally kept
    /// small, since it would be easy to add a service that expects time to be
    /// reasonably synchronized.
    fn requires_timesync(&self) -> bool;

    /// Returns the underlay IP address associated with this zone.
    ///
    /// Assumes all zone have exactly one underlay IP address (which is
    /// currently true).
    fn underlay_ip(&self) -> Ipv6Addr;

    /// Identifies whether this is an NTP zone
    fn is_ntp(&self) -> bool;

    /// Identifies whether this is a boundary NTP zone
    fn is_boundary_ntp(&self) -> bool;

    /// Identifies whether this is a Nexus zone
    fn is_nexus(&self) -> bool;

    /// Identifies whether this a Crucible (not Crucible pantry) zone
    fn is_crucible(&self) -> bool;

    /// This zone's external IP
    fn external_ip(&self) -> Option<IpAddr>;

    /// The service vNIC providing external connectivity to this zone
    fn service_vnic(
        &self,
    ) -> Option<&omicron_common::api::internal::shared::NetworkInterface>;

    /// If this kind of zone has an associated dataset, return the dataset's
    /// name. Otherwise, return `None`.
    fn dataset_name(&self) -> Option<DatasetName>;
}

impl OmicronZoneTypeExt for OmicronZoneType {
    fn kind(&self) -> ZoneKind {
        match self {
            OmicronZoneType::BoundaryNtp { .. } => ZoneKind::BoundaryNtp,
            OmicronZoneType::Clickhouse { .. } => ZoneKind::Clickhouse,
            OmicronZoneType::ClickhouseKeeper { .. } => {
                ZoneKind::ClickhouseKeeper
            }
            OmicronZoneType::ClickhouseServer { .. } => {
                ZoneKind::ClickhouseServer
            }
            OmicronZoneType::CockroachDb { .. } => ZoneKind::CockroachDb,
            OmicronZoneType::Crucible { .. } => ZoneKind::Crucible,
            OmicronZoneType::CruciblePantry { .. } => ZoneKind::CruciblePantry,
            OmicronZoneType::ExternalDns { .. } => ZoneKind::ExternalDns,
            OmicronZoneType::InternalDns { .. } => ZoneKind::InternalDns,
            OmicronZoneType::InternalNtp { .. } => ZoneKind::InternalNtp,
            OmicronZoneType::Nexus { .. } => ZoneKind::Nexus,
            OmicronZoneType::Oximeter { .. } => ZoneKind::Oximeter,
        }
    }

    fn requires_timesync(&self) -> bool {
        match self {
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::InternalDns { .. } => false,
            _ => true,
        }
    }

    fn underlay_ip(&self) -> Ipv6Addr {
        match self {
            OmicronZoneType::BoundaryNtp { address, .. }
            | OmicronZoneType::Clickhouse { address, .. }
            | OmicronZoneType::ClickhouseKeeper { address, .. }
            | OmicronZoneType::ClickhouseServer { address, .. }
            | OmicronZoneType::CockroachDb { address, .. }
            | OmicronZoneType::Crucible { address, .. }
            | OmicronZoneType::CruciblePantry { address }
            | OmicronZoneType::ExternalDns { http_address: address, .. }
            | OmicronZoneType::InternalNtp { address }
            | OmicronZoneType::Nexus { internal_address: address, .. }
            | OmicronZoneType::Oximeter { address } => *address.ip(),
            OmicronZoneType::InternalDns {
                http_address: address,
                dns_address,
                ..
            } => {
                debug_assert_eq!(address.ip(), dns_address.ip());
                *address.ip()
            }
        }
    }

    fn is_ntp(&self) -> bool {
        matches!(
            self,
            OmicronZoneType::BoundaryNtp { .. }
                | OmicronZoneType::InternalNtp { .. }
        )
    }

    fn is_boundary_ntp(&self) -> bool {
        matches!(self, OmicronZoneType::BoundaryNtp { .. })
    }

    fn is_nexus(&self) -> bool {
        matches!(self, OmicronZoneType::Nexus { .. })
    }

    fn is_crucible(&self) -> bool {
        matches!(self, OmicronZoneType::Crucible { .. })
    }

    fn external_ip(&self) -> Option<IpAddr> {
        match self {
            OmicronZoneType::Nexus { external_ip, .. } => Some(*external_ip),
            OmicronZoneType::ExternalDns { dns_address, .. } => {
                Some(dns_address.ip())
            }
            OmicronZoneType::BoundaryNtp { snat_cfg, .. } => Some(snat_cfg.ip),
            _ => None,
        }
    }

    fn service_vnic(
        &self,
    ) -> Option<&omicron_common::api::internal::shared::NetworkInterface> {
        match self {
            OmicronZoneType::Nexus { nic, .. }
            | OmicronZoneType::ExternalDns { nic, .. }
            | OmicronZoneType::BoundaryNtp { nic, .. } => Some(nic),
            _ => None,
        }
    }

    fn dataset_name(&self) -> Option<DatasetName> {
        let (dataset, dataset_kind) = match self {
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Nexus { .. }
            | OmicronZoneType::Oximeter { .. }
            | OmicronZoneType::CruciblePantry { .. } => None,
            OmicronZoneType::Clickhouse { dataset, .. } => {
                Some((dataset, DatasetKind::Clickhouse))
            }
            OmicronZoneType::ClickhouseKeeper { dataset, .. } => {
                Some((dataset, DatasetKind::ClickhouseKeeper))
            }
            OmicronZoneType::ClickhouseServer { dataset, .. } => {
                Some((dataset, DatasetKind::ClickhouseServer))
            }
            OmicronZoneType::CockroachDb { dataset, .. } => {
                Some((dataset, DatasetKind::Cockroach))
            }
            OmicronZoneType::Crucible { dataset, .. } => {
                Some((dataset, DatasetKind::Crucible))
            }
            OmicronZoneType::ExternalDns { dataset, .. } => {
                Some((dataset, DatasetKind::ExternalDns))
            }
            OmicronZoneType::InternalDns { dataset, .. } => {
                Some((dataset, DatasetKind::InternalDns))
            }
        }?;
        Some(DatasetName::new(dataset.pool_name, dataset_kind))
    }
}

pub trait OmicronZoneConfigExt {
    /// Returns the underlay IP address associated with this zone.
    ///
    /// Assumes all zone have exactly one underlay IP address (which is
    /// currently true).
    fn underlay_ip(&self) -> Ipv6Addr;

    fn zone_name(&self) -> String;

    fn dataset_name(&self) -> Option<DatasetName>;
}

impl OmicronZoneConfigExt for OmicronZoneConfig {
    fn underlay_ip(&self) -> Ipv6Addr {
        self.zone_type.underlay_ip()
    }

    fn zone_name(&self) -> String {
        illumos_utils::running_zone::InstalledZone::get_zone_name(
            self.zone_type.kind().zone_prefix(),
            Some(self.id),
        )
    }

    fn dataset_name(&self) -> Option<DatasetName> {
        self.zone_type.dataset_name()
    }
}

pub trait ConfigReconcilerInventoryExt {
    /// Iterate over all running zones as reported by the last reconciliation
    /// result.
    ///
    /// This includes zones that are both present in `last_reconciled_config`
    /// and whose status in `zones` indicates "successfully running".
    fn running_omicron_zones(&self)
    -> impl Iterator<Item = &OmicronZoneConfig>;

    /// Iterate over all zones contained in the most-recently-reconciled sled
    /// config and report their status as of that reconciliation.
    fn reconciled_omicron_zones(
        &self,
    ) -> impl Iterator<Item = (&OmicronZoneConfig, &ConfigReconcilerInventoryResult)>;

    /// Given a sled config, produce a reconciler result that sled-agent could
    /// have emitted if reconciliation succeeded.
    ///
    /// This method should only be used by tests and dev tools; real code should
    /// look at the actual `last_reconciliation` value from the parent
    /// [`Inventory`].
    fn debug_assume_success(
        config: OmicronSledConfig,
    ) -> ConfigReconcilerInventory;

    /// Given a sled config, update an existing reconciler result to simulate an
    /// output that sled-agent could have emitted if reconciliation succeeded.
    ///
    /// This method should only be used by tests and dev tools; real code should
    /// look at the actual `last_reconciliation` value from the parent
    /// [`Inventory`].
    fn debug_update_assume_success(&mut self, config: OmicronSledConfig);
}

impl ConfigReconcilerInventoryExt for ConfigReconcilerInventory {
    fn running_omicron_zones(
        &self,
    ) -> impl Iterator<Item = &OmicronZoneConfig> {
        self.zones.iter().filter_map(|(zone_id, result)| match result {
            ConfigReconcilerInventoryResult::Ok => {
                self.last_reconciled_config.zones.get(zone_id)
            }
            ConfigReconcilerInventoryResult::Err { .. } => None,
        })
    }

    fn reconciled_omicron_zones(
        &self,
    ) -> impl Iterator<Item = (&OmicronZoneConfig, &ConfigReconcilerInventoryResult)>
    {
        self.zones.iter().filter_map(|(zone_id, result)| {
            let config = self.last_reconciled_config.zones.get(zone_id)?;
            Some((config, result))
        })
    }

    fn debug_assume_success(
        config: OmicronSledConfig,
    ) -> ConfigReconcilerInventory {
        let mut ret = ConfigReconcilerInventory {
            last_reconciled_config: OmicronSledConfig::default(),
            external_disks: BTreeMap::new(),
            datasets: BTreeMap::new(),
            orphaned_datasets: IdOrdMap::new(),
            zones: BTreeMap::new(),
            remove_mupdate_override: None,
            boot_partitions: <BootPartitionContents as BootPartitionContentsExt>::debug_assume_success(),
        };
        ret.debug_update_assume_success(config);
        ret
    }

    fn debug_update_assume_success(&mut self, config: OmicronSledConfig) {
        let external_disks = config
            .disks
            .iter()
            .map(|d| (d.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let datasets = config
            .datasets
            .iter()
            .map(|d| (d.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let zones = config
            .zones
            .iter()
            .map(|z| (z.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let remove_mupdate_override =
            config.remove_mupdate_override.map(|_| {
                RemoveMupdateOverrideInventory {
                boot_disk_result: Ok(
                    RemoveMupdateOverrideBootSuccessInventory::Removed,
                ),
                non_boot_message:
                    "mupdate override successfully removed on non-boot disks"
                        .to_owned(),
            }
            });

        self.last_reconciled_config = config;
        self.external_disks = external_disks;
        self.datasets = datasets;
        self.orphaned_datasets = IdOrdMap::new();
        self.zones = zones;
        self.remove_mupdate_override = remove_mupdate_override;
    }
}

pub trait HostPhase2DesiredContentsExt {
    /// The artifact hash described by `self`, if it has one.
    fn artifact_hash(&self) -> Option<ArtifactHash>;
}

impl HostPhase2DesiredContentsExt for HostPhase2DesiredContents {
    fn artifact_hash(&self) -> Option<ArtifactHash> {
        match self {
            Self::CurrentContents => None,
            Self::Artifact { hash } => Some(*hash),
        }
    }
}

pub trait OmicronZoneImageSourceExt {
    /// Return the artifact hash used for the zone image, if the zone's image
    /// source is from the artifact store.
    fn artifact_hash(&self) -> Option<ArtifactHash>;
}

impl OmicronZoneImageSourceExt for OmicronZoneImageSource {
    fn artifact_hash(&self) -> Option<ArtifactHash> {
        if let OmicronZoneImageSource::Artifact { hash } = self {
            Some(*hash)
        } else {
            None
        }
    }
}

pub trait BootPartitionContentsExt {
    fn slot_details(
        &self,
        slot: M2Slot,
    ) -> &Result<BootPartitionDetails, String>;
    fn debug_assume_success() -> BootPartitionContents;
}

impl BootPartitionContentsExt for BootPartitionContents {
    fn slot_details(
        &self,
        slot: M2Slot,
    ) -> &Result<BootPartitionDetails, String> {
        match slot {
            M2Slot::A => &self.slot_a,
            M2Slot::B => &self.slot_b,
        }
    }

    fn debug_assume_success() -> BootPartitionContents {
        BootPartitionContents {
            boot_disk: Ok(M2Slot::A),
            slot_a: Ok(BootPartitionDetails {
                header: BootImageHeader {
                    flags: 0,
                    data_size: 1000,
                    image_size: 1000,
                    target_size: 1000,
                    sha256: [0; 32],
                    image_name: "fake from debug_assume_success()".to_string(),
                },
                artifact_hash: ArtifactHash([0x0a; 32]),
                artifact_size: 1000,
            }),
            slot_b: Ok(BootPartitionDetails {
                header: BootImageHeader {
                    flags: 0,
                    data_size: 1000,
                    image_size: 1000,
                    target_size: 1000,
                    sha256: [1; 32],
                    image_name: "fake from debug_assume_success()".to_string(),
                },
                artifact_hash: ArtifactHash([0x0b; 32]),
                artifact_size: 1000,
            }),
        }
    }
}

// Display helper types

pub struct ZoneImageResolverInventoryDisplay<'a> {
    inner: &'a ZoneImageResolverInventory,
}

impl fmt::Display for ZoneImageResolverInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ZoneImageResolverInventory { zone_manifest, mupdate_override } =
            self.inner;
        writeln!(f, "zone manifest:")?;
        let mut indented = IndentWriter::new("    ", f);
        write!(indented, "{}", zone_manifest.display())?;
        let f = indented.into_inner();
        writeln!(f, "mupdate override:")?;
        let mut indented = IndentWriter::new("    ", f);
        write!(indented, "{}", mupdate_override.display())?;
        Ok(())
    }
}

pub struct ZoneManifestInventoryDisplay<'a> {
    inner: &'a ZoneManifestInventory,
}

impl fmt::Display for ZoneManifestInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f;
        let ZoneManifestInventory {
            boot_disk_path,
            boot_inventory,
            non_boot_status,
        } = self.inner;
        writeln!(f, "path on boot disk: {}", boot_disk_path)?;
        match boot_inventory {
            Ok(boot_inventory) => {
                writeln!(f, "boot disk inventory:")?;
                let mut indented = IndentWriter::new("    ", f);
                write!(indented, "{}", boot_inventory.display())?;
                f = indented.into_inner();
            }
            Err(error) => {
                writeln!(
                    f,
                    "error obtaining zone manifest on boot disk: {error}"
                )?;
            }
        }
        if non_boot_status.is_empty() {
            writeln!(f, "no non-boot disks")?;
        } else {
            writeln!(f, "non-boot disk status:")?;
            for non_boot in non_boot_status {
                let mut indented = IndentWriter::new_skip_initial("    ", f);
                writeln!(indented, "  - {}", non_boot.display())?;
                f = indented.into_inner();
            }
        }
        Ok(())
    }
}

pub struct ZoneManifestBootInventoryDisplay<'a> {
    inner: &'a ZoneManifestBootInventory,
}

impl fmt::Display for ZoneManifestBootInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f;
        let ZoneManifestBootInventory { source, artifacts } = self.inner;
        writeln!(f, "manifest generated by {}", source)?;
        if artifacts.is_empty() {
            writeln!(
                f,
                "no artifacts in install dataset (this should only be seen in simulated systems)"
            )?;
        } else {
            writeln!(f, "artifacts in install dataset:")?;
            for artifact in artifacts {
                let mut indented = IndentWriter::new_skip_initial("    ", f);
                writeln!(indented, "  - {}", artifact.display())?;
                f = indented.into_inner();
            }
        }
        Ok(())
    }
}

pub struct ZoneArtifactInventoryDisplay<'a> {
    inner: &'a ZoneArtifactInventory,
}

impl fmt::Display for ZoneArtifactInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ZoneArtifactInventory {
            file_name,
            path: _,
            expected_size,
            expected_hash,
            status,
        } = self.inner;
        write!(
            f,
            "{file_name} (expected {expected_size} bytes with hash {expected_hash}): "
        )?;
        match status {
            Ok(()) => write!(f, "ok"),
            Err(message) => write!(f, "error: {message}"),
        }
    }
}

pub struct ZoneManifestNonBootInventoryDisplay<'a> {
    inner: &'a ZoneManifestNonBootInventory,
}

impl fmt::Display for ZoneManifestNonBootInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ZoneManifestNonBootInventory {
            zpool_id: _,
            path,
            is_valid,
            message,
        } = self.inner;
        write!(
            f,
            "{path} ({}): {message}",
            if *is_valid { "valid" } else { "invalid" }
        )
    }
}

pub struct MupdateOverrideInventoryDisplay<'a> {
    inner: &'a MupdateOverrideInventory,
}

impl fmt::Display for MupdateOverrideInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f;
        let MupdateOverrideInventory {
            boot_disk_path,
            boot_override,
            non_boot_status,
        } = self.inner;
        writeln!(f, "path on boot disk: {boot_disk_path}")?;
        match boot_override {
            Ok(Some(boot_override)) => {
                writeln!(
                    f,
                    "override on boot disk: {}",
                    boot_override.display()
                )?;
            }
            Ok(None) => {
                writeln!(f, "no override on boot disk")?;
            }
            Err(error) => {
                writeln!(f, "error obtaining override on boot disk: {error}")?;
            }
        }
        if non_boot_status.is_empty() {
            writeln!(f, "no non-boot disks")?;
        } else {
            writeln!(f, "non-boot disk status:")?;
            for non_boot in non_boot_status {
                let mut indented = IndentWriter::new_skip_initial("    ", f);
                writeln!(indented, "  - {}", non_boot.display())?;
                f = indented.into_inner();
            }
        }
        Ok(())
    }
}

pub struct MupdateOverrideBootInventoryDisplay<'a> {
    inner: &'a MupdateOverrideBootInventory,
}

impl fmt::Display for MupdateOverrideBootInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let MupdateOverrideBootInventory { mupdate_override_id } = self.inner;
        write!(f, "{}", mupdate_override_id)
    }
}

pub struct MupdateOverrideNonBootInventoryDisplay<'a> {
    inner: &'a MupdateOverrideNonBootInventory,
}

impl fmt::Display for MupdateOverrideNonBootInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let MupdateOverrideNonBootInventory {
            zpool_id: _,
            path,
            is_valid,
            message,
        } = self.inner;
        write!(
            f,
            "{path} ({}): {message}",
            if *is_valid { "valid" } else { "invalid" }
        )
    }
}

pub trait ZoneImageResolverInventoryExt {
    /// Returns a displayer for this inventory.
    fn display(&self) -> ZoneImageResolverInventoryDisplay<'_>;

    /// Returns a new, fake inventory for tests.
    fn new_fake() -> ZoneImageResolverInventory;
}

impl ZoneImageResolverInventoryExt for ZoneImageResolverInventory {
    fn display(&self) -> ZoneImageResolverInventoryDisplay<'_> {
        ZoneImageResolverInventoryDisplay { inner: self }
    }

    fn new_fake() -> ZoneImageResolverInventory {
        ZoneImageResolverInventory {
            zone_manifest: <ZoneManifestInventory as ZoneManifestInventoryExt>::new_fake(),
            mupdate_override: <MupdateOverrideInventory as MupdateOverrideInventoryExt>::new_fake(),
        }
    }
}

pub trait ZoneManifestInventoryExt {
    /// Returns a displayer for this inventory.
    fn display(&self) -> ZoneManifestInventoryDisplay<'_>;

    /// Returns a new, empty inventory for tests.
    fn new_fake() -> ZoneManifestInventory;
}

impl ZoneManifestInventoryExt for ZoneManifestInventory {
    fn display(&self) -> ZoneManifestInventoryDisplay<'_> {
        ZoneManifestInventoryDisplay { inner: self }
    }

    fn new_fake() -> ZoneManifestInventory {
        ZoneManifestInventory {
            boot_disk_path: Utf8PathBuf::from("/fake/path/install/zones.json"),
            boot_inventory: Ok(<ZoneManifestBootInventory as ZoneManifestBootInventoryExt>::new_fake()),
            non_boot_status: IdOrdMap::new(),
        }
    }
}

pub trait ZoneManifestBootInventoryExt {
    /// Returns a displayer for this inventory.
    fn display(&self) -> ZoneManifestBootInventoryDisplay<'_>;

    /// Returns a new, empty inventory for tests.
    ///
    /// For a more representative selection of real zones, see `representative`
    /// in `nexus-inventory`.
    fn new_fake() -> ZoneManifestBootInventory;
}

impl ZoneManifestBootInventoryExt for ZoneManifestBootInventory {
    fn display(&self) -> ZoneManifestBootInventoryDisplay<'_> {
        ZoneManifestBootInventoryDisplay { inner: self }
    }

    fn new_fake() -> ZoneManifestBootInventory {
        ZoneManifestBootInventory {
            source: OmicronZoneManifestSource::Installinator {
                mupdate_id: MupdateUuid::nil(),
            },
            artifacts: IdOrdMap::new(),
        }
    }
}

pub trait ZoneArtifactInventoryExt {
    /// Returns a displayer for this inventory.
    fn display(&self) -> ZoneArtifactInventoryDisplay<'_>;
}

impl ZoneArtifactInventoryExt for ZoneArtifactInventory {
    fn display(&self) -> ZoneArtifactInventoryDisplay<'_> {
        ZoneArtifactInventoryDisplay { inner: self }
    }
}

pub trait ZoneManifestNonBootInventoryExt {
    /// Returns a displayer for this inventory.
    fn display(&self) -> ZoneManifestNonBootInventoryDisplay<'_>;
}

impl ZoneManifestNonBootInventoryExt for ZoneManifestNonBootInventory {
    fn display(&self) -> ZoneManifestNonBootInventoryDisplay<'_> {
        ZoneManifestNonBootInventoryDisplay { inner: self }
    }
}

pub trait MupdateOverrideInventoryExt {
    /// Returns a displayer for this inventory.
    fn display(&self) -> MupdateOverrideInventoryDisplay<'_>;

    /// Returns a new, empty inventory for tests.
    fn new_fake() -> MupdateOverrideInventory;
}

impl MupdateOverrideInventoryExt for MupdateOverrideInventory {
    fn display(&self) -> MupdateOverrideInventoryDisplay<'_> {
        MupdateOverrideInventoryDisplay { inner: self }
    }

    fn new_fake() -> MupdateOverrideInventory {
        MupdateOverrideInventory {
            boot_disk_path: Utf8PathBuf::from(
                "/fake/path/install/mupdate_override.json",
            ),
            boot_override: Ok(None),
            non_boot_status: IdOrdMap::new(),
        }
    }
}

pub trait MupdateOverrideBootInventoryExt {
    /// Returns a displayer for this inventory.
    fn display(&self) -> MupdateOverrideBootInventoryDisplay<'_>;
}

impl MupdateOverrideBootInventoryExt for MupdateOverrideBootInventory {
    fn display(&self) -> MupdateOverrideBootInventoryDisplay<'_> {
        MupdateOverrideBootInventoryDisplay { inner: self }
    }
}

pub trait MupdateOverrideNonBootInventoryExt {
    /// Returns a displayer for this inventory.
    fn display(&self) -> MupdateOverrideNonBootInventoryDisplay<'_>;
}

impl MupdateOverrideNonBootInventoryExt for MupdateOverrideNonBootInventory {
    fn display(&self) -> MupdateOverrideNonBootInventoryDisplay<'_> {
        MupdateOverrideNonBootInventoryDisplay { inner: self }
    }
}

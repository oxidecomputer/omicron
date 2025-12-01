// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for version 10 of the API

use crate::v1;

/// Identity and basic status information about this sled agent
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Inventory {
    pub sled_id: SledUuid,
    pub sled_agent_address: SocketAddrV6,
    pub sled_role: SledRole,
    pub baseboard: Baseboard,
    pub usable_hardware_threads: u32,
    pub usable_physical_ram: ByteCount,
    pub cpu_family: SledCpuFamily,
    pub reservoir_size: ByteCount,
    pub disks: Vec<InventoryDisk>,
    pub zpools: Vec<InventoryZpool>,
    pub datasets: Vec<InventoryDataset>,
    pub ledgered_sled_config: Option<OmicronSledConfig>,
    pub reconciler_status: ConfigReconcilerInventoryStatus,
    pub last_reconciliation: Option<ConfigReconcilerInventory>,
    pub zone_image_resolver: ZoneImageResolverInventory,
}

impl TryFrom<Inventory> for v1::Inventory {
    type Error = external::Error;

    fn try_from(value: inventory::Inventory) -> Result<Self, Self::Error> {
        let ledgered_sled_config =
            value.ledgered_sled_config.map(TryInto::try_into).transpose()?;
        let reconciler_status = value.reconciler_status.try_into()?;
        let last_reconciliation =
            value.last_reconciliation.map(TryInto::try_into).transpose()?;
        Ok(Self {
            sled_id: value.sled_id,
            sled_agent_address: value.sled_agent_address,
            sled_role: value.sled_role,
            baseboard: value.baseboard,
            usable_hardware_threads: value.usable_hardware_threads,
            usable_physical_ram: value.usable_physical_ram,
            cpu_family: value.cpu_family,
            reservoir_size: value.reservoir_size,
            disks: value.disks,
            zpools: value.zpools,
            datasets: value.datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            zone_image_resolver: value.zone_image_resolver,
        })
    }
}

/// Describes the set of Reconfigurator-managed configuration elements of a sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct OmicronSledConfig {
    pub generation: Generation,
    // Serialize and deserialize disks, datasets, and zones as maps for
    // backwards compatibility. Newer IdOrdMaps should not use IdOrdMapAsMap.
    #[serde(
        with = "iddqd::id_ord_map::IdOrdMapAsMap::<OmicronPhysicalDiskConfig>"
    )]
    pub disks: IdOrdMap<OmicronPhysicalDiskConfig>,
    #[serde(with = "iddqd::id_ord_map::IdOrdMapAsMap::<DatasetConfig>")]
    pub datasets: IdOrdMap<DatasetConfig>,
    #[serde(with = "iddqd::id_ord_map::IdOrdMapAsMap::<OmicronZoneConfig>")]
    pub zones: IdOrdMap<OmicronZoneConfig>,
    pub remove_mupdate_override: Option<MupdateOverrideUuid>,
    #[serde(default = "HostPhase2DesiredSlots::current_contents")]
    pub host_phase_2: HostPhase2DesiredSlots,
}

impl Default for OmicronSledConfig {
    fn default() -> Self {
        Self {
            generation: Generation::new(),
            disks: IdOrdMap::default(),
            datasets: IdOrdMap::default(),
            zones: IdOrdMap::default(),
            remove_mupdate_override: None,
            host_phase_2: HostPhase2DesiredSlots::current_contents(),
        }
    }
}

impl Ledgerable for OmicronSledConfig {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.generation > other.generation
    }

    fn generation_bump(&mut self) {
        // DO NOTHING!
        //
        // Generation bumps must only ever come from nexus and will be encoded
        // in the struct itself
    }
}

impl TryFrom<v1::OmicronSledConfig> for OmicronSledConfig {
    type Error = external::Error;

    fn try_from(value: OmicronSledConfig) -> Result<Self, Self::Error> {
        let zones = value
            .zones
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones,
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
        })
    }
}

impl TryFrom<OmicronSledConfig> for v1::OmicronSledConfig {
    type Error = external::Error;
    fn try_from(
        value: inventory::OmicronSledConfig,
    ) -> Result<Self, Self::Error> {
        let zones = value
            .zones
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones,
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
        })
    }
}

/// Describes one Omicron-managed zone running on a sled
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronZoneConfig {
    pub id: OmicronZoneUuid,

    /// The pool on which we'll place this zone's root filesystem.
    ///
    /// Note that the root filesystem is transient -- the sled agent is
    /// permitted to destroy this dataset each time the zone is initialized.
    pub filesystem_pool: Option<ZpoolName>,
    pub zone_type: OmicronZoneType,
    // Use `InstallDataset` if this field is not present in a deserialized
    // blueprint or ledger.
    #[serde(default = "OmicronZoneImageSource::deserialize_default")]
    pub image_source: OmicronZoneImageSource,
}

impl IdOrdItem for OmicronZoneConfig {
    type Key<'a> = OmicronZoneUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

impl OmicronZoneConfig {
    /// Returns the underlay IP address associated with this zone.
    ///
    /// Assumes all zone have exactly one underlay IP address (which is
    /// currently true).
    pub fn underlay_ip(&self) -> Ipv6Addr {
        self.zone_type.underlay_ip()
    }

    pub fn zone_name(&self) -> String {
        illumos_utils::running_zone::InstalledZone::get_zone_name(
            self.zone_type.kind().zone_prefix(),
            Some(self.id),
        )
    }

    pub fn dataset_name(&self) -> Option<DatasetName> {
        self.zone_type.dataset_name()
    }
}

impl TryFrom<v1::OmicronZoneConfig> for OmicronZoneConfig {
    type Error = external::Error;

    fn try_from(value: v1::OmicronZoneConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            filesystem_pool: value.filesystem_pool,
            zone_type: value.zone_type.try_into()?,
            image_source: value.image_source,
        })
    }
}

impl TryFrom<OmicronZoneConfig> for v1::OmicronZoneConfig {
    type Error = external::Error;

    fn try_from(value: OmicronZoneConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            filesystem_pool: value.filesystem_pool,
            zone_type: value.zone_type.try_into()?,
            image_source: value.image_source,
        })
    }
}

/// Describes the last attempt made by the sled-agent-config-reconciler to
/// reconcile the current sled config against the actual state of the sled.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ConfigReconcilerInventory {
    pub last_reconciled_config: OmicronSledConfig,
    pub external_disks:
        BTreeMap<PhysicalDiskUuid, ConfigReconcilerInventoryResult>,
    pub datasets: BTreeMap<DatasetUuid, ConfigReconcilerInventoryResult>,
    pub orphaned_datasets: IdOrdMap<OrphanedDataset>,
    pub zones: BTreeMap<OmicronZoneUuid, ConfigReconcilerInventoryResult>,
    pub boot_partitions: BootPartitionContents,
    /// The result of removing the mupdate override file on disk.
    ///
    /// `None` if `remove_mupdate_override` was not provided in the sled config.
    pub remove_mupdate_override: Option<RemoveMupdateOverrideInventory>,
}

impl TryFrom<ConfigReconcilerInventory> for v1::ConfigReconcilerInventory {
    type Error = external::Error;

    fn try_from(
        value: inventory::ConfigReconcilerInventory,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            last_reconciled_config: value.last_reconciled_config.try_into()?,
            external_disks: value.external_disks,
            datasets: value.datasets,
            orphaned_datasets: value.orphaned_datasets,
            zones: value.zones,
            boot_partitions: value.boot_partitions,
            remove_mupdate_override: value.remove_mupdate_override,
        })
    }
}

impl ConfigReconcilerInventory {
    /// Iterate over all running zones as reported by the last reconciliation
    /// result.
    ///
    /// This includes zones that are both present in `last_reconciled_config`
    /// and whose status in `zones` indicates "successfully running".
    pub fn running_omicron_zones(
        &self,
    ) -> impl Iterator<Item = &OmicronZoneConfig> {
        self.zones.iter().filter_map(|(zone_id, result)| match result {
            ConfigReconcilerInventoryResult::Ok => {
                self.last_reconciled_config.zones.get(zone_id)
            }
            ConfigReconcilerInventoryResult::Err { .. } => None,
        })
    }

    /// Iterate over all zones contained in the most-recently-reconciled sled
    /// config and report their status as of that reconciliation.
    pub fn reconciled_omicron_zones(
        &self,
    ) -> impl Iterator<Item = (&OmicronZoneConfig, &ConfigReconcilerInventoryResult)>
    {
        // `self.zones` may contain zone IDs that aren't present in
        // `last_reconciled_config` at all, if we failed to _shut down_ zones
        // that are no longer present in the config. We use `filter_map` to
        // strip those out, and only report on the configured zones.
        self.zones.iter().filter_map(|(zone_id, result)| {
            let config = self.last_reconciled_config.zones.get(zone_id)?;
            Some((config, result))
        })
    }

    /// Given a sled config, produce a reconciler result that sled-agent could
    /// have emitted if reconciliation succeeded.
    ///
    /// This method should only be used by tests and dev tools; real code should
    /// look at the actual `last_reconciliation` value from the parent
    /// [`Inventory`].
    pub fn debug_assume_success(config: OmicronSledConfig) -> Self {
        let mut ret = Self {
            // These fields will be filled in by `debug_update_assume_success`.
            last_reconciled_config: OmicronSledConfig::default(),
            external_disks: BTreeMap::new(),
            datasets: BTreeMap::new(),
            orphaned_datasets: IdOrdMap::new(),
            zones: BTreeMap::new(),
            remove_mupdate_override: None,

            // These fields will not.
            boot_partitions: BootPartitionContents::debug_assume_success(),
        };

        ret.debug_update_assume_success(config);

        ret
    }

    /// Given a sled config, update an existing reconciler result to simulate an
    /// output that sled-agent could have emitted if reconciliation succeeded.
    ///
    /// This method should only be used by tests and dev tools; real code should
    /// look at the actual `last_reconciliation` value from the parent
    /// [`Inventory`].
    pub fn debug_update_assume_success(&mut self, config: OmicronSledConfig) {
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
                    non_boot_message: "mupdate override successfully removed \
                                       on non-boot disks"
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

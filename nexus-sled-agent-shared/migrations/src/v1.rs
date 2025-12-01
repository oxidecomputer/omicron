// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for version 1 of the API

/// Identity and basic status information about this sled agent
#[derive(Deserialize, Serialize, JsonSchema)]
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

/// Describes the set of Reconfigurator-managed configuration elements of a sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct OmicronSledConfig {
    pub generation: Generation,
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

/// Describes one Omicron-managed zone running on a sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
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

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct BootPartitionContents {
    #[serde(with = "snake_case_result")]
    #[schemars(schema_with = "SnakeCaseResult::<M2Slot, String>::json_schema")]
    pub boot_disk: Result<M2Slot, String>,
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<BootPartitionDetails, String>::json_schema"
    )]
    pub slot_a: Result<BootPartitionDetails, String>,
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<BootPartitionDetails, String>::json_schema"
    )]
    pub slot_b: Result<BootPartitionDetails, String>,
}

impl BootPartitionContents {
    pub fn slot_details(
        &self,
        slot: M2Slot,
    ) -> &Result<BootPartitionDetails, String> {
        match slot {
            M2Slot::A => &self.slot_a,
            M2Slot::B => &self.slot_b,
        }
    }

    pub fn debug_assume_success() -> Self {
        Self {
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

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct BootPartitionDetails {
    pub header: BootImageHeader,
    pub artifact_hash: ArtifactHash,
    pub artifact_size: usize,
}

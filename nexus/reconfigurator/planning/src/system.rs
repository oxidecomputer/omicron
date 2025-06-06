// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Builders for constructing descriptions of systems (real or synthetic) and
//! associated inventory collections and blueprints

use anyhow::{Context, anyhow, bail, ensure};
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use indexmap::IndexMap;
use ipnet::Ipv6Net;
use ipnet::Ipv6Subnets;
use nexus_inventory::CollectionBuilder;
use nexus_sled_agent_shared::inventory::Baseboard;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::Inventory;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use nexus_sled_agent_shared::inventory::InventoryDisk;
use nexus_sled_agent_shared::inventory::InventoryZpool;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_types::deployment::ClickhousePolicy;
use nexus_types::deployment::CockroachDbClusterVersion;
use nexus_types::deployment::CockroachDbSettings;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::OximeterReadPolicy;
use nexus_types::deployment::PlanningInputBuilder;
use nexus_types::deployment::Policy;
use nexus_types::deployment::SledDetails;
use nexus_types::deployment::SledDisk;
use nexus_types::deployment::SledResources;
use nexus_types::external_api::views::PhysicalDiskPolicy;
use nexus_types::external_api::views::PhysicalDiskState;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::Caboose;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::PowerState;
use nexus_types::inventory::RotSlot;
use nexus_types::inventory::SpType;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::RACK_PREFIX;
use omicron_common::address::SLED_PREFIX;
use omicron_common::address::get_sled_address;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::api::external::TufRepoDescription;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::DiskVariant;
use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
use omicron_common::policy::NEXUS_REDUNDANCY;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use tufaceous_artifact::ArtifactVersion;

/// Describes an actual or synthetic Oxide rack for planning and testing
///
/// From this description, you can extract a `PlanningInput` or inventory
/// `Collection`. There are a few intended purposes here:
///
/// 1. to easily construct fake racks in automated tests for the Planner and
///    other parts of Reconfigurator
///
/// 2. to explore the Planner's behavior via the `reconfigurator-cli` tool
///
/// 3. eventually: to commonize code between Reconfigurator and RSS.  This is
///    more speculative at this point, but the idea here is that RSS itself
///    could construct a `SystemDescription` and then use the facilities here to
///    assign subnets and maybe even lay out the initial set of zones (which
///    does not exist here yet).  This way Reconfigurator and RSS are using the
///    same code to do this.
///
/// This is cheaply cloneable, and uses copy-on-write semantics for data inside.
#[derive(Clone, Debug)]
pub struct SystemDescription {
    collector: Option<String>,
    // Arc<Sled> to make cloning cheap. Mutating sleds is uncommon but
    // possible, in which case we'll clone-on-write with Arc::make_mut.
    sleds: IndexMap<SledUuid, Arc<Sled>>,
    sled_subnets: SubnetIterator,
    available_non_scrimlet_slots: BTreeSet<u16>,
    available_scrimlet_slots: BTreeSet<u16>,
    target_boundary_ntp_zone_count: usize,
    target_nexus_zone_count: usize,
    target_internal_dns_zone_count: usize,
    target_oximeter_zone_count: usize,
    target_cockroachdb_zone_count: usize,
    target_cockroachdb_cluster_version: CockroachDbClusterVersion,
    target_crucible_pantry_zone_count: usize,
    service_ip_pool_ranges: Vec<IpRange>,
    internal_dns_version: Generation,
    external_dns_version: Generation,
    clickhouse_policy: Option<ClickhousePolicy>,
    oximeter_read_policy: OximeterReadPolicy,
    tuf_repo: Option<TufRepoDescription>,
    old_repo: Option<TufRepoDescription>,
}

impl SystemDescription {
    pub fn new() -> Self {
        // Prepare sets of available slots (cubby numbers) for (1) all
        // non-Scrimlet sleds, and (2) Scrimlets in particular.  These do not
        // overlap.
        //
        // We will use these in two places:
        //
        // (1) when the caller specifies what slot a sled should go into, to
        //     validate that the slot is available and make sure we don't use it
        //     again
        //
        // (2) when the caller adds a sled but leaves the slot unspecified, so
        //     that we can assign it a slot number
        //
        // We use `BTreeSet` because it efficiently expresses what we want,
        // though the set sizes are small enough that it doesn't much matter.
        let available_scrimlet_slots: BTreeSet<u16> = BTreeSet::from([14, 16]);
        let available_non_scrimlet_slots: BTreeSet<u16> = (0..=31)
            .collect::<BTreeSet<_>>()
            .difference(&available_scrimlet_slots)
            .copied()
            .collect();

        // Prepare an iterator to allow us to assign sled subnets.
        let rack_subnet_base: Ipv6Addr =
            "fd00:1122:3344:0100::".parse().unwrap();
        let rack_subnet =
            ipnet::Ipv6Net::new(rack_subnet_base, RACK_PREFIX).unwrap();
        // Skip the initial DNS subnet.
        // (The same behavior is replicated in RSS in `Plan::create()` in
        // sled-agent/src/rack_setup/plan/sled.rs.)
        let sled_subnets = SubnetIterator::new(rack_subnet);

        // Policy defaults
        let target_nexus_zone_count = NEXUS_REDUNDANCY;
        let target_internal_dns_zone_count = INTERNAL_DNS_REDUNDANCY;

        // TODO-cleanup These are wrong, but we don't currently set up any
        // of these zones in our fake system, so this prevents downstream test
        // issues with the planner thinking our system is out of date from the
        // gate.
        let target_boundary_ntp_zone_count = 0;
        let target_cockroachdb_zone_count = 0;
        let target_oximeter_zone_count = 0;
        let target_crucible_pantry_zone_count = 0;

        let target_cockroachdb_cluster_version =
            CockroachDbClusterVersion::POLICY;

        // IPs from TEST-NET-1 (RFC 5737)
        let service_ip_pool_ranges = vec![
            IpRange::try_from((
                "192.0.2.2".parse::<Ipv4Addr>().unwrap(),
                "192.0.2.20".parse::<Ipv4Addr>().unwrap(),
            ))
            .unwrap(),
        ];

        SystemDescription {
            sleds: IndexMap::new(),
            collector: None,
            sled_subnets,
            available_non_scrimlet_slots,
            available_scrimlet_slots,
            target_boundary_ntp_zone_count,
            target_nexus_zone_count,
            target_internal_dns_zone_count,
            target_oximeter_zone_count,
            target_cockroachdb_zone_count,
            target_cockroachdb_cluster_version,
            target_crucible_pantry_zone_count,
            service_ip_pool_ranges,
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            clickhouse_policy: None,
            oximeter_read_policy: OximeterReadPolicy::new(1),
            tuf_repo: None,
            old_repo: None,
        }
    }

    /// Returns a complete system deployed on a single Sled
    pub fn single_sled() -> anyhow::Result<Self> {
        let mut builder = SystemDescription::new();
        let sled = SledBuilder::new();
        builder.sled(sled)?;
        Ok(builder)
    }

    /// Returns a complete system resembling a full rack
    pub fn full_rack() -> anyhow::Result<Self> {
        let mut builder = SystemDescription::new();
        for slot_number in 1..32 {
            let mut sled = SledBuilder::new();
            if slot_number == 14 || slot_number == 16 {
                sled = sled.sled_role(SledRole::Scrimlet);
            }
            builder.sled(sled)?;
        }
        Ok(builder)
    }

    pub fn collector_label<S>(&mut self, collector_label: S) -> &mut Self
    where
        String: From<S>,
    {
        self.collector = Some(String::from(collector_label));
        self
    }

    pub fn target_nexus_zone_count(&mut self, count: usize) -> &mut Self {
        self.target_nexus_zone_count = count;
        self
    }

    pub fn get_target_nexus_zone_count(&self) -> usize {
        self.target_nexus_zone_count
    }

    pub fn target_crucible_pantry_zone_count(
        &mut self,
        count: usize,
    ) -> &mut Self {
        self.target_crucible_pantry_zone_count = count;
        self
    }

    pub fn get_target_crucible_pantry_zone_count(&self) -> usize {
        self.target_crucible_pantry_zone_count
    }

    pub fn target_internal_dns_zone_count(
        &mut self,
        count: usize,
    ) -> &mut Self {
        self.target_internal_dns_zone_count = count;
        self
    }

    pub fn get_target_internal_dns_zone_count(&self) -> usize {
        self.target_internal_dns_zone_count
    }

    pub fn service_ip_pool_ranges(
        &mut self,
        ranges: Vec<IpRange>,
    ) -> &mut Self {
        self.service_ip_pool_ranges = ranges;
        self
    }

    /// Set the clickhouse policy
    pub fn clickhouse_policy(&mut self, policy: ClickhousePolicy) -> &mut Self {
        self.clickhouse_policy = Some(policy);
        self
    }

    pub fn get_sled_mut(
        &mut self,
        sled_id: SledUuid,
    ) -> anyhow::Result<&mut Sled> {
        let Some(sled) = self.sleds.get_mut(&sled_id) else {
            bail!("Sled not found with id {sled_id}");
        };
        Ok(Arc::make_mut(sled))
    }

    /// Add a sled to the system, as described by a SledBuilder
    pub fn sled(&mut self, sled: SledBuilder) -> anyhow::Result<&mut Self> {
        let sled_id = sled.id.unwrap_or_else(SledUuid::new_v4);
        ensure!(
            !self.sleds.contains_key(&sled_id),
            "attempted to add sled with the same id as an existing one: {}",
            sled_id
        );
        let sled_subnet = self
            .sled_subnets
            .next()
            .ok_or_else(|| anyhow!("ran out of IPv6 subnets for sleds"))?;
        let hardware_slot = if let Some(slot) = sled.hardware_slot {
            // If the caller specified a slot number, use that.
            // Make sure it's still available, though.
            if !self.available_scrimlet_slots.remove(&slot)
                && !self.available_non_scrimlet_slots.remove(&slot)
            {
                bail!("sled slot {} was used twice", slot);
            }
            slot
        } else if sled.sled_role == SledRole::Scrimlet {
            // Otherwise, if this is a Scrimlet, it must be in one of the
            // allowed Scrimlet slots.
            self.available_scrimlet_slots
                .pop_first()
                .ok_or_else(|| anyhow!("ran out of slots for Scrimlets"))?
        } else {
            // Otherwise, prefer a non-Scrimlet slots, but allow a Scrimlet slot
            // to be used if we run out of non-Scrimlet slots.
            self.available_non_scrimlet_slots
                .pop_first()
                .or_else(|| self.available_scrimlet_slots.pop_first())
                .ok_or_else(|| anyhow!("ran out of slots for non-Scrimlets"))?
        };

        let sled = Sled::new_simulated(
            sled_id,
            sled_subnet,
            sled.sled_role,
            sled.unique,
            sled.hardware,
            hardware_slot,
            sled.sled_config,
            sled.npools,
        );
        self.sleds.insert(sled_id, Arc::new(sled));
        Ok(self)
    }

    /// Add a sled to the system based on information that came from the
    /// database of an existing system.
    ///
    /// Note that `sled_policy` and `sled_state` are currently not checked for
    /// internal consistency! This is to permit testing of the Planner with
    /// invalid inputs.
    pub fn sled_full(
        &mut self,
        sled_id: SledUuid,
        sled_policy: SledPolicy,
        sled_state: SledState,
        sled_resources: SledResources,
        inventory_sp: Option<SledHwInventory<'_>>,
        inventory_sled_agent: &nexus_types::inventory::SledAgent,
    ) -> anyhow::Result<&mut Self> {
        ensure!(
            !self.sleds.contains_key(&sled_id),
            "attempted to add sled with the same id as an existing one: {}",
            sled_id
        );
        self.sleds.insert(
            sled_id,
            Arc::new(Sled::new_full(
                sled_id,
                sled_policy,
                sled_state,
                sled_resources,
                inventory_sp,
                inventory_sled_agent,
            )),
        );
        Ok(self)
    }

    /// Remove a sled from the system.
    ///
    /// Returns the [`Sled`] structure that was just removed.
    pub fn sled_remove(
        &mut self,
        sled_id: SledUuid,
    ) -> anyhow::Result<Arc<Sled>> {
        self.sleds.shift_remove(&sled_id).ok_or_else(|| {
            anyhow!(
                "attempted to remove sled with id {sled_id} that does not exist"
            )
        })
    }

    /// Return true if the system has any sleds in it.
    pub fn has_sleds(&self) -> bool {
        !self.sleds.is_empty()
    }

    /// Set Omicron config for a sled.
    ///
    /// The zones will be reported in the collection generated by
    /// [`Self::to_collection_builder`].
    ///
    /// Returns an error if the sled is not found.
    ///
    /// # Notes
    ///
    /// It is okay to call `sled_set_omicron_config` in ways that wouldn't
    /// happen in production, such as to test illegal states.
    pub fn sled_set_omicron_config(
        &mut self,
        sled_id: SledUuid,
        sled_config: OmicronSledConfig,
    ) -> anyhow::Result<&mut Self> {
        let sled = self.sleds.get_mut(&sled_id).with_context(|| {
            format!("attempted to access sled {} not found in system", sled_id)
        })?;
        let sled = Arc::make_mut(sled);

        sled.inventory_sled_agent.ledgered_sled_config = Some(sled_config);
        sled.inventory_sled_agent.reconciler_status =
            ConfigReconcilerInventoryStatus::NotYetRun;
        sled.inventory_sled_agent.last_reconciliation = None;

        Ok(self)
    }

    /// Set the policy for a sled in the system.
    pub fn sled_set_policy(
        &mut self,
        sled_id: SledUuid,
        policy: SledPolicy,
    ) -> anyhow::Result<&mut Self> {
        let sled = self.sleds.get_mut(&sled_id).with_context(|| {
            format!("attempted to access sled {} not found in system", sled_id)
        })?;
        Arc::make_mut(sled).policy = policy;
        Ok(self)
    }

    /// Update the SP versions reported for a sled.
    ///
    /// Where `None` is provided, no changes are made.
    pub fn sled_update_sp_versions(
        &mut self,
        sled_id: SledUuid,
        active_version: Option<ArtifactVersion>,
        inactive_version: Option<ExpectedVersion>,
    ) -> anyhow::Result<&mut Self> {
        let sled = self.sleds.get_mut(&sled_id).with_context(|| {
            format!("attempted to access sled {} not found in system", sled_id)
        })?;
        let sled = Arc::make_mut(sled);
        sled.set_sp_versions(active_version, inactive_version);
        Ok(self)
    }

    pub fn sled_sp_active_version(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<Option<&str>> {
        let sled = self.sleds.get(&sled_id).with_context(|| {
            format!("attempted to access sled {} not found in system", sled_id)
        })?;
        Ok(sled.sp_active_caboose().map(|c| c.version.as_ref()))
    }

    pub fn sled_sp_inactive_version(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<Option<&str>> {
        let sled = self.sleds.get(&sled_id).with_context(|| {
            format!("attempted to access sled {} not found in system", sled_id)
        })?;
        Ok(sled.sp_inactive_caboose().map(|c| c.version.as_ref()))
    }

    pub fn set_target_release(
        &mut self,
        tuf_repo: Option<TufRepoDescription>,
    ) -> &mut Self {
        self.tuf_repo = tuf_repo;
        self
    }

    pub fn target_release(&self) -> Option<&TufRepoDescription> {
        self.tuf_repo.as_ref()
    }

    pub fn to_collection_builder(&self) -> anyhow::Result<CollectionBuilder> {
        let collector_label = self
            .collector
            .as_ref()
            .cloned()
            .unwrap_or_else(|| String::from("example"));
        let mut builder = CollectionBuilder::new(collector_label);

        for s in self.sleds.values() {
            if let Some((slot, sp_state)) = s.sp_state() {
                builder
                    .found_sp_state(
                        "fake MGS 1",
                        SpType::Sled,
                        u32::from(*slot),
                        sp_state.clone(),
                    )
                    .context("recording SP state")?;

                let baseboard_id = BaseboardId {
                    part_number: sp_state.model.clone(),
                    serial_number: sp_state.serial_number.clone(),
                };
                if let Some(active) = &s.sp_active_caboose() {
                    builder
                        .found_caboose(
                            &baseboard_id,
                            CabooseWhich::SpSlot0,
                            "fake MGS 1",
                            SpComponentCaboose {
                                board: active.board.clone(),
                                epoch: None,
                                git_commit: active.git_commit.clone(),
                                name: active.name.clone(),
                                sign: active.sign.clone(),
                                version: active.version.clone(),
                            },
                        )
                        .context("recording SP active caboose")?;
                }

                if let Some(inactive) = &s.sp_inactive_caboose() {
                    builder
                        .found_caboose(
                            &baseboard_id,
                            CabooseWhich::SpSlot1,
                            "fake MGS 1",
                            SpComponentCaboose {
                                board: inactive.board.clone(),
                                epoch: None,
                                git_commit: inactive.git_commit.clone(),
                                name: inactive.name.clone(),
                                sign: inactive.sign.clone(),
                                version: inactive.version.clone(),
                            },
                        )
                        .context("recording SP inactive caboose")?;
                }
            }

            builder
                .found_sled_inventory(
                    "fake sled agent",
                    s.sled_agent_inventory().clone(),
                )
                .context("recording sled agent")?;
        }

        Ok(builder)
    }

    /// Construct a [`PlanningInputBuilder`] primed with all this system's sleds
    ///
    /// Does not populate extra information like Omicron zone external IPs or
    /// NICs.
    pub fn to_planning_input_builder(
        &self,
    ) -> anyhow::Result<PlanningInputBuilder> {
        let policy = Policy {
            service_ip_pool_ranges: self.service_ip_pool_ranges.clone(),
            target_boundary_ntp_zone_count: self.target_boundary_ntp_zone_count,
            target_nexus_zone_count: self.target_nexus_zone_count,
            target_internal_dns_zone_count: self.target_internal_dns_zone_count,
            target_oximeter_zone_count: self.target_oximeter_zone_count,
            target_cockroachdb_zone_count: self.target_cockroachdb_zone_count,
            target_cockroachdb_cluster_version: self
                .target_cockroachdb_cluster_version,
            target_crucible_pantry_zone_count: self
                .target_crucible_pantry_zone_count,
            clickhouse_policy: self.clickhouse_policy.clone(),
            oximeter_read_policy: self.oximeter_read_policy.clone(),
            tuf_repo: self.tuf_repo.clone(),
            old_repo: self.old_repo.clone(),
        };
        let mut builder = PlanningInputBuilder::new(
            policy,
            self.internal_dns_version,
            self.external_dns_version,
            CockroachDbSettings::empty(),
        );

        for sled in self.sleds.values() {
            let sled_details = SledDetails {
                policy: sled.policy,
                state: sled.state,
                resources: sled.resources.clone(),
                baseboard_id: BaseboardId {
                    part_number: sled
                        .inventory_sled_agent
                        .baseboard
                        .model()
                        .to_owned(),
                    serial_number: sled
                        .inventory_sled_agent
                        .baseboard
                        .identifier()
                        .to_owned(),
                },
            };
            builder.add_sled(sled.sled_id, sled_details)?;
        }

        Ok(builder)
    }
}

#[derive(Clone, Debug)]
pub enum SledHardware {
    Gimlet,
    Pc,
    Unknown,
    Empty,
}

#[derive(Clone, Debug)]
pub struct SledBuilder {
    id: Option<SledUuid>,
    unique: Option<String>,
    hardware: SledHardware,
    hardware_slot: Option<u16>,
    sled_role: SledRole,
    sled_config: OmicronSledConfig,
    npools: u8,
}

impl SledBuilder {
    /// The default number of U.2 (external) pools for a sled.
    ///
    /// The default is `10` based on the typical value for a Gimlet.
    pub const DEFAULT_NPOOLS: u8 = 10;

    /// Begin describing a sled to be added to a `SystemDescription`
    pub fn new() -> Self {
        SledBuilder {
            id: None,
            unique: None,
            hardware: SledHardware::Gimlet,
            hardware_slot: None,
            sled_role: SledRole::Gimlet,
            sled_config: OmicronSledConfig::default(),
            npools: Self::DEFAULT_NPOOLS,
        }
    }

    /// Set the id of the sled
    ///
    /// Default: randomly generated
    pub fn id(mut self, id: SledUuid) -> Self {
        self.id = Some(id);
        self
    }

    /// Set a unique string used to generate the serial number and other
    /// identifiers
    ///
    /// Default: randomly generated
    pub fn unique<S>(mut self, unique: S) -> Self
    where
        String: From<S>,
    {
        self.unique = Some(String::from(unique));
        self
    }

    /// Set the number of U.2 (external) pools this sled should have
    ///
    /// The default is [`Self::DEFAULT_NPOOLS`].
    pub fn npools(mut self, npools: u8) -> Self {
        self.npools = npools;
        self
    }

    /// Sets what type of hardware this sled uses
    ///
    /// Default: `SledHarware::Gimlet`
    pub fn hardware(mut self, hardware: SledHardware) -> Self {
        self.hardware = hardware;
        self
    }

    /// Sets which cubby in the rack the sled is in
    ///
    /// Default: determined based on sled role and unused slots
    pub fn hardware_slot(mut self, hardware_slot: u16) -> Self {
        self.hardware_slot = Some(hardware_slot);
        self
    }

    /// Sets whether this sled is attached to a switch (`SledRole::Scrimlet`) or
    /// not (`SledRole::Gimlet`)
    pub fn sled_role(mut self, sled_role: SledRole) -> Self {
        self.sled_role = sled_role;
        self
    }
}

/// Convenience structure summarizing `Sled` inputs that come from inventory
#[derive(Debug)]
pub struct SledHwInventory<'a> {
    pub baseboard_id: &'a BaseboardId,
    pub sp: &'a nexus_types::inventory::ServiceProcessor,
    pub rot: &'a nexus_types::inventory::RotState,
    pub sp_active: Option<Arc<nexus_types::inventory::Caboose>>,
    pub sp_inactive: Option<Arc<nexus_types::inventory::Caboose>>,
}

/// Our abstract description of a `Sled`
///
/// This needs to be rich enough to generate a PlanningInput and inventory
/// Collection.
#[derive(Clone, Debug)]
pub struct Sled {
    sled_id: SledUuid,
    inventory_sp: Option<(u16, SpState)>,
    inventory_sled_agent: Inventory,
    policy: SledPolicy,
    state: SledState,
    resources: SledResources,
    sp_active_caboose: Option<Arc<nexus_types::inventory::Caboose>>,
    sp_inactive_caboose: Option<Arc<nexus_types::inventory::Caboose>>,
}

impl Sled {
    /// Create a `Sled` using faked-up information based on a `SledBuilder`
    #[allow(clippy::too_many_arguments)]
    fn new_simulated(
        sled_id: SledUuid,
        sled_subnet: Ipv6Subnet<SLED_PREFIX>,
        sled_role: SledRole,
        unique: Option<String>,
        hardware: SledHardware,
        hardware_slot: u16,
        sled_config: OmicronSledConfig,
        nzpools: u8,
    ) -> Sled {
        use typed_rng::TypedUuidRng;
        let unique = unique.unwrap_or_else(|| hardware_slot.to_string());
        let model = format!("model{}", unique);
        let serial = format!("serial{}", unique);
        let revision = 0;
        let mut zpool_rng = TypedUuidRng::from_seed(
            "SystemSimultatedSled",
            (sled_id, "ZpoolUuid"),
        );
        let mut physical_disk_rng = TypedUuidRng::from_seed(
            "SystemSimulatedSled",
            (sled_id, "PhysicalDiskUuid"),
        );
        let zpools: BTreeMap<_, _> = (0..nzpools)
            .map(|_| {
                let zpool = ZpoolUuid::from(zpool_rng.next());
                let disk = SledDisk {
                    disk_identity: DiskIdentity {
                        vendor: String::from("fake-vendor"),
                        serial: format!("serial-{zpool}"),
                        model: String::from("fake-model"),
                    },
                    disk_id: physical_disk_rng.next(),
                    policy: PhysicalDiskPolicy::InService,
                    state: PhysicalDiskState::Active,
                };
                (zpool, disk)
            })
            .collect();
        let inventory_sp = match hardware {
            SledHardware::Empty => None,
            SledHardware::Gimlet | SledHardware::Pc | SledHardware::Unknown => {
                Some((
                    hardware_slot,
                    SpState {
                        base_mac_address: [0; 6],
                        hubris_archive_id: format!("hubris{}", unique),
                        model: model.clone(),
                        power_state: PowerState::A2,
                        revision,
                        rot: RotState::V3 {
                            active: RotSlot::A,
                            pending_persistent_boot_preference: None,
                            persistent_boot_preference: RotSlot::A,
                            slot_a_fwid: String::from("slotAdigest1"),
                            slot_b_fwid: String::from("slotBdigest1"),
                            stage0_fwid: String::from("stage0_fwid"),
                            stage0next_fwid: String::from("stage0next_fwid"),
                            slot_a_error: None,
                            slot_b_error: None,
                            stage0_error: None,
                            stage0next_error: None,
                            transient_boot_preference: None,
                        },
                        serial_number: serial.clone(),
                    },
                ))
            }
        };

        let inventory_sled_agent = {
            let baseboard = match hardware {
                SledHardware::Gimlet => Baseboard::Gimlet {
                    identifier: serial.clone(),
                    model: model.clone(),
                    revision,
                },
                SledHardware::Pc => Baseboard::Pc {
                    identifier: serial.clone(),
                    model: model.clone(),
                },
                SledHardware::Unknown | SledHardware::Empty => {
                    Baseboard::Unknown
                }
            };
            let sled_agent_address = get_sled_address(sled_subnet);
            Inventory {
                baseboard,
                reservoir_size: ByteCount::from(1024),
                sled_role,
                sled_agent_address,
                sled_id,
                usable_hardware_threads: 10,
                usable_physical_ram: ByteCount::from(1024 * 1024),
                // Populate disks, appearing like a real device.
                disks: zpools
                    .values()
                    .enumerate()
                    .map(|(i, disk)| InventoryDisk {
                        identity: disk.disk_identity.clone(),
                        variant: DiskVariant::U2,
                        slot: i64::try_from(i).unwrap(),
                        active_firmware_slot: 1,
                        next_active_firmware_slot: None,
                        number_of_firmware_slots: 1,
                        slot1_is_read_only: true,
                        slot_firmware_versions: vec![Some(
                            "SIMUL1".to_string(),
                        )],
                    })
                    .collect(),
                zpools: zpools
                    .keys()
                    .map(|id| InventoryZpool {
                        id: *id,
                        total_size: ByteCount::from_gibibytes_u32(100),
                    })
                    .collect(),
                datasets: vec![],
                ledgered_sled_config: Some(sled_config),
                reconciler_status: ConfigReconcilerInventoryStatus::NotYetRun,
                last_reconciliation: None,
            }
        };

        Sled {
            sled_id,
            inventory_sp,
            inventory_sled_agent,
            policy: SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            },
            state: SledState::Active,
            resources: SledResources { subnet: sled_subnet, zpools },
            sp_active_caboose: Some(Arc::new(Self::default_sp_caboose(
                String::from("0.0.1"),
            ))),
            sp_inactive_caboose: None,
        }
    }

    /// Create a `Sled` based on real information from another `Policy` and
    /// inventory `Collection`
    fn new_full(
        sled_id: SledUuid,
        sled_policy: SledPolicy,
        sled_state: SledState,
        sled_resources: SledResources,
        inventory_sp: Option<SledHwInventory<'_>>,
        inv_sled_agent: &nexus_types::inventory::SledAgent,
    ) -> Sled {
        // Elsewhere, the user gives us some rough parameters (like a unique
        // string) that we use to construct fake `sled_agent_client` types that
        // we can provide to the inventory builder so that _it_ can construct
        // the corresponding inventory types.  Here, we're working backwards,
        // which is a little weird: we're given inventory types and we construct
        // the fake `sled_agent_client` types, again so that we can later pass
        // them to the inventory builder so that it can construct the same
        // inventory types again.  This is a little goofy.
        let baseboard = inventory_sp
            .as_ref()
            .map(|sledhw| Baseboard::Gimlet {
                identifier: sledhw.baseboard_id.serial_number.clone(),
                model: sledhw.baseboard_id.part_number.clone(),
                revision: sledhw.sp.baseboard_revision,
            })
            .unwrap_or(Baseboard::Unknown);

        let sp_active_caboose =
            inventory_sp.as_ref().and_then(|hw| hw.sp_active.clone());
        let sp_inactive_caboose =
            inventory_sp.as_ref().and_then(|hw| hw.sp_inactive.clone());
        let inventory_sp = inventory_sp.map(|sledhw| {
            // RotStateV3 unconditionally sets all of these
            let sp_state = if sledhw.rot.slot_a_sha3_256_digest.is_some()
                && sledhw.rot.slot_b_sha3_256_digest.is_some()
                && sledhw.rot.stage0_digest.is_some()
                && sledhw.rot.stage0next_digest.is_some()
            {
                SpState {
                    base_mac_address: [0; 6],
                    hubris_archive_id: sledhw.sp.hubris_archive.clone(),
                    model: sledhw.baseboard_id.part_number.clone(),
                    power_state: sledhw.sp.power_state,
                    revision: sledhw.sp.baseboard_revision,
                    rot: RotState::V3 {
                        active: sledhw.rot.active_slot,
                        pending_persistent_boot_preference: sledhw
                            .rot
                            .pending_persistent_boot_preference,
                        persistent_boot_preference: sledhw
                            .rot
                            .persistent_boot_preference,
                        slot_a_fwid: sledhw
                            .rot
                            .slot_a_sha3_256_digest
                            .clone()
                            .expect("slot_a_fwid should be set"),
                        slot_b_fwid: sledhw
                            .rot
                            .slot_b_sha3_256_digest
                            .clone()
                            .expect("slot_b_fwid should be set"),
                        stage0_fwid: sledhw
                            .rot
                            .stage0_digest
                            .clone()
                            .expect("stage0 fwid should be set"),
                        stage0next_fwid: sledhw
                            .rot
                            .stage0next_digest
                            .clone()
                            .expect("stage0 fwid should be set"),
                        transient_boot_preference: sledhw
                            .rot
                            .transient_boot_preference,
                        slot_a_error: sledhw.rot.slot_a_error,
                        slot_b_error: sledhw.rot.slot_b_error,
                        stage0_error: sledhw.rot.stage0_error,
                        stage0next_error: sledhw.rot.stage0next_error,
                    },
                    serial_number: sledhw.baseboard_id.serial_number.clone(),
                }
            } else {
                SpState {
                    base_mac_address: [0; 6],
                    hubris_archive_id: sledhw.sp.hubris_archive.clone(),
                    model: sledhw.baseboard_id.part_number.clone(),
                    power_state: sledhw.sp.power_state,
                    revision: sledhw.sp.baseboard_revision,
                    rot: RotState::V2 {
                        active: sledhw.rot.active_slot,
                        pending_persistent_boot_preference: sledhw
                            .rot
                            .pending_persistent_boot_preference,
                        persistent_boot_preference: sledhw
                            .rot
                            .persistent_boot_preference,
                        slot_a_sha3_256_digest: sledhw
                            .rot
                            .slot_a_sha3_256_digest
                            .clone(),
                        slot_b_sha3_256_digest: sledhw
                            .rot
                            .slot_b_sha3_256_digest
                            .clone(),
                        transient_boot_preference: sledhw
                            .rot
                            .transient_boot_preference,
                    },
                    serial_number: sledhw.baseboard_id.serial_number.clone(),
                }
            };
            (sledhw.sp.sp_slot, sp_state)
        });

        let inventory_sled_agent = Inventory {
            baseboard,
            reservoir_size: inv_sled_agent.reservoir_size,
            sled_role: inv_sled_agent.sled_role,
            sled_agent_address: inv_sled_agent.sled_agent_address,
            sled_id,
            usable_hardware_threads: inv_sled_agent.usable_hardware_threads,
            usable_physical_ram: inv_sled_agent.usable_physical_ram,
            disks: vec![],
            zpools: vec![],
            datasets: vec![],
            ledgered_sled_config: inv_sled_agent.ledgered_sled_config.clone(),
            reconciler_status: inv_sled_agent.reconciler_status.clone(),
            last_reconciliation: inv_sled_agent.last_reconciliation.clone(),
        };

        Sled {
            sled_id,
            inventory_sp,
            inventory_sled_agent,
            policy: sled_policy,
            state: sled_state,
            resources: sled_resources,
            sp_active_caboose,
            sp_inactive_caboose,
        }
    }

    /// Adds a dataset to the system description.
    ///
    /// The inventory values for "available space" and "used space" are
    /// made up, since this is a synthetic dataset.
    pub fn add_synthetic_dataset(
        &mut self,
        config: omicron_common::disk::DatasetConfig,
    ) {
        self.inventory_sled_agent.datasets.push(InventoryDataset {
            id: Some(config.id),
            name: config.name.full_name(),
            available: ByteCount::from_gibibytes_u32(1),
            used: ByteCount::from_gibibytes_u32(0),
            quota: config.inner.quota,
            reservation: config.inner.reservation,
            compression: config.inner.compression.to_string(),
        });
    }

    fn sp_state(&self) -> Option<&(u16, SpState)> {
        self.inventory_sp.as_ref()
    }

    fn sled_agent_inventory(&self) -> &Inventory {
        &self.inventory_sled_agent
    }

    fn sp_active_caboose(&self) -> Option<&Caboose> {
        self.sp_active_caboose.as_deref()
    }

    fn sp_inactive_caboose(&self) -> Option<&Caboose> {
        self.sp_inactive_caboose.as_deref()
    }

    /// Update the reported SP versions
    ///
    /// If either field is `None`, that field is _unchanged_.
    // Note that this means there's no way to _unset_ the version.
    fn set_sp_versions(
        &mut self,
        active_version: Option<ArtifactVersion>,
        inactive_version: Option<ExpectedVersion>,
    ) {
        if let Some(active_version) = active_version {
            match &mut self.sp_active_caboose {
                Some(caboose) => {
                    Arc::make_mut(caboose).version = active_version.to_string()
                }
                new @ None => {
                    *new = Some(Arc::new(Self::default_sp_caboose(
                        active_version.to_string(),
                    )));
                }
            }
        }

        if let Some(inactive_version) = inactive_version {
            match inactive_version {
                ExpectedVersion::NoValidVersion => {
                    self.sp_inactive_caboose = None;
                }
                ExpectedVersion::Version(v) => {
                    match &mut self.sp_inactive_caboose {
                        Some(caboose) => {
                            Arc::make_mut(caboose).version = v.to_string()
                        }
                        new @ None => {
                            *new = Some(Arc::new(Self::default_sp_caboose(
                                v.to_string(),
                            )));
                        }
                    }
                }
            }
        }
    }

    fn default_sp_caboose(version: String) -> Caboose {
        let board = sp_sim::SIM_GIMLET_BOARD.to_string();
        Caboose {
            board: board.clone(),
            git_commit: String::from("unknown"),
            name: board,
            version: version.to_string(),
            sign: None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SubnetIterator {
    subnets: Ipv6Subnets,
}

impl SubnetIterator {
    fn new(rack_subnet: Ipv6Net) -> Self {
        let mut subnets = rack_subnet.subnets(SLED_PREFIX).unwrap();
        // Skip the initial DNS subnet.
        // (The same behavior is replicated in RSS in `Plan::create()` in
        // sled-agent/src/rack_setup/plan/sled.rs.)
        subnets.next();
        Self { subnets }
    }
}

impl Iterator for SubnetIterator {
    type Item = Ipv6Subnet<SLED_PREFIX>;

    fn next(&mut self) -> Option<Self::Item> {
        self.subnets.next().map(|s| Ipv6Subnet::new(s.network()))
    }
}

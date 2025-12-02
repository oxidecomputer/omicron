// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Builders for constructing descriptions of systems (real or synthetic) and
//! associated inventory collections and blueprints

use anyhow::{Context, anyhow, bail, ensure};
use chrono::DateTime;
use chrono::Utc;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use indexmap::IndexMap;
use ipnet::Ipv6Net;
use ipnet::Ipv6Subnets;
use nexus_inventory::CollectionBuilder;
use nexus_sled_agent_shared::inventory::Baseboard;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::Inventory;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use nexus_sled_agent_shared::inventory::InventoryDisk;
use nexus_sled_agent_shared::inventory::InventoryZpool;
use nexus_sled_agent_shared::inventory::MupdateOverrideBootInventory;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::SledCpuFamily;
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_sled_agent_shared::inventory::ZoneImageResolverInventory;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_sled_agent_shared::inventory::ZoneManifestBootInventory;
use nexus_types::deployment::ClickhousePolicy;
use nexus_types::deployment::CockroachDbClusterVersion;
use nexus_types::deployment::CockroachDbSettings;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::ExternalIpPolicy;
use nexus_types::deployment::OximeterReadPolicy;
use nexus_types::deployment::PlannerConfig;
use nexus_types::deployment::PlanningInputBuilder;
use nexus_types::deployment::Policy;
use nexus_types::deployment::SledDetails;
use nexus_types::deployment::SledDisk;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::TargetReleaseDescription;
use nexus_types::deployment::TufRepoPolicy;
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
use omicron_common::address::Ipv4Range;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::RACK_PREFIX;
use omicron_common::address::SLED_PREFIX;
use omicron_common::address::get_sled_address;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::DiskVariant;
use omicron_common::disk::M2Slot;
use omicron_common::policy::CRUCIBLE_PANTRY_REDUNDANCY;
use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
use omicron_common::policy::NEXUS_REDUNDANCY;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use sled_hardware_types::GIMLET_SLED_MODEL;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Debug;
use std::mem;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::time::Duration;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;

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
    external_ip_policy: ExternalIpPolicy,
    internal_dns_version: Generation,
    external_dns_version: Generation,
    clickhouse_policy: Option<ClickhousePolicy>,
    oximeter_read_policy: OximeterReadPolicy,
    tuf_repo: TufRepoPolicy,
    old_repo: TufRepoPolicy,
    planner_config: PlannerConfig,
    ignore_impossible_mgs_updates_since: DateTime<Utc>,
    active_nexus_zones: BTreeSet<OmicronZoneUuid>,
    not_yet_nexus_zones: BTreeSet<OmicronZoneUuid>,
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
        let target_crucible_pantry_zone_count = CRUCIBLE_PANTRY_REDUNDANCY;

        // TODO-cleanup These are wrong, but we don't currently set up any
        // of these zones in our fake system, so this prevents downstream test
        // issues with the planner thinking our system is out of date from the
        // gate.
        let target_boundary_ntp_zone_count = 0;
        let target_cockroachdb_zone_count = 0;
        let target_oximeter_zone_count = 0;

        let target_cockroachdb_cluster_version =
            CockroachDbClusterVersion::POLICY;

        // Nexus / Boundary NTPs IPs from TEST-NET-1 (RFC 5737).
        //
        // This policy doesn't configure any external DNS IPs.
        let external_ip_policy = {
            let mut builder = ExternalIpPolicy::builder();
            builder
                .push_service_pool_ipv4_range(
                    Ipv4Range::new(
                        "192.0.2.2".parse::<Ipv4Addr>().unwrap(),
                        "192.0.2.20".parse::<Ipv4Addr>().unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();
            builder.build()
        };

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
            external_ip_policy,
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            clickhouse_policy: None,
            oximeter_read_policy: OximeterReadPolicy::new(1),
            tuf_repo: TufRepoPolicy::initial(),
            old_repo: TufRepoPolicy::initial(),
            planner_config: PlannerConfig::default(),
            ignore_impossible_mgs_updates_since: Utc::now(),
            active_nexus_zones: BTreeSet::new(),
            not_yet_nexus_zones: BTreeSet::new(),
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

    pub fn set_target_nexus_zone_count(&mut self, count: usize) -> &mut Self {
        self.target_nexus_zone_count = count;
        self
    }

    pub fn target_nexus_zone_count(&self) -> usize {
        self.target_nexus_zone_count
    }

    pub fn set_target_cockroachdb_zone_count(
        &mut self,
        count: usize,
    ) -> &mut Self {
        self.target_cockroachdb_zone_count = count;
        self
    }

    pub fn target_cockroachdb_zone_count(&self) -> usize {
        self.target_cockroachdb_zone_count
    }

    pub fn set_target_boundary_ntp_zone_count(
        &mut self,
        count: usize,
    ) -> &mut Self {
        self.target_boundary_ntp_zone_count = count;
        self
    }

    pub fn target_boundary_ntp_zone_count(&self) -> usize {
        self.target_boundary_ntp_zone_count
    }

    pub fn set_target_crucible_pantry_zone_count(
        &mut self,
        count: usize,
    ) -> &mut Self {
        self.target_crucible_pantry_zone_count = count;
        self
    }

    pub fn target_crucible_pantry_zone_count(&self) -> usize {
        self.target_crucible_pantry_zone_count
    }

    pub fn set_target_internal_dns_zone_count(
        &mut self,
        count: usize,
    ) -> &mut Self {
        self.target_internal_dns_zone_count = count;
        self
    }

    pub fn target_internal_dns_zone_count(&self) -> usize {
        self.target_internal_dns_zone_count
    }

    pub fn external_ip_policy(&self) -> &ExternalIpPolicy {
        &self.external_ip_policy
    }

    pub fn set_external_ip_policy(
        &mut self,
        policy: ExternalIpPolicy,
    ) -> &mut Self {
        self.external_ip_policy = policy;
        self
    }

    /// Set the clickhouse policy
    pub fn clickhouse_policy(&mut self, policy: ClickhousePolicy) -> &mut Self {
        self.clickhouse_policy = Some(policy);
        self
    }

    /// Resolve a serial number into a sled ID.
    pub fn serial_to_sled_id(&self, serial: &str) -> anyhow::Result<SledUuid> {
        let sled_id = self.sleds.values().find_map(|sled| {
            if let Some((_, sp_state)) = sled.sp_state() {
                if sp_state.serial_number == serial {
                    return Some(sled.sled_id);
                }
            }
            None
        });
        sled_id.with_context(|| {
            let known_serials = self
                .sleds
                .values()
                .filter_map(|sled| {
                    sled.sp_state()
                        .map(|(_, sp_state)| sp_state.serial_number.as_str())
                })
                .collect::<Vec<_>>();
            format!(
                "sled not found with serial {serial} (known serials: {})",
                known_serials.join(", "),
            )
        })
    }

    pub fn get_sled(&self, sled_id: SledUuid) -> anyhow::Result<&Sled> {
        let Some(sled) = self.sleds.get(&sled_id) else {
            bail!("Sled not found with id {sled_id}");
        };
        Ok(sled)
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
            sled.policy,
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
        let sled = self.get_sled_mut(sled_id)?;

        sled.inventory_sled_agent.ledgered_sled_config =
            Some(sled_config.clone());

        // Present results as though the reconciler has successfully completed.
        sled.inventory_sled_agent.reconciler_status =
            ConfigReconcilerInventoryStatus::Idle {
                completed_at: Utc::now(),
                ran_for: Duration::from_secs(5),
            };
        match sled.inventory_sled_agent.last_reconciliation.as_mut() {
            Some(last_reconciliation) => {
                last_reconciliation.debug_update_assume_success(sled_config);
            }
            None => {
                sled.inventory_sled_agent.last_reconciliation =
                    Some(ConfigReconcilerInventory::debug_assume_success(
                        sled_config,
                    ));
            }
        };

        Ok(self)
    }

    /// Set the policy for a sled in the system.
    pub fn sled_set_policy(
        &mut self,
        sled_id: SledUuid,
        policy: SledPolicy,
    ) -> anyhow::Result<&mut Self> {
        let sled = self.get_sled_mut(sled_id)?;
        sled.policy = policy;
        Ok(self)
    }

    /// Set whether a sled is visible in the inventory.
    ///
    /// Returns the previous visibility setting.
    pub fn sled_set_inventory_visibility(
        &mut self,
        sled_id: SledUuid,
        visibility: SledInventoryVisibility,
    ) -> anyhow::Result<SledInventoryVisibility> {
        let sled = self.get_sled_mut(sled_id)?;
        let prev = sled.inventory_visibility;
        sled.inventory_visibility = visibility;
        Ok(prev)
    }

    /// Update the RoT bootloader versions reported for a sled.
    ///
    /// Where `None` is provided, no changes are made.
    pub fn sled_update_rot_bootloader_versions(
        &mut self,
        sled_id: SledUuid,
        stage0_version: Option<ArtifactVersion>,
        stage0_next_version: Option<ExpectedVersion>,
    ) -> anyhow::Result<&mut Self> {
        let sled = self.get_sled_mut(sled_id)?;
        sled.set_rot_bootloader_versions(stage0_version, stage0_next_version);
        Ok(self)
    }

    pub fn sled_stage0_version(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<Option<&str>> {
        let sled = self.sleds.get(&sled_id).with_context(|| {
            format!("attempted to access sled {} not found in system", sled_id)
        })?;
        Ok(sled.stage0_caboose().map(|c| c.version.as_ref()))
    }

    pub fn sled_stage0_next_version(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<Option<&str>> {
        let sled = self.sleds.get(&sled_id).with_context(|| {
            format!("attempted to access sled {} not found in system", sled_id)
        })?;
        Ok(sled.stage0_next_caboose().map(|c| c.version.as_ref()))
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
        let sled = self.get_sled_mut(sled_id)?;
        sled.set_sp_versions(active_version, inactive_version);
        Ok(self)
    }

    /// Update the host OS phase 1 artifacts reported for a sled.
    ///
    /// Where `None` is provided, no changes are made.
    pub fn sled_update_host_phase_1_artifacts(
        &mut self,
        sled_id: SledUuid,
        active: Option<M2Slot>,
        slot_a: Option<ArtifactHash>,
        slot_b: Option<ArtifactHash>,
    ) -> anyhow::Result<&mut Self> {
        let sled = self.get_sled_mut(sled_id)?;
        sled.set_host_phase_1_artifacts(active, slot_a, slot_b);
        Ok(self)
    }

    /// Update the host OS phase 2 artifacts reported for a sled.
    ///
    /// Where `None` is provided, no changes are made.
    pub fn sled_update_host_phase_2_artifacts(
        &mut self,
        sled_id: SledUuid,
        boot_disk: Option<M2Slot>,
        slot_a: Option<ArtifactHash>,
        slot_b: Option<ArtifactHash>,
    ) -> anyhow::Result<&mut Self> {
        let sled = self.get_sled_mut(sled_id)?;
        sled.set_host_phase_2_artifacts(boot_disk, slot_a, slot_b);
        Ok(self)
    }

    /// Set the zone manifest for a sled from a provided `TufRepoDescription`.
    pub fn sled_set_zone_manifest(
        &mut self,
        sled_id: SledUuid,
        boot_inventory: Result<ZoneManifestBootInventory, String>,
    ) -> anyhow::Result<&mut Self> {
        let sled = self.get_sled_mut(sled_id)?;
        sled.set_zone_manifest(boot_inventory);
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

    pub fn sled_sp_state(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<Option<&(u16, SpState)>> {
        let sled = self.get_sled(sled_id)?;
        Ok(sled.sp_state())
    }

    /// Update the RoT versions reported for a sled.
    ///
    /// Where `None` is provided, no changes are made.
    pub fn sled_update_rot_versions(
        &mut self,
        sled_id: SledUuid,
        overrides: RotStateOverrides,
    ) -> anyhow::Result<&mut Self> {
        let sled = self.get_sled_mut(sled_id)?;
        sled.set_rot_versions(overrides);
        Ok(self)
    }

    pub fn sled_rot_active_slot(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<RotSlot> {
        let sp_state = self.sled_sp_state(sled_id)?;
        sp_state
            .ok_or_else(|| {
                anyhow!("failed to retrieve SP state from sled id: {sled_id}")
            })
            .and_then(|(_hw_slot, sp_state)| match sp_state.rot.clone() {
                RotState::V2 { active, .. } | RotState::V3 { active, .. } => {
                    Ok(active)
                }
                RotState::CommunicationFailed { message } => Err(anyhow!(
                    "failed to retrieve active RoT slot due to \
                        communication failure: {message}"
                )),
            })
    }

    pub fn sled_rot_persistent_boot_preference(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<RotSlot> {
        let sp_state = self.sled_sp_state(sled_id)?;
        sp_state
            .ok_or_else(|| {
                anyhow!("failed to retrieve SP state from sled id: {sled_id}")
            })
            .and_then(|(_hw_slot, sp_state)| match sp_state.rot.clone() {
                RotState::V2 { persistent_boot_preference, .. }
                | RotState::V3 { persistent_boot_preference, .. } => {
                    Ok(persistent_boot_preference)
                }
                RotState::CommunicationFailed { message } => Err(anyhow!(
                    "failed to retrieve persistent boot preference slot \
                        due to communication failure: {message}"
                )),
            })
    }

    pub fn sled_rot_pending_persistent_boot_preference(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<Option<RotSlot>> {
        let sp_state = self.sled_sp_state(sled_id)?;
        sp_state
            .ok_or_else(|| {
                anyhow!("failed to retrieve SP state from sled id: {sled_id}")
            })
            .and_then(|(_hw_slot, sp_state)| match sp_state.rot.clone() {
                RotState::V2 { pending_persistent_boot_preference, .. }
                | RotState::V3 { pending_persistent_boot_preference, .. } => {
                    Ok(pending_persistent_boot_preference)
                }
                RotState::CommunicationFailed { message } => Err(anyhow!(
                    "failed to retrieve pending persistent boot \
                        preference slot due to communication failure: {message}"
                )),
            })
    }

    pub fn sled_rot_transient_boot_preference(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<Option<RotSlot>> {
        let sp_state = self.sled_sp_state(sled_id)?;
        sp_state
            .ok_or_else(|| {
                anyhow!("failed to retrieve SP state from sled id: {sled_id}")
            })
            .and_then(|(_hw_slot, sp_state)| match sp_state.rot.clone() {
                RotState::V2 { transient_boot_preference, .. }
                | RotState::V3 { transient_boot_preference, .. } => {
                    Ok(transient_boot_preference)
                }
                RotState::CommunicationFailed { message } => Err(anyhow!(
                    "failed to retrieve transient boot preference slot \
                        due to communication failure: {message}"
                )),
            })
    }

    pub fn sled_rot_slot_a_version(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<Option<&str>> {
        let sled = self.get_sled(sled_id)?;
        Ok(sled.rot_slot_a_caboose().map(|c| c.version.as_ref()))
    }

    pub fn sled_rot_slot_b_version(
        &self,
        sled_id: SledUuid,
    ) -> anyhow::Result<Option<&str>> {
        let sled = self.get_sled(sled_id)?;
        Ok(sled.rot_slot_b_caboose().map(|c| c.version.as_ref()))
    }

    /// Set a sled's mupdate override field.
    ///
    /// Returns the previous value, or previous error if set.
    pub fn sled_set_mupdate_override(
        &mut self,
        sled_id: SledUuid,
        mupdate_override: Option<MupdateOverrideUuid>,
    ) -> anyhow::Result<Result<Option<MupdateOverrideUuid>, String>> {
        let sled = self.get_sled_mut(sled_id)?;
        Ok(sled.set_mupdate_override(Ok(mupdate_override)))
    }

    /// Set a sled's mupdate override field to an error.
    ///
    /// Returns the previous value, or previous error if set.
    pub fn sled_set_mupdate_override_error(
        &mut self,
        sled_id: SledUuid,
        message: String,
    ) -> anyhow::Result<Result<Option<MupdateOverrideUuid>, String>> {
        let sled = self.get_sled_mut(sled_id)?;
        Ok(sled.set_mupdate_override(Err(message)))
    }

    pub fn set_tuf_repo(&mut self, tuf_repo: TufRepoPolicy) {
        self.tuf_repo = tuf_repo;
    }

    /// Get the planner's configuration.
    pub fn get_planner_config(&self) -> PlannerConfig {
        self.planner_config
    }

    /// Set the planner's configuration.
    ///
    /// Returns the previous value.
    pub fn set_planner_config(
        &mut self,
        config: PlannerConfig,
    ) -> PlannerConfig {
        mem::replace(&mut self.planner_config, config)
    }

    pub fn set_target_release(
        &mut self,
        description: TargetReleaseDescription,
    ) -> &mut Self {
        // Create a new TufRepoPolicy by bumping the generation.
        let new_repo = TufRepoPolicy {
            target_release_generation: self
                .tuf_repo
                .target_release_generation
                .next(),
            description,
        };

        let _old_repo = self.set_tuf_repo_inner(new_repo);

        // It's tempting to consider setting old_repo to the current tuf_repo,
        // but that requires the invariant that old_repo is always the current
        // target release and that an update isn't currently in progress. See
        // https://github.com/oxidecomputer/omicron/issues/8056 for some
        // discussion.
        //
        // We provide a method to set the old repo explicitly with these
        // assumptions in mind: `set_target_release_and_old_repo`.

        self
    }

    pub fn set_target_release_and_old_repo(
        &mut self,
        description: TargetReleaseDescription,
    ) -> &mut Self {
        let new_repo = TufRepoPolicy {
            target_release_generation: self
                .tuf_repo
                .target_release_generation
                .next(),
            description,
        };

        let old_repo = self.set_tuf_repo_inner(new_repo);
        self.old_repo = old_repo;

        self
    }

    fn set_tuf_repo_inner(&mut self, new_repo: TufRepoPolicy) -> TufRepoPolicy {
        mem::replace(&mut self.tuf_repo, new_repo)
    }

    pub fn set_ignore_impossible_mgs_updates_since(
        &mut self,
        since: DateTime<Utc>,
    ) -> &mut Self {
        self.ignore_impossible_mgs_updates_since = since;
        self
    }

    pub fn set_active_nexus_zones(
        &mut self,
        active_nexus_zones: BTreeSet<OmicronZoneUuid>,
    ) -> &mut Self {
        self.active_nexus_zones = active_nexus_zones;
        self
    }

    pub fn set_not_yet_nexus_zones(
        &mut self,
        not_yet_nexus_zones: BTreeSet<OmicronZoneUuid>,
    ) -> &mut Self {
        self.not_yet_nexus_zones = not_yet_nexus_zones;
        self
    }

    pub fn target_release(&self) -> &TufRepoPolicy {
        &self.tuf_repo
    }

    pub fn to_collection_builder(&self) -> anyhow::Result<CollectionBuilder> {
        let collector_label = self
            .collector
            .as_ref()
            .cloned()
            .unwrap_or_else(|| String::from("example"));
        let mut builder = CollectionBuilder::new(collector_label);

        for s in self.sleds.values() {
            if s.inventory_visibility == SledInventoryVisibility::Hidden {
                // Don't return this sled as part of the inventory collection.
                continue;
            }
            if let Some((slot, sp_state)) = s.sp_state() {
                builder
                    .found_sp_state(
                        "fake MGS 1",
                        SpType::Sled,
                        *slot,
                        sp_state.clone(),
                    )
                    .context("recording SP state")?;

                let baseboard_id = BaseboardId {
                    part_number: sp_state.model.clone(),
                    serial_number: sp_state.serial_number.clone(),
                };
                if let Some(stage0) = s.stage0_caboose() {
                    builder
                        .found_caboose(
                            &baseboard_id,
                            CabooseWhich::Stage0,
                            "fake MGS 1",
                            SpComponentCaboose {
                                board: stage0.board.clone(),
                                epoch: None,
                                git_commit: stage0.git_commit.clone(),
                                name: stage0.name.clone(),
                                sign: stage0.sign.clone(),
                                version: stage0.version.clone(),
                            },
                        )
                        .context("recording RoT bootloader stage0 caboose")?;
                }

                if let Some(stage0_next) = s.stage0_next_caboose() {
                    builder
                        .found_caboose(
                            &baseboard_id,
                            CabooseWhich::Stage0Next,
                            "fake MGS 1",
                            SpComponentCaboose {
                                board: stage0_next.board.clone(),
                                epoch: None,
                                git_commit: stage0_next.git_commit.clone(),
                                name: stage0_next.name.clone(),
                                sign: stage0_next.sign.clone(),
                                version: stage0_next.version.clone(),
                            },
                        )
                        .context(
                            "recording RoT bootloader stage0_next caboose",
                        )?;
                }

                if let Some(active_slot) = s.sp_host_phase_1_active_slot() {
                    builder
                        .found_host_phase_1_active_slot(
                            &baseboard_id,
                            "fake MGS 1",
                            active_slot,
                        )
                        .context("recording SP host phase 1 active slot")?;
                }

                for (m2_slot, hash) in s.sp_host_phase_1_hash_flash() {
                    builder
                        .found_host_phase_1_flash_hash(
                            &baseboard_id,
                            m2_slot,
                            "fake MGS 1",
                            hash,
                        )
                        .context("recording SP host phase 1 flash hash")?;
                }

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

                if let Some(slot_a) = &s.rot_slot_a_caboose() {
                    builder
                        .found_caboose(
                            &baseboard_id,
                            CabooseWhich::RotSlotA,
                            "fake MGS 1",
                            SpComponentCaboose {
                                board: slot_a.board.clone(),
                                epoch: None,
                                git_commit: slot_a.git_commit.clone(),
                                name: slot_a.name.clone(),
                                sign: slot_a.sign.clone(),
                                version: slot_a.version.clone(),
                            },
                        )
                        .context("recording RoT slot a caboose")?;
                }

                if let Some(slot_b) = &s.rot_slot_b_caboose() {
                    builder
                        .found_caboose(
                            &baseboard_id,
                            CabooseWhich::RotSlotB,
                            "fake MGS 1",
                            SpComponentCaboose {
                                board: slot_b.board.clone(),
                                epoch: None,
                                git_commit: slot_b.git_commit.clone(),
                                name: slot_b.name.clone(),
                                sign: slot_b.sign.clone(),
                                version: slot_b.version.clone(),
                            },
                        )
                        .context("recording RoT slot b caboose")?;
                }
            }

            let sled_inventory = s.sled_agent_inventory();
            builder
                .found_sled_inventory("fake sled agent", sled_inventory.clone())
                .context("recording sled agent")?;

            // Create responses we'd expect from all internal DNS zones, if
            // they successfully receive their expected most-recent set of data.
            if let Some(config) = sled_inventory.ledgered_sled_config.as_ref() {
                for zone in &config.zones {
                    if matches!(zone.zone_type.kind(), ZoneKind::InternalDns) {
                        builder.found_internal_dns_generation_status(
                            nexus_types::inventory::InternalDnsGenerationStatus {
                                zone_id: zone.id,
                                generation: self.internal_dns_version,
                            }
                        ).unwrap();
                    }

                    // TODO: We may want to include responses from Boundary NTP
                    // and CockroachDb zones here too - but neither of those are
                    // currently part of the example system, so their synthetic
                    // responses to inventory collection aren't necessary yet.
                }
            }
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
            external_ips: self.external_ip_policy.clone(),
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
            planner_config: self.planner_config,
        };
        let mut builder = PlanningInputBuilder::new(
            policy,
            self.internal_dns_version,
            self.external_dns_version,
            CockroachDbSettings::empty(),
        );
        builder.set_active_nexus_zones(self.active_nexus_zones.clone());
        builder.set_not_yet_nexus_zones(self.not_yet_nexus_zones.clone());
        builder.set_ignore_impossible_mgs_updates_since(
            self.ignore_impossible_mgs_updates_since,
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
    policy: SledPolicy,
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
            policy: SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            },
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

    /// Sets this sled's policy.
    pub fn policy(mut self, policy: SledPolicy) -> Self {
        self.policy = policy;
        self
    }
}

/// Convenience structure summarizing `Sled` inputs that come from inventory
#[derive(Debug)]
pub struct SledHwInventory<'a> {
    pub baseboard_id: &'a BaseboardId,
    pub sp: &'a nexus_types::inventory::ServiceProcessor,
    pub rot: &'a nexus_types::inventory::RotState,
    pub stage0: Option<Arc<nexus_types::inventory::Caboose>>,
    pub stage0_next: Option<Arc<nexus_types::inventory::Caboose>>,
    pub sp_host_phase_1_active_slot: Option<M2Slot>,
    pub sp_host_phase_1_hash_flash: BTreeMap<M2Slot, ArtifactHash>,
    pub sp_active: Option<Arc<nexus_types::inventory::Caboose>>,
    pub sp_inactive: Option<Arc<nexus_types::inventory::Caboose>>,
    pub rot_slot_a: Option<Arc<nexus_types::inventory::Caboose>>,
    pub rot_slot_b: Option<Arc<nexus_types::inventory::Caboose>>,
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
    inventory_visibility: SledInventoryVisibility,
    policy: SledPolicy,
    state: SledState,
    resources: SledResources,
    stage0_caboose: Option<Arc<nexus_types::inventory::Caboose>>,
    stage0_next_caboose: Option<Arc<nexus_types::inventory::Caboose>>,
    sp_host_phase_1_active_slot: Option<M2Slot>,
    sp_host_phase_1_hash_flash: BTreeMap<M2Slot, ArtifactHash>,
    sp_active_caboose: Option<Arc<nexus_types::inventory::Caboose>>,
    sp_inactive_caboose: Option<Arc<nexus_types::inventory::Caboose>>,
    rot_slot_a_caboose: Option<Arc<nexus_types::inventory::Caboose>>,
    rot_slot_b_caboose: Option<Arc<nexus_types::inventory::Caboose>>,
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
        policy: SledPolicy,
        sled_config: OmicronSledConfig,
        nzpools: u8,
    ) -> Sled {
        use typed_rng::TypedUuidRng;
        let unique = unique.unwrap_or_else(|| hardware_slot.to_string());
        let model = GIMLET_SLED_MODEL.to_string();
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
                cpu_family: SledCpuFamily::AmdMilan,
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
                ledgered_sled_config: Some(sled_config.clone()),
                reconciler_status: ConfigReconcilerInventoryStatus::Idle {
                    completed_at: Utc::now(),
                    ran_for: Duration::from_secs(5),
                },
                last_reconciliation: Some(
                    ConfigReconcilerInventory::debug_assume_success(
                        sled_config,
                    ),
                ),
                // XXX: return something more reasonable here?
                zone_image_resolver: ZoneImageResolverInventory::new_fake(),
            }
        };

        Sled {
            sled_id,
            inventory_sp,
            inventory_sled_agent,
            inventory_visibility: SledInventoryVisibility::Visible,
            policy,
            state: SledState::Active,
            resources: SledResources { subnet: sled_subnet, zpools },
            stage0_caboose: Some(Arc::new(
                Self::default_rot_bootloader_caboose(String::from("0.0.1")),
            )),
            stage0_next_caboose: None,
            sp_host_phase_1_active_slot: Some(M2Slot::A),
            sp_host_phase_1_hash_flash: [
                (M2Slot::A, ArtifactHash([1; 32])),
                (M2Slot::B, ArtifactHash([2; 32])),
            ]
            .into_iter()
            .collect(),
            sp_active_caboose: Some(Arc::new(Self::default_sp_caboose(
                String::from("0.0.1"),
            ))),
            sp_inactive_caboose: None,
            rot_slot_a_caboose: Some(Arc::new(Self::default_rot_caboose(
                String::from("0.0.2"),
            ))),
            rot_slot_b_caboose: None,
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

        let stage0_caboose =
            inventory_sp.as_ref().and_then(|hw| hw.stage0.clone());
        let stage0_next_caboose =
            inventory_sp.as_ref().and_then(|hw| hw.stage0_next.clone());
        let sp_host_phase_1_active_slot =
            inventory_sp.as_ref().and_then(|hw| hw.sp_host_phase_1_active_slot);
        let sp_host_phase_1_hash_flash = inventory_sp
            .as_ref()
            .map(|hw| hw.sp_host_phase_1_hash_flash.clone())
            .unwrap_or_default();
        let sp_active_caboose =
            inventory_sp.as_ref().and_then(|hw| hw.sp_active.clone());
        let sp_inactive_caboose =
            inventory_sp.as_ref().and_then(|hw| hw.sp_inactive.clone());
        let rot_slot_a_caboose =
            inventory_sp.as_ref().and_then(|hw| hw.rot_slot_a.clone());
        let rot_slot_b_caboose =
            inventory_sp.as_ref().and_then(|hw| hw.rot_slot_b.clone());
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
            cpu_family: inv_sled_agent.cpu_family,
            disks: vec![],
            zpools: vec![],
            datasets: vec![],
            ledgered_sled_config: inv_sled_agent.ledgered_sled_config.clone(),
            reconciler_status: inv_sled_agent.reconciler_status.clone(),
            last_reconciliation: inv_sled_agent.last_reconciliation.clone(),
            zone_image_resolver: inv_sled_agent.zone_image_resolver.clone(),
        };

        Sled {
            sled_id,
            inventory_sp,
            inventory_sled_agent,
            inventory_visibility: SledInventoryVisibility::Visible,
            policy: sled_policy,
            state: sled_state,
            resources: sled_resources,
            stage0_caboose,
            stage0_next_caboose,
            sp_host_phase_1_active_slot,
            sp_host_phase_1_hash_flash,
            sp_active_caboose,
            sp_inactive_caboose,
            rot_slot_a_caboose,
            rot_slot_b_caboose,
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

    pub fn resources_mut(&mut self) -> &mut SledResources {
        &mut self.resources
    }

    pub fn sp_state(&self) -> Option<&(u16, SpState)> {
        self.inventory_sp.as_ref()
    }

    pub fn sp_host_phase_1_active_slot(&self) -> Option<M2Slot> {
        self.sp_host_phase_1_active_slot
    }

    pub fn sp_host_phase_1_hash_flash(
        &self,
    ) -> impl Iterator<Item = (M2Slot, ArtifactHash)> + '_ {
        self.sp_host_phase_1_hash_flash
            .iter()
            .map(|(&slot, &hash)| (slot, hash))
    }

    fn sled_agent_inventory(&self) -> &Inventory {
        &self.inventory_sled_agent
    }

    fn rot_slot_a_caboose(&self) -> Option<&Caboose> {
        self.rot_slot_a_caboose.as_deref()
    }

    fn rot_slot_b_caboose(&self) -> Option<&Caboose> {
        self.rot_slot_b_caboose.as_deref()
    }

    fn sp_active_caboose(&self) -> Option<&Caboose> {
        self.sp_active_caboose.as_deref()
    }

    fn sp_inactive_caboose(&self) -> Option<&Caboose> {
        self.sp_inactive_caboose.as_deref()
    }

    fn stage0_caboose(&self) -> Option<&Caboose> {
        self.stage0_caboose.as_deref()
    }

    fn stage0_next_caboose(&self) -> Option<&Caboose> {
        self.stage0_next_caboose.as_deref()
    }

    fn set_zone_manifest(
        &mut self,
        boot_inventory: Result<ZoneManifestBootInventory, String>,
    ) {
        self.inventory_sled_agent
            .zone_image_resolver
            .zone_manifest
            .boot_inventory = boot_inventory;
    }

    /// Update the reported RoT bootloader versions
    ///
    /// If either field is `None`, that field is _unchanged_.
    fn set_rot_bootloader_versions(
        &mut self,
        stage0_version: Option<ArtifactVersion>,
        stage0_next_version: Option<ExpectedVersion>,
    ) {
        if let Some(stage0_version) = stage0_version {
            match &mut self.stage0_caboose {
                Some(caboose) => {
                    Arc::make_mut(caboose).version = stage0_version.to_string()
                }
                new @ None => {
                    *new =
                        Some(Arc::new(Self::default_rot_bootloader_caboose(
                            stage0_version.to_string(),
                        )));
                }
            }
        }

        if let Some(stage0_next_version) = stage0_next_version {
            match stage0_next_version {
                ExpectedVersion::NoValidVersion => {
                    self.stage0_next_caboose = None;
                }
                ExpectedVersion::Version(v) => {
                    match &mut self.stage0_next_caboose {
                        Some(caboose) => {
                            Arc::make_mut(caboose).version = v.to_string()
                        }
                        new @ None => {
                            *new = Some(Arc::new(
                                Self::default_rot_bootloader_caboose(
                                    v.to_string(),
                                ),
                            ));
                        }
                    }
                }
            }
        }
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

    /// Update the reported RoT versions
    ///
    /// If any of the overrides are `None`, that field is _unchanged_.
    // Note that this means there's no way to _unset_ the version.
    fn set_rot_versions(&mut self, overrides: RotStateOverrides) {
        let RotStateOverrides {
            active_slot_override,
            slot_a_version_override,
            slot_b_version_override,
            persistent_boot_preference_override,
            pending_persistent_boot_preference_override,
            transient_boot_preference_override,
        } = overrides;

        if let Some((_slot, sp_state)) = self.inventory_sp.as_mut() {
            match &mut sp_state.rot {
                RotState::V3 {
                    active,
                    persistent_boot_preference,
                    pending_persistent_boot_preference,
                    transient_boot_preference,
                    ..
                } => {
                    if let Some(active_slot_override) = active_slot_override {
                        *active = active_slot_override;
                    }
                    if let Some(persistent_boot_preference_override) =
                        persistent_boot_preference_override
                    {
                        *persistent_boot_preference =
                            persistent_boot_preference_override;
                    }

                    if let Some(pending_persistent_boot_preference_override) =
                        pending_persistent_boot_preference_override
                    {
                        *pending_persistent_boot_preference =
                            pending_persistent_boot_preference_override;
                    }

                    if let Some(transient_boot_preference_override) =
                        transient_boot_preference_override
                    {
                        *transient_boot_preference =
                            transient_boot_preference_override;
                    }
                }
                // We will only support RotState::V3
                _ => unreachable!(),
            };
        }

        if let Some(slot_a_version) = slot_a_version_override {
            match slot_a_version {
                ExpectedVersion::NoValidVersion => {
                    self.rot_slot_a_caboose = None;
                }
                ExpectedVersion::Version(v) => {
                    match &mut self.rot_slot_a_caboose {
                        Some(caboose) => {
                            Arc::make_mut(caboose).version = v.to_string()
                        }
                        new @ None => {
                            *new = Some(Arc::new(Self::default_rot_caboose(
                                v.to_string(),
                            )));
                        }
                    }
                }
            }
        }

        if let Some(slot_b_version) = slot_b_version_override {
            match slot_b_version {
                ExpectedVersion::NoValidVersion => {
                    self.rot_slot_b_caboose = None;
                }
                ExpectedVersion::Version(v) => {
                    match &mut self.rot_slot_b_caboose {
                        Some(caboose) => {
                            Arc::make_mut(caboose).version = v.to_string()
                        }
                        new @ None => {
                            *new = Some(Arc::new(Self::default_rot_caboose(
                                v.to_string(),
                            )));
                        }
                    }
                }
            }
        }
    }

    /// Update the reported host OS phase 1 artifacts
    ///
    /// If either field is `None`, that field is _unchanged_.
    // Note that this means there's no way to _unset_ the version.
    fn set_host_phase_1_artifacts(
        &mut self,
        active: Option<M2Slot>,
        slot_a: Option<ArtifactHash>,
        slot_b: Option<ArtifactHash>,
    ) {
        if let Some(active) = active {
            self.sp_host_phase_1_active_slot = Some(active);
        }

        if let Some(slot_a) = slot_a {
            self.sp_host_phase_1_hash_flash.insert(M2Slot::A, slot_a);
        }

        if let Some(slot_b) = slot_b {
            self.sp_host_phase_1_hash_flash.insert(M2Slot::B, slot_b);
        }
    }

    /// Update the reported host OS phase 2 artifacts
    ///
    /// If either field is `None`, that field is _unchanged_.
    // Note that this means there's no way to _unset_ the version.
    fn set_host_phase_2_artifacts(
        &mut self,
        boot_disk: Option<M2Slot>,
        slot_a: Option<ArtifactHash>,
        slot_b: Option<ArtifactHash>,
    ) {
        let last_reconciliation = self
            .inventory_sled_agent
            .last_reconciliation
            .as_mut()
            .expect("simulated system populates last reconciliation");

        if let Some(boot_disk) = boot_disk {
            last_reconciliation.boot_partitions.boot_disk = Ok(boot_disk);
        }

        if let Some(slot_a) = slot_a {
            last_reconciliation
                .boot_partitions
                .slot_a
                .as_mut()
                .expect("simulated system populates OS slots")
                .artifact_hash = slot_a;
        }

        if let Some(slot_b) = slot_b {
            last_reconciliation
                .boot_partitions
                .slot_b
                .as_mut()
                .expect("simulated system populates OS slots")
                .artifact_hash = slot_b;
        }
    }

    fn default_rot_bootloader_caboose(version: String) -> Caboose {
        let board = sp_sim::SIM_ROT_BOARD.to_string();
        Caboose {
            board: board.clone(),
            git_commit: String::from("unknown"),
            name: board.clone(),
            version: version.to_string(),
            sign: KnownArtifactKind::GimletRotBootloader
                .fake_artifact_hubris_sign(),
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

    fn default_rot_caboose(version: String) -> Caboose {
        let board = sp_sim::SIM_ROT_BOARD.to_string();
        Caboose {
            board: board.clone(),
            git_commit: String::from("unknown"),
            name: board.clone(),
            version: version.to_string(),
            sign: KnownArtifactKind::GimletRot.fake_artifact_hubris_sign(),
        }
    }

    /// Set the mupdate override field for a sled, returning the previous value.
    fn set_mupdate_override(
        &mut self,
        mupdate_override_id: Result<Option<MupdateOverrideUuid>, String>,
    ) -> Result<Option<MupdateOverrideUuid>, String> {
        // We don't alter the non-boot override because it's not used in this process.
        let inv = match mupdate_override_id {
            Ok(Some(id)) => Ok(Some(MupdateOverrideBootInventory {
                mupdate_override_id: id,
            })),
            Ok(None) => Ok(None),
            Err(message) => Err(message),
        };
        let prev = mem::replace(
            &mut self
                .inventory_sled_agent
                .zone_image_resolver
                .mupdate_override
                .boot_override,
            inv,
        );
        prev.map(|prev| prev.map(|prev| prev.mupdate_override_id))
    }
}

/// Settings that can be overriden in a simulated sled's RotState
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RotStateOverrides {
    pub active_slot_override: Option<RotSlot>,
    pub slot_a_version_override: Option<ExpectedVersion>,
    pub slot_b_version_override: Option<ExpectedVersion>,
    pub persistent_boot_preference_override: Option<RotSlot>,
    pub pending_persistent_boot_preference_override: Option<Option<RotSlot>>,
    pub transient_boot_preference_override: Option<Option<RotSlot>>,
}

/// The visibility of a sled in the inventory.
///
/// This enum can be used to simulate a sled temporarily dropping out and it not
/// being reported in the inventory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SledInventoryVisibility {
    Visible,
    Hidden,
}

impl fmt::Display for SledInventoryVisibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SledInventoryVisibility::Visible => write!(f, "visible"),
            SledInventoryVisibility::Hidden => write!(f, "hidden"),
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

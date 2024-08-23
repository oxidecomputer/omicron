// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level facility for generating Blueprints

use crate::ip_allocator::IpAllocator;
use crate::planner::zone_needs_expungement;
use crate::planner::ZoneExpungeReason;
use anyhow::anyhow;
use internal_dns::config::Host;
use internal_dns::config::Zone;
use ipnet::IpAdd;
use nexus_inventory::now_db_precision;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::OmicronZoneExternalFloatingIp;
use nexus_types::deployment::OmicronZoneExternalSnatIp;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledDetails;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::ZpoolFilter;
use nexus_types::deployment::ZpoolName;
use nexus_types::external_api::views::SledState;
use omicron_common::address::get_internal_dns_server_addresses;
use omicron_common::address::get_sled_address;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::ReservedRackSubnet;
use omicron_common::address::CP_SERVICES_RESERVED_ADDRESSES;
use omicron_common::address::DNS_HTTP_PORT;
use omicron_common::address::DNS_PORT;
use omicron_common::address::NTP_PORT;
use omicron_common::address::SLED_RESERVED_ADDRESSES;
use omicron_common::api::external::Generation;
use omicron_common::api::external::Vni;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_common::policy::MAX_DNS_REDUNDANCY;
use omicron_uuid_kinds::ExternalIpKind;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use rand::rngs::StdRng;
use rand::SeedableRng;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use thiserror::Error;
use typed_rng::TypedUuidRng;
use typed_rng::UuidRng;

use super::external_networking::BuilderExternalNetworking;
use super::external_networking::ExternalNetworkingChoice;
use super::external_networking::ExternalSnatNetworkingChoice;
use super::internal_dns::DnsSubnetAllocator;
use super::zones::is_already_expunged;
use super::zones::BuilderZoneState;
use super::zones::BuilderZonesConfig;

/// Errors encountered while assembling blueprints
#[derive(Debug, Error)]
pub enum Error {
    #[error("sled {sled_id}: ran out of available addresses for sled")]
    OutOfAddresses { sled_id: SledUuid },
    #[error(
        "sled {sled_id}: no available zpools for additional {kind:?} zones"
    )]
    NoAvailableZpool { sled_id: SledUuid, kind: ZoneKind },
    #[error("no Nexus zones exist in parent blueprint")]
    NoNexusZonesInParentBlueprint,
    #[error("no Boundary NTP zones exist in parent blueprint")]
    NoBoundaryNtpZonesInParentBlueprint,
    #[error("no external service IP addresses are available")]
    NoExternalServiceIpAvailable,
    #[error("no system MAC addresses are available")]
    NoSystemMacAddressAvailable,
    #[error("exhausted available OPTE IP addresses for service {kind:?}")]
    ExhaustedOpteIps { kind: ZoneKind },
    #[error(
        "invariant violation: found decommissioned sled with \
         {num_zones} non-expunged zones: {sled_id}"
    )]
    DecommissionedSledWithNonExpungedZones {
        sled_id: SledUuid,
        num_zones: usize,
    },
    #[error("programming error in planner")]
    Planner(#[source] anyhow::Error),
    #[error("no reserved subnets available for DNS")]
    NoAvailableDnsSubnets,
    #[error("can only have {MAX_DNS_REDUNDANCY} internal DNS servers")]
    TooManyDnsServers,
}

/// Describes whether an idempotent "ensure" operation resulted in action taken
/// or no action was necessary
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Ensure {
    /// action was taken
    Added,
    /// no action was necessary
    NotNeeded,
}

/// Describes whether an idempotent "ensure" operation resulted in multiple
/// actions taken or no action was necessary
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EnsureMultiple {
    /// action was taken, and multiple items were added
    Changed { added: usize, removed: usize },

    /// no action was necessary
    NotNeeded,
}

/// Describes operations which the BlueprintBuilder has performed to arrive
/// at its state.
///
/// This information is meant to act as a more strongly-typed flavor of
/// "comment", identifying which operations have occurred on the blueprint.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum Operation {
    AddZone { sled_id: SledUuid, kind: ZoneKind },
    UpdateDisks { sled_id: SledUuid, added: usize, removed: usize },
    ZoneExpunged { sled_id: SledUuid, reason: ZoneExpungeReason, count: usize },
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddZone { sled_id, kind } => {
                write!(f, "sled {sled_id}: added zone: {}", kind.report_str())
            }
            Self::UpdateDisks { sled_id, added, removed } => {
                write!(f, "sled {sled_id}: added {added} disks, removed {removed} disks")
            }
            Self::ZoneExpunged { sled_id, reason, count } => {
                let reason = match reason {
                    ZoneExpungeReason::DiskExpunged => {
                        "zone using expunged disk"
                    }
                    ZoneExpungeReason::SledDecommissioned => {
                        "sled state is decomissioned"
                    }
                    ZoneExpungeReason::SledExpunged => {
                        "sled policy is expunged"
                    }
                };
                write!(
                    f,
                    "sled {sled_id}: expunged {count} zones because: {reason}"
                )
            }
        }
    }
}

/// Helper for assembling a blueprint
///
/// There are two basic ways to assemble a new blueprint:
///
/// 1. Build one directly. This would generally only be used once in the
///    lifetime of a rack, to assemble the first blueprint during rack setup.
///    It is also common in tests. To start with a blueprint that contains an
///    empty zone config for some number of sleds, use
///    [`BlueprintBuilder::build_empty_with_sleds`].
///
/// 2. Build one _from_ another blueprint, called the "parent", making changes
///    as desired.  Use [`BlueprintBuilder::new_based_on`] for this.  Once the
///    new blueprint is created, there is no dependency on the parent one.
///    However, the new blueprint can only be made the system's target if its
///    parent is the current target.
pub struct BlueprintBuilder<'a> {
    #[allow(dead_code)]
    log: Logger,

    /// previous blueprint, on which this one will be based
    parent_blueprint: &'a Blueprint,

    // These fields are used to allocate resources for sleds.
    input: &'a PlanningInput,
    sled_ip_allocators: BTreeMap<SledUuid, IpAllocator>,
    external_networking: BuilderExternalNetworking<'a>,
    internal_dns_subnets: DnsSubnetAllocator,

    // These fields will become part of the final blueprint.  See the
    // corresponding fields in `Blueprint`.
    pub(super) zones: BlueprintZonesBuilder<'a>,
    disks: BlueprintDisksBuilder<'a>,
    sled_state: BTreeMap<SledUuid, SledState>,
    cockroachdb_setting_preserve_downgrade: CockroachDbPreserveDowngrade,

    creator: String,
    operations: Vec<Operation>,
    comments: Vec<String>,

    // Random number generator for new UUIDs
    rng: BlueprintBuilderRng,
}

impl<'a> BlueprintBuilder<'a> {
    /// Directly construct a `Blueprint` that contains an empty zone config for
    /// the given sleds.
    pub fn build_empty_with_sleds(
        sled_ids: impl Iterator<Item = SledUuid>,
        creator: &str,
    ) -> Blueprint {
        Self::build_empty_with_sleds_impl(
            sled_ids,
            creator,
            BlueprintBuilderRng::new(),
        )
    }

    /// A version of [`Self::build_empty_with_sleds`] that allows the
    /// blueprint ID to be generated from a random seed.
    pub fn build_empty_with_sleds_seeded<H: Hash>(
        sled_ids: impl Iterator<Item = SledUuid>,
        creator: &str,
        seed: H,
    ) -> Blueprint {
        let mut rng = BlueprintBuilderRng::new();
        rng.set_seed(seed);
        Self::build_empty_with_sleds_impl(sled_ids, creator, rng)
    }

    fn build_empty_with_sleds_impl(
        sled_ids: impl Iterator<Item = SledUuid>,
        creator: &str,
        mut rng: BlueprintBuilderRng,
    ) -> Blueprint {
        let blueprint_zones = sled_ids
            .map(|sled_id| {
                let config = BlueprintZonesConfig {
                    generation: Generation::new(),
                    zones: Vec::new(),
                };
                (sled_id, config)
            })
            .collect::<BTreeMap<_, _>>();
        let num_sleds = blueprint_zones.len();
        let sled_state = blueprint_zones
            .keys()
            .copied()
            .map(|sled_id| (sled_id, SledState::Active))
            .collect();
        Blueprint {
            id: rng.blueprint_rng.next(),
            blueprint_zones,
            blueprint_disks: BTreeMap::new(),
            sled_state,
            parent_blueprint_id: None,
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            cockroachdb_fingerprint: String::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            time_created: now_db_precision(),
            creator: creator.to_owned(),
            comment: format!("starting blueprint with {num_sleds} empty sleds"),
        }
    }

    /// Construct a new `BlueprintBuilder` based on a previous blueprint,
    /// starting with no changes from that state
    pub fn new_based_on(
        log: &Logger,
        parent_blueprint: &'a Blueprint,
        input: &'a PlanningInput,
        creator: &str,
    ) -> anyhow::Result<BlueprintBuilder<'a>> {
        let log = log.new(o!(
            "component" => "BlueprintBuilder",
            "parent_id" => parent_blueprint.id.to_string(),
        ));

        let external_networking =
            BuilderExternalNetworking::new(parent_blueprint, input)?;
        let internal_dns_subnets =
            DnsSubnetAllocator::new(parent_blueprint, input)?;

        // Prefer the sled state from our parent blueprint for sleds
        // that were in it; there may be new sleds in `input`, in which
        // case we'll use their current state as our starting point.
        let mut sled_state = parent_blueprint.sled_state.clone();
        let mut commissioned_sled_ids = BTreeSet::new();
        for (sled_id, details) in input.all_sleds(SledFilter::Commissioned) {
            commissioned_sled_ids.insert(sled_id);
            sled_state.entry(sled_id).or_insert(details.state);
        }

        // Make a garbage collection pass through `sled_state`. We want to keep
        // any sleds which either:
        //
        // 1. do not have a desired state of `Decommissioned`
        // 2. do have a desired state of `Decommissioned` and are still included
        //    in our input's list of commissioned sleds
        //
        // Sleds that don't fall into either of these cases have reached the
        // actual `Decommissioned` state, which means we no longer need to carry
        // forward that desired state.
        sled_state.retain(|sled_id, state| {
            *state != SledState::Decommissioned
                || commissioned_sled_ids.contains(sled_id)
        });

        Ok(BlueprintBuilder {
            log,
            parent_blueprint,
            input,
            sled_ip_allocators: BTreeMap::new(),
            external_networking,
            internal_dns_subnets,
            zones: BlueprintZonesBuilder::new(parent_blueprint),
            disks: BlueprintDisksBuilder::new(parent_blueprint),
            sled_state,
            cockroachdb_setting_preserve_downgrade: parent_blueprint
                .cockroachdb_setting_preserve_downgrade,
            creator: creator.to_owned(),
            operations: Vec::new(),
            comments: Vec::new(),
            rng: BlueprintBuilderRng::new(),
        })
    }

    /// Iterates over the list of sled IDs for which we have zones.
    ///
    /// This may include decommissioned sleds.
    pub fn sled_ids_with_zones(&self) -> impl Iterator<Item = SledUuid> {
        self.zones.sled_ids_with_zones()
    }

    pub fn current_sled_zones(
        &self,
        sled_id: SledUuid,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        self.zones.current_sled_zones(sled_id, filter).map(|(config, _)| config)
    }

    /// Assemble a final [`Blueprint`] based on the contents of the builder
    pub fn build(mut self) -> Blueprint {
        // Collect the Omicron zones config for all sleds, including sleds that
        // are no longer in service and need expungement work.
        let blueprint_zones = self
            .zones
            .into_zones_map(self.input.all_sled_ids(SledFilter::Commissioned));
        let blueprint_disks = self
            .disks
            .into_disks_map(self.input.all_sled_ids(SledFilter::InService));
        Blueprint {
            id: self.rng.blueprint_rng.next(),
            blueprint_zones,
            blueprint_disks,
            sled_state: self.sled_state,
            parent_blueprint_id: Some(self.parent_blueprint.id),
            internal_dns_version: self.input.internal_dns_version(),
            external_dns_version: self.input.external_dns_version(),
            cockroachdb_fingerprint: self
                .input
                .cockroachdb_settings()
                .state_fingerprint
                .clone(),
            cockroachdb_setting_preserve_downgrade: self
                .cockroachdb_setting_preserve_downgrade,
            time_created: now_db_precision(),
            creator: self.creator,
            comment: self
                .comments
                .into_iter()
                .chain(self.operations.iter().map(|op| op.to_string()))
                .collect::<Vec<String>>()
                .join(", "),
        }
    }

    /// Set the desired state of the given sled.
    pub fn set_sled_state(
        &mut self,
        sled_id: SledUuid,
        desired_state: SledState,
    ) {
        self.sled_state.insert(sled_id, desired_state);
    }

    /// Within tests, set a seeded RNG for deterministic results.
    ///
    /// This will ensure that tests that use this builder will produce the same
    /// results each time they are run.
    pub fn set_rng_seed<H: Hash>(&mut self, seed: H) -> &mut Self {
        self.rng.set_seed(seed);
        self
    }

    /// This is a short human-readable string summarizing the changes reflected
    /// in the blueprint. This is only intended for debugging.
    pub fn comment<S>(&mut self, comment: S)
    where
        String: From<S>,
    {
        self.comments.push(String::from(comment));
    }

    /// Records an operation to the blueprint, identifying what changes have
    /// occurred.
    ///
    /// This is currently intended only for debugging.
    pub(crate) fn record_operation(&mut self, operation: Operation) {
        self.operations.push(operation);
    }

    /// Expunges all zones requiring expungement from a sled.
    ///
    /// Returns a list of zone IDs expunged (excluding zones that were already
    /// expunged). If the list is empty, then the operation was a no-op.
    pub(crate) fn expunge_zones_for_sled(
        &mut self,
        sled_id: SledUuid,
        sled_details: &SledDetails,
    ) -> Result<BTreeMap<OmicronZoneUuid, ZoneExpungeReason>, Error> {
        let log = self.log.new(o!(
            "sled_id" => sled_id.to_string(),
        ));

        // Do any zones need to be marked expunged?
        let mut zones_to_expunge = BTreeMap::new();

        let sled_zones =
            self.zones.current_sled_zones(sled_id, BlueprintZoneFilter::All);
        for (zone_config, state) in sled_zones {
            let zone_id = zone_config.id;
            let log = log.new(o!(
                "zone_id" => zone_id.to_string()
            ));

            let Some(reason) =
                zone_needs_expungement(sled_details, zone_config)
            else {
                continue;
            };

            let is_expunged =
                is_already_expunged(zone_config, state).map_err(|error| {
                    Error::Planner(anyhow!(error).context(format!(
                        "for sled {sled_id}, error computing zones to expunge"
                    )))
                })?;

            if !is_expunged {
                match reason {
                    ZoneExpungeReason::DiskExpunged => {
                        info!(
                            &log,
                            "expunged disk with non-expunged zone was found"
                        );
                    }
                    ZoneExpungeReason::SledDecommissioned => {
                        // A sled marked as decommissioned should have no resources
                        // allocated to it. If it does, it's an illegal state, possibly
                        // introduced by a bug elsewhere in the system -- we need to
                        // produce a loud warning (i.e. an ERROR-level log message) on
                        // this, while still removing the zones.
                        error!(
                            &log,
                            "sled has state Decommissioned, yet has zone \
                             allocated to it; will expunge it"
                        );
                    }
                    ZoneExpungeReason::SledExpunged => {
                        // This is the expected situation.
                        info!(
                            &log,
                            "expunged sled with non-expunged zone found"
                        );
                    }
                }

                zones_to_expunge.insert(zone_id, reason);
            }
        }

        if zones_to_expunge.is_empty() {
            debug!(
                log,
                "sled has no zones that need expungement; skipping";
            );
            return Ok(zones_to_expunge);
        }

        // Now expunge all the zones that need it.
        let change = self.zones.change_sled_zones(sled_id);
        change
            .expunge_zones(zones_to_expunge.keys().cloned().collect())
            .map_err(|error| {
                Error::Planner(anyhow!(error).context(format!(
                    "for sled {sled_id}, error expunging zones"
                )))
            })?;

        // Finally, add comments describing what happened.
        //
        // Group the zones by their reason for expungement.
        let mut count_disk_expunged = 0;
        let mut count_sled_decommissioned = 0;
        let mut count_sled_expunged = 0;
        for reason in zones_to_expunge.values() {
            match reason {
                ZoneExpungeReason::DiskExpunged => count_disk_expunged += 1,
                ZoneExpungeReason::SledDecommissioned => {
                    count_sled_decommissioned += 1;
                }
                ZoneExpungeReason::SledExpunged => count_sled_expunged += 1,
            };
        }
        let count_and_reason = [
            (count_disk_expunged, ZoneExpungeReason::DiskExpunged),
            (count_sled_decommissioned, ZoneExpungeReason::SledDecommissioned),
            (count_sled_expunged, ZoneExpungeReason::SledExpunged),
        ];
        for (count, reason) in count_and_reason {
            if count > 0 {
                self.record_operation(Operation::ZoneExpunged {
                    sled_id,
                    reason,
                    count,
                });
            }
        }

        Ok(zones_to_expunge)
    }

    /// Ensures that the blueprint contains disks for a sled which already
    /// exists in the database.
    ///
    /// This operation must perform the following:
    /// - Ensure that any disks / zpools that exist in the database
    ///   are propagated into the blueprint.
    /// - Ensure that any disks that are expunged from the database are
    ///   removed from the blueprint.
    pub fn sled_ensure_disks(
        &mut self,
        sled_id: SledUuid,
        resources: &SledResources,
    ) -> Result<EnsureMultiple, Error> {
        let (mut additions, removals) = {
            // These are the disks known to our (last?) blueprint
            let blueprint_disks: BTreeMap<_, _> = self
                .disks
                .current_sled_disks(sled_id)
                .map(|disk| {
                    (PhysicalDiskUuid::from_untyped_uuid(disk.id), disk)
                })
                .collect();

            // These are the in-service disks as we observed them in the database,
            // during the planning phase
            let database_disks: BTreeMap<_, _> = resources
                .all_disks(DiskFilter::InService)
                .map(|(zpool, disk)| (disk.disk_id, (zpool, disk)))
                .collect();

            // Add any disks that appear in the database, but not the blueprint
            let additions = database_disks
                .iter()
                .filter_map(|(disk_id, (zpool, disk))| {
                    if !blueprint_disks.contains_key(disk_id) {
                        Some(BlueprintPhysicalDiskConfig {
                            identity: disk.disk_identity.clone(),
                            id: disk_id.into_untyped_uuid(),
                            pool_id: **zpool,
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<BlueprintPhysicalDiskConfig>>();

            // Remove any disks that appear in the blueprint, but not the database
            let removals: HashSet<PhysicalDiskUuid> = blueprint_disks
                .keys()
                .filter_map(|disk_id| {
                    if !database_disks.contains_key(disk_id) {
                        Some(*disk_id)
                    } else {
                        None
                    }
                })
                .collect();

            (additions, removals)
        };

        if additions.is_empty() && removals.is_empty() {
            return Ok(EnsureMultiple::NotNeeded);
        }
        let added = additions.len();
        let removed = removals.len();

        let disks = &mut self.disks.change_sled_disks(sled_id).disks;

        disks.append(&mut additions);
        disks.retain(|config| {
            !removals.contains(&PhysicalDiskUuid::from_untyped_uuid(config.id))
        });

        Ok(EnsureMultiple::Changed { added, removed })
    }

    fn sled_ensure_zone_internal_dns(
        &mut self,
        sled_id: SledUuid,
        gz_address_index: u32,
    ) -> Result<Ensure, Error> {
        let sled_subnet = self.sled_resources(sled_id)?.subnet;
        let rack_subnet = ReservedRackSubnet::from(sled_subnet);
        let dns_subnet = self
            .internal_dns_subnets
            .alloc_or_else(|| rack_subnet.get_dns_subnet(1))?;
        let address = dns_subnet.dns_address();
        let zpool = self.sled_select_zpool(sled_id, ZoneKind::InternalDns)?;
        let zone_type =
            BlueprintZoneType::InternalDns(blueprint_zone_type::InternalDns {
                dataset: OmicronZoneDataset { pool_name: zpool.clone() },
                dns_address: SocketAddrV6::new(address, DNS_PORT, 0, 0),
                http_address: SocketAddrV6::new(address, DNS_HTTP_PORT, 0, 0),
                gz_address: dns_subnet.gz_address(),
                gz_address_index,
            });

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.zone_rng.next(),
            underlay_address: address,
            filesystem_pool: Some(zpool),
            zone_type,
        };

        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    pub fn sled_ensure_zone_multiple_internal_dns(
        &mut self,
        sled_id: SledUuid,
        desired_zone_count: usize,
    ) -> Result<EnsureMultiple, Error> {
        // How many internal DNS zones do we need to add?
        let count =
            self.sled_num_running_zones_of_kind(sled_id, ZoneKind::InternalDns);
        let to_add = match desired_zone_count.checked_sub(count) {
            Some(0) => return Ok(EnsureMultiple::NotNeeded),
            Some(n) => n,
            None => {
                return Err(Error::Planner(anyhow!(
                    "removing an internal DNS zone not yet supported \
                     (sled {sled_id} has {count}; \
                     planner wants {desired_zone_count})"
                )));
            }
        };

        for i in count..desired_zone_count {
            self.sled_ensure_zone_internal_dns(
                sled_id,
                i.try_into().map_err(|_| {
                    Error::Planner(anyhow!("zone index overflow"))
                })?,
            )?;
        }

        Ok(EnsureMultiple::Changed { added: to_add, removed: 0 })
    }

    pub fn sled_ensure_zone_ntp(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<Ensure, Error> {
        // If there's already an NTP zone on this sled, do nothing.
        let has_ntp = self
            .zones
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
            .any(|(z, _)| z.zone_type.is_ntp());
        if has_ntp {
            return Ok(Ensure::NotNeeded);
        }

        let sled_info = self.sled_resources(sled_id)?;
        let sled_subnet = sled_info.subnet;
        let ip = self.sled_alloc_ip(sled_id)?;
        let ntp_address = SocketAddrV6::new(ip, NTP_PORT, 0, 0);

        // Construct the list of internal DNS servers.
        //
        // It'd be tempting to get this list from the other internal NTP
        // servers, but there may not be any of those.  We could also
        // construct it manually from the set of internal DNS servers
        // actually deployed, or ask the DNS subnet allocator; but those
        // would both require that all the internal DNS zones be added
        // before any NTP zones, a constraint we don't currently enforce.
        // Instead, we take the same approach as RSS: they are at known,
        // fixed addresses relative to the AZ subnet (which itself is a
        // known-prefix parent subnet of the sled subnet).
        let dns_servers =
            get_internal_dns_server_addresses(sled_subnet.net().prefix());

        // The list of boundary NTP servers is not necessarily stored
        // anywhere (unless there happens to be another internal NTP zone
        // lying around).  Recompute it based on what boundary servers
        // currently exist.
        let ntp_servers = self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::All)
            .filter_map(|(_, z)| {
                if matches!(z.zone_type, BlueprintZoneType::BoundaryNtp(_)) {
                    Some(Host::for_zone(Zone::Other(z.id)).fqdn())
                } else {
                    None
                }
            })
            .collect();

        let zone_type =
            BlueprintZoneType::InternalNtp(blueprint_zone_type::InternalNtp {
                address: ntp_address,
                ntp_servers,
                dns_servers,
                domain: None,
            });
        let filesystem_pool =
            self.sled_select_zpool(sled_id, zone_type.kind())?;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.zone_rng.next(),
            underlay_address: ip,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };

        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    pub fn sled_ensure_zone_crucible(
        &mut self,
        sled_id: SledUuid,
        zpool_id: ZpoolUuid,
    ) -> Result<Ensure, Error> {
        let pool_name = ZpoolName::new_external(zpool_id);

        // If this sled already has a Crucible zone on this pool, do nothing.
        let has_crucible_on_this_pool = self
            .zones
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
            .any(|(z, _)| {
                matches!(
                    &z.zone_type,
                    BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                        dataset,
                        ..
                    })
                    if dataset.pool_name == pool_name
                )
            });
        if has_crucible_on_this_pool {
            return Ok(Ensure::NotNeeded);
        }

        let sled_info = self.sled_resources(sled_id)?;
        if !sled_info.zpools.contains_key(&zpool_id) {
            return Err(Error::Planner(anyhow!(
                "adding crucible zone for sled {:?}: \
                attempted to use unknown zpool {:?}",
                sled_id,
                pool_name
            )));
        }

        let ip = self.sled_alloc_ip(sled_id)?;
        let port = omicron_common::address::CRUCIBLE_PORT;
        let address = SocketAddrV6::new(ip, port, 0, 0);
        let zone_type =
            BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            });
        let filesystem_pool = pool_name;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.zone_rng.next(),
            underlay_address: ip,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };

        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    /// Return the number of zones of a given kind that would be configured to
    /// run on the given sled if this builder generated a blueprint.
    ///
    /// This value may change before a blueprint is actually generated if
    /// further changes are made to the builder.
    pub fn sled_num_running_zones_of_kind(
        &self,
        sled_id: SledUuid,
        kind: ZoneKind,
    ) -> usize {
        self.zones
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
            .filter(|(z, _)| z.zone_type.kind() == kind)
            .count()
    }

    pub fn sled_ensure_zone_multiple_nexus(
        &mut self,
        sled_id: SledUuid,
        desired_zone_count: usize,
    ) -> Result<EnsureMultiple, Error> {
        // Whether Nexus should use TLS and what the external DNS servers it
        // should use are currently provided at rack-setup time, and should be
        // consistent across all Nexus instances. We'll assume we can copy them
        // from any other Nexus zone in our parent blueprint.
        //
        // TODO-correctness Once these properties can be changed by a rack
        // operator, this will need more work. At a minimum, if such a change
        // goes through the blueprint system (which seems likely), we'll need to
        // check that we're if this builder is being used to make such a change,
        // that change is also reflected here in a new zone. Perhaps these
        // settings should be part of `Policy` instead?
        let (external_tls, external_dns_servers) = self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::All)
            .find_map(|(_, z)| match &z.zone_type {
                BlueprintZoneType::Nexus(nexus) => Some((
                    nexus.external_tls,
                    nexus.external_dns_servers.clone(),
                )),
                _ => None,
            })
            .ok_or(Error::NoNexusZonesInParentBlueprint)?;
        self.sled_ensure_zone_multiple_nexus_with_config(
            sled_id,
            desired_zone_count,
            external_tls,
            external_dns_servers,
        )
    }

    pub fn sled_ensure_zone_multiple_nexus_with_config(
        &mut self,
        sled_id: SledUuid,
        desired_zone_count: usize,
        external_tls: bool,
        external_dns_servers: Vec<IpAddr>,
    ) -> Result<EnsureMultiple, Error> {
        // How many Nexus zones do we need to add?
        let nexus_count =
            self.sled_num_running_zones_of_kind(sled_id, ZoneKind::Nexus);
        let num_nexus_to_add = match desired_zone_count.checked_sub(nexus_count)
        {
            Some(0) => return Ok(EnsureMultiple::NotNeeded),
            Some(n) => n,
            None => {
                return Err(Error::Planner(anyhow!(
                    "removing a Nexus zone not yet supported \
                     (sled {sled_id} has {nexus_count}; \
                     planner wants {desired_zone_count})"
                )));
            }
        };

        for _ in 0..num_nexus_to_add {
            let nexus_id = self.rng.zone_rng.next();
            let ExternalNetworkingChoice {
                external_ip,
                nic_ip,
                nic_subnet,
                nic_mac,
            } = self.external_networking.for_new_nexus()?;
            let external_ip = OmicronZoneExternalFloatingIp {
                id: self.rng.external_ip_rng.next(),
                ip: external_ip,
            };

            let nic = NetworkInterface {
                id: self.rng.network_interface_rng.next(),
                kind: NetworkInterfaceKind::Service {
                    id: nexus_id.into_untyped_uuid(),
                },
                name: format!("nexus-{nexus_id}").parse().unwrap(),
                ip: nic_ip,
                mac: nic_mac,
                subnet: nic_subnet,
                vni: Vni::SERVICES_VNI,
                primary: true,
                slot: 0,
                transit_ips: vec![],
            };

            let ip = self.sled_alloc_ip(sled_id)?;
            let port = omicron_common::address::NEXUS_INTERNAL_PORT;
            let internal_address = SocketAddrV6::new(ip, port, 0, 0);
            let zone_type =
                BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    internal_address,
                    external_ip,
                    nic,
                    external_tls,
                    external_dns_servers: external_dns_servers.clone(),
                });
            let filesystem_pool =
                self.sled_select_zpool(sled_id, zone_type.kind())?;

            let zone = BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: nexus_id,
                underlay_address: ip,
                filesystem_pool: Some(filesystem_pool),
                zone_type,
            };
            self.sled_add_zone(sled_id, zone)?;
        }

        Ok(EnsureMultiple::Changed { added: num_nexus_to_add, removed: 0 })
    }

    pub fn cockroachdb_preserve_downgrade(
        &mut self,
        version: CockroachDbPreserveDowngrade,
    ) {
        self.cockroachdb_setting_preserve_downgrade = version;
    }

    pub fn sled_ensure_zone_multiple_cockroachdb(
        &mut self,
        sled_id: SledUuid,
        desired_zone_count: usize,
    ) -> Result<EnsureMultiple, Error> {
        // How many CRDB zones do we need to add?
        let crdb_count =
            self.sled_num_running_zones_of_kind(sled_id, ZoneKind::CockroachDb);
        let num_crdb_to_add = match desired_zone_count.checked_sub(crdb_count) {
            Some(0) => return Ok(EnsureMultiple::NotNeeded),
            Some(n) => n,
            None => {
                return Err(Error::Planner(anyhow!(
                    "removing a CockroachDb zone not yet supported \
                     (sled {sled_id} has {crdb_count}; \
                     planner wants {desired_zone_count})"
                )));
            }
        };
        for _ in 0..num_crdb_to_add {
            let zone_id = self.rng.zone_rng.next();
            let underlay_ip = self.sled_alloc_ip(sled_id)?;
            let pool_name =
                self.sled_select_zpool(sled_id, ZoneKind::CockroachDb)?;
            let port = omicron_common::address::COCKROACH_PORT;
            let address = SocketAddrV6::new(underlay_ip, port, 0, 0);
            let zone_type = BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb {
                    address,
                    dataset: OmicronZoneDataset {
                        pool_name: pool_name.clone(),
                    },
                },
            );
            let filesystem_pool = pool_name;

            let zone = BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: zone_id,
                underlay_address: underlay_ip,
                filesystem_pool: Some(filesystem_pool),
                zone_type,
            };
            self.sled_add_zone(sled_id, zone)?;
        }

        Ok(EnsureMultiple::Changed { added: num_crdb_to_add, removed: 0 })
    }

    pub fn sled_promote_internal_ntp_to_boundary_ntp(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<EnsureMultiple, Error> {
        // The upstream NTP/DNS servers and domain _should_ come from Nexus and
        // be modifiable by the operator, but currently can only be set at RSS.
        // We can only promote a new boundary NTP zone by copying these settings
        // from an existing one.
        let (ntp_servers, dns_servers, domain) = self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::All)
            .find_map(|(_, z)| match &z.zone_type {
                BlueprintZoneType::BoundaryNtp(zone_config) => Some((
                    zone_config.ntp_servers.clone(),
                    zone_config.dns_servers.clone(),
                    zone_config.domain.clone(),
                )),
                _ => None,
            })
            .ok_or(Error::NoBoundaryNtpZonesInParentBlueprint)?;

        self.sled_promote_internal_ntp_to_boundary_ntp_with_config(
            sled_id,
            ntp_servers,
            dns_servers,
            domain,
        )
    }

    pub fn sled_promote_internal_ntp_to_boundary_ntp_with_config(
        &mut self,
        sled_id: SledUuid,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        domain: Option<String>,
    ) -> Result<EnsureMultiple, Error> {
        // Check the sled id and return an appropriate error if it's invalid.
        let _ = self.sled_resources(sled_id)?;

        let sled_zones = self.zones.change_sled_zones(sled_id);

        // Find the internal NTP zone and expunge it.
        let mut internal_ntp_zone_id_iter = sled_zones
            .iter_zones(BlueprintZoneFilter::ShouldBeRunning)
            .filter_map(|config| {
                if matches!(
                    config.zone().zone_type,
                    BlueprintZoneType::InternalNtp(_)
                ) {
                    Some(config.zone().id)
                } else {
                    None
                }
            });

        // We should have exactly one internal NTP zone.
        let internal_ntp_zone_id =
            internal_ntp_zone_id_iter.next().ok_or_else(|| {
                Error::Planner(anyhow!(
                    "cannot promote internal NTP zone on sled {sled_id}: \
                     no internal NTP zone found"
                ))
            })?;
        if internal_ntp_zone_id_iter.next().is_some() {
            return Err(Error::Planner(anyhow!(
                "sled {sled_id} has multiple internal NTP zones"
            )));
        }
        std::mem::drop(internal_ntp_zone_id_iter);

        // Expunge the internal NTP zone.
        sled_zones.expunge_zone(internal_ntp_zone_id).map_err(|error| {
            Error::Planner(anyhow!(error).context(format!(
                "error expunging internal NTP zone from sled {sled_id}"
            )))
        })?;

        // Add the new boundary NTP zone.
        let new_zone_id = self.rng.zone_rng.next();
        let ExternalSnatNetworkingChoice {
            snat_cfg,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self.external_networking.for_new_boundary_ntp()?;
        let external_ip = OmicronZoneExternalSnatIp {
            id: self.rng.external_ip_rng.next(),
            snat_cfg,
        };
        let nic = NetworkInterface {
            id: self.rng.network_interface_rng.next(),
            kind: NetworkInterfaceKind::Service {
                id: new_zone_id.into_untyped_uuid(),
            },
            name: format!("ntp-{new_zone_id}").parse().unwrap(),
            ip: nic_ip,
            mac: nic_mac,
            subnet: nic_subnet,
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
            transit_ips: vec![],
        };

        let underlay_ip = self.sled_alloc_ip(sled_id)?;
        let port = omicron_common::address::NTP_PORT;
        let zone_type =
            BlueprintZoneType::BoundaryNtp(blueprint_zone_type::BoundaryNtp {
                address: SocketAddrV6::new(underlay_ip, port, 0, 0),
                ntp_servers,
                dns_servers,
                domain,
                nic,
                external_ip,
            });
        let filesystem_pool =
            self.sled_select_zpool(sled_id, zone_type.kind())?;

        self.sled_add_zone(
            sled_id,
            BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: new_zone_id,
                underlay_address: underlay_ip,
                filesystem_pool: Some(filesystem_pool),
                zone_type,
            },
        )?;

        Ok(EnsureMultiple::Changed { added: 1, removed: 1 })
    }

    pub fn sled_expunge_zone(
        &mut self,
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
    ) -> Result<(), Error> {
        // Check the sled id and return an appropriate error if it's invalid.
        let _ = self.sled_resources(sled_id)?;

        let sled_zones = self.zones.change_sled_zones(sled_id);
        sled_zones.expunge_zone(zone_id).map_err(|error| {
            Error::Planner(
                anyhow!(error)
                    .context("failed to expunge zone from sled {sled_id}"),
            )
        })?;

        Ok(())
    }

    fn sled_add_zone(
        &mut self,
        sled_id: SledUuid,
        zone: BlueprintZoneConfig,
    ) -> Result<(), Error> {
        // Check the sled id and return an appropriate error if it's invalid.
        let _ = self.sled_resources(sled_id)?;

        let sled_zones = self.zones.change_sled_zones(sled_id);
        sled_zones.add_zone(zone).map_err(|error| {
            Error::Planner(
                anyhow!(error)
                    .context(format!("error adding zone to sled {sled_id}")),
            )
        })?;

        Ok(())
    }

    /// Returns a newly-allocated underlay address suitable for use by Omicron
    /// zones
    fn sled_alloc_ip(&mut self, sled_id: SledUuid) -> Result<Ipv6Addr, Error> {
        let sled_subnet = self.sled_resources(sled_id)?.subnet;
        let allocator =
            self.sled_ip_allocators.entry(sled_id).or_insert_with(|| {
                let sled_subnet_addr = sled_subnet.net().prefix();
                let minimum = sled_subnet_addr
                    .saturating_add(u128::from(SLED_RESERVED_ADDRESSES));
                let maximum = sled_subnet_addr
                    .saturating_add(u128::from(CP_SERVICES_RESERVED_ADDRESSES));
                assert!(sled_subnet.net().contains(minimum));
                assert!(sled_subnet.net().contains(maximum));
                let mut allocator = IpAllocator::new(minimum, maximum);

                // We shouldn't need to explicitly reserve the sled's global
                // zone and switch addresses because they should be out of our
                // range, but we do so just to be sure.
                let sled_gz_addr = *get_sled_address(sled_subnet).ip();
                assert!(sled_subnet.net().contains(sled_gz_addr));
                assert!(minimum > sled_gz_addr);
                assert!(maximum > sled_gz_addr);
                let switch_zone_addr = get_switch_zone_address(sled_subnet);
                assert!(sled_subnet.net().contains(switch_zone_addr));
                assert!(minimum > switch_zone_addr);
                assert!(maximum > switch_zone_addr);

                // Record each of the sled's zones' underlay addresses as
                // allocated.
                for (z, _) in self
                    .zones
                    .current_sled_zones(sled_id, BlueprintZoneFilter::All)
                {
                    allocator.reserve(z.underlay_address);
                }

                allocator
            });

        allocator.alloc().ok_or(Error::OutOfAddresses { sled_id })
    }

    // Selects a zpools for this zone type.
    //
    // This zpool may be used for either durable storage or transient
    // storage - the usage is up to the caller.
    //
    // If `zone_kind` already exists on `sled_id`, it is prevented
    // from using the same zpool as exisitng zones with the same kind.
    fn sled_select_zpool(
        &self,
        sled_id: SledUuid,
        zone_kind: ZoneKind,
    ) -> Result<ZpoolName, Error> {
        let all_in_service_zpools =
            self.sled_resources(sled_id)?.all_zpools(ZpoolFilter::InService);

        // We refuse to choose a zpool for a zone of a given `zone_kind` if this
        // sled already has a durable zone of that kind on the same zpool. Build
        // up a set of invalid zpools for this sled/kind pair.
        let mut skip_zpools = BTreeSet::new();
        for zone_config in self
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
        {
            if let Some(zpool) = zone_config.zone_type.durable_zpool() {
                if zone_kind == zone_config.zone_type.kind() {
                    skip_zpools.insert(zpool);
                }
            }
        }

        for &zpool_id in all_in_service_zpools {
            let zpool_name = ZpoolName::new_external(zpool_id);
            if !skip_zpools.contains(&zpool_name) {
                return Ok(zpool_name);
            }
        }
        Err(Error::NoAvailableZpool { sled_id, kind: zone_kind })
    }

    fn sled_resources(
        &self,
        sled_id: SledUuid,
    ) -> Result<&SledResources, Error> {
        self.input.sled_resources(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "attempted to use sled that is not currently known: {}",
                sled_id
            ))
        })
    }
}

#[derive(Debug)]
struct BlueprintBuilderRng {
    // Have separate RNGs for the different kinds of UUIDs we might add,
    // generated from the main RNG. This is so that e.g. adding a new network
    // interface doesn't alter the blueprint or sled UUID.
    //
    // In the future, when we switch to typed UUIDs, each of these will be
    // associated with a specific `TypedUuidKind`.
    blueprint_rng: UuidRng,
    zone_rng: TypedUuidRng<OmicronZoneKind>,
    network_interface_rng: UuidRng,
    external_ip_rng: TypedUuidRng<ExternalIpKind>,
}

impl BlueprintBuilderRng {
    fn new() -> Self {
        Self::new_from_parent(StdRng::from_entropy())
    }

    fn new_from_parent(mut parent: StdRng) -> Self {
        let blueprint_rng = UuidRng::from_parent_rng(&mut parent, "blueprint");
        let zone_rng = TypedUuidRng::from_parent_rng(&mut parent, "zone");
        let network_interface_rng =
            UuidRng::from_parent_rng(&mut parent, "network_interface");
        let external_ip_rng =
            TypedUuidRng::from_parent_rng(&mut parent, "external_ip");

        BlueprintBuilderRng {
            blueprint_rng,
            zone_rng,
            network_interface_rng,
            external_ip_rng,
        }
    }

    fn set_seed<H: Hash>(&mut self, seed: H) {
        // Important to add some more bytes here, so that builders with the
        // same seed but different purposes don't end up with the same UUIDs.
        const SEED_EXTRA: &str = "blueprint-builder";
        *self = Self::new_from_parent(typed_rng::from_seed(seed, SEED_EXTRA));
    }
}

/// Helper for working with sets of zones on each sled
///
/// Tracking the set of zones is slightly non-trivial because we need to bump
/// the per-sled generation number iff the zones are changed.  So we need to
/// keep track of whether we've changed the zones relative to the parent
/// blueprint.  We do this by keeping a copy of any [`BlueprintZonesConfig`]
/// that we've changed and a _reference_ to the parent blueprint's zones.  This
/// struct makes it easy for callers iterate over the right set of zones.
pub(super) struct BlueprintZonesBuilder<'a> {
    changed_zones: BTreeMap<SledUuid, BuilderZonesConfig>,
    parent_zones: &'a BTreeMap<SledUuid, BlueprintZonesConfig>,
}

impl<'a> BlueprintZonesBuilder<'a> {
    pub fn new(parent_blueprint: &'a Blueprint) -> BlueprintZonesBuilder {
        BlueprintZonesBuilder {
            changed_zones: BTreeMap::new(),
            parent_zones: &parent_blueprint.blueprint_zones,
        }
    }

    /// Returns a mutable reference to a sled's Omicron zones *because* we're
    /// going to change them.  It's essential that the caller _does_ change them
    /// because we will have bumped the generation number and we don't want to
    /// do that if no changes are being made.
    pub fn change_sled_zones(
        &mut self,
        sled_id: SledUuid,
    ) -> &mut BuilderZonesConfig {
        self.changed_zones.entry(sled_id).or_insert_with(|| {
            if let Some(old_sled_zones) = self.parent_zones.get(&sled_id) {
                BuilderZonesConfig::from_parent(old_sled_zones)
            } else {
                BuilderZonesConfig::new()
            }
        })
    }

    /// Iterates over the list of sled IDs for which we have zones.
    ///
    /// This may include decommissioned sleds.
    pub fn sled_ids_with_zones(&self) -> impl Iterator<Item = SledUuid> {
        let mut sled_ids =
            self.changed_zones.keys().copied().collect::<BTreeSet<_>>();
        for &sled_id in self.parent_zones.keys() {
            sled_ids.insert(sled_id);
        }
        sled_ids.into_iter()
    }

    /// Iterates over the list of Omicron zones currently configured for this
    /// sled in the blueprint that's being built, along with each zone's state
    /// in the builder.
    pub fn current_sled_zones(
        &self,
        sled_id: SledUuid,
        filter: BlueprintZoneFilter,
    ) -> Box<dyn Iterator<Item = (&BlueprintZoneConfig, BuilderZoneState)> + '_>
    {
        if let Some(sled_zones) = self.changed_zones.get(&sled_id) {
            Box::new(
                sled_zones.iter_zones(filter).map(|z| (z.zone(), z.state())),
            )
        } else if let Some(parent_zones) = self.parent_zones.get(&sled_id) {
            Box::new(parent_zones.zones.iter().filter_map(move |z| {
                if z.disposition.matches(filter) {
                    Some((z, BuilderZoneState::Unchanged))
                } else {
                    None
                }
            }))
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Produces an owned map of zones for the sleds recorded in this blueprint
    /// plus any newly-added sleds
    pub fn into_zones_map(
        self,
        added_sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<SledUuid, BlueprintZonesConfig> {
        // Start with self.changed_zones, which contains entries for any
        // sled whose zones config is changing in this blueprint.
        let mut zones = self
            .changed_zones
            .into_iter()
            .map(|(sled_id, zones)| (sled_id, zones.build()))
            .collect::<BTreeMap<_, _>>();

        // Carry forward any zones from our parent blueprint. This may include
        // zones for decommissioned sleds.
        for (sled_id, parent_zones) in self.parent_zones {
            zones.entry(*sled_id).or_insert_with(|| parent_zones.clone());
        }

        // Finally, insert any newly-added sleds.
        for sled_id in added_sled_ids {
            zones.entry(sled_id).or_insert_with(|| BlueprintZonesConfig {
                generation: Generation::new(),
                zones: vec![],
            });
        }

        zones
    }
}

/// Helper for working with sets of disks on each sled
///
/// Tracking the set of disks is slightly non-trivial because we need to
/// bump the per-sled generation number iff the disks are changed.  So
/// we need to keep track of whether we've changed the disks relative
/// to the parent blueprint.  We do this by keeping a copy of any
/// [`BlueprintPhysicalDisksConfig`] that we've changed and a _reference_ to
/// the parent blueprint's disks.  This struct makes it easy for callers iterate
/// over the right set of disks.
struct BlueprintDisksBuilder<'a> {
    changed_disks: BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
    parent_disks: &'a BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
}

impl<'a> BlueprintDisksBuilder<'a> {
    pub fn new(parent_blueprint: &'a Blueprint) -> BlueprintDisksBuilder {
        BlueprintDisksBuilder {
            changed_disks: BTreeMap::new(),
            parent_disks: &parent_blueprint.blueprint_disks,
        }
    }

    /// Returns a mutable reference to a sled's Omicron disks *because* we're
    /// going to change them.  It's essential that the caller _does_ change them
    /// because we will have bumped the generation number and we don't want to
    /// do that if no changes are being made.
    pub fn change_sled_disks(
        &mut self,
        sled_id: SledUuid,
    ) -> &mut BlueprintPhysicalDisksConfig {
        self.changed_disks.entry(sled_id).or_insert_with(|| {
            if let Some(old_sled_disks) = self.parent_disks.get(&sled_id) {
                BlueprintPhysicalDisksConfig {
                    generation: old_sled_disks.generation.next(),
                    disks: old_sled_disks.disks.clone(),
                }
            } else {
                // No requests have been sent to the disk previously,
                // we should be able to use the first generation.
                BlueprintPhysicalDisksConfig {
                    generation: Generation::new(),
                    disks: vec![],
                }
            }
        })
    }

    /// Iterates over the list of Omicron disks currently configured for this
    /// sled in the blueprint that's being built
    pub fn current_sled_disks(
        &self,
        sled_id: SledUuid,
    ) -> Box<dyn Iterator<Item = &BlueprintPhysicalDiskConfig> + '_> {
        if let Some(sled_disks) = self
            .changed_disks
            .get(&sled_id)
            .or_else(|| self.parent_disks.get(&sled_id))
        {
            Box::new(sled_disks.disks.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Produces an owned map of disks for the requested sleds
    pub fn into_disks_map(
        mut self,
        sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<SledUuid, BlueprintPhysicalDisksConfig> {
        sled_ids
            .map(|sled_id| {
                // Start with self.changed_disks, which contains entries for any
                // sled whose disks config is changing in this blueprint.
                let mut disks = self
                    .changed_disks
                    .remove(&sled_id)
                    // If it's not there, use the config from the parent
                    // blueprint.
                    .or_else(|| self.parent_disks.get(&sled_id).cloned())
                    // If it's not there either, then this must be a new sled
                    // and we haven't added any disks to it yet.  Use the
                    // standard initial config.
                    .unwrap_or_else(|| BlueprintPhysicalDisksConfig {
                        generation: Generation::new(),
                        disks: vec![],
                    });
                disks.disks.sort_unstable_by_key(|d| d.id);

                (sled_id, disks)
            })
            .collect()
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::example::example;
    use crate::example::ExampleSystem;
    use crate::system::SledBuilder;
    use expectorate::assert_contents;
    use nexus_types::deployment::BlueprintOrCollectionZoneConfig;
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::deployment::OmicronZoneNetworkResources;
    use nexus_types::external_api::views::SledPolicy;
    use omicron_common::address::IpRange;
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::BTreeSet;
    use std::mem;

    pub const DEFAULT_N_SLEDS: usize = 3;

    /// Checks various conditions that should be true for all blueprints
    pub fn verify_blueprint(blueprint: &Blueprint) {
        // There should be no duplicate underlay IPs.
        let mut underlay_ips: BTreeMap<Ipv6Addr, &BlueprintZoneConfig> =
            BTreeMap::new();
        for (_, zone) in blueprint.all_omicron_zones(BlueprintZoneFilter::All) {
            if let Some(previous) =
                underlay_ips.insert(zone.underlay_address, zone)
            {
                panic!(
                    "found duplicate underlay IP {} in zones {} and {}\
                    \n\n\
                    blueprint: {}",
                    zone.underlay_address,
                    zone.id,
                    previous.id,
                    blueprint.display(),
                );
            }
        }

        // On any given zpool, we should have at most one zone of any given
        // kind.
        //
        // TODO: we may want a similar check for non-durable datasets?
        let mut kinds_by_zpool: BTreeMap<
            ZpoolUuid,
            BTreeMap<ZoneKind, OmicronZoneUuid>,
        > = BTreeMap::new();
        for (_, zone) in blueprint.all_omicron_zones(BlueprintZoneFilter::All) {
            if let Some(dataset) = zone.zone_type.durable_dataset() {
                let kind = zone.zone_type.kind();
                if let Some(previous) = kinds_by_zpool
                    .entry(dataset.dataset.pool_name.id())
                    .or_default()
                    .insert(kind, zone.id)
                {
                    panic!(
                        "zpool {} has two zones of kind {kind:?}: {} and {}\
                            \n\n\
                            blueprint: {}",
                        dataset.dataset.pool_name,
                        zone.id,
                        previous,
                        blueprint.display(),
                    );
                }
            }
        }
    }

    #[test]
    fn test_initial() {
        // Test creating a blueprint from a collection and verifying that it
        // describes no changes.
        static TEST_NAME: &str = "blueprint_builder_test_initial";
        let logctx = test_setup_log(TEST_NAME);
        let (collection, input, blueprint_initial) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        verify_blueprint(&blueprint_initial);

        let diff = blueprint_initial.diff_since_collection(&collection);
        // There are some differences with even a no-op diff between a
        // collection and a blueprint, such as new data being added to
        // blueprints like DNS generation numbers.
        println!(
            "collection -> initial blueprint \
             (expected no non-trivial changes):\n{}",
            diff.display()
        );
        assert_contents(
            "tests/output/blueprint_builder_initial_diff.txt",
            &diff.display().to_string(),
        );
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);

        // Test a no-op blueprint.
        let builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint_initial,
            &input,
            "test_basic",
        )
        .expect("failed to create builder");
        let blueprint = builder.build();
        verify_blueprint(&blueprint);
        let diff = blueprint.diff_since_blueprint(&blueprint_initial);
        println!(
            "initial blueprint -> next blueprint (expected no changes):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_basic() {
        static TEST_NAME: &str = "blueprint_builder_test_basic";
        let logctx = test_setup_log(TEST_NAME);
        let mut example =
            ExampleSystem::new(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        let blueprint1 = &example.blueprint;
        verify_blueprint(blueprint1);

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            blueprint1,
            &example.input,
            "test_basic",
        )
        .expect("failed to create builder");

        // The example blueprint should have internal NTP zones on all the
        // existing sleds, plus Crucible zones on all pools.  So if we ensure
        // all these zones exist, we should see no change.
        for (sled_id, sled_resources) in
            example.input.all_sled_resources(SledFilter::Commissioned)
        {
            builder.sled_ensure_zone_ntp(sled_id).unwrap();
            for pool_id in sled_resources.zpools.keys() {
                builder.sled_ensure_zone_crucible(sled_id, *pool_id).unwrap();
            }
        }

        let blueprint2 = builder.build();
        verify_blueprint(&blueprint2);
        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!(
            "initial blueprint -> next blueprint (expected no changes):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);

        // The next step is adding these zones to a new sled.
        let new_sled_id = example.sled_rng.next();
        let _ =
            example.system.sled(SledBuilder::new().id(new_sled_id)).unwrap();
        let input = example.system.to_planning_input_builder().unwrap().build();
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint2,
            &input,
            "test_basic",
        )
        .expect("failed to create builder");
        builder.sled_ensure_zone_ntp(new_sled_id).unwrap();
        // TODO-cleanup use `TypedUuid` everywhere
        let new_sled_resources = input.sled_resources(&new_sled_id).unwrap();
        for pool_id in new_sled_resources.zpools.keys() {
            builder.sled_ensure_zone_crucible(new_sled_id, *pool_id).unwrap();
        }

        let blueprint3 = builder.build();
        verify_blueprint(&blueprint3);
        let diff = blueprint3.diff_since_blueprint(&blueprint2);
        println!("expecting new NTP and Crucible zones:\n{}", diff.display());

        // No sleds were changed or removed.
        assert_eq!(diff.sleds_modified.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);

        // One sled was added.
        assert_eq!(diff.sleds_added.len(), 1);
        let sled_id = diff.sleds_added.first().unwrap();
        let new_sled_zones = diff.zones.added.get(sled_id).unwrap();
        assert_eq!(*sled_id, new_sled_id);
        // The generation number should be newer than the initial default.
        assert!(new_sled_zones.generation_after.unwrap() > Generation::new());

        // All zones' underlay addresses ought to be on the sled's subnet.
        for z in &new_sled_zones.zones {
            assert!(new_sled_resources
                .subnet
                .net()
                .contains(z.underlay_address()));
        }

        // Check for an NTP zone.  Its sockaddr's IP should also be on the
        // sled's subnet.
        assert!(new_sled_zones.zones.iter().any(|z| {
            if let BlueprintOrCollectionZoneConfig::Blueprint(
                BlueprintZoneConfig {
                    zone_type:
                        BlueprintZoneType::InternalNtp(
                            blueprint_zone_type::InternalNtp {
                                address, ..
                            },
                        ),
                    ..
                },
            ) = &z
            {
                assert!(new_sled_resources
                    .subnet
                    .net()
                    .contains(*address.ip()));
                true
            } else {
                false
            }
        }));
        let crucible_pool_names = new_sled_zones
            .zones
            .iter()
            .filter_map(|z| {
                if let BlueprintOrCollectionZoneConfig::Blueprint(
                    BlueprintZoneConfig {
                        zone_type:
                            BlueprintZoneType::Crucible(
                                blueprint_zone_type::Crucible {
                                    address,
                                    dataset,
                                },
                            ),
                        ..
                    },
                ) = &z
                {
                    let ip = address.ip();
                    assert!(new_sled_resources.subnet.net().contains(*ip));
                    Some(dataset.pool_name.clone())
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            crucible_pool_names,
            new_sled_resources
                .zpools
                .keys()
                .map(|id| { ZpoolName::new_external(*id) })
                .collect()
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_prune_decommissioned_sleds() {
        static TEST_NAME: &str =
            "blueprint_builder_test_prune_decommissioned_sleds";
        let logctx = test_setup_log(TEST_NAME);
        let (_, input, mut blueprint1) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        verify_blueprint(&blueprint1);

        // Mark one sled as having a desired state of decommissioned.
        let decommision_sled_id = blueprint1
            .sled_state
            .keys()
            .copied()
            .next()
            .expect("at least one sled");
        *blueprint1.sled_state.get_mut(&decommision_sled_id).unwrap() =
            SledState::Decommissioned;

        // Change the input to note that the sled is expunged, but still active.
        let mut builder = input.into_builder();
        builder.sleds_mut().get_mut(&decommision_sled_id).unwrap().policy =
            SledPolicy::Expunged;
        builder.sleds_mut().get_mut(&decommision_sled_id).unwrap().state =
            SledState::Active;
        let input = builder.build();

        // Generate a new blueprint. This sled should still be included: even
        // though the desired state is decommissioned, the current state is
        // still active, so we should carry it forward.
        let blueprint2 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &input,
            "test_prune_decommissioned_sleds",
        )
        .expect("created builder")
        .build();
        verify_blueprint(&blueprint2);

        // We carried forward the desired state.
        assert_eq!(
            blueprint2.sled_state.get(&decommision_sled_id).copied(),
            Some(SledState::Decommissioned)
        );

        // Change the input to mark the sled decommissioned. (Normally realizing
        // blueprint2 would make this change.)
        let mut builder = input.into_builder();
        builder.sleds_mut().get_mut(&decommision_sled_id).unwrap().state =
            SledState::Decommissioned;
        let input = builder.build();

        // Generate a new blueprint. This desired sled state should no longer be
        // present: it has reached the terminal decommissioned state, so there's
        // no more work to be done.
        let blueprint3 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint2,
            &input,
            "test_prune_decommissioned_sleds",
        )
        .expect("created builder")
        .build();
        verify_blueprint(&blueprint3);

        // Ensure we've dropped the decommissioned sled. (We may still have
        // _zones_ for it that need cleanup work, but all state transitions for
        // it are complete.)
        assert_eq!(
            blueprint3.sled_state.get(&decommision_sled_id).copied(),
            None,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_add_physical_disks() {
        static TEST_NAME: &str = "blueprint_builder_test_add_physical_disks";
        let logctx = test_setup_log(TEST_NAME);
        let (_, input, _) = example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        let input = {
            // Clear out the external networking records from `input`, since
            // we're building an empty blueprint.
            let mut builder = input.into_builder();
            *builder.network_resources_mut() =
                OmicronZoneNetworkResources::new();
            builder.build()
        };

        // Start with an empty blueprint (sleds with no zones).
        let parent = BlueprintBuilder::build_empty_with_sleds_seeded(
            input.all_sled_ids(SledFilter::Commissioned),
            "test",
            TEST_NAME,
        );

        {
            // We start empty, and can add a disk
            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &parent,
                &input,
                "test",
            )
            .expect("failed to create builder");

            assert!(builder.disks.changed_disks.is_empty());
            assert!(builder.disks.parent_disks.is_empty());

            for (sled_id, sled_resources) in
                input.all_sled_resources(SledFilter::InService)
            {
                assert_eq!(
                    builder
                        .sled_ensure_disks(sled_id, &sled_resources)
                        .unwrap(),
                    EnsureMultiple::Changed { added: 10, removed: 0 },
                );
            }

            assert!(!builder.disks.changed_disks.is_empty());
            assert!(builder.disks.parent_disks.is_empty());
        }

        logctx.cleanup_successful();
    }

    // Tests that provisioning zones with durable zones co-locates their zone filesystems.
    #[test]
    fn test_zone_filesystem_zpool_colocated() {
        static TEST_NAME: &str =
            "blueprint_builder_test_zone_filesystem_zpool_colocated";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, blueprint) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        for (_, zone_config) in &blueprint.blueprint_zones {
            for zone in &zone_config.zones {
                // The pool should only be optional for backwards compatibility.
                let filesystem_pool = zone
                    .filesystem_pool
                    .as_ref()
                    .expect("Should have filesystem pool");

                if let Some(durable_pool) = zone.zone_type.durable_zpool() {
                    assert_eq!(durable_pool, filesystem_pool);
                }
            }
        }
        logctx.cleanup_successful();
    }

    #[test]
    fn test_add_nexus_with_no_existing_nexus_zones() {
        static TEST_NAME: &str =
            "blueprint_builder_test_add_nexus_with_no_existing_nexus_zones";
        let logctx = test_setup_log(TEST_NAME);

        // Discard the example blueprint and start with an empty one.
        let (collection, input, _) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        let input = {
            // Clear out the external networking records from `input`, since
            // we're building an empty blueprint.
            let mut builder = input.into_builder();
            *builder.network_resources_mut() =
                OmicronZoneNetworkResources::new();
            builder.build()
        };
        let parent = BlueprintBuilder::build_empty_with_sleds_seeded(
            input.all_sled_ids(SledFilter::Commissioned),
            "test",
            TEST_NAME,
        );

        // Adding a new Nexus zone currently requires copying settings from an
        // existing Nexus zone. `parent` has no zones, so we should fail if we
        // try to add a Nexus zone.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            &input,
            "test",
        )
        .expect("failed to create builder");

        let err = builder
            .sled_ensure_zone_multiple_nexus(
                collection
                    .omicron_zones
                    .keys()
                    .next()
                    .copied()
                    .expect("no sleds present"),
                1,
            )
            .unwrap_err();

        assert!(
            matches!(err, Error::NoNexusZonesInParentBlueprint),
            "unexpected error {err}"
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_add_nexus_error_cases() {
        static TEST_NAME: &str = "blueprint_builder_test_add_nexus_error_cases";
        let logctx = test_setup_log(TEST_NAME);
        let (mut collection, mut input, mut parent) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // Remove the Nexus zone from one of the sleds so that
        // `sled_ensure_zone_nexus` can attempt to add a Nexus zone to
        // `sled_id`.
        let sled_id = {
            let mut selected_sled_id = None;
            for (sled_id, zones) in &mut collection.omicron_zones {
                let nzones_before_retain = zones.zones.zones.len();
                zones.zones.zones.retain(|z| !z.zone_type.is_nexus());
                if zones.zones.zones.len() < nzones_before_retain {
                    selected_sled_id = Some(*sled_id);
                    // Also remove this zone from the blueprint.
                    let mut removed_nexus = None;
                    parent
                        .blueprint_zones
                        .get_mut(sled_id)
                        .expect("missing sled")
                        .zones
                        .retain(|z| match &z.zone_type {
                            BlueprintZoneType::Nexus(z) => {
                                removed_nexus = Some(z.clone());
                                false
                            }
                            _ => true,
                        });
                    let removed_nexus =
                        removed_nexus.expect("removed Nexus from blueprint");

                    // Also remove this Nexus's external networking resources
                    // from `input`.
                    let mut builder = input.into_builder();
                    let mut new_network_resources =
                        OmicronZoneNetworkResources::new();
                    let old_network_resources = builder.network_resources_mut();
                    for ip in old_network_resources.omicron_zone_external_ips()
                    {
                        if ip.ip.id() != removed_nexus.external_ip.id {
                            new_network_resources
                                .add_external_ip(ip.zone_id, ip.ip)
                                .expect("copied IP to new input");
                        }
                    }
                    for nic in old_network_resources.omicron_zone_nics() {
                        if nic.nic.id.into_untyped_uuid()
                            != removed_nexus.nic.id
                        {
                            new_network_resources
                                .add_nic(nic.zone_id, nic.nic)
                                .expect("copied NIC to new input");
                        }
                    }
                    mem::swap(
                        old_network_resources,
                        &mut new_network_resources,
                    );
                    input = builder.build();

                    break;
                }
            }
            selected_sled_id.expect("found no sleds with Nexus zone")
        };

        {
            // Attempting to add Nexus to the sled we removed it from (with no
            // other changes to the environment) should succeed.
            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &parent,
                &input,
                "test",
            )
            .expect("failed to create builder");
            let added = builder
                .sled_ensure_zone_multiple_nexus(sled_id, 1)
                .expect("failed to ensure nexus zone");

            assert_eq!(added, EnsureMultiple::Changed { added: 1, removed: 0 });
        }

        {
            // Attempting to add multiple Nexus zones to the sled we removed it
            // from (with no other changes to the environment) should also
            // succeed.
            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &parent,
                &input,
                "test",
            )
            .expect("failed to create builder");
            let added = builder
                .sled_ensure_zone_multiple_nexus(sled_id, 3)
                .expect("failed to ensure nexus zone");

            assert_eq!(added, EnsureMultiple::Changed { added: 3, removed: 0 });
        }

        {
            // Replace the policy's external service IP pool ranges with ranges
            // that are already in use by existing zones. Attempting to add a
            // Nexus with no remaining external IPs should fail.
            let mut used_ip_ranges = Vec::new();
            for (_, z) in parent.all_omicron_zones(BlueprintZoneFilter::All) {
                if let Some((external_ip, _)) =
                    z.zone_type.external_networking()
                {
                    used_ip_ranges.push(IpRange::from(external_ip.ip()));
                }
            }
            assert!(!used_ip_ranges.is_empty());
            let input = {
                let mut builder = input.into_builder();
                builder.policy_mut().service_ip_pool_ranges = used_ip_ranges;
                builder.build()
            };

            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &parent,
                &input,
                "test",
            )
            .expect("failed to create builder");
            let err = builder
                .sled_ensure_zone_multiple_nexus(sled_id, 1)
                .unwrap_err();

            assert!(
                matches!(err, Error::NoExternalServiceIpAvailable),
                "unexpected error {err}"
            );
        }

        // We're not testing the `ExhaustedNexusIps` error case (where we've run
        // out of Nexus OPTE addresses), because it's fairly diffiult to induce
        // that from outside: we would need to start from a parent blueprint
        // that contained a Nexus instance for every IP in the
        // `NEXUS_OPTE_*_SUBNET`. We could hack around that by creating the
        // `BlueprintBuilder` and mucking with its internals, but that doesn't
        // seem like a particularly useful test either.

        logctx.cleanup_successful();
    }

    #[test]
    fn test_invalid_parent_blueprint_two_zones_with_same_external_ip() {
        static TEST_NAME: &str =
            "blueprint_builder_test_invalid_parent_blueprint_\
             two_zones_with_same_external_ip";
        let logctx = test_setup_log(TEST_NAME);
        let (_, input, mut parent) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // We should fail if the parent blueprint claims to contain two
        // zones with the same external IP. Skim through the zones, copy the
        // external IP from one Nexus zone, then assign it to a later Nexus
        // zone.
        let mut found_second_nexus_zone = false;
        let mut nexus_external_ip = None;

        'outer: for zones in parent.blueprint_zones.values_mut() {
            for z in zones.zones.iter_mut() {
                if let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    external_ip,
                    ..
                }) = &mut z.zone_type
                {
                    if let Some(ip) = nexus_external_ip {
                        *external_ip = ip;
                        found_second_nexus_zone = true;
                        break 'outer;
                    } else {
                        nexus_external_ip = Some(*external_ip);
                        continue 'outer;
                    }
                }
            }
        }
        assert!(found_second_nexus_zone, "only one Nexus zone present?");

        match BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            &input,
            "test",
        ) {
            Ok(_) => panic!("unexpected success"),
            Err(err) => assert!(
                err.to_string().contains("duplicate external IP"),
                "unexpected error: {err:#}"
            ),
        };

        logctx.cleanup_successful();
    }

    #[test]
    fn test_invalid_parent_blueprint_two_nexus_zones_with_same_nic_ip() {
        static TEST_NAME: &str =
            "blueprint_builder_test_invalid_parent_blueprint_\
             two_nexus_zones_with_same_nic_ip";
        let logctx = test_setup_log(TEST_NAME);
        let (_, input, mut parent) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // We should fail if the parent blueprint claims to contain two
        // Nexus zones with the same NIC IP. Skim through the zones, copy
        // the NIC IP from one Nexus zone, then assign it to a later
        // Nexus zone.
        let mut found_second_nexus_zone = false;
        let mut nexus_nic_ip = None;

        'outer: for zones in parent.blueprint_zones.values_mut() {
            for z in zones.zones.iter_mut() {
                if let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    nic,
                    ..
                }) = &mut z.zone_type
                {
                    if let Some(ip) = nexus_nic_ip {
                        nic.ip = ip;
                        found_second_nexus_zone = true;
                        break 'outer;
                    } else {
                        nexus_nic_ip = Some(nic.ip);
                        continue 'outer;
                    }
                }
            }
        }
        assert!(found_second_nexus_zone, "only one Nexus zone present?");

        match BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            &input,
            "test",
        ) {
            Ok(_) => panic!("unexpected success"),
            Err(err) => assert!(
                err.to_string().contains("duplicate Nexus NIC IP"),
                "unexpected error: {err:#}"
            ),
        };

        logctx.cleanup_successful();
    }

    #[test]
    fn test_invalid_parent_blueprint_two_zones_with_same_vnic_mac() {
        static TEST_NAME: &str =
            "blueprint_builder_test_invalid_parent_blueprint_\
             two_zones_with_same_vnic_mac";
        let logctx = test_setup_log(TEST_NAME);
        let (_, input, mut parent) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // We should fail if the parent blueprint claims to contain two
        // zones with the same service vNIC MAC address. Skim through the
        // zones, copy the NIC MAC from one Nexus zone, then assign it to a
        // later Nexus zone.
        let mut found_second_nexus_zone = false;
        let mut nexus_nic_mac = None;

        'outer: for zones in parent.blueprint_zones.values_mut() {
            for z in zones.zones.iter_mut() {
                if let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    nic,
                    ..
                }) = &mut z.zone_type
                {
                    if let Some(mac) = nexus_nic_mac {
                        nic.mac = mac;
                        found_second_nexus_zone = true;
                        break 'outer;
                    } else {
                        nexus_nic_mac = Some(nic.mac);
                        continue 'outer;
                    }
                }
            }
        }
        assert!(found_second_nexus_zone, "only one Nexus zone present?");

        match BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            &input,
            "test",
        ) {
            Ok(_) => panic!("unexpected success"),
            Err(err) => assert!(
                err.to_string().contains("duplicate service vNIC MAC"),
                "unexpected error: {err:#}"
            ),
        };

        logctx.cleanup_successful();
    }

    #[test]
    fn test_ensure_cockroachdb() {
        static TEST_NAME: &str = "blueprint_builder_test_ensure_cockroachdb";
        let logctx = test_setup_log(TEST_NAME);

        // Discard the example blueprint and start with an empty one.
        let (_, input, _) = example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        let input = {
            // Clear out the external networking records from `input`, since
            // we're building an empty blueprint.
            let mut builder = input.into_builder();
            *builder.network_resources_mut() =
                OmicronZoneNetworkResources::new();
            builder.build()
        };
        let parent = BlueprintBuilder::build_empty_with_sleds_seeded(
            input.all_sled_ids(SledFilter::Commissioned),
            "test",
            TEST_NAME,
        );

        // Pick an arbitrary sled.
        let (target_sled_id, sled_resources) = input
            .all_sled_resources(SledFilter::InService)
            .next()
            .expect("at least one sled");

        // It should have multiple zpools.
        let num_sled_zpools = sled_resources.zpools.len();
        assert!(
            num_sled_zpools > 1,
            "expected more than 1 zpool, got {num_sled_zpools}"
        );

        // We should be able to ask for a CRDB zone per zpool.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            &input,
            "test",
        )
        .expect("constructed builder");
        let ensure_result = builder
            .sled_ensure_zone_multiple_cockroachdb(
                target_sled_id,
                num_sled_zpools,
            )
            .expect("ensured multiple CRDB zones");
        assert_eq!(
            ensure_result,
            EnsureMultiple::Changed { added: num_sled_zpools, removed: 0 }
        );

        let blueprint = builder.build();
        verify_blueprint(&blueprint);
        assert_eq!(
            blueprint
                .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
                .filter(|(sled_id, z)| {
                    *sled_id == target_sled_id
                        && z.zone_type.kind() == ZoneKind::CockroachDb
                })
                .count(),
            num_sled_zpools
        );

        // If we instead ask for one more zone than there are zpools, we should
        // get a zpool allocation error.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            &input,
            "test",
        )
        .expect("constructed builder");
        let ensure_error = builder
            .sled_ensure_zone_multiple_cockroachdb(
                target_sled_id,
                num_sled_zpools + 1,
            )
            .expect_err("failed to create too many CRDB zones");
        match ensure_error {
            Error::NoAvailableZpool { sled_id, kind } => {
                assert_eq!(target_sled_id, sled_id);
                assert_eq!(kind, ZoneKind::CockroachDb);
            }
            _ => panic!("unexpected error {ensure_error}"),
        }

        logctx.cleanup_successful();
    }
}

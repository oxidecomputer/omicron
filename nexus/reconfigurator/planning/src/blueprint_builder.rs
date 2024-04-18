// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level facility for generating Blueprints

use crate::ip_allocator::IpAllocator;
use anyhow::anyhow;
use anyhow::bail;
use internal_dns::config::Host;
use internal_dns::config::Zone;
use ipnet::IpAdd;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use nexus_inventory::now_db_precision;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::InvalidOmicronZoneType;
use nexus_types::deployment::OmicronZoneDataset;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::ZpoolName;
use nexus_types::inventory::Collection;
use omicron_common::address::get_internal_dns_server_addresses;
use omicron_common::address::get_sled_address;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::CP_SERVICES_RESERVED_ADDRESSES;
use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
use omicron_common::address::NEXUS_OPTE_IPV6_SUBNET;
use omicron_common::address::NTP_PORT;
use omicron_common::address::SLED_RESERVED_ADDRESSES;
use omicron_common::api::external::Generation;
use omicron_common::api::external::IpNet;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::Vni;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use rand::rngs::StdRng;
use rand::SeedableRng;
use slog::o;
use slog::Logger;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::str::FromStr;
use thiserror::Error;
use typed_rng::TypedUuidRng;
use typed_rng::UuidRng;
use uuid::Uuid;

/// Errors encountered while assembling blueprints
#[derive(Debug, Error)]
pub enum Error {
    #[error("sled {sled_id}: ran out of available addresses for sled")]
    OutOfAddresses { sled_id: SledUuid },
    #[error("no Nexus zones exist in parent blueprint")]
    NoNexusZonesInParentBlueprint,
    #[error("no external service IP addresses are available")]
    NoExternalServiceIpAvailable,
    #[error("no system MAC addresses are available")]
    NoSystemMacAddressAvailable,
    #[error("exhausted available Nexus IP addresses")]
    ExhaustedNexusIps,
    #[error("programming error in planner")]
    Planner(#[from] anyhow::Error),
    #[error("invalid OmicronZoneType in collection")]
    InvalidOmicronZoneType(#[from] InvalidOmicronZoneType),
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
    Added(usize),
    /// no action was necessary
    NotNeeded,
}

fn zpool_id_to_external_name(zpool_id: ZpoolUuid) -> anyhow::Result<ZpoolName> {
    let pool_name_generated =
        illumos_utils::zpool::ZpoolName::new_external(zpool_id).to_string();
    let pool_name = ZpoolName::from_str(&pool_name_generated).map_err(|e| {
        anyhow!("Failed to create zpool name from {zpool_id}: {e}")
    })?;
    Ok(pool_name)
}

/// Helper for assembling a blueprint
///
/// There are two basic ways to assemble a new blueprint:
///
/// 1. Build one directly from a collection.  Such blueprints have no parent
///    blueprint.  They are not customizable.  Use
///    [`BlueprintBuilder::build_initial_from_collection`] for this.  This would
///    generally only be used once in the lifetime of a rack, to assemble the
///    first blueprint.
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

    // These fields are used to allocate resources from sleds.
    input: &'a PlanningInput,
    sled_ip_allocators: BTreeMap<SledUuid, IpAllocator>,

    // These fields will become part of the final blueprint.  See the
    // corresponding fields in `Blueprint`.
    zones: BlueprintZonesBuilder<'a>,
    disks: BlueprintDisksBuilder<'a>,

    creator: String,
    comments: Vec<String>,

    // These fields mirror how RSS chooses addresses for zone NICs.
    nexus_v4_ips: Box<dyn Iterator<Item = Ipv4Addr> + Send>,
    nexus_v6_ips: Box<dyn Iterator<Item = Ipv6Addr> + Send>,

    // Iterator of available external IPs for service zones
    available_external_ips: Box<dyn Iterator<Item = IpAddr> + Send + 'a>,

    // Iterator of available MAC addresses in the system address range
    available_system_macs: Box<dyn Iterator<Item = MacAddr>>,

    // Random number generator for new UUIDs
    rng: BlueprintBuilderRng,
}

impl<'a> BlueprintBuilder<'a> {
    /// Directly construct a `Blueprint` from the contents of a particular
    /// collection (representing no changes from the collection state)
    pub fn build_initial_from_collection(
        collection: &Collection,
        internal_dns_version: Generation,
        external_dns_version: Generation,
        all_sleds: impl Iterator<Item = SledUuid>,
        creator: &str,
    ) -> Result<Blueprint, Error> {
        Self::build_initial_impl(
            collection,
            internal_dns_version,
            external_dns_version,
            all_sleds,
            creator,
            BlueprintBuilderRng::new(),
        )
    }

    /// A version of [`Self::build_initial_from_collection`] that allows the
    /// blueprint ID to be generated from a random seed.
    pub fn build_initial_from_collection_seeded<H: Hash>(
        collection: &Collection,
        internal_dns_version: Generation,
        external_dns_version: Generation,
        all_sleds: impl Iterator<Item = SledUuid>,
        creator: &str,
        seed: H,
    ) -> Result<Blueprint, Error> {
        let mut rng = BlueprintBuilderRng::new();
        rng.set_seed(seed);
        Self::build_initial_impl(
            collection,
            internal_dns_version,
            external_dns_version,
            all_sleds,
            creator,
            rng,
        )
    }

    fn build_initial_impl(
        collection: &Collection,
        internal_dns_version: Generation,
        external_dns_version: Generation,
        all_sleds: impl Iterator<Item = SledUuid>,
        creator: &str,
        mut rng: BlueprintBuilderRng,
    ) -> Result<Blueprint, Error> {
        let blueprint_zones = all_sleds
            .map(|sled_id| {
                let zones = collection
                    .omicron_zones
                    .get(&sled_id)
                    .map(|z| &z.zones)
                    .ok_or_else(|| {
                        // We should not find a sled that's supposed to be
                        // in-service but is not part of the inventory.  It's
                        // not that that can't ever happen.  This could happen
                        // when a sled is first being added to the system.  Of
                        // course it could also happen if this sled agent failed
                        // our inventory request.  But this is the initial
                        // blueprint (so this shouldn't be the "add sled" case)
                        // and we want to get it right (so we don't want to
                        // leave out sleds whose sled agent happened to be down
                        // when we tried to do this).  The operator (or, more
                        // likely, a support person) will have to sort out
                        // what's going on if this happens.
                        Error::Planner(anyhow!(
                            "building initial blueprint: sled {:?} is \
                            supposed to be in service but has no zones \
                            in inventory",
                            sled_id
                        ))
                    })?;
                let config =
                    BlueprintZonesConfig::initial_from_collection(&zones)?;

                Ok((
                    // TODO-cleanup use `TypedUuid` everywhere
                    sled_id.into_untyped_uuid(),
                    config,
                ))
            })
            .collect::<Result<_, Error>>()?;
        Ok(Blueprint {
            id: rng.blueprint_rng.next(),
            blueprint_zones,
            blueprint_disks: BTreeMap::new(),
            parent_blueprint_id: None,
            internal_dns_version,
            external_dns_version,
            time_created: now_db_precision(),
            creator: creator.to_owned(),
            comment: format!("from collection {}", collection.id),
        })
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

        // Scan through the parent blueprint and build several sets of "used
        // resources". When adding new control plane zones to a sled, we may
        // need to allocate new resources to that zone. However, allocation at
        // this point is entirely optimistic and theoretical: our caller may
        // discard the blueprint we create without ever making it the new
        // target, or it might be an arbitrarily long time before it becomes the
        // target. We need to be able to make allocation decisions that we
        // expect the blueprint executor to be able to realize successfully if
        // and when we become the target, but we cannot _actually_ perform
        // resource allocation.
        //
        // To do this, we look at our parent blueprint's used resources, and
        // then choose new resources that aren't already in use (if possible; if
        // we need to allocate a new resource and the parent blueprint appears
        // to be using all the resources of that kind, our blueprint generation
        // will fail).
        //
        // For example, RSS assigns Nexus NIC IPs by stepping through a list of
        // addresses based on `NEXUS_OPTE_IPVx_SUBNET` (as in the iterators
        // below). We use the same list of addresses, but additionally need to
        // filter out the existing IPs for any Nexus instances that already
        // exist.
        //
        // Note that by building these iterators up front based on
        // `parent_blueprint`, we cannot reuse resources in a case where we
        // remove a zone that used a resource and then add another zone that
        // wants the same kind of resource. We don't support zone removal yet,
        // but expect this to be okay: we don't anticipate removal and addition
        // to frequently be combined into the exact same blueprint, particularly
        // in a way that expects the addition to reuse resources from the
        // removal; we won't want to attempt to reuse resources from a zone
        // until we know it's been fully removed.
        let mut existing_nexus_v4_ips: HashSet<Ipv4Addr> = HashSet::new();
        let mut existing_nexus_v6_ips: HashSet<Ipv6Addr> = HashSet::new();
        let mut used_external_ips: HashSet<IpAddr> = HashSet::new();
        let mut used_macs: HashSet<MacAddr> = HashSet::new();

        for (_, z) in
            parent_blueprint.all_omicron_zones(BlueprintZoneFilter::All)
        {
            let zone_type = &z.zone_type;
            if let BlueprintZoneType::Nexus(nexus) = zone_type {
                match nexus.nic.ip {
                    IpAddr::V4(ip) => {
                        if !existing_nexus_v4_ips.insert(ip) {
                            bail!("duplicate Nexus NIC IP: {ip}");
                        }
                    }
                    IpAddr::V6(ip) => {
                        if !existing_nexus_v6_ips.insert(ip) {
                            bail!("duplicate Nexus NIC IP: {ip}");
                        }
                    }
                }
            }

            if let Some(external_ip) = zone_type.external_ip() {
                // For the test suite, ignore localhost.  It gets reused many
                // times and that's okay.  We don't expect to see localhost
                // outside the test suite.
                if !external_ip.is_loopback()
                    && !used_external_ips.insert(external_ip)
                {
                    bail!("duplicate external IP: {external_ip}");
                }
            }
            if let Some(nic) = zone_type.opte_vnic() {
                if !used_macs.insert(nic.mac) {
                    bail!("duplicate service vNIC MAC: {}", nic.mac);
                }
            }
        }

        // TODO-performance Building these iterators as "walk through the list
        // and skip anything we've used already" is fine as long as we're
        // talking about a small number of resources (e.g., single-digit number
        // of Nexus instances), but wouldn't be ideal if we have many resources
        // we need to skip. We could do something smarter here based on the sets
        // of used resources we built above if needed.
        let nexus_v4_ips = Box::new(
            NEXUS_OPTE_IPV4_SUBNET
                .0
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES)
                .filter(move |ip| !existing_nexus_v4_ips.contains(ip)),
        );
        let nexus_v6_ips = Box::new(
            NEXUS_OPTE_IPV6_SUBNET
                .0
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES)
                .filter(move |ip| !existing_nexus_v6_ips.contains(ip)),
        );
        let available_external_ips = Box::new(
            input
                .service_ip_pool_ranges()
                .iter()
                .flat_map(|r| r.iter())
                .filter(move |ip| !used_external_ips.contains(ip)),
        );
        let available_system_macs = Box::new(
            MacAddr::iter_system().filter(move |mac| !used_macs.contains(mac)),
        );

        Ok(BlueprintBuilder {
            log,
            parent_blueprint,
            input,
            sled_ip_allocators: BTreeMap::new(),
            zones: BlueprintZonesBuilder::new(parent_blueprint),
            disks: BlueprintDisksBuilder::new(parent_blueprint),
            creator: creator.to_owned(),
            comments: Vec::new(),
            nexus_v4_ips,
            nexus_v6_ips,
            available_external_ips,
            available_system_macs,
            rng: BlueprintBuilderRng::new(),
        })
    }

    /// Assemble a final [`Blueprint`] based on the contents of the builder
    pub fn build(mut self) -> Blueprint {
        // Collect the Omicron zones config for all sleds, including sleds that
        // are no longer in service and need expungement work.
        let blueprint_zones =
            self.zones.into_zones_map(self.input.all_sled_ids(SledFilter::All));
        let blueprint_disks =
            self.disks.into_disks_map(self.input.all_sled_ids(SledFilter::All));
        Blueprint {
            id: self.rng.blueprint_rng.next(),
            blueprint_zones,
            blueprint_disks,
            parent_blueprint_id: Some(self.parent_blueprint.id),
            internal_dns_version: self.input.internal_dns_version(),
            external_dns_version: self.input.external_dns_version(),
            time_created: now_db_precision(),
            creator: self.creator,
            comment: self.comments.join(", "),
        }
    }

    /// Within tests, set a seeded RNG for deterministic results.
    ///
    /// This will ensure that tests that use this builder will produce the same
    /// results each time they are run.
    pub fn set_rng_seed<H: Hash>(&mut self, seed: H) -> &mut Self {
        self.rng.set_seed(seed);
        self
    }

    /// Sets the blueprints "comment"
    ///
    /// This is a short human-readable string summarizing the changes reflected
    /// in the blueprint.  This is only intended for debugging.
    pub fn comment<S>(&mut self, comment: S)
    where
        String: From<S>,
    {
        self.comments.push(String::from(comment));
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
    ) -> Result<Ensure, Error> {
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
            return Ok(Ensure::NotNeeded);
        }

        let disks = &mut self.disks.change_sled_disks(sled_id).disks;

        disks.append(&mut additions);
        disks.retain(|config| {
            !removals.contains(&PhysicalDiskUuid::from_untyped_uuid(config.id))
        });

        Ok(Ensure::Added)
    }

    pub fn sled_ensure_zone_ntp(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<Ensure, Error> {
        // If there's already an NTP zone on this sled, do nothing.
        let has_ntp = self
            .zones
            .current_sled_zones(sled_id)
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
        // servers but there may not be any of those.  We could also
        // construct this list manually from the set of internal DNS servers
        // actually deployed.  Instead, we take the same approach as RSS:
        // these are at known, fixed addresses relative to the AZ subnet
        // (which itself is a known-prefix parent subnet of the sled subnet).
        let dns_servers =
            get_internal_dns_server_addresses(sled_subnet.net().network());

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

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.zone_rng.next(),
            underlay_address: ip,
            zone_type: BlueprintZoneType::InternalNtp(
                blueprint_zone_type::InternalNtp {
                    address: ntp_address,
                    ntp_servers,
                    dns_servers,
                    domain: None,
                },
            ),
        };

        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    pub fn sled_ensure_zone_crucible(
        &mut self,
        sled_id: SledUuid,
        zpool_id: ZpoolUuid,
    ) -> Result<Ensure, Error> {
        let pool_name = zpool_id_to_external_name(zpool_id)?;

        // If this sled already has a Crucible zone on this pool, do nothing.
        let has_crucible_on_this_pool =
            self.zones.current_sled_zones(sled_id).any(|(z, _)| {
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
        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.zone_rng.next(),
            underlay_address: ip,
            zone_type: BlueprintZoneType::Crucible(
                blueprint_zone_type::Crucible {
                    address,
                    dataset: OmicronZoneDataset { pool_name },
                },
            ),
        };

        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    /// Return the number of Nexus zones that would be configured to run on the
    /// given sled if this builder generated a blueprint
    ///
    /// This value may change before a blueprint is actually generated if
    /// further changes are made to the builder.
    pub fn sled_num_nexus_zones(&self, sled_id: SledUuid) -> usize {
        self.zones
            .current_sled_zones(sled_id)
            .filter(|(z, _)| z.zone_type.is_nexus())
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
        let nexus_count = self.sled_num_nexus_zones(sled_id);
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
            let external_ip = self
                .available_external_ips
                .next()
                .ok_or(Error::NoExternalServiceIpAvailable)?;

            let nic = {
                let (ip, subnet) = match external_ip {
                    IpAddr::V4(_) => (
                        self.nexus_v4_ips
                            .next()
                            .ok_or(Error::ExhaustedNexusIps)?
                            .into(),
                        IpNet::from(*NEXUS_OPTE_IPV4_SUBNET),
                    ),
                    IpAddr::V6(_) => (
                        self.nexus_v6_ips
                            .next()
                            .ok_or(Error::ExhaustedNexusIps)?
                            .into(),
                        IpNet::from(*NEXUS_OPTE_IPV6_SUBNET),
                    ),
                };
                let mac = self
                    .available_system_macs
                    .next()
                    .ok_or(Error::NoSystemMacAddressAvailable)?;
                NetworkInterface {
                    id: self.rng.network_interface_rng.next(),
                    kind: NetworkInterfaceKind::Service {
                        id: nexus_id.into_untyped_uuid(),
                    },
                    name: format!("nexus-{nexus_id}").parse().unwrap(),
                    ip,
                    mac,
                    subnet,
                    vni: Vni::SERVICES_VNI,
                    primary: true,
                    slot: 0,
                }
            };

            let ip = self.sled_alloc_ip(sled_id)?;
            let port = omicron_common::address::NEXUS_INTERNAL_PORT;
            let internal_address = SocketAddrV6::new(ip, port, 0, 0);
            let zone = BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: nexus_id,
                underlay_address: ip,
                zone_type: BlueprintZoneType::Nexus(
                    blueprint_zone_type::Nexus {
                        internal_address,
                        external_ip,
                        nic,
                        external_tls,
                        external_dns_servers: external_dns_servers.clone(),
                    },
                ),
            };
            self.sled_add_zone(sled_id, zone)?;
        }

        Ok(EnsureMultiple::Added(num_nexus_to_add))
    }

    fn sled_add_zone(
        &mut self,
        sled_id: SledUuid,
        zone: BlueprintZoneConfig,
    ) -> Result<(), Error> {
        // Check the sled id and return an appropriate error if it's invalid.
        let _ = self.sled_resources(sled_id)?;

        let sled_zones = self.zones.change_sled_zones(sled_id);
        sled_zones.add_zone(zone)?;

        Ok(())
    }

    /// Returns a newly-allocated underlay address suitable for use by Omicron
    /// zones
    fn sled_alloc_ip(&mut self, sled_id: SledUuid) -> Result<Ipv6Addr, Error> {
        let sled_subnet = self.sled_resources(sled_id)?.subnet;
        let allocator =
            self.sled_ip_allocators.entry(sled_id).or_insert_with(|| {
                let sled_subnet_addr = sled_subnet.net().network();
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
                for (z, _) in self.zones.current_sled_zones(sled_id) {
                    allocator.reserve(z.underlay_address);
                }

                allocator
            });

        allocator.alloc().ok_or(Error::OutOfAddresses { sled_id })
    }

    fn sled_resources(
        &self,
        sled_id: SledUuid,
    ) -> Result<&SledResources, Error> {
        self.input.sled_resources(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "attempted to use sled that is not in service: {}",
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

        BlueprintBuilderRng { blueprint_rng, zone_rng, network_interface_rng }
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
struct BlueprintZonesBuilder<'a> {
    changed_zones: BTreeMap<SledUuid, BuilderZonesConfig>,
    // Temporarily make a clone of the parent blueprint's zones so we can use
    // typed UUIDs everywhere. Once we're done migrating, this `Cow` can be
    // removed.
    parent_zones: Cow<'a, BTreeMap<SledUuid, BlueprintZonesConfig>>,
}

impl<'a> BlueprintZonesBuilder<'a> {
    pub fn new(parent_blueprint: &'a Blueprint) -> BlueprintZonesBuilder {
        BlueprintZonesBuilder {
            changed_zones: BTreeMap::new(),
            parent_zones: Cow::Owned(parent_blueprint.typed_blueprint_zones()),
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

    /// Iterates over the list of Omicron zones currently configured for this
    /// sled in the blueprint that's being built, along with each zone's state
    /// in the builder.
    pub fn current_sled_zones(
        &self,
        sled_id: SledUuid,
    ) -> Box<dyn Iterator<Item = (&BlueprintZoneConfig, BuilderZoneState)> + '_>
    {
        if let Some(sled_zones) = self.changed_zones.get(&sled_id) {
            Box::new(sled_zones.iter_zones().map(|z| (z.zone(), z.state())))
        } else if let Some(parent_zones) = self.parent_zones.get(&sled_id) {
            Box::new(
                parent_zones
                    .zones
                    .iter()
                    .map(|z| (z, BuilderZoneState::Unchanged)),
            )
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Produces an owned map of zones for the requested sleds
    pub fn into_zones_map(
        mut self,
        sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<Uuid, BlueprintZonesConfig> {
        sled_ids
            .map(|sled_id| {
                // Start with self.changed_zones, which contains entries for any
                // sled whose zones config is changing in this blueprint.
                if let Some(zones) = self.changed_zones.remove(&sled_id) {
                    (sled_id.into_untyped_uuid(), zones.build())
                }
                // Next, check self.parent_zones, to represent an unchanged sled.
                else if let Some(parent_zones) =
                    self.parent_zones.get(&sled_id)
                {
                    (sled_id.into_untyped_uuid(), parent_zones.clone())
                } else {
                    // If the sled is not in self.parent_zones, then it must be a
                    // new sled and we haven't added any zones to it yet.  Use the
                    // standard initial config.
                    (
                        sled_id.into_untyped_uuid(),
                        BlueprintZonesConfig {
                            generation: Generation::new(),
                            zones: vec![],
                        },
                    )
                }
            })
            .collect()
    }
}

// This is a sub-module to hide implementation details from the rest of
// blueprint_builder.
mod builder_zones {
    use super::*;

    #[derive(Debug)]
    #[must_use]
    pub(crate) struct BuilderZonesConfig {
        // The current generation -- this is bumped at blueprint build time and is
        // otherwise not exposed to callers.
        generation: Generation,

        // The list of zones, along with their state.
        zones: Vec<BuilderZoneConfig>,
    }

    impl BuilderZonesConfig {
        pub(super) fn new() -> Self {
            Self {
                // Note that the first generation is reserved to mean the one
                // containing no zones. See
                // OmicronZonesConfig::INITIAL_GENERATION.
                //
                // Since we're currently assuming that creating a new
                // `BuilderZonesConfig` means that we're going to add new zones
                // shortly, we start with Generation::new() here. It'll get
                // bumped up to the next one in `Self::build`.
                generation: Generation::new(),
                zones: vec![],
            }
        }

        pub(super) fn from_parent(parent: &BlueprintZonesConfig) -> Self {
            Self {
                // We'll bump this up at build time.
                generation: parent.generation,

                zones: parent
                    .zones
                    .iter()
                    .map(|zone| BuilderZoneConfig {
                        zone: zone.clone(),
                        state: BuilderZoneState::Unchanged,
                    })
                    .collect(),
            }
        }

        pub(super) fn add_zone(
            &mut self,
            zone: BlueprintZoneConfig,
        ) -> Result<(), Error> {
            if self.zones.iter().any(|z| z.zone.id == zone.id) {
                return Err(Error::Planner(anyhow!(
                    "attempted to add zone that already exists: {}",
                    zone.id
                )));
            };

            self.zones.push(BuilderZoneConfig {
                zone,
                state: BuilderZoneState::Added,
            });
            Ok(())
        }

        pub(super) fn iter_zones(
            &self,
        ) -> impl Iterator<Item = &BuilderZoneConfig> {
            self.zones.iter()
        }

        pub(super) fn build(self) -> BlueprintZonesConfig {
            let mut ret = BlueprintZonesConfig {
                // Something we could do here is to check if any zones have
                // actually been modified, and if not, return the parent's
                // generation. For now, we depend on callers to only call
                // `BlueprintZonesBuilder::change_sled_zones` when they really
                // mean it.
                generation: self.generation.next(),
                zones: self.zones.into_iter().map(|z| z.zone).collect(),
            };
            ret.sort();
            ret
        }
    }

    #[derive(Debug)]
    pub(crate) struct BuilderZoneConfig {
        zone: BlueprintZoneConfig,
        state: BuilderZoneState,
    }

    impl BuilderZoneConfig {
        pub(super) fn zone(&self) -> &BlueprintZoneConfig {
            &self.zone
        }

        pub(super) fn state(&self) -> BuilderZoneState {
            self.state
        }
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub(crate) enum BuilderZoneState {
        Unchanged,
        // Currently unused: Modified
        Added,
    }
}

use builder_zones::*;

/// Helper for working with sets of disks on each sled
///
/// Tracking the set of disks is slightly non-trivial because we need to bump
/// the per-sled generation number iff the disks are changed.  So we need to
/// keep track of whether we've changed the disks relative to the parent
/// blueprint.  We do this by keeping a copy of any [`BlueprintDisksConfig`]
/// that we've changed and a _reference_ to the parent blueprint's disks.  This
/// struct makes it easy for callers iterate over the right set of disks.
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
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::deployment::SledDetails;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::external_api::views::SledState;
    use omicron_common::address::IpRange;
    use omicron_common::address::Ipv6Subnet;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use sled_agent_client::types::OmicronZoneType;
    use std::collections::BTreeSet;

    pub const DEFAULT_N_SLEDS: usize = 3;

    /// Checks various conditions that should be true for all blueprints
    pub fn verify_blueprint(blueprint: &Blueprint) {
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
    }

    #[test]
    fn test_initial() {
        // Test creating a blueprint from a collection and verifying that it
        // describes no changes.
        static TEST_NAME: &str = "blueprint_builder_test_initial";
        let logctx = test_setup_log(TEST_NAME);
        let (collection, input) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        let blueprint_initial =
            BlueprintBuilder::build_initial_from_collection_seeded(
                &collection,
                Generation::new(),
                Generation::new(),
                input.all_sled_ids(SledFilter::All),
                "the_test",
                TEST_NAME,
            )
            .expect("failed to create initial blueprint");
        verify_blueprint(&blueprint_initial);

        let diff =
            blueprint_initial.diff_since_collection(&collection).unwrap();
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
        assert_eq!(diff.sleds_added().len(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);
        assert_eq!(diff.sleds_modified().count(), 0);

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
        let diff = blueprint.diff_since_blueprint(&blueprint_initial).unwrap();
        println!(
            "initial blueprint -> next blueprint (expected no changes):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added().len(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);
        assert_eq!(diff.sleds_modified().count(), 0);

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
            example.input.all_sled_resources(SledFilter::All)
        {
            builder.sled_ensure_zone_ntp(sled_id).unwrap();
            for pool_id in sled_resources.zpools.keys() {
                builder.sled_ensure_zone_crucible(sled_id, *pool_id).unwrap();
            }
        }

        let blueprint2 = builder.build();
        verify_blueprint(&blueprint2);
        let diff = blueprint2.diff_since_blueprint(&blueprint1).unwrap();
        println!(
            "initial blueprint -> next blueprint (expected no changes):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added().len(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);
        assert_eq!(diff.sleds_modified().count(), 0);

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
        let diff = blueprint3.diff_since_blueprint(&blueprint2).unwrap();
        println!("expecting new NTP and Crucible zones:\n{}", diff.display());

        // No sleds were changed or removed.
        assert_eq!(diff.sleds_modified().count(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);

        // One sled was added.
        let sleds: Vec<_> = diff.sleds_added().collect();
        assert_eq!(sleds.len(), 1);
        let (sled_id, new_sled_zones) = sleds[0];
        assert_eq!(sled_id, new_sled_id);
        // The generation number should be newer than the initial default.
        assert!(new_sled_zones.generation > Generation::new());

        // All zones' underlay addresses ought to be on the sled's subnet.
        for z in &new_sled_zones.zones {
            assert!(new_sled_resources
                .subnet
                .net()
                .contains(z.underlay_address));
        }

        // Check for an NTP zone.  Its sockaddr's IP should also be on the
        // sled's subnet.
        assert!(new_sled_zones.zones.iter().any(|z| {
            if let BlueprintZoneType::InternalNtp(
                blueprint_zone_type::InternalNtp { address, .. },
            ) = &z.zone_type
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
                if let BlueprintZoneType::Crucible(
                    blueprint_zone_type::Crucible { address, dataset },
                ) = &z.zone_type
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
                .map(|id| { zpool_id_to_external_name(*id).unwrap() })
                .collect()
        );

        logctx.cleanup_successful();
    }

    /// A test focusing on `BlueprintZonesBuilder` and its internal logic.
    #[test]
    fn test_builder_zones() {
        static TEST_NAME: &str = "blueprint_test_builder_zones";
        let logctx = test_setup_log(TEST_NAME);
        let mut example =
            ExampleSystem::new(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        let blueprint_initial =
            BlueprintBuilder::build_initial_from_collection_seeded(
                &example.collection,
                Generation::new(),
                Generation::new(),
                example.input.all_sled_ids(SledFilter::All),
                "the_test",
                TEST_NAME,
            )
            .expect("creating initial blueprint");

        // Add a completely bare sled to the input.
        let (new_sled_id, input2) = {
            let mut input = example.input.clone().into_builder();
            let new_sled_id = example.sled_rng.next();
            input
                .add_sled(
                    new_sled_id,
                    SledDetails {
                        policy: SledPolicy::provisionable(),
                        state: SledState::Active,
                        resources: SledResources {
                            subnet: Ipv6Subnet::new(
                                "fd00:1::".parse().unwrap(),
                            ),
                            zpools: BTreeMap::new(),
                        },
                    },
                )
                .expect("adding new sled");

            (new_sled_id, input.build())
        };

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint_initial,
            &input2,
            "the_test",
        )
        .expect("creating blueprint builder");
        builder.set_rng_seed((TEST_NAME, "bp2"));

        // Test adding a new sled with an NTP zone.
        assert_eq!(
            builder.sled_ensure_zone_ntp(new_sled_id).unwrap(),
            Ensure::Added
        );

        // Iterate over the zones for the sled and ensure that the NTP zone is
        // present.
        {
            let mut zones = builder.zones.current_sled_zones(new_sled_id);
            let (_, state) = zones.next().expect("exactly one zone for sled");
            assert!(zones.next().is_none(), "exactly one zone for sled");
            assert_eq!(
                state,
                BuilderZoneState::Added,
                "NTP zone should have been added"
            );
        }

        // Now, test adding a new zone (Oximeter, picked arbitrarily) to an
        // existing sled.
        let existing_sled_id = example
            .input
            .all_sled_ids(SledFilter::All)
            .next()
            .expect("at least one sled present");
        let change = builder.zones.change_sled_zones(existing_sled_id);

        let new_zone_id = OmicronZoneUuid::new_v4();
        change
            .add_zone(BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: new_zone_id,
                underlay_address: Ipv6Addr::UNSPECIFIED,
                zone_type: BlueprintZoneType::Oximeter(
                    blueprint_zone_type::Oximeter {
                        address: SocketAddrV6::new(
                            Ipv6Addr::UNSPECIFIED,
                            0,
                            0,
                            0,
                        ),
                    },
                ),
            })
            .expect("adding new zone");

        {
            // Iterate over the zones and ensure that the Oximeter zone is
            // present, and marked added.
            let mut zones = builder.zones.current_sled_zones(existing_sled_id);
            zones
                .find_map(|(z, state)| {
                    if z.id == new_zone_id {
                        assert_eq!(
                            state,
                            BuilderZoneState::Added,
                            "new zone ID {new_zone_id} should be marked added"
                        );
                        Some(())
                    } else {
                        None
                    }
                })
                .expect("new zone ID should be present");
        }

        // Also call change_sled_zones without making any changes. This
        // currently bumps the generation number, but in the future might
        // become smarter.
        let control_sled_id = example
            .input
            .all_sled_ids(SledFilter::All)
            .nth(2)
            .expect("at least 2 sleds present");
        _ = builder.zones.change_sled_zones(control_sled_id);

        // Now build the blueprint and ensure that all the changes we described
        // above are present.
        let blueprint = builder.build();
        verify_blueprint(&blueprint);
        let diff = blueprint.diff_since_blueprint(&blueprint_initial).unwrap();
        println!("expecting new NTP and Oximeter zones:\n{}", diff.display());

        // No sleds were removed.
        assert_eq!(diff.sleds_removed().len(), 0);

        // One sled was added.
        let sleds: Vec<_> = diff.sleds_added().collect();
        assert_eq!(sleds.len(), 1);
        let (sled_id, new_sled_zones) = sleds[0];
        assert_eq!(sled_id, new_sled_id);
        // The generation number should be newer than the initial default.
        assert_eq!(new_sled_zones.generation, Generation::new().next());
        assert_eq!(new_sled_zones.zones.len(), 1);

        // Two sled was modified: existing_sled_id and control_sled_id.
        let sleds = diff.sleds_modified();
        assert_eq!(sleds.len(), 2, "2 sleds modified");
        for (sled_id, sled_modified) in sleds {
            if sled_id == existing_sled_id {
                assert_eq!(
                    sled_modified.generation_after,
                    sled_modified.generation_before.next()
                );
                assert_eq!(sled_modified.zones_added().len(), 1);
                let added_zone = sled_modified.zones_added().next().unwrap();
                assert_eq!(added_zone.id, new_zone_id);
            } else {
                assert_eq!(sled_id, control_sled_id);

                // The generation number is bumped, but nothing else.
                assert_eq!(
                    sled_modified.generation_after,
                    sled_modified.generation_before.next(),
                    "control sled has generation number bumped"
                );
                assert_eq!(sled_modified.zones_added().len(), 0);
                assert_eq!(sled_modified.zones_removed().len(), 0);
                assert_eq!(sled_modified.zones_modified().count(), 0);
            }
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_add_physical_disks() {
        static TEST_NAME: &str = "blueprint_builder_test_add_physical_disks";
        let logctx = test_setup_log(TEST_NAME);
        let (collection, input) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // We don't care about the DNS versions here.
        let internal_dns_version = Generation::new();
        let external_dns_version = Generation::new();
        let parent = BlueprintBuilder::build_initial_from_collection_seeded(
            &collection,
            internal_dns_version,
            external_dns_version,
            input.all_sled_ids(SledFilter::All),
            "test",
            TEST_NAME,
        )
        .expect("failed to create initial blueprint");

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
                    Ensure::Added,
                );
            }

            assert!(!builder.disks.changed_disks.is_empty());
            assert!(builder.disks.parent_disks.is_empty());
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_add_nexus_with_no_existing_nexus_zones() {
        static TEST_NAME: &str =
            "blueprint_builder_test_add_nexus_with_no_existing_nexus_zones";
        let logctx = test_setup_log(TEST_NAME);
        let (mut collection, input) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // We don't care about the DNS versions here.
        let internal_dns_version = Generation::new();
        let external_dns_version = Generation::new();

        // Adding a new Nexus zone currently requires copying settings from an
        // existing Nexus zone. If we remove all Nexus zones from the
        // collection, create a blueprint, then try to add a Nexus zone, it
        // should fail.
        for zones in collection.omicron_zones.values_mut() {
            zones.zones.zones.retain(|z| {
                !matches!(z.zone_type, OmicronZoneType::Nexus { .. })
            });
        }

        let parent = BlueprintBuilder::build_initial_from_collection_seeded(
            &collection,
            internal_dns_version,
            external_dns_version,
            input.all_sled_ids(SledFilter::All),
            "test",
            TEST_NAME,
        )
        .expect("failed to create initial blueprint");

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
        let (mut collection, input) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // We don't care about the DNS versions here.
        let internal_dns_version = Generation::new();
        let external_dns_version = Generation::new();

        // Remove the Nexus zone from one of the sleds so that
        // `sled_ensure_zone_nexus` can attempt to add a Nexus zone to
        // `sled_id`.
        let sled_id = {
            let mut selected_sled_id = None;
            for (sled_id, zones) in &mut collection.omicron_zones {
                let nzones_before_retain = zones.zones.zones.len();
                zones.zones.zones.retain(|z| {
                    !matches!(z.zone_type, OmicronZoneType::Nexus { .. })
                });
                if zones.zones.zones.len() < nzones_before_retain {
                    selected_sled_id = Some(*sled_id);
                    break;
                }
            }
            selected_sled_id.expect("found no sleds with Nexus zone")
        };

        let parent = BlueprintBuilder::build_initial_from_collection_seeded(
            &collection,
            internal_dns_version,
            external_dns_version,
            input.all_sled_ids(SledFilter::All),
            "test",
            TEST_NAME,
        )
        .expect("failed to create initial blueprint");

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

            assert_eq!(added, EnsureMultiple::Added(1));
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

            assert_eq!(added, EnsureMultiple::Added(3));
        }

        {
            // Replace the policy's external service IP pool ranges with ranges
            // that are already in use by existing zones. Attempting to add a
            // Nexus with no remaining external IPs should fail.
            let mut used_ip_ranges = Vec::new();
            for (_, z) in parent.all_omicron_zones(BlueprintZoneFilter::All) {
                if let Some(ip) = z.zone_type.external_ip() {
                    used_ip_ranges.push(IpRange::from(ip));
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
        let (mut collection, input) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // We should fail if the parent blueprint claims to contain two
        // zones with the same external IP. Skim through the zones, copy the
        // external IP from one Nexus zone, then assign it to a later Nexus
        // zone.
        let mut found_second_nexus_zone = false;
        let mut nexus_external_ip = None;

        'outer: for zones in collection.omicron_zones.values_mut() {
            for z in zones.zones.zones.iter_mut() {
                if let OmicronZoneType::Nexus { external_ip, .. } =
                    &mut z.zone_type
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

        let parent = BlueprintBuilder::build_initial_from_collection_seeded(
            &collection,
            Generation::new(),
            Generation::new(),
            input.all_sled_ids(SledFilter::All),
            "test",
            TEST_NAME,
        )
        .unwrap();

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
        let (mut collection, input) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // We should fail if the parent blueprint claims to contain two
        // Nexus zones with the same NIC IP. Skim through the zones, copy
        // the NIC IP from one Nexus zone, then assign it to a later
        // Nexus zone.
        let mut found_second_nexus_zone = false;
        let mut nexus_nic_ip = None;

        'outer: for zones in collection.omicron_zones.values_mut() {
            for z in zones.zones.zones.iter_mut() {
                if let OmicronZoneType::Nexus { nic, .. } = &mut z.zone_type {
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

        let parent = BlueprintBuilder::build_initial_from_collection_seeded(
            &collection,
            Generation::new(),
            Generation::new(),
            input.all_sled_ids(SledFilter::All),
            "test",
            TEST_NAME,
        )
        .unwrap();

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
        let (mut collection, input) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // We should fail if the parent blueprint claims to contain two
        // zones with the same service vNIC MAC address. Skim through the
        // zones, copy the NIC MAC from one Nexus zone, then assign it to a
        // later Nexus zone.
        let mut found_second_nexus_zone = false;
        let mut nexus_nic_mac = None;

        'outer: for zones in collection.omicron_zones.values_mut() {
            for z in zones.zones.zones.iter_mut() {
                if let OmicronZoneType::Nexus { nic, .. } = &mut z.zone_type {
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

        let parent = BlueprintBuilder::build_initial_from_collection_seeded(
            &collection,
            Generation::new(),
            Generation::new(),
            input.all_sled_ids(SledFilter::All),
            "test",
            TEST_NAME,
        )
        .unwrap();

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
}

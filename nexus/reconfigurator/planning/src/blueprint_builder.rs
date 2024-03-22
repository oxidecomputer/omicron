// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level facility for generating Blueprints

use crate::ip_allocator::IpAllocator;
use anyhow::anyhow;
use anyhow::bail;
use internal_dns::config::Host;
use internal_dns::config::ZoneVariant;
use ipnet::IpAdd;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use nexus_inventory::now_db_precision;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::OmicronZoneConfig;
use nexus_types::deployment::OmicronZoneDataset;
use nexus_types::deployment::OmicronZoneType;
use nexus_types::deployment::Policy;
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
use rand::rngs::StdRng;
use rand::RngCore;
use rand::SeedableRng;
use slog::o;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use thiserror::Error;
use uuid::Uuid;

/// Errors encountered while assembling blueprints
#[derive(Debug, Error)]
pub enum Error {
    #[error("sled {sled_id}: ran out of available addresses for sled")]
    OutOfAddresses { sled_id: Uuid },
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
    internal_dns_version: Generation,
    external_dns_version: Generation,

    // These fields are used to allocate resources from sleds.
    policy: &'a Policy,
    sled_ip_allocators: BTreeMap<Uuid, IpAllocator>,

    // These fields will become part of the final blueprint.  See the
    // corresponding fields in `Blueprint`.
    zones: BlueprintZonesBuilder<'a>,
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
        policy: &Policy,
        creator: &str,
    ) -> Result<Blueprint, Error> {
        Self::build_initial_impl(
            collection,
            internal_dns_version,
            external_dns_version,
            policy,
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
        policy: &Policy,
        creator: &str,
        seed: H,
    ) -> Result<Blueprint, Error> {
        let mut rng = BlueprintBuilderRng::new();
        rng.set_seed(seed);
        Self::build_initial_impl(
            collection,
            internal_dns_version,
            external_dns_version,
            policy,
            creator,
            rng,
        )
    }

    fn build_initial_impl(
        collection: &Collection,
        internal_dns_version: Generation,
        external_dns_version: Generation,
        policy: &Policy,
        creator: &str,
        mut rng: BlueprintBuilderRng,
    ) -> Result<Blueprint, Error> {
        let blueprint_zones = policy
            .sleds
            .keys()
            .map(|sled_id| {
                let zones = collection
                    .omicron_zones
                    .get(sled_id)
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

                Ok((
                    *sled_id,
                    BlueprintZonesConfig::initial_from_collection(&zones),
                ))
            })
            .collect::<Result<_, Error>>()?;
        Ok(Blueprint {
            id: rng.blueprint_rng.next_uuid(),
            blueprint_zones,
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
        internal_dns_version: Generation,
        external_dns_version: Generation,
        policy: &'a Policy,
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

        for (_, z) in parent_blueprint.all_omicron_zones() {
            let zone_type = &z.zone_type;
            if let OmicronZoneType::Nexus { nic, .. } = zone_type {
                match nic.ip {
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

            if let Some(external_ip) = zone_type.external_ip()? {
                // For the test suite, ignore localhost.  It gets reused many
                // times and that's okay.  We don't expect to see localhost
                // outside the test suite.
                if !external_ip.is_loopback()
                    && !used_external_ips.insert(external_ip)
                {
                    bail!("duplicate external IP: {external_ip}");
                }
            }
            if let Some(nic) = zone_type.service_vnic() {
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
            policy
                .service_ip_pool_ranges
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
            internal_dns_version,
            external_dns_version,
            policy,
            sled_ip_allocators: BTreeMap::new(),
            zones: BlueprintZonesBuilder::new(parent_blueprint),
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
        // Collect the Omicron zones config for each in-service sled.
        let blueprint_zones =
            self.zones.into_zones_map(self.policy.sleds.keys().copied());
        Blueprint {
            id: self.rng.blueprint_rng.next_uuid(),
            blueprint_zones,
            parent_blueprint_id: Some(self.parent_blueprint.id),
            internal_dns_version: self.internal_dns_version,
            external_dns_version: self.external_dns_version,
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

    pub fn sled_ensure_zone_ntp(
        &mut self,
        sled_id: Uuid,
    ) -> Result<Ensure, Error> {
        // If there's already an NTP zone on this sled, do nothing.
        let has_ntp = self
            .zones
            .current_sled_zones(sled_id)
            .any(|z| z.config.zone_type.is_ntp());
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
            .all_omicron_zones()
            .filter_map(|(_, z)| {
                if matches!(z.zone_type, OmicronZoneType::BoundaryNtp { .. }) {
                    Some(Host::for_zone(z.id, ZoneVariant::Other).fqdn())
                } else {
                    None
                }
            })
            .collect();

        let zone = OmicronZoneConfig {
            id: self.rng.zone_rng.next_uuid(),
            underlay_address: ip,
            zone_type: OmicronZoneType::InternalNtp {
                address: ntp_address.to_string(),
                ntp_servers,
                dns_servers,
                domain: None,
            },
        };
        let zone = BlueprintZoneConfig {
            config: zone,
            disposition: BlueprintZoneDisposition::InService,
        };

        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    pub fn sled_ensure_zone_crucible(
        &mut self,
        sled_id: Uuid,
        pool_name: ZpoolName,
    ) -> Result<Ensure, Error> {
        // If this sled already has a Crucible zone on this pool, do nothing.
        let has_crucible_on_this_pool =
            self.zones.current_sled_zones(sled_id).any(|z| {
                matches!(
                    &z.config.zone_type,
                    OmicronZoneType::Crucible { dataset, .. }
                    if dataset.pool_name == pool_name
                )
            });
        if has_crucible_on_this_pool {
            return Ok(Ensure::NotNeeded);
        }

        let sled_info = self.sled_resources(sled_id)?;
        if !sled_info.zpools.contains(&pool_name) {
            return Err(Error::Planner(anyhow!(
                "adding crucible zone for sled {:?}: \
                attempted to use unknown zpool {:?}",
                sled_id,
                pool_name
            )));
        }

        let ip = self.sled_alloc_ip(sled_id)?;
        let port = omicron_common::address::CRUCIBLE_PORT;
        let address = SocketAddrV6::new(ip, port, 0, 0).to_string();
        let zone = OmicronZoneConfig {
            id: self.rng.zone_rng.next_uuid(),
            underlay_address: ip,
            zone_type: OmicronZoneType::Crucible {
                address,
                dataset: OmicronZoneDataset { pool_name },
            },
        };

        let zone = BlueprintZoneConfig {
            config: zone,
            disposition: BlueprintZoneDisposition::InService,
        };
        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    /// Return the number of Nexus zones that would be configured to run on the
    /// given sled if this builder generated a blueprint
    ///
    /// This value may change before a blueprint is actually generated if
    /// further changes are made to the builder.
    pub fn sled_num_nexus_zones(&self, sled_id: Uuid) -> usize {
        self.zones
            .current_sled_zones(sled_id)
            .filter(|z| z.config.zone_type.is_nexus())
            .count()
    }

    pub fn sled_ensure_zone_multiple_nexus(
        &mut self,
        sled_id: Uuid,
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
            .all_omicron_zones()
            .find_map(|(_, z)| match &z.zone_type {
                OmicronZoneType::Nexus {
                    external_tls,
                    external_dns_servers,
                    ..
                } => Some((*external_tls, external_dns_servers.clone())),
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
        sled_id: Uuid,
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
            let nexus_id = self.rng.zone_rng.next_uuid();
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
                    id: self.rng.network_interface_rng.next_uuid(),
                    kind: NetworkInterfaceKind::Service { id: nexus_id },
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
            let internal_address =
                SocketAddrV6::new(ip, port, 0, 0).to_string();
            let zone = OmicronZoneConfig {
                id: nexus_id,
                underlay_address: ip,
                zone_type: OmicronZoneType::Nexus {
                    internal_address,
                    external_ip,
                    nic,
                    external_tls,
                    external_dns_servers: external_dns_servers.clone(),
                },
            };
            let zone = BlueprintZoneConfig {
                config: zone,
                disposition: BlueprintZoneDisposition::InService,
            };
            self.sled_add_zone(sled_id, zone)?;
        }

        Ok(EnsureMultiple::Added(num_nexus_to_add))
    }

    fn sled_add_zone(
        &mut self,
        sled_id: Uuid,
        zone: BlueprintZoneConfig,
    ) -> Result<(), Error> {
        // Check the sled id and return an appropriate error if it's invalid.
        let _ = self.sled_resources(sled_id)?;

        let sled_zones = self.zones.change_sled_zones(sled_id);
        // A sled should have a small number (< 20) of zones so a linear search
        // should be very fast.
        if sled_zones.zones.iter().any(|z| z.config.id == zone.config.id) {
            return Err(Error::Planner(anyhow!(
                "attempted to add zone that already exists: {}",
                zone.config.id
            )));
        }
        sled_zones.zones.push(zone);
        Ok(())
    }

    /// Returns a newly-allocated underlay address suitable for use by Omicron
    /// zones
    fn sled_alloc_ip(&mut self, sled_id: Uuid) -> Result<Ipv6Addr, Error> {
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
                for z in self.zones.current_sled_zones(sled_id) {
                    allocator.reserve(z.config.underlay_address);
                }

                allocator
            });

        allocator.alloc().ok_or(Error::OutOfAddresses { sled_id })
    }

    fn sled_resources(&self, sled_id: Uuid) -> Result<&SledResources, Error> {
        self.policy.sleds.get(&sled_id).ok_or_else(|| {
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
    zone_rng: UuidRng,
    network_interface_rng: UuidRng,
}

impl BlueprintBuilderRng {
    fn new() -> Self {
        Self::new_from_rng(StdRng::from_entropy())
    }

    fn new_from_rng(mut root_rng: StdRng) -> Self {
        let blueprint_rng = UuidRng::from_root_rng(&mut root_rng, "blueprint");
        let zone_rng = UuidRng::from_root_rng(&mut root_rng, "zone");
        let network_interface_rng =
            UuidRng::from_root_rng(&mut root_rng, "network_interface");

        BlueprintBuilderRng { blueprint_rng, zone_rng, network_interface_rng }
    }

    fn set_seed<H: Hash>(&mut self, seed: H) {
        // Important to add some more bytes here, so that builders with the
        // same seed but different purposes don't end up with the same UUIDs.
        const SEED_EXTRA: &str = "blueprint-builder";
        let mut seeder = rand_seeder::Seeder::from((seed, SEED_EXTRA));
        *self = Self::new_from_rng(seeder.make_rng::<StdRng>());
    }
}

#[derive(Debug)]
pub(crate) struct UuidRng {
    rng: StdRng,
}

impl UuidRng {
    /// Returns a new `UuidRng` generated from the root RNG.
    ///
    /// `extra` is a string that should be unique to the purpose of the UUIDs.
    fn from_root_rng(root_rng: &mut StdRng, extra: &'static str) -> Self {
        let seed = root_rng.next_u64();
        let mut seeder = rand_seeder::Seeder::from((seed, extra));
        Self { rng: seeder.make_rng::<StdRng>() }
    }

    /// `extra` is a string that should be unique to the purpose of the UUIDs.
    pub(crate) fn from_seed<H: Hash>(seed: H, extra: &'static str) -> Self {
        let mut seeder = rand_seeder::Seeder::from((seed, extra));
        Self { rng: seeder.make_rng::<StdRng>() }
    }

    /// Returns a new UUIDv4 generated from the RNG.
    pub(crate) fn next_uuid(&mut self) -> Uuid {
        let mut bytes = [0; 16];
        self.rng.fill_bytes(&mut bytes);
        // Builder::from_random_bytes will turn the random bytes into a valid
        // UUIDv4. (Parts of the system depend on the UUID actually being valid
        // v4, so it's important that we don't just use `uuid::from_bytes`.)
        uuid::Builder::from_random_bytes(bytes).into_uuid()
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
    changed_zones: BTreeMap<Uuid, BlueprintZonesConfig>,
    parent_zones: &'a BTreeMap<Uuid, BlueprintZonesConfig>,
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
        sled_id: Uuid,
    ) -> &mut BlueprintZonesConfig {
        self.changed_zones.entry(sled_id).or_insert_with(|| {
            if let Some(old_sled_zones) = self.parent_zones.get(&sled_id) {
                BlueprintZonesConfig {
                    generation: old_sled_zones.generation.next(),
                    zones: old_sled_zones.zones.clone(),
                }
            } else {
                // The first generation is reserved to mean the one containing
                // no zones.  See OmicronZonesConfig::INITIAL_GENERATION.  So
                // we start with the next one.
                BlueprintZonesConfig {
                    generation: Generation::new().next(),
                    zones: vec![],
                }
            }
        })
    }

    /// Iterates over the list of Omicron zones currently configured for this
    /// sled in the blueprint that's being built
    pub fn current_sled_zones(
        &self,
        sled_id: Uuid,
    ) -> Box<dyn Iterator<Item = &BlueprintZoneConfig> + '_> {
        if let Some(sled_zones) = self
            .changed_zones
            .get(&sled_id)
            .or_else(|| self.parent_zones.get(&sled_id))
        {
            Box::new(sled_zones.zones.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Produces an owned map of zones for the requested sleds
    pub fn into_zones_map(
        mut self,
        sled_ids: impl Iterator<Item = Uuid>,
    ) -> BTreeMap<Uuid, BlueprintZonesConfig> {
        sled_ids
            .map(|sled_id| {
                // Start with self.changed_zones, which contains entries for any
                // sled whose zones config is changing in this blueprint.
                let mut zones = self
                    .changed_zones
                    .remove(&sled_id)
                    // If it's not there, use the config from the parent
                    // blueprint.
                    .or_else(|| self.parent_zones.get(&sled_id).cloned())
                    // If it's not there either, then this must be a new sled
                    // and we haven't added any zones to it yet.  Use the
                    // standard initial config.
                    .unwrap_or_else(|| BlueprintZonesConfig {
                        generation: Generation::new(),
                        zones: vec![],
                    });

                zones.sort();

                (sled_id, zones)
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
    use omicron_common::address::IpRange;
    use omicron_test_utils::dev::test_setup_log;
    use sled_agent_client::types::{OmicronZoneConfig, OmicronZoneType};
    use std::collections::BTreeSet;

    pub const DEFAULT_N_SLEDS: usize = 3;

    /// Checks various conditions that should be true for all blueprints
    pub fn verify_blueprint(blueprint: &Blueprint) {
        let mut underlay_ips: BTreeMap<Ipv6Addr, &OmicronZoneConfig> =
            BTreeMap::new();
        for (_, zone) in blueprint.all_omicron_zones() {
            if let Some(previous) =
                underlay_ips.insert(zone.underlay_address, zone)
            {
                panic!(
                    "found duplicate underlay IP {} in zones {} and \
                        {}\n\nblueprint: {:#?}",
                    zone.underlay_address, zone.id, previous.id, blueprint
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
        let (collection, policy) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        let blueprint_initial =
            BlueprintBuilder::build_initial_from_collection_seeded(
                &collection,
                Generation::new(),
                Generation::new(),
                &policy,
                "the_test",
                TEST_NAME,
            )
            .expect("failed to create initial blueprint");
        verify_blueprint(&blueprint_initial);

        let diff = blueprint_initial.diff_sleds_from_collection(&collection);
        println!(
            "collection -> initial blueprint (expected no changes):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);

        // Test a no-op blueprint.
        let builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint_initial,
            Generation::new(),
            Generation::new(),
            &policy,
            "test_basic",
        )
        .expect("failed to create builder");
        let blueprint = builder.build();
        verify_blueprint(&blueprint);
        let diff = blueprint_initial.diff_sleds(&blueprint);
        println!(
            "initial blueprint -> next blueprint (expected no changes):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);

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
            Generation::new(),
            Generation::new(),
            &example.policy,
            "test_basic",
        )
        .expect("failed to create builder");

        // The example blueprint should have internal NTP zones on all the
        // existing sleds, plus Crucible zones on all pools.  So if we ensure
        // all these zones exist, we should see no change.
        for (sled_id, sled_resources) in &example.policy.sleds {
            builder.sled_ensure_zone_ntp(*sled_id).unwrap();
            for pool_name in &sled_resources.zpools {
                builder
                    .sled_ensure_zone_crucible(*sled_id, pool_name.clone())
                    .unwrap();
            }
        }

        let blueprint2 = builder.build();
        verify_blueprint(&blueprint2);
        let diff = blueprint1.diff_sleds(&blueprint2);
        println!(
            "initial blueprint -> next blueprint (expected no changes):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);

        // The next step is adding these zones to a new sled.
        let new_sled_id = example.sled_rng.next_uuid();
        let _ =
            example.system.sled(SledBuilder::new().id(new_sled_id)).unwrap();
        let policy = example.system.to_policy().unwrap();
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint2,
            Generation::new(),
            Generation::new(),
            &policy,
            "test_basic",
        )
        .expect("failed to create builder");
        builder.sled_ensure_zone_ntp(new_sled_id).unwrap();
        let new_sled_resources = policy.sleds.get(&new_sled_id).unwrap();
        for pool_name in &new_sled_resources.zpools {
            builder
                .sled_ensure_zone_crucible(new_sled_id, pool_name.clone())
                .unwrap();
        }

        let blueprint3 = builder.build();
        verify_blueprint(&blueprint3);
        let diff = blueprint2.diff_sleds(&blueprint3);
        println!("expecting new NTP and Crucible zones:\n{}", diff.display());

        // No sleds were changed or removed.
        assert_eq!(diff.sleds_changed().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);

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
                .contains(z.config.underlay_address));
        }

        // Check for an NTP zone.  Its sockaddr's IP should also be on the
        // sled's subnet.
        assert!(new_sled_zones.zones.iter().any(|z| {
            if let OmicronZoneType::InternalNtp { address, .. } =
                &z.config.zone_type
            {
                let sockaddr = address.parse::<SocketAddrV6>().unwrap();
                assert!(new_sled_resources
                    .subnet
                    .net()
                    .contains(*sockaddr.ip()));
                true
            } else {
                false
            }
        }));
        let crucible_pool_names = new_sled_zones
            .zones
            .iter()
            .filter_map(|z| {
                if let OmicronZoneType::Crucible { address, dataset } =
                    &z.config.zone_type
                {
                    let sockaddr = address.parse::<SocketAddrV6>().unwrap();
                    let ip = sockaddr.ip();
                    assert!(new_sled_resources.subnet.net().contains(*ip));
                    Some(dataset.pool_name.clone())
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(crucible_pool_names, new_sled_resources.zpools);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_add_nexus_with_no_existing_nexus_zones() {
        static TEST_NAME: &str =
            "blueprint_builder_test_add_nexus_with_no_existing_nexus_zones";
        let logctx = test_setup_log(TEST_NAME);
        let (mut collection, policy) =
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
            &policy,
            "test",
            TEST_NAME,
        )
        .expect("failed to create initial blueprint");

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            internal_dns_version,
            external_dns_version,
            &policy,
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
        let (mut collection, policy) =
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
            Generation::new(),
            Generation::new(),
            &policy,
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
                internal_dns_version,
                external_dns_version,
                &policy,
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
                internal_dns_version,
                external_dns_version,
                &policy,
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
            let mut policy = policy.clone();
            let mut used_ip_ranges = Vec::new();
            for (_, z) in parent.all_omicron_zones() {
                if let Some(ip) = z
                    .zone_type
                    .external_ip()
                    .expect("failed to check for external IP")
                {
                    used_ip_ranges.push(IpRange::from(ip));
                }
            }
            assert!(!used_ip_ranges.is_empty());
            policy.service_ip_pool_ranges = used_ip_ranges;

            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &parent,
                internal_dns_version,
                external_dns_version,
                &policy,
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
        let (mut collection, policy) =
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
            &policy,
            "test",
            TEST_NAME,
        )
        .unwrap();

        match BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            Generation::new(),
            Generation::new(),
            &policy,
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
        let (mut collection, policy) =
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
            &policy,
            "test",
            TEST_NAME,
        )
        .unwrap();

        match BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            Generation::new(),
            Generation::new(),
            &policy,
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
        let (mut collection, policy) =
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
            &policy,
            "test",
            TEST_NAME,
        )
        .unwrap();

        match BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            Generation::new(),
            Generation::new(),
            &policy,
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

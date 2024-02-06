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
use nexus_inventory::now_db_precision;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::NetworkInterface;
use nexus_types::deployment::NetworkInterfaceKind;
use nexus_types::deployment::OmicronZoneConfig;
use nexus_types::deployment::OmicronZoneDataset;
use nexus_types::deployment::OmicronZoneType;
use nexus_types::deployment::OmicronZonesConfig;
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
use omicron_common::nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
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
    /// previous blueprint, on which this one will be based
    parent_blueprint: &'a Blueprint,

    // These fields are used to allocate resources from sleds.
    policy: &'a Policy,
    sled_ip_allocators: BTreeMap<Uuid, IpAllocator>,

    // These fields will become part of the final blueprint.  See the
    // corresponding fields in `Blueprint`.
    omicron_zones: BTreeMap<Uuid, OmicronZonesConfig>,
    zones_in_service: BTreeSet<Uuid>,
    creator: String,
    comments: Vec<String>,

    // These fields mirror how RSS chooses addresses for zone NICs.
    nexus_v4_ips: Box<dyn Iterator<Item = Ipv4Addr> + Send>,
    nexus_v6_ips: Box<dyn Iterator<Item = Ipv6Addr> + Send>,

    // Iterator of available external IPs for service zones
    available_external_ips: Box<dyn Iterator<Item = IpAddr> + Send + 'a>,

    // Iterator of available MAC addresses in the system address range
    available_system_macs: Box<dyn Iterator<Item = MacAddr>>,
}

impl<'a> BlueprintBuilder<'a> {
    /// Directly construct a `Blueprint` from the contents of a particular
    /// collection (representing no changes from the collection state)
    pub fn build_initial_from_collection(
        collection: &'a Collection,
        policy: &'a Policy,
        creator: &str,
    ) -> Result<Blueprint, Error> {
        let omicron_zones = policy
            .sleds
            .keys()
            .map(|sled_id| {
                let mut zones = collection
                    .omicron_zones
                    .get(sled_id)
                    .map(|z| z.zones.clone())
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

                // This is not strictly necessary.  But for testing, it's
                // helpful for things to be in sorted order.
                zones.zones.sort_by_key(|zone| zone.id);

                Ok((*sled_id, zones))
            })
            .collect::<Result<_, Error>>()?;
        let zones_in_service =
            collection.all_omicron_zones().map(|z| z.id).collect();
        Ok(Blueprint {
            id: Uuid::new_v4(),
            omicron_zones,
            zones_in_service,
            parent_blueprint_id: None,
            time_created: now_db_precision(),
            creator: creator.to_owned(),
            comment: format!("from collection {}", collection.id),
        })
    }

    /// Construct a new `BlueprintBuilder` based on a previous blueprint,
    /// starting with no changes from that state
    pub fn new_based_on(
        parent_blueprint: &'a Blueprint,
        policy: &'a Policy,
        creator: &str,
    ) -> anyhow::Result<BlueprintBuilder<'a>> {
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
            if let OmicronZoneType::Nexus { nic, .. } = &z.zone_type {
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
            if let Some(external_ip) = z.zone_type.external_ip()? {
                if !used_external_ips.insert(external_ip) {
                    bail!("duplicate external IP: {external_ip}");
                }
            }
            if let Some(nic) = z.zone_type.service_vnic() {
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
            parent_blueprint,
            policy,
            sled_ip_allocators: BTreeMap::new(),
            omicron_zones: BTreeMap::new(),
            zones_in_service: parent_blueprint.zones_in_service.clone(),
            creator: creator.to_owned(),
            comments: Vec::new(),
            nexus_v4_ips,
            nexus_v6_ips,
            available_external_ips,
            available_system_macs,
        })
    }

    /// Assemble a final [`Blueprint`] based on the contents of the builder
    pub fn build(mut self) -> Blueprint {
        // Collect the Omicron zones config for each in-service sled.
        let omicron_zones = self
            .policy
            .sleds
            .keys()
            .map(|sled_id| {
                // Start with self.omicron_zones, which contains entries for any
                // sled whose zones config is changing in this blueprint.
                let mut zones = self
                    .omicron_zones
                    .remove(sled_id)
                    // If it's not there, use the config from the parent
                    // blueprint.
                    .or_else(|| {
                        self.parent_blueprint
                            .omicron_zones
                            .get(sled_id)
                            .cloned()
                    })
                    // If it's not there either, then this must be a new sled
                    // and we haven't added any zones to it yet.  Use the
                    // standard initial config.
                    .unwrap_or_else(|| OmicronZonesConfig {
                        generation: Generation::new(),
                        zones: vec![],
                    });

                // This is not strictly necessary.  But for testing, it's
                // helpful for things to be in sorted order.
                zones.zones.sort_by_key(|zone| zone.id);

                (*sled_id, zones)
            })
            .collect();
        Blueprint {
            id: Uuid::new_v4(),
            omicron_zones,
            zones_in_service: self.zones_in_service,
            parent_blueprint_id: Some(self.parent_blueprint.id),
            time_created: now_db_precision(),
            creator: self.creator,
            comment: self.comments.join(", "),
        }
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
            .parent_blueprint
            .omicron_zones
            .get(&sled_id)
            .map(|found_zones| {
                found_zones.zones.iter().any(|z| z.zone_type.is_ntp())
            })
            .unwrap_or(false);
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
            id: Uuid::new_v4(),
            underlay_address: ip,
            zone_type: OmicronZoneType::InternalNtp {
                address: ntp_address.to_string(),
                ntp_servers,
                dns_servers,
                domain: None,
            },
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
        let has_crucible_on_this_pool = self
            .parent_blueprint
            .omicron_zones
            .get(&sled_id)
            .map(|found_zones| {
                found_zones.zones.iter().any(|z| {
                    matches!(
                        &z.zone_type,
                        OmicronZoneType::Crucible { dataset, .. }
                        if dataset.pool_name == pool_name
                    )
                })
            })
            .unwrap_or(false);
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
            id: Uuid::new_v4(),
            underlay_address: ip,
            zone_type: OmicronZoneType::Crucible {
                address,
                dataset: OmicronZoneDataset { pool_name },
            },
        };
        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    /// Return the number of Nexus zones that would be configured to run on the
    /// given sled if this builder generated a blueprint
    ///
    /// This value may change before a blueprint is actually generated if
    /// further changes are made to the builder.
    pub fn sled_num_nexus_zones(&self, sled_id: Uuid) -> Result<usize, Error> {
        // Find the current config for this sled.
        //
        // Start with self.omicron_zones, which contains entries for any
        // sled whose zones config is changing in this blueprint.
        let Some(sled_config) =
            self.omicron_zones.get(&sled_id).or_else(|| {
                // If it's not there, use the config from the parent
                // blueprint.
                self.parent_blueprint.omicron_zones.get(&sled_id)
            })
        else {
            return Ok(0);
        };

        Ok(sled_config.zones.iter().filter(|z| z.zone_type.is_nexus()).count())
    }

    pub fn sled_ensure_zone_multiple_nexus(
        &mut self,
        sled_id: Uuid,
        desired_zone_count: usize,
    ) -> Result<EnsureMultiple, Error> {
        // How many Nexus zones are already running on this sled?
        let nexus_count = self
            .parent_blueprint
            .omicron_zones
            .get(&sled_id)
            .map(|found_zones| {
                found_zones
                    .zones
                    .iter()
                    .filter(|z| z.zone_type.is_nexus())
                    .count()
            })
            .unwrap_or(0);

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
            .omicron_zones
            .values()
            .find_map(|sled_zones| {
                sled_zones.zones.iter().find_map(|z| match &z.zone_type {
                    OmicronZoneType::Nexus {
                        external_tls,
                        external_dns_servers,
                        ..
                    } => Some((*external_tls, external_dns_servers.clone())),
                    _ => None,
                })
            })
            .ok_or(Error::NoNexusZonesInParentBlueprint)?;

        for _ in 0..num_nexus_to_add {
            let nexus_id = Uuid::new_v4();
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
                        IpNet::from(*NEXUS_OPTE_IPV4_SUBNET).into(),
                    ),
                    IpAddr::V6(_) => (
                        self.nexus_v6_ips
                            .next()
                            .ok_or(Error::ExhaustedNexusIps)?
                            .into(),
                        IpNet::from(*NEXUS_OPTE_IPV6_SUBNET).into(),
                    ),
                };
                let mac = self
                    .available_system_macs
                    .next()
                    .ok_or(Error::NoSystemMacAddressAvailable)?;
                NetworkInterface {
                    id: Uuid::new_v4(),
                    kind: NetworkInterfaceKind::Service(nexus_id),
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
            self.sled_add_zone(sled_id, zone)?;
        }

        Ok(EnsureMultiple::Added(num_nexus_to_add))
    }

    fn sled_add_zone(
        &mut self,
        sled_id: Uuid,
        zone: OmicronZoneConfig,
    ) -> Result<(), Error> {
        // Check the sled id and return an appropriate error if it's invalid.
        let _ = self.sled_resources(sled_id)?;

        if !self.zones_in_service.insert(zone.id) {
            return Err(Error::Planner(anyhow!(
                "attempted to add zone that already exists: {}",
                zone.id
            )));
        }

        let sled_zones =
            self.omicron_zones.entry(sled_id).or_insert_with(|| {
                if let Some(old_sled_zones) =
                    self.parent_blueprint.omicron_zones.get(&sled_id)
                {
                    OmicronZonesConfig {
                        generation: old_sled_zones.generation.next(),
                        zones: old_sled_zones.zones.clone(),
                    }
                } else {
                    // The first generation is reserved to mean the one
                    // containing no zones.  See
                    // OMICRON_ZONES_CONFIG_INITIAL_GENERATION.  So we start
                    // with the next one.
                    OmicronZonesConfig {
                        generation: Generation::new().next(),
                        zones: vec![],
                    }
                }
            });

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
                if let Some(sled_zones) = self.omicron_zones.get(&sled_id) {
                    for z in &sled_zones.zones {
                        allocator.reserve(z.underlay_address);
                    }
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

#[cfg(test)]
pub mod test {
    use super::*;
    use nexus_types::external_api::views::SledProvisionState;
    use omicron_common::address::IpRange;
    use omicron_common::address::Ipv4Range;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::address::SLED_PREFIX;
    use omicron_common::api::external::ByteCount;
    use sled_agent_client::types::{
        Baseboard, Inventory, OmicronZoneConfig, OmicronZoneDataset,
        OmicronZoneType, OmicronZonesConfig, SledRole,
    };
    use std::str::FromStr;

    /// Returns a collection and policy describing a pretty simple system
    pub fn example() -> (Collection, Policy) {
        let mut builder = nexus_inventory::CollectionBuilder::new("test-suite");

        let sled_ids = [
            "72443b6c-b8bb-4ffa-ab3a-aeaa428ed79b",
            "a5f3db3a-61aa-4f90-ad3e-02833c253bf5",
            "0d168386-2551-44e8-98dd-ae7a7570f8a0",
        ];
        let mut policy = Policy {
            sleds: BTreeMap::new(),
            // IPs from TEST-NET-1 (RFC 5737)
            service_ip_pool_ranges: vec![Ipv4Range::new(
                "192.0.2.2".parse().unwrap(),
                "192.0.2.20".parse().unwrap(),
            )
            .unwrap()
            .into()],
            target_nexus_zone_count: 3,
        };
        let mut service_ip_pool_range = policy.service_ip_pool_ranges[0].iter();
        let mut nexus_nic_ips = NEXUS_OPTE_IPV4_SUBNET
            .iter()
            .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES);
        let mut nexus_nic_macs = {
            let mut used = HashSet::new();
            std::iter::from_fn(move || {
                let mut mac = MacAddr::random_system();
                while !used.insert(mac) {
                    mac = MacAddr::random_system();
                }
                Some(mac)
            })
        };

        for sled_id_str in sled_ids.iter() {
            let sled_id: Uuid = sled_id_str.parse().unwrap();
            let sled_ip = policy_add_sled(&mut policy, sled_id);
            let serial_number = format!("s{}", policy.sleds.len());
            builder
                .found_sled_inventory(
                    "test-suite",
                    Inventory {
                        baseboard: Baseboard::Gimlet {
                            identifier: serial_number,
                            model: String::from("model1"),
                            revision: 0,
                        },
                        reservoir_size: ByteCount::from(1024),
                        sled_role: SledRole::Gimlet,
                        sled_agent_address: SocketAddrV6::new(
                            sled_ip, 12345, 0, 0,
                        )
                        .to_string(),
                        sled_id,
                        usable_hardware_threads: 10,
                        usable_physical_ram: ByteCount::from(1024 * 1024),
                    },
                )
                .unwrap();

            let zpools = &policy.sleds.get(&sled_id).unwrap().zpools;
            let mut sled_ips =
                std::iter::successors(Some(sled_ip.saturating_add(1)), |ip| {
                    Some(ip.saturating_add(1))
                });
            let zones: Vec<_> = std::iter::once({
                let ip = sled_ips.next().unwrap();
                OmicronZoneConfig {
                    id: Uuid::new_v4(),
                    underlay_address: ip,
                    zone_type: OmicronZoneType::InternalNtp {
                        address: SocketAddrV6::new(ip, 12345, 0, 0).to_string(),
                        dns_servers: vec![],
                        domain: None,
                        ntp_servers: vec![],
                    },
                }
            })
            .chain(std::iter::once({
                let id = Uuid::new_v4();
                let ip = sled_ips.next().unwrap();
                let external_ip =
                    service_ip_pool_range.next().expect("no service IPs left");
                let nic_ip =
                    nexus_nic_ips.next().expect("no nexus nic IPs left");
                OmicronZoneConfig {
                    id,
                    underlay_address: ip,
                    zone_type: OmicronZoneType::Nexus {
                        internal_address: SocketAddrV6::new(ip, 12346, 0, 0)
                            .to_string(),
                        external_ip,
                        nic: NetworkInterface {
                            id: Uuid::new_v4(),
                            kind: NetworkInterfaceKind::Service(id),
                            name: format!("nexus-{id}").parse().unwrap(),
                            ip: nic_ip.into(),
                            mac: nexus_nic_macs
                                .next()
                                .expect("no nexus nic MACs left"),
                            subnet: IpNet::from(*NEXUS_OPTE_IPV4_SUBNET).into(),
                            vni: Vni::SERVICES_VNI,
                            primary: true,
                            slot: 0,
                        },
                        external_tls: false,
                        external_dns_servers: Vec::new(),
                    },
                }
            }))
            .chain(zpools.iter().enumerate().map(|(i, zpool_name)| {
                let ip = sled_ip.saturating_add(u128::try_from(i + 2).unwrap());
                OmicronZoneConfig {
                    id: Uuid::new_v4(),
                    underlay_address: ip,
                    zone_type: OmicronZoneType::Crucible {
                        address: String::from("[::1]:12345"),
                        dataset: OmicronZoneDataset {
                            pool_name: zpool_name.clone(),
                        },
                    },
                }
            }))
            .collect();

            builder
                .found_sled_omicron_zones(
                    "test-suite",
                    sled_id,
                    OmicronZonesConfig {
                        generation: Generation::new().next(),
                        zones,
                    },
                )
                .unwrap();
        }

        let collection = builder.build();

        (collection, policy)
    }

    pub fn policy_add_sled(policy: &mut Policy, sled_id: Uuid) -> Ipv6Addr {
        let i = policy.sleds.len() + 1;
        let sled_ip: Ipv6Addr =
            format!("fd00:1122:3344:{}::1", i + 1).parse().unwrap();

        let zpools: BTreeSet<ZpoolName> = [
            "oxp_be776cf5-4cba-4b7d-8109-3dfd020f22ee",
            "oxp_aee23a17-b2ce-43f2-9302-c738d92cca28",
            "oxp_f7940a6b-c865-41cf-ad61-1b831d594286",
        ]
        .iter()
        .map(|name_str| {
            ZpoolName::from_str(name_str).expect("not a valid zpool name")
        })
        .collect();

        let subnet = Ipv6Subnet::<SLED_PREFIX>::new(sled_ip);
        policy.sleds.insert(
            sled_id,
            SledResources {
                provision_state: SledProvisionState::Provisionable,
                zpools,
                subnet,
            },
        );
        sled_ip
    }

    #[test]
    fn test_initial() {
        // Test creating a blueprint from a collection and verifying that it
        // describes no changes.
        let (collection, policy) = example();
        let blueprint_initial =
            BlueprintBuilder::build_initial_from_collection(
                &collection,
                &policy,
                "the_test",
            )
            .expect("failed to create initial blueprint");

        // Since collections don't include what was in service, we have to
        // provide that ourselves.  For our purposes though we don't care.
        let zones_in_service = blueprint_initial.zones_in_service.clone();
        let diff = blueprint_initial
            .diff_from_collection(&collection, &zones_in_service);
        println!(
            "collection -> initial blueprint (expected no changes):\n{}",
            diff
        );
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);

        // Test a no-op blueprint.
        let builder = BlueprintBuilder::new_based_on(
            &blueprint_initial,
            &policy,
            "test_basic",
        )
        .expect("failed to create builder");
        let blueprint = builder.build();
        let diff = blueprint_initial.diff(&blueprint);
        println!(
            "initial blueprint -> next blueprint (expected no changes):\n{}",
            diff
        );
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);
    }

    #[test]
    fn test_basic() {
        let (collection, mut policy) = example();
        let blueprint1 = BlueprintBuilder::build_initial_from_collection(
            &collection,
            &policy,
            "the_test",
        )
        .expect("failed to create initial blueprint");

        let mut builder =
            BlueprintBuilder::new_based_on(&blueprint1, &policy, "test_basic")
                .expect("failed to create builder");

        // The initial blueprint should have internal NTP zones on all the
        // existing sleds, plus Crucible zones on all pools.  So if we ensure
        // all these zones exist, we should see no change.
        for (sled_id, sled_resources) in &policy.sleds {
            builder.sled_ensure_zone_ntp(*sled_id).unwrap();
            for pool_name in &sled_resources.zpools {
                builder
                    .sled_ensure_zone_crucible(*sled_id, pool_name.clone())
                    .unwrap();
            }
        }

        let blueprint2 = builder.build();
        let diff = blueprint1.diff(&blueprint2);
        println!(
            "initial blueprint -> next blueprint (expected no changes):\n{}",
            diff
        );
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);

        // The next step is adding these zones to a new sled.
        let new_sled_id = Uuid::new_v4();
        let _ = policy_add_sled(&mut policy, new_sled_id);
        let mut builder =
            BlueprintBuilder::new_based_on(&blueprint2, &policy, "test_basic")
                .expect("failed to create builder");
        builder.sled_ensure_zone_ntp(new_sled_id).unwrap();
        let new_sled_resources = policy.sleds.get(&new_sled_id).unwrap();
        for pool_name in &new_sled_resources.zpools {
            builder
                .sled_ensure_zone_crucible(new_sled_id, pool_name.clone())
                .unwrap();
        }

        let blueprint3 = builder.build();
        let diff = blueprint2.diff(&blueprint3);
        println!("expecting new NTP and Crucible zones:\n{}", diff);

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
                .contains(z.underlay_address));
        }

        // Check for an NTP zone.  Its sockaddr's IP should also be on the
        // sled's subnet.
        assert!(new_sled_zones.zones.iter().any(|z| {
            if let OmicronZoneType::InternalNtp { address, .. } = &z.zone_type {
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
                    &z.zone_type
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
    }

    #[test]
    fn test_add_nexus_with_no_existing_nexus_zones() {
        let (mut collection, policy) = example();

        // Adding a new Nexus zone currently requires copying settings from an
        // existing Nexus zone. If we remove all Nexus zones from the
        // collection, create a blueprint, then try to add a Nexus zone, it
        // should fail.
        for zones in collection.omicron_zones.values_mut() {
            zones.zones.zones.retain(|z| {
                !matches!(z.zone_type, OmicronZoneType::Nexus { .. })
            });
        }

        let parent = BlueprintBuilder::build_initial_from_collection(
            &collection,
            &policy,
            "test",
        )
        .expect("failed to create initial blueprint");

        let mut builder =
            BlueprintBuilder::new_based_on(&parent, &policy, "test")
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
    }

    #[test]
    fn test_add_nexus_error_cases() {
        let (mut collection, policy) = example();

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

        let parent = BlueprintBuilder::build_initial_from_collection(
            &collection,
            &policy,
            "test",
        )
        .expect("failed to create initial blueprint");

        {
            // Attempting to add Nexus to the sled we removed it from (with no
            // other changes to the environment) should succeed.
            let mut builder =
                BlueprintBuilder::new_based_on(&parent, &policy, "test")
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
            let mut builder =
                BlueprintBuilder::new_based_on(&parent, &policy, "test")
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

            let mut builder =
                BlueprintBuilder::new_based_on(&parent, &policy, "test")
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
    }

    #[test]
    fn test_invalid_parent_blueprint_two_zones_with_same_external_ip() {
        let (mut collection, policy) = example();

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

        let parent = BlueprintBuilder::build_initial_from_collection(
            &collection,
            &policy,
            "test",
        )
        .unwrap();

        match BlueprintBuilder::new_based_on(&parent, &policy, "test") {
            Ok(_) => panic!("unexpected success"),
            Err(err) => assert!(
                err.to_string().contains("duplicate external IP"),
                "unexpected error: {err:#}"
            ),
        };
    }

    #[test]
    fn test_invalid_parent_blueprint_two_nexus_zones_with_same_nic_ip() {
        let (mut collection, policy) = example();

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

        let parent = BlueprintBuilder::build_initial_from_collection(
            &collection,
            &policy,
            "test",
        )
        .unwrap();

        match BlueprintBuilder::new_based_on(&parent, &policy, "test") {
            Ok(_) => panic!("unexpected success"),
            Err(err) => assert!(
                err.to_string().contains("duplicate Nexus NIC IP"),
                "unexpected error: {err:#}"
            ),
        };
    }

    #[test]
    fn test_invalid_parent_blueprint_two_zones_with_same_vnic_mac() {
        let (mut collection, policy) = example();

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

        let parent = BlueprintBuilder::build_initial_from_collection(
            &collection,
            &policy,
            "test",
        )
        .unwrap();

        match BlueprintBuilder::new_based_on(&parent, &policy, "test") {
            Ok(_) => panic!("unexpected success"),
            Err(err) => assert!(
                err.to_string().contains("duplicate service vNIC MAC"),
                "unexpected error: {err:#}"
            ),
        };
    }
}

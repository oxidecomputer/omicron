// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities related to generating Blueprints
//!
//! See crate-level documentation for details.

use anyhow::anyhow;
use internal_dns::DNS_ZONE;
use ipnet::IpAdd;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::OmicronZoneConfig;
use nexus_types::deployment::OmicronZoneDataset;
use nexus_types::deployment::OmicronZoneType;
use nexus_types::deployment::OmicronZonesConfig;
use nexus_types::deployment::ZpoolName;
use nexus_types::inventory::Collection;
use omicron_common::address::get_sled_address;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::ReservedRackSubnet;
use omicron_common::address::AZ_PREFIX;
use omicron_common::address::DNS_REDUNDANCY;
use omicron_common::address::NTP_PORT;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Generation;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum Error {
    #[error("ran out of available addresses for sled")]
    OutOfAddresses,
    #[error("programming error in planner")]
    Planner(#[from] anyhow::Error),
}

pub struct BlueprintBuilder<'a> {
    /// underlying collection on which this blueprint is based
    collection: &'a Collection,

    // These fields are used to allocate resources from sleds.
    sled_zpools: BTreeMap<Uuid, BTreeSet<ZpoolName>>,
    sled_ip_allocators: BTreeMap<Uuid, IpAllocator>,

    // These fields will become part of the final blueprint.  See the
    // corresponding fields in `Blueprint`.
    sleds: BTreeSet<Uuid>,
    omicron_zones: BTreeMap<Uuid, OmicronZonesConfig>,
    zones_in_service: BTreeSet<Uuid>,
    creator: String,
    reason: String,
}

impl<'a> BlueprintBuilder<'a> {
    /// Construct a new `BlueprintBuilder` based on an inventory collection that
    /// starts with no changes from the that state
    pub fn new_based_on(
        collection: &'a Collection,
        sled_zpools: BTreeMap<Uuid, BTreeSet<ZpoolName>>,
        zones_in_service: BTreeSet<Uuid>,
        creator: &str,
        reason: &str,
    ) -> BlueprintBuilder<'a> {
        let omicron_zones = collection
            .omicron_zones
            .iter()
            .map(|(sled_id, found)| {
                let zones = found.zones.clone();
                (*sled_id, zones)
            })
            .collect();
        // XXX-dap I think this function needs to accept a list of sleds that
        // are known to be part of the system, plus information like what their
        // subnets are.  Then we could use that for "sleds".
        BlueprintBuilder {
            sleds: sled_zpools.keys().copied().collect(),
            collection,
            omicron_zones,
            sled_zpools,
            zones_in_service,
            sled_ip_allocators: BTreeMap::new(),
            creator: creator.to_owned(),
            reason: reason.to_owned(),
        }
    }

    pub fn build(mut self) -> Blueprint {
        // Collect the Omicron zones config for each in-service sled.
        let omicron_zones = self
            .sleds
            .iter()
            .map(|sled_id| {
                // Start with self.omicron_zones, which contains entries for any
                // sled whose zones config is changing in this blueprint.
                let zones = self.omicron_zones
                    .remove(sled_id)
                    // If it's not there, use the config from the collection
                    // we're working with.
                    .or_else(|| {
                        self.collection
                            .omicron_zones
                            .get(sled_id)
                            .map(|z| z.zones.clone())
                    })
                    // If it's not there either, then this must be a new sled
                    // and we haven't added any zones to it yet.  Use the
                    // standard initial config.
                    .unwrap_or_else(|| OmicronZonesConfig {
                        generation: Generation::new(),
                        zones: vec![],
                    });
                (*sled_id, zones)
            })
            .collect();
        Blueprint {
            sleds: self.sleds,
            omicron_zones: omicron_zones,
            zones_in_service: self.zones_in_service,
            built_from_collection: self.collection.id,
            time_created: chrono::Utc::now(),
            creator: self.creator,
            reason: self.reason,
        }
    }

    pub fn sled_ensure(mut self, sled_id: Uuid) -> Self {
        self.sleds.insert(sled_id);
        self
    }

    fn sled_check(&self, sled_id: Uuid) -> Result<(), Error> {
        if !self.sleds.contains(&sled_id) {
            Err(Error::Planner(anyhow!(
                "attempted to use sled that is not in service: {}",
                sled_id
            )))
        } else {
            Ok(())
        }
    }

    // pub fn ensure_sled(
    //     mut self,
    //     sled_id: Uuid,
    //     sled_subnet: Ipv6Subnet<SLED_PREFIX>,
    // ) -> Result<Self, Error> {
    //     // Work around rust-lang/rust-clippy#11935 and related issues.
    //     // Clippy doesn't grok that we can't use
    //     // `entry(&sled_id).or_insert_with(closure)` because `closure` would
    //     // borrow `self`, which we can't do because we already have an immutable
    //     // borrow of `self` by calling `entry()`.  Using `match` on the result
    //     // of `entry` has a similar problem.
    //     #[allow(clippy::map_entry)]
    //     if !self.sled_ip_allocators.contains_key(&sled_id) {
    //         self.sled_ip_allocators
    //             .insert(sled_id, self.sled_ip_allocator(sled_id, sled_subnet));
    //     }

    //     let mut new_zones = Vec::new();

    //     // Every sled needs an NTP zone.  Currently, we assume the boundary NTP
    //     // zones have already been provisioned elsewhere, so this will be an
    //     // internal NTP zone.
    //     let has_ntp = self
    //         .omicron_zones
    //         .get(&sled_id)
    //         .map(|zones_config| {
    //             zones_config.zones.iter().any(|z| {
    //                 matches!(
    //                     z.zone_type,
    //                     OmicronZoneType::BoundaryNtp { .. }
    //                         | OmicronZoneType::InternalNtp { .. }
    //                 )
    //             })
    //         })
    //         .unwrap_or(false);
    //     if !has_ntp {
    //         let sled_addr_allocator =
    //             self.sled_ip_allocators.get_mut(&sled_id).unwrap();
    //         let ip =
    //             sled_addr_allocator.alloc().ok_or(Error::OutOfAddresses)?;
    //         new_zones.push(self.new_ntp_zone(sled_subnet, ip));
    //     }

    //     // On every sled, for every zpool, there should be one Crucible zone.
    //     if let Some(sled_zpools) = self.sled_zpools.get(&sled_id) {
    //         for zpool_name in sled_zpools {
    //             if !self
    //                 .omicron_zones
    //                 .get(&sled_id)
    //                 .map(|zones_config| {
    //                     zones_config.zones.iter().any(|z| {
    //                         matches!(
    //                             &z.zone_type,
    //                             OmicronZoneType::Crucible { dataset, .. }
    //                             if dataset.pool_name == *zpool_name
    //                         )
    //                     })
    //                 })
    //                 .unwrap_or(false)
    //             {
    //                 let sled_addr_allocator =
    //                     self.sled_ip_allocators.get_mut(&sled_id).unwrap();
    //                 let ip = sled_addr_allocator
    //                     .alloc()
    //                     .ok_or(Error::OutOfAddresses)?;
    //                 new_zones
    //                     .push(self.new_crucible_zone(ip, zpool_name.clone()));
    //             }
    //         }
    //     }

    //     // If we wound up adding any zones, update DNS as well as the
    //     // corresponding sled's config.
    //     if !new_zones.is_empty() {
    //         for z in &new_zones {
    //             assert!(
    //                 !self.zones_in_service.contains(&z.id),
    //                 "DNS updated more than once"
    //             );
    //             self.zones_in_service.insert(z.id);
    //         }

    //         let zones_config = self
    //             .omicron_zones
    //             .entry(sled_id)
    //             .or_insert_with(|| OmicronZonesConfig {
    //                 generation: Generation::new(),
    //                 zones: vec![],
    //             });

    //         zones_config.generation = zones_config.generation.next();
    //         zones_config.zones.extend(new_zones);
    //     }

    //     Ok(self)
    // }

    pub fn sled_add_zone(
        mut self,
        sled_id: Uuid,
        zone: OmicronZoneConfig,
    ) -> Result<Self, Error> {
        self.sled_check(sled_id)?;

        if !self.zones_in_service.insert(zone.id) {
            return Err(Error::Planner(anyhow!(
                "attempted to add zone that already exists: {}",
                zone.id
            )));
        }

        let sled_zones =
            self.omicron_zones.entry(sled_id).or_insert_with(|| {
                if let Some(old_sled_zones) =
                    self.collection.omicron_zones.get(&sled_id)
                {
                    OmicronZonesConfig {
                        generation: old_sled_zones.zones.generation.next(),
                        zones: old_sled_zones.zones.zones.clone(),
                    }
                } else {
                    OmicronZonesConfig {
                        generation: Generation::new(),
                        zones: vec![],
                    }
                }
            });

        sled_zones.zones.push(zone);
        Ok(self)
    }

    // XXX-dap we should already have the prefixes somehow.
    pub fn sled_alloc_ip(
        &mut self,
        sled_id: Uuid,
        sled_subnet: Ipv6Subnet<SLED_PREFIX>,
    ) -> Result<Ipv6Addr, Error> {
        // Work around rust-lang/rust-clippy#11935 and related issues.
        // Clippy doesn't grok that we can't use
        // `entry(&sled_id).or_insert_with(closure)` because `closure` would
        // borrow `self`, which we can't do because we already have an immutable
        // borrow of `self` by calling `entry()`.  Using `match` on the result
        // of `entry` has a similar problem.
        #[allow(clippy::map_entry)]
        if !self.sled_ip_allocators.contains_key(&sled_id) {
            self.sled_ip_allocators
                .insert(sled_id, self.sled_ip_allocator(sled_id, sled_subnet));
        }

        let allocator = self.sled_ip_allocators.get_mut(&sled_id).unwrap();
        // XXX-dap more error context
        allocator.alloc().ok_or(Error::OutOfAddresses)
    }

    pub fn new_ntp_zone(
        &self,
        sled_subnet: Ipv6Subnet<SLED_PREFIX>,
        ip: Ipv6Addr,
    ) -> OmicronZoneConfig {
        let ntp_address = SocketAddrV6::new(ip, NTP_PORT, 0, 0);

        // Construct the list of internal DNS servers.
        //
        // It'd be tempting to get this list from the other internal NTP
        // servers but there may not be any of those.  We could also
        // construct this list manually from the set of internal DNS servers
        // actually deployed.  Instead, we take the same approach as RSS:
        // these are at known, fixed addresses relative to the AZ subnet
        // (which itself is a known-prefix parent subnet of the sled subnet).
        // XXX-dap commonize with RSS?
        let dns_servers: Vec<IpAddr> = {
            let az_subnet =
                Ipv6Subnet::<AZ_PREFIX>::new(sled_subnet.net().network());
            let reserved_rack_subnet = ReservedRackSubnet::new(az_subnet);
            let dns_subnets =
                &reserved_rack_subnet.get_dns_subnets()[0..DNS_REDUNDANCY];
            dns_subnets
                .iter()
                .map(|dns_subnet| IpAddr::from(dns_subnet.dns_address().ip()))
                .collect()
        };

        // The list of boundary NTP servers is not necessarily stored
        // anywhere (unless there happens to be another internal NTP zone
        // lying around).  Recompute it based on what boundary servers
        // currently exist.
        // XXX-dap is this right?  how would we keep this in sync if we
        // wanted to put a new boundary service somewhere else?  Shouldn't
        // we instead have a DNS name that resolves to all the boundary
        // NTP servers?
        let ntp_servers = self
            .all_existing_zones()
            .filter_map(|z| {
                if matches!(z.zone_type, OmicronZoneType::BoundaryNtp { .. }) {
                    Some(format!("{}.host.{}", z.id, DNS_ZONE))
                } else {
                    None
                }
            })
            .collect();
        OmicronZoneConfig {
            id: Uuid::new_v4(),
            underlay_address: ip,
            zone_type: OmicronZoneType::InternalNtp {
                address: ntp_address.to_string(),
                ntp_servers,
                dns_servers,
                domain: None,
            },
        }
    }

    pub fn new_crucible_zone(
        &self,
        ip: Ipv6Addr,
        pool_name: ZpoolName,
    ) -> OmicronZoneConfig {
        let port = omicron_common::address::CRUCIBLE_PORT;
        let address = SocketAddrV6::new(ip, port, 0, 0).to_string();
        OmicronZoneConfig {
            id: Uuid::new_v4(),
            underlay_address: ip,
            zone_type: OmicronZoneType::Crucible {
                address,
                dataset: OmicronZoneDataset { pool_name },
            },
        }
    }

    /// Returns an object for allocating IP addresses for the given sled
    ///
    /// This should not be used unless the caller has already checked
    /// `self.sled_ip_allocators`.  We'd do that ourselves except that runs
    /// afoul of the borrowing rules.
    /// XXX-dap once I figure that out, revisit that ^
    fn sled_ip_allocator(
        &self,
        sled_id: Uuid,
        sled_subnet: Ipv6Subnet<SLED_PREFIX>,
    ) -> IpAllocator {
        // XXX-dap this should be a constant in "addresses"
        let control_plane_subnet: ipnetwork::Ipv6Network =
            ipnetwork::Ipv6Network::new(
                sled_subnet.net().network(),
                SLED_PREFIX + 16,
            )
            .expect("control plane prefix was too large");

        let mut allocator = IpAllocator::new(control_plane_subnet);

        // XXX-dap consider replacing the sled address and switch zone
        // address reservations here with a single reserved number of
        // addresses.

        // Record the sled's address itself as allocated.
        allocator.reserve(*get_sled_address(sled_subnet).ip());

        // Record the switch zone's address as allocated.  We do this
        // regardless of whether a sled is currently attached to a switch.
        allocator.reserve(get_switch_zone_address(sled_subnet));

        // Record each of the sled's zones' underlay addresses as allocated.
        if let Some(sled_zones) = self.omicron_zones.get(&sled_id) {
            for z in &sled_zones.zones {
                allocator.reserve(z.underlay_address);
            }
        }

        allocator
    }

    /// Iterate over all the Omicron zones in the original collection from which
    /// this blueprint was based
    fn all_existing_zones(&self) -> impl Iterator<Item = &OmicronZoneConfig> {
        self.collection
            .omicron_zones
            .values()
            .flat_map(|z| z.zones.zones.iter())
    }
}

/// Very simple allocator for picking addresses from a sled's subnet
///
/// The current implementation takes the max address seen so far and uses the
/// next one.  This will never reuse old IPs.  That avoids a bunch of
/// operational issues.  It does mean we will eventually run out of IPs.  But we
/// do have a big space right now (2^16).
// XXX-dap This will be general enough to use in RSS, but the one in RSS is not
// general enough to use here.
struct IpAllocator {
    pub subnet: ipnetwork::Ipv6Network,
    pub max_seen: Ipv6Addr,
}

impl IpAllocator {
    pub fn new(subnet: ipnetwork::Ipv6Network) -> IpAllocator {
        IpAllocator {
            subnet,
            max_seen: subnet
                .iter()
                .next()
                .expect("expected at least one address in the subnet"),
        }
    }

    pub fn reserve(&mut self, addr: Ipv6Addr) {
        assert!(
            self.subnet.contains(addr),
            "attempted to reserve IP {} which is outside \
            the current subnet {:?}",
            addr,
            self.subnet
        );
        if addr > self.max_seen {
            self.max_seen = addr;
        }
    }

    pub fn alloc(&mut self) -> Option<Ipv6Addr> {
        let next = self.max_seen.saturating_add(1);
        if next == self.max_seen {
            // We ran out of the entire IPv6 address space.
            return None;
        }

        if !self.subnet.contains(next) {
            // We ran out of this subnet.
            return None;
        }

        self.max_seen = next;
        Some(next)
    }
}

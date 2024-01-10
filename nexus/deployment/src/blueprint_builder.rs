// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level facility for generating Blueprints

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

// XXX-dap TODO-doc
#[derive(Debug, Error)]
pub enum Error {
    #[error("ran out of available addresses for sled")]
    OutOfAddresses,
    #[error("programming error in planner")]
    Planner(#[from] anyhow::Error),
}

// XXX-dap TODO-doc
pub struct SledInfo {
    pub zpools: BTreeSet<ZpoolName>,
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

// XXX-dap TODO-doc
pub struct BlueprintBuilder<'a> {
    /// previous blueprint, on which this one will be based
    parent_blueprint: &'a Blueprint,

    // These fields are used to allocate resources from sleds.
    sleds: &'a BTreeMap<Uuid, SledInfo>,
    sled_ip_allocators: BTreeMap<Uuid, IpAllocator>,

    // These fields will become part of the final blueprint.  See the
    // corresponding fields in `Blueprint`.
    omicron_zones: BTreeMap<Uuid, OmicronZonesConfig>,
    zones_in_service: BTreeSet<Uuid>,
    creator: String,
    reason: String,
}

impl<'a> BlueprintBuilder<'a> {
    /// Directly construct a `Blueprint` from the contents of a particular
    /// collection (representing no changes from the collection state)
    pub fn build_initial_from_collection(
        collection: &'a Collection,
        sleds: &'a BTreeMap<Uuid, SledInfo>,
        creator: &str,
        reason: &str,
    ) -> Result<Blueprint, Error> {
        let omicron_zones = sleds
            .keys()
            .map(|sled_id| {
                let zones = collection
                    .omicron_zones
                    .get(sled_id)
                    .map(|z| z.zones.clone())
                    .ok_or_else(|| {
                        // We should not find a sled that's supposed to be
                        // in-service but is not part of the inventory.  It's
                        // not that that can't ever happen.  But the initial
                        // blueprint is supposed to reflect the current state
                        // and this is more likely an "add sled" case or a case
                        // where there was an inventory collection error that we
                        // want to deal with before proceeding.
                        // XXX-dap re-evaluate this.  Are we sure this would
                        // never happen in a real deployment?  Would it be bad
                        // to just trust whatever's in the inventory?  I think
                        // so because if the inventory was somehow incomplete
                        // then we'd leave out a sled erroneously.
                        Error::Planner(anyhow!(
                            "building initial blueprint: sled {:?} is \
                            supposed to be in service but has no zones \
                            in inventory",
                            sled_id
                        ))
                    })?;
                Ok((*sled_id, zones))
            })
            .collect::<Result<_, Error>>()?;
        let zones_in_service =
            collection.all_omicron_zones().map(|z| z.id).collect();
        Ok(Blueprint {
            id: Uuid::new_v4(),
            sleds: sleds.keys().copied().collect(),
            omicron_zones: omicron_zones,
            zones_in_service,
            parent_blueprint_id: None,
            time_created: chrono::Utc::now(),
            creator: creator.to_owned(),
            reason: reason.to_owned(),
        })
    }

    /// Construct a new `BlueprintBuilder` based on a previous blueprint,
    /// starting with no changes from that state
    pub fn new_based_on(
        parent_blueprint: &'a Blueprint,
        sleds: &'a BTreeMap<Uuid, SledInfo>,
        creator: &str,
        reason: &str,
    ) -> BlueprintBuilder<'a> {
        BlueprintBuilder {
            parent_blueprint,
            sleds,
            sled_ip_allocators: BTreeMap::new(),
            omicron_zones: BTreeMap::new(),
            zones_in_service: parent_blueprint.zones_in_service.clone(),
            creator: creator.to_owned(),
            reason: reason.to_owned(),
        }
    }

    pub fn build(mut self) -> Blueprint {
        // Collect the Omicron zones config for each in-service sled.
        let omicron_zones = self
            .sleds
            .keys()
            .map(|sled_id| {
                // Start with self.omicron_zones, which contains entries for any
                // sled whose zones config is changing in this blueprint.
                let zones = self
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
                (*sled_id, zones)
            })
            .collect();
        Blueprint {
            id: Uuid::new_v4(),
            sleds: self.sleds.keys().copied().collect(),
            omicron_zones: omicron_zones,
            zones_in_service: self.zones_in_service,
            parent_blueprint_id: Some(self.parent_blueprint.id),
            time_created: chrono::Utc::now(),
            creator: self.creator,
            reason: self.reason,
        }
    }

    pub fn sled_add_zone_internal_ntp(
        &mut self,
        sled_id: Uuid,
    ) -> Result<(), Error> {
        let sled_info = self.sled_info(sled_id)?;
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
        let ntp_servers = self
            .parent_blueprint
            .all_omicron_zones()
            .filter_map(|z| {
                if matches!(z.zone_type, OmicronZoneType::BoundaryNtp { .. }) {
                    Some(format!("{}.host.{}", z.id, DNS_ZONE))
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

        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_crucible(
        &mut self,
        sled_id: Uuid,
        pool_name: ZpoolName,
    ) -> Result<(), Error> {
        // XXX-dap check that there's not already a crucible zone on this pool
        let sled_info = self.sled_info(sled_id)?;
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
        self.sled_add_zone(sled_id, zone)
    }

    fn sled_add_zone(
        &mut self,
        sled_id: Uuid,
        zone: OmicronZoneConfig,
    ) -> Result<(), Error> {
        let _ = self.sled_info(sled_id)?;

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
                    OmicronZonesConfig {
                        generation: Generation::new(),
                        zones: vec![],
                    }
                }
            });

        sled_zones.zones.push(zone);
        Ok(())
    }

    fn sled_alloc_ip(&mut self, sled_id: Uuid) -> Result<Ipv6Addr, Error> {
        let sled_info = self.sled_info(sled_id)?;

        // Work around rust-lang/rust-clippy#11935 and related issues.
        // Clippy doesn't grok that we can't use
        // `entry(&sled_id).or_insert_with(closure)` because `closure` would
        // borrow `self`, which we can't do because we already have an immutable
        // borrow of `self` by calling `entry()`.  Using `match` on the result
        // of `entry` has a similar problem.
        #[allow(clippy::map_entry)]
        if !self.sled_ip_allocators.contains_key(&sled_id) {
            let sled_subnet = sled_info.subnet;
            let sled_control_plane_subnet: ipnetwork::Ipv6Network =
                ipnetwork::Ipv6Network::new(
                    sled_subnet.net().network(),
                    // XXX-dap this should be a constant in "addresses"
                    SLED_PREFIX + 16,
                )
                .expect("control plane prefix was too large");

            let mut allocator = IpAllocator::new(sled_control_plane_subnet);

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

            self.sled_ip_allocators.insert(sled_id, allocator);
        }

        let allocator = self.sled_ip_allocators.get_mut(&sled_id).unwrap();
        // XXX-dap more error context
        allocator.alloc().ok_or(Error::OutOfAddresses)
    }

    fn sled_info(&self, sled_id: Uuid) -> Result<&SledInfo, Error> {
        self.sleds.get(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "attempted to use sled that is not in service: {}",
                sled_id
            ))
        })
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
// XXX-dap move to a separate module
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

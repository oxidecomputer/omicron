// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level facility for generating Blueprints

use crate::ip_allocator::IpAllocator;
use anyhow::anyhow;
use internal_dns::DNS_ZONE;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::OmicronZoneConfig;
use nexus_types::deployment::OmicronZoneDataset;
use nexus_types::deployment::OmicronZoneType;
use nexus_types::deployment::OmicronZonesConfig;
use nexus_types::deployment::Policy;
use nexus_types::deployment::SledResources;
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

/// Errors encountered while assembling blueprints
#[derive(Debug, Error)]
pub enum Error {
    #[error("sled {sled_id}: ran out of available addresses for sled")]
    OutOfAddresses { sled_id: Uuid },
    #[error("programming error in planner")]
    Planner(#[from] anyhow::Error),
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
}

impl<'a> BlueprintBuilder<'a> {
    /// Directly construct a `Blueprint` from the contents of a particular
    /// collection (representing no changes from the collection state)
    pub fn build_initial_from_collection(
        collection: &'a Collection,
        policy: &'a Policy,
        creator: &str,
    ) -> Result<Blueprint, Error> {
        let sleds: BTreeSet<_> = policy.sleds.keys().copied().collect();
        let omicron_zones = sleds
            .iter()
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
            sleds_in_cluster: sleds,
            omicron_zones: omicron_zones,
            zones_in_service,
            parent_blueprint_id: None,
            time_created: chrono::Utc::now(),
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
    ) -> BlueprintBuilder<'a> {
        BlueprintBuilder {
            parent_blueprint,
            policy,
            sled_ip_allocators: BTreeMap::new(),
            omicron_zones: BTreeMap::new(),
            zones_in_service: parent_blueprint.zones_in_service.clone(),
            creator: creator.to_owned(),
            comments: Vec::new(),
        }
    }

    /// Assemble a final [`Blueprint`] based on the contents of the builder
    pub fn build(mut self) -> Blueprint {
        // Collect the Omicron zones config for each in-service sled.
        let sleds: BTreeSet<_> = self.policy.sleds.keys().copied().collect();
        let omicron_zones = sleds
            .iter()
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
            sleds_in_cluster: sleds,
            omicron_zones: omicron_zones,
            zones_in_service: self.zones_in_service,
            parent_blueprint_id: Some(self.parent_blueprint.id),
            time_created: chrono::Utc::now(),
            creator: self.creator,
            comment: self.comments.join(", "),
        }
    }

    /// Sets the blueprints "comment"
    ///
    /// This is a short human-readable string summarizing the changes reflected
    /// in the blueprint.  This is only intended for debugging.
    pub fn comment(&mut self, comment: &str) {
        self.comments.push(comment.to_owned());
    }

    pub fn sled_ensure_zone_internal_ntp(
        &mut self,
        sled_id: Uuid,
    ) -> Result<bool, Error> {
        // If there's already an NTP zone on this sled, do nothing.
        let has_ntp = self
            .parent_blueprint
            .omicron_zones
            .get(&sled_id)
            .map(|found_zones| {
                found_zones.zones.iter().any(|z| {
                    matches!(
                        z.zone_type,
                        OmicronZoneType::BoundaryNtp { .. }
                            | OmicronZoneType::InternalNtp { .. }
                    )
                })
            })
            .unwrap_or(false);
        if has_ntp {
            return Ok(false);
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
            .filter_map(|(_, z)| {
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

        self.sled_add_zone(sled_id, zone)?;
        Ok(true)
    }

    pub fn sled_ensure_zone_crucible(
        &mut self,
        sled_id: Uuid,
        pool_name: ZpoolName,
    ) -> Result<bool, Error> {
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
            return Ok(false);
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
        Ok(true)
    }

    fn sled_add_zone(
        &mut self,
        sled_id: Uuid,
        zone: OmicronZoneConfig,
    ) -> Result<(), Error> {
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
                    OmicronZonesConfig {
                        generation: Generation::new(),
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
        let sled_info = self.sled_resources(sled_id)?;

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
        allocator.alloc().ok_or_else(|| Error::OutOfAddresses { sled_id })
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

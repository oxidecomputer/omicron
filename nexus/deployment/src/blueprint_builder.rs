// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level facility for generating Blueprints

use crate::ip_allocator::IpAllocator;
use anyhow::anyhow;
use internal_dns::config::Host;
use internal_dns::config::ZoneVariant;
use ipnet::IpAdd;
use nexus_inventory::now_db_precision;
use nexus_types::deployment::Blueprint;
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
use omicron_common::address::NTP_PORT;
use omicron_common::address::SLED_RESERVED_ADDRESSES;
use omicron_common::api::external::Generation;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
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

/// Describes whether an idempotent "ensure" operation resulted in action taken
/// or no action was necessary
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Ensure {
    /// action was taken
    Added,
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
    internal_dns_version: Generation,

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
        internal_dns_version: Generation,
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
            internal_dns_version,
            time_created: now_db_precision(),
            creator: creator.to_owned(),
            comment: format!("from collection {}", collection.id),
        })
    }

    /// Construct a new `BlueprintBuilder` based on a previous blueprint,
    /// starting with no changes from that state
    pub fn new_based_on(
        parent_blueprint: &'a Blueprint,
        internal_dns_version: Generation,
        policy: &'a Policy,
        creator: &str,
    ) -> BlueprintBuilder<'a> {
        BlueprintBuilder {
            parent_blueprint,
            internal_dns_version,
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
            internal_dns_version: self.internal_dns_version,
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

#[cfg(test)]
pub mod test {
    use super::BlueprintBuilder;
    use ipnet::IpAdd;
    use nexus_types::deployment::Policy;
    use nexus_types::deployment::SledResources;
    use nexus_types::deployment::ZpoolName;
    use nexus_types::inventory::Collection;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::address::SLED_PREFIX;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::Generation;
    use sled_agent_client::types::{
        Baseboard, Inventory, OmicronZoneConfig, OmicronZoneDataset,
        OmicronZoneType, OmicronZonesConfig, SledRole,
    };
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use std::str::FromStr;
    use uuid::Uuid;

    /// Returns a collection and policy describing a pretty simple system
    pub fn example() -> (Collection, Policy) {
        let mut builder = nexus_inventory::CollectionBuilder::new("test-suite");

        let sled_ids = [
            "72443b6c-b8bb-4ffa-ab3a-aeaa428ed79b",
            "a5f3db3a-61aa-4f90-ad3e-02833c253bf5",
            "0d168386-2551-44e8-98dd-ae7a7570f8a0",
        ];
        let mut policy = Policy { sleds: BTreeMap::new() };
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
            let ip1 = sled_ip.saturating_add(1);
            let zones: Vec<_> = std::iter::once(OmicronZoneConfig {
                id: Uuid::new_v4(),
                underlay_address: sled_ip.saturating_add(1),
                zone_type: OmicronZoneType::InternalNtp {
                    address: SocketAddrV6::new(ip1, 12345, 0, 0).to_string(),
                    dns_servers: vec![],
                    domain: None,
                    ntp_servers: vec![],
                },
            })
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
        policy.sleds.insert(sled_id, SledResources { zpools, subnet });
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
                Generation::new(),
                &policy,
                "the_test",
            )
            .expect("failed to create initial blueprint");

        // Since collections don't include what was in service, we have to
        // provide that ourselves.  For our purposes though we don't care.
        let zones_in_service = blueprint_initial.zones_in_service.clone();
        let diff = blueprint_initial
            .diff_sleds_from_collection(&collection, &zones_in_service);
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
            Generation::new(),
            &policy,
            "test_basic",
        );
        let blueprint = builder.build();
        let diff = blueprint_initial.diff_sleds(&blueprint);
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
            Generation::new(),
            &policy,
            "the_test",
        )
        .expect("failed to create initial blueprint");

        let mut builder = BlueprintBuilder::new_based_on(
            &blueprint1,
            Generation::new(),
            &policy,
            "test_basic",
        );

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
        let diff = blueprint1.diff_sleds(&blueprint2);
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
        let mut builder = BlueprintBuilder::new_based_on(
            &blueprint2,
            Generation::new(),
            &policy,
            "test_basic",
        );
        builder.sled_ensure_zone_ntp(new_sled_id).unwrap();
        let new_sled_resources = policy.sleds.get(&new_sled_id).unwrap();
        for pool_name in &new_sled_resources.zpools {
            builder
                .sled_ensure_zone_crucible(new_sled_id, pool_name.clone())
                .unwrap();
        }

        let blueprint3 = builder.build();
        let diff = blueprint2.diff_sleds(&blueprint3);
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
}

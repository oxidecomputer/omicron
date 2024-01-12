// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level facilities for generating Blueprints
//!
//! See crate-level documentation for details.

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_builder::Error;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::Policy;
use slog::{info, Logger};

pub struct Planner<'a> {
    log: Logger,
    policy: &'a Policy,
    blueprint: BlueprintBuilder<'a>,
}

impl<'a> Planner<'a> {
    pub fn new_based_on(
        log: Logger,
        parent_blueprint: &'a Blueprint,
        policy: &'a Policy,
        creator: &str,
    ) -> Planner<'a> {
        let blueprint =
            BlueprintBuilder::new_based_on(parent_blueprint, policy, creator);
        Planner { log, policy, blueprint }
    }

    pub fn plan(mut self) -> Result<Blueprint, Error> {
        self.do_plan()?;
        Ok(self.blueprint.build())
    }

    fn do_plan(&mut self) -> Result<(), Error> {
        // The only thing this planner currently knows how to do is add services
        // to a sled that's missing them.  So let's see if we're in that case.

        // Internal DNS is a prerequisite for bringing up all other zones.  At
        // this point, we assume that internal DNS (as a service) is already
        // functioning.  At some point, this function will have to grow the
        // ability to determine whether more internal DNS zones need to be
        // added and where they should go.  And the blueprint builder will need
        // to grow the ability to provision one.

        for (sled_id, sled_info) in &self.policy.sleds {
            // Check for an NTP zone.  Every sled should have one.  If it's not
            // there, all we can do is provision that one zone.  We have to wait
            // for that to succeed and synchronize the clock before we can
            // provision anything else.
            if self.blueprint.sled_ensure_zone_internal_ntp(*sled_id)? {
                info!(
                    &self.log,
                    "found sled missing NTP zone (will add one)";
                    "sled_id" => ?sled_id
                );
                self.blueprint
                    .comment(&format!("sled {}: add NTP zone", sled_id));
                // Don't make any other changes to this sled.  However, this
                // change is compatible with any other changes to other sleds,
                // so we can "continue" here rather than "break".
                continue;
            }

            // Every zpool on the sled should have a Crucible zone on it.
            let mut ncrucibles_added = 0;
            for zpool_name in &sled_info.zpools {
                if self
                    .blueprint
                    .sled_ensure_zone_crucible(*sled_id, zpool_name.clone())?
                {
                    info!(
                        &self.log,
                        "found sled zpool missing Crucible zone (will add one)";
                        "sled_id" => ?sled_id,
                        "zpool_name" => ?zpool_name,
                    );
                    ncrucibles_added += 1;
                }
            }

            if ncrucibles_added > 0 {
                // Don't make any other changes to this sled.  However, this
                // change is compatible with any other changes to other sleds,
                // so we can "continue" here rather than "break".
                // (Yes, it's currently the last thing in the loop, but being
                // explicit here means we won't forget to do this when more code
                // is added below.)
                self.blueprint.comment(&format!("sled {}: add zones", sled_id));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::Planner;
    use crate::blueprint_builder::BlueprintBuilder;
    use ipnet::IpAdd;
    use nexus_types::deployment::Policy;
    use nexus_types::deployment::SledResources;
    use nexus_types::deployment::ZpoolName;
    use nexus_types::inventory::Collection;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::address::SLED_PREFIX;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::test_setup_log;
    use rand::Fill;
    use rand::SeedableRng;
    use sled_agent_client::types::{
        Baseboard, Inventory, OmicronZoneConfig, OmicronZoneDataset,
        OmicronZoneType, OmicronZonesConfig, SledRole,
    };
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::fmt::Write;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use std::str::FromStr;
    use uuid::Uuid;

    fn policy_add_sled(policy: &mut Policy, sled_id: Uuid) -> Ipv6Addr {
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

    /// Returns a collection and policy describing a pretty simple system
    fn example() -> (Collection, Policy) {
        let mut builder = nexus_inventory::CollectionBuilder::new("test-suite");

        // We want deterministic uuid generation in this test so that we get
        // consistent output for expectorate.
        // XXX-dap this is not quite good enough because the blueprint ids and
        // the ids for new sleds and zones are generated elsewhere with
        // Uuid::new_v4().  What to do?  Pass the RNG around everywhere?
        // Find/replace these uuids in the output?  Add a programmatic interface
        // to the differ?
        let mut rng = rand::rngs::StdRng::from_seed(Default::default());
        fn new_uuid(r: &mut rand::rngs::StdRng) -> Uuid {
            let mut bytes = uuid::Bytes::default();
            bytes.try_fill(r).unwrap();
            uuid::Builder::from_random_bytes(bytes).into_uuid()
        }

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
                id: new_uuid(&mut rng),
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
                    id: new_uuid(&mut rng),
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

    /// Runs through a basic sequence of blueprints for adding a sled
    #[test]
    fn test_basic_add_sled() {
        let logctx = test_setup_log("planner_basic_add_sled");

        // Assemble an output string that we'll check at the end.
        let mut out = String::new();

        // Use our example inventory collection.
        let (collection, mut policy) = example();

        // Build the initial blueprint.
        let blueprint1 = BlueprintBuilder::build_initial_from_collection(
            &collection,
            &policy,
            "the_test",
        )
        .expect("failed to create initial blueprint");

        // XXX-dap implement diff against collection and check that

        // Now run the planner.  It should do nothing because our initial
        // system didn't have any issues that the planner currently knows how to
        // fix.
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &policy,
            "no-op?",
        )
        .plan()
        .expect("failed to plan");

        writeln!(
            &mut out,
            "1 -> 2 (expected no changes):\n{}",
            blueprint1.diff(&blueprint2)
        )
        .unwrap();

        // Now add a new sled.
        let new_sled_id =
            "7097f5b3-5896-4fff-bd97-63a9a69563a9".parse().unwrap();
        let _ = policy_add_sled(&mut policy, new_sled_id);

        // Check that the first step is to add an NTP zone
        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &policy,
            "test: add NTP?",
        )
        .plan()
        .expect("failed to plan");

        writeln!(
            &mut out,
            "2 -> 3 (expect new NTP zone on new sled):\n{}",
            blueprint2.diff(&blueprint3)
        )
        .unwrap();

        // Check that the next step is to add Crucible zones
        let blueprint4 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &policy,
            "test: add Crucible zones?",
        )
        .plan()
        .expect("failed to plan");

        writeln!(
            &mut out,
            "3 -> 4 (expect Crucible zones):\n{}",
            blueprint3.diff(&blueprint4)
        )
        .unwrap();

        // Check that there are no more steps
        let blueprint5 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint4,
            &policy,
            "test: no-op?",
        )
        .plan()
        .expect("failed to plan");

        writeln!(
            &mut out,
            "4 -> 5 (expect no changes):\n{}",
            blueprint4.diff(&blueprint5)
        )
        .unwrap();

        expectorate::assert_contents("tests/output/planner_basic.out", &out);

        logctx.cleanup_successful();
    }
}

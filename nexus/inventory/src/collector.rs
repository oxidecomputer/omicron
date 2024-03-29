// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collection of inventory from Omicron components

use crate::builder::CollectionBuilder;
use crate::builder::InventoryError;
use crate::SledAgentEnumerator;
use anyhow::Context;
use gateway_client::types::GetCfpaParams;
use gateway_client::types::RotCfpaSlot;
use gateway_messages::SpComponent;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use nexus_types::inventory::RotPage;
use nexus_types::inventory::RotPageWhich;
use slog::o;
use slog::{debug, error};
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;

/// connection and request timeout used for Sled Agent HTTP client
const SLED_AGENT_TIMEOUT: Duration = Duration::from_secs(60);

/// Collect all inventory data from an Oxide system
pub struct Collector<'a> {
    log: slog::Logger,
    mgs_clients: Vec<Arc<gateway_client::Client>>,
    sled_agent_lister: &'a (dyn SledAgentEnumerator + Send + Sync),
    in_progress: CollectionBuilder,
}

impl<'a> Collector<'a> {
    pub fn new(
        creator: &str,
        mgs_clients: &[Arc<gateway_client::Client>],
        sled_agent_lister: &'a (dyn SledAgentEnumerator + Send + Sync),
        log: slog::Logger,
    ) -> Self {
        Collector {
            log,
            mgs_clients: mgs_clients.to_vec(),
            sled_agent_lister,
            in_progress: CollectionBuilder::new(creator),
        }
    }

    /// Begin the process of collecting a complete hardware/software inventory
    /// of the rack
    ///
    /// The collection process makes a bunch of requests to a bunch of
    /// components.  This can take a while and produce any number of errors.
    /// Such errors generally don't cause this function to fail.  Rather, the
    /// returned `Collection` keeps track of these errors.
    pub async fn collect_all(mut self) -> Result<Collection, anyhow::Error> {
        // We're about to do a bunch of asynchronous operations.  With a
        // combination of async, futures, and some cleverness, we could do much
        // of this in parallel.  But this code path is not remotely
        // latency-sensitive.  And there's real risk of overloading our
        // downstream services.  So we just do one step at a time.  This also
        // keeps the code simpler.

        debug!(&self.log, "begin collection");

        self.collect_all_mgs().await;
        self.collect_all_sled_agents().await;

        debug!(&self.log, "finished collection");

        Ok(self.in_progress.build())
    }

    /// Collect inventory from all MGS instances
    async fn collect_all_mgs(&mut self) {
        let clients = self.mgs_clients.clone();
        for client in &clients {
            self.collect_one_mgs(&client).await;
        }
    }

    async fn collect_one_mgs(&mut self, client: &gateway_client::Client) {
        debug!(&self.log, "begin collection from MGS";
            "mgs_url" => client.baseurl()
        );

        // First, see which SPs MGS can see via Ignition.
        let ignition_result = client.ignition_list().await.with_context(|| {
            format!("MGS {:?}: listing ignition targets", client.baseurl())
        });

        // Select only the SPs that appear powered on.
        //
        // This choice is debatable.  It's conceivable that an SP could be
        // functioning but not visible to ignition.  In that case, we'd be
        // better off trying to ask MGS about it even though ignition reports it
        // powered off.  But in practice, if ignition can't see it, it's much
        // more likely that there's just nothing plugged in.  And in that case,
        // if we try to ask MGS about it, we have to wait for MGS to time out
        // its attempt to reach it (currently several seconds).  This choice
        // enables inventory to complete much faster, at the expense of not
        // being able to identify this particular condition.
        let sps = match ignition_result {
            Err(error) => {
                self.in_progress.found_error(InventoryError::from(error));
                return;
            }

            Ok(targets) => {
                targets.into_inner().into_iter().filter_map(|sp_ignition| {
                    match sp_ignition.details {
                        gateway_client::types::SpIgnition::No => None,
                        gateway_client::types::SpIgnition::Yes {
                            power: false,
                            ..
                        } => None,
                        gateway_client::types::SpIgnition::Yes {
                            power: true,
                            ..
                        } => Some(sp_ignition.id),
                    }
                })
            }
        };

        // For each SP that ignition reports up, fetch the state and caboose
        // information.
        for sp in sps {
            // First, fetch the state of the SP.  If that fails, report the
            // error but continue.
            let result =
                client.sp_get(sp.type_, sp.slot).await.with_context(|| {
                    format!(
                        "MGS {:?}: fetching state of SP {:?}",
                        client.baseurl(),
                        sp
                    )
                });
            let sp_state = match result {
                Err(error) => {
                    self.in_progress.found_error(InventoryError::from(error));
                    continue;
                }
                Ok(response) => response.into_inner(),
            };

            // Record the state that we found.
            let Some(baseboard_id) = self.in_progress.found_sp_state(
                client.baseurl(),
                sp.type_,
                sp.slot,
                sp_state,
            ) else {
                // We failed to parse this SP for some reason.  The error was
                // reported already.  Move on.
                continue;
            };

            // For each kind of caboose that we care about, if it hasn't been
            // fetched already, fetch it and record it.  Generally, we'd only
            // get here for the first MGS client.  Assuming that one succeeds,
            // the other(s) will skip this loop.
            for which in CabooseWhich::iter() {
                if self.in_progress.found_caboose_already(&baseboard_id, which)
                {
                    continue;
                }

                let (component, slot) = match which {
                    CabooseWhich::SpSlot0 => ("sp", 0),
                    CabooseWhich::SpSlot1 => ("sp", 1),
                    CabooseWhich::RotSlotA => ("rot", 0),
                    CabooseWhich::RotSlotB => ("rot", 1),
                };

                let result = client
                    .sp_component_caboose_get(
                        sp.type_, sp.slot, component, slot,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "MGS {:?}: SP {:?}: caboose {:?}",
                            client.baseurl(),
                            sp,
                            which
                        )
                    });
                let caboose = match result {
                    Err(error) => {
                        self.in_progress
                            .found_error(InventoryError::from(error));
                        continue;
                    }
                    Ok(response) => response.into_inner(),
                };
                if let Err(error) = self.in_progress.found_caboose(
                    &baseboard_id,
                    which,
                    client.baseurl(),
                    caboose,
                ) {
                    error!(
                        &self.log,
                        "error reporting caboose: {:?} {:?} {:?}: {:#}",
                        baseboard_id,
                        which,
                        client.baseurl(),
                        error
                    );
                }
            }

            // For each kind of RoT page that we care about, if it hasn't been
            // fetched already, fetch it and record it.  Generally, we'd only
            // get here for the first MGS client.  Assuming that one succeeds,
            // the other(s) will skip this loop.
            for which in RotPageWhich::iter() {
                if self.in_progress.found_rot_page_already(&baseboard_id, which)
                {
                    continue;
                }

                let component = SpComponent::ROT.const_as_str();

                let result = match which {
                    RotPageWhich::Cmpa => client
                        .sp_rot_cmpa_get(sp.type_, sp.slot, component)
                        .await
                        .map(|response| response.into_inner().base64_data),
                    RotPageWhich::CfpaActive => client
                        .sp_rot_cfpa_get(
                            sp.type_,
                            sp.slot,
                            component,
                            &GetCfpaParams { slot: RotCfpaSlot::Active },
                        )
                        .await
                        .map(|response| response.into_inner().base64_data),
                    RotPageWhich::CfpaInactive => client
                        .sp_rot_cfpa_get(
                            sp.type_,
                            sp.slot,
                            component,
                            &GetCfpaParams { slot: RotCfpaSlot::Inactive },
                        )
                        .await
                        .map(|response| response.into_inner().base64_data),
                    RotPageWhich::CfpaScratch => client
                        .sp_rot_cfpa_get(
                            sp.type_,
                            sp.slot,
                            component,
                            &GetCfpaParams { slot: RotCfpaSlot::Scratch },
                        )
                        .await
                        .map(|response| response.into_inner().base64_data),
                }
                .with_context(|| {
                    format!(
                        "MGS {:?}: SP {:?}: rot page {:?}",
                        client.baseurl(),
                        sp,
                        which
                    )
                });

                let page = match result {
                    Err(error) => {
                        self.in_progress
                            .found_error(InventoryError::from(error));
                        continue;
                    }
                    Ok(data_base64) => RotPage { data_base64 },
                };
                if let Err(error) = self.in_progress.found_rot_page(
                    &baseboard_id,
                    which,
                    client.baseurl(),
                    page,
                ) {
                    error!(
                        &self.log,
                        "error reporting rot page: {:?} {:?} {:?}: {:#}",
                        baseboard_id,
                        which,
                        client.baseurl(),
                        error
                    );
                }
            }
        }
    }

    /// Collect inventory from all sled agent instances
    async fn collect_all_sled_agents(&mut self) {
        let urls = match self.sled_agent_lister.list_sled_agents().await {
            Err(error) => {
                self.in_progress.found_error(error);
                return;
            }
            Ok(clients) => clients,
        };

        for url in urls {
            let log = self.log.new(o!("SledAgent" => url.clone()));
            let reqwest_client = reqwest::ClientBuilder::new()
                .connect_timeout(SLED_AGENT_TIMEOUT)
                .timeout(SLED_AGENT_TIMEOUT)
                .build()
                .unwrap();
            let client = Arc::new(sled_agent_client::Client::new_with_client(
                &url,
                reqwest_client,
                log,
            ));

            if let Err(error) = self.collect_one_sled_agent(&client).await {
                error!(
                    &self.log,
                    "sled agent {:?}: {:#}",
                    client.baseurl(),
                    error
                );
            }
        }
    }

    async fn collect_one_sled_agent(
        &mut self,
        client: &sled_agent_client::Client,
    ) -> Result<(), anyhow::Error> {
        let sled_agent_url = client.baseurl();
        debug!(&self.log, "begin collection from Sled Agent";
            "sled_agent_url" => client.baseurl()
        );

        let maybe_ident = client.inventory().await.with_context(|| {
            format!("Sled Agent {:?}: inventory", &sled_agent_url)
        });
        let inventory = match maybe_ident {
            Ok(inventory) => inventory.into_inner(),
            Err(error) => {
                self.in_progress.found_error(InventoryError::from(error));
                return Ok(());
            }
        };

        let sled_id = inventory.sled_id;
        self.in_progress.found_sled_inventory(&sled_agent_url, inventory)?;

        let maybe_config =
            client.omicron_zones_get().await.with_context(|| {
                format!("Sled Agent {:?}: omicron zones", &sled_agent_url)
            });
        match maybe_config {
            Err(error) => {
                self.in_progress.found_error(InventoryError::from(error));
                Ok(())
            }
            Ok(zones) => self.in_progress.found_sled_omicron_zones(
                &sled_agent_url,
                sled_id,
                zones.into_inner(),
            ),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Collector;
    use crate::StaticSledAgentEnumerator;
    use gateway_messages::SpPort;
    use nexus_types::inventory::Collection;
    use omicron_common::api::external::Generation;
    use omicron_sled_agent::sim;
    use std::fmt::Write;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use std::sync::Arc;
    use uuid::Uuid;

    fn dump_collection(collection: &Collection) -> String {
        // Construct a stable, human-readable summary of the Collection
        // contents.  We could use a `Debug` impl for this, but that's not quite
        // right: when debugging, for example, we want fields like the ids, but
        // these change each time and we don't want to include them here.
        // `Serialize` has the same problem -- the set of fields to include
        // depends on what the serialization is for.  It's easy enough to just
        // print what we want here.
        let mut s = String::new();
        write!(&mut s, "baseboards:\n").unwrap();
        for b in &collection.baseboards {
            write!(
                &mut s,
                "    part {:?} serial {:?}\n",
                b.part_number, b.serial_number
            )
            .unwrap();
        }

        write!(&mut s, "\ncabooses:\n").unwrap();
        for c in &collection.cabooses {
            write!(
                &mut s,
                "    board {:?} name {:?} version {:?} git_commit {:?}\n",
                c.board, c.name, c.version, c.git_commit,
            )
            .unwrap();
        }

        write!(&mut s, "\nrot pages:\n").unwrap();
        for p in &collection.rot_pages {
            write!(&mut s, "    data_base64 {:?}\n", p.data_base64).unwrap();
        }

        // All we really need to check here is that we're reporting the right
        // SPs, RoTs, and cabooses.  The actual SP data, RoT data, and caboose
        // data comes straight from MGS.  And proper handling of that data is
        // tested in the builder.
        write!(&mut s, "\nSPs:\n").unwrap();
        for (bb, _) in &collection.sps {
            write!(
                &mut s,
                "    baseboard part {:?} serial {:?}\n",
                bb.part_number, bb.serial_number,
            )
            .unwrap();
        }

        write!(&mut s, "\nRoTs:\n").unwrap();
        for (bb, _) in &collection.rots {
            write!(
                &mut s,
                "    baseboard part {:?} serial {:?}\n",
                bb.part_number, bb.serial_number,
            )
            .unwrap();
        }

        write!(&mut s, "\ncabooses found:\n").unwrap();
        for (kind, bb_to_found) in &collection.cabooses_found {
            for (bb, found) in bb_to_found {
                write!(
                    &mut s,
                    "    {:?} baseboard part {:?} serial {:?}: board {:?}\n",
                    kind, bb.part_number, bb.serial_number, found.caboose.board,
                )
                .unwrap();
            }
        }

        write!(&mut s, "\nrot pages found:\n").unwrap();
        for (kind, bb_to_found) in &collection.rot_pages_found {
            for (bb, found) in bb_to_found {
                write!(
                    &mut s,
                    "    {:?} baseboard part {:?} serial {:?}: \
                              data_base64 {:?}\n",
                    kind,
                    bb.part_number,
                    bb.serial_number,
                    found.page.data_base64
                )
                .unwrap();
            }
        }

        write!(&mut s, "\nsled agents found:\n").unwrap();
        for (sled_id, sled_info) in &collection.sled_agents {
            assert_eq!(*sled_id, sled_info.sled_id);
            write!(&mut s, "  sled {} ({:?})\n", sled_id, sled_info.sled_role)
                .unwrap();
            write!(&mut s, "    baseboard {:?}\n", sled_info.baseboard_id)
                .unwrap();

            if let Some(found_zones) = collection.omicron_zones.get(sled_id) {
                assert_eq!(*sled_id, found_zones.sled_id);
                write!(
                    &mut s,
                    "    zone generation: {:?}\n",
                    found_zones.zones.generation
                )
                .unwrap();
                write!(&mut s, "    zones found:\n").unwrap();
                for zone in &found_zones.zones.zones {
                    write!(
                        &mut s,
                        "        zone {} type {}\n",
                        zone.id,
                        zone.zone_type.kind(),
                    )
                    .unwrap();
                }
            }
        }

        write!(&mut s, "\nerrors:\n").unwrap();
        for e in &collection.errors {
            // Some error strings have OS error numbers in them.  We want to
            // ignore those, particularly for CI, which runs these tests on
            // multiple OSes.
            let message = regex::Regex::new(r"os error \d+")
                .unwrap()
                .replace_all(&e, "os error <<redacted>>");
            // Communication errors differ based on the configuration of the
            // machine running the test. For example whether or not the machine
            // has IPv6 configured will determine if an error is network
            // unreachable or a timeout due to sending a packet to a known
            // discard prefix. So just key in on the communication error in a
            // general sense.
            let message = regex::Regex::new(r"Communication Error.*")
                .unwrap()
                .replace_all(&message, "Communication Error <<redacted>>");
            write!(&mut s, "error: {}\n", message).unwrap();
        }

        s
    }

    async fn sim_sled_agent(
        log: slog::Logger,
        sled_id: Uuid,
        zone_id: Uuid,
    ) -> sim::Server {
        // Start a simulated sled agent.
        let config = sim::Config::for_testing(
            sled_id,
            sim::SimMode::Auto,
            None,
            None,
            Some(vec![]),
        );
        let agent = sim::Server::start(&config, &log, false).await.unwrap();

        // Pretend to put some zones onto this sled.  We don't need to test this
        // exhaustively here because there are builder tests that exercise a
        // variety of different data.  We just want to make sure that if the
        // sled agent reports something specific (some non-degenerate case),
        // then it shows up in the resulting collection.
        let sled_url = format!("http://{}/", agent.http_server.local_addr());
        let client = sled_agent_client::Client::new(&sled_url, log);

        let zone_address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0);
        client
            .omicron_zones_put(&sled_agent_client::types::OmicronZonesConfig {
                generation: Generation::from(3),
                zones: vec![sled_agent_client::types::OmicronZoneConfig {
                    id: zone_id,
                    underlay_address: *zone_address.ip(),
                    zone_type:
                        sled_agent_client::types::OmicronZoneType::Oximeter {
                            address: zone_address.to_string(),
                        },
                }],
            })
            .await
            .expect("failed to write initial zone version to fake sled agent");

        agent
    }

    #[tokio::test]
    async fn test_basic() {
        // Set up the stock MGS test setup (which includes a couple of fake SPs)
        // and a simulated sled agent.  Then run a collection against these.
        let gwtestctx =
            gateway_test_utils::setup::test_setup("test_basic", SpPort::One)
                .await;
        let log = &gwtestctx.logctx.log;
        let sled1 = sim_sled_agent(
            log.clone(),
            "9cb9b78f-5614-440c-b66d-e8e81fab69b0".parse().unwrap(),
            "5125277f-0988-490b-ac01-3bba20cc8f07".parse().unwrap(),
        )
        .await;
        let sled2 = sim_sled_agent(
            log.clone(),
            "03265caf-da7d-46c7-b1c2-39fa90ce5c65".parse().unwrap(),
            "8b88a56f-3eb6-4d80-ba42-75d867bc427d".parse().unwrap(),
        )
        .await;
        let sled1_url = format!("http://{}/", sled1.http_server.local_addr());
        let sled2_url = format!("http://{}/", sled2.http_server.local_addr());
        let mgs_url = format!("http://{}/", gwtestctx.client.bind_address);
        let mgs_client =
            Arc::new(gateway_client::Client::new(&mgs_url, log.clone()));
        let sled_enum = StaticSledAgentEnumerator::new([sled1_url, sled2_url]);
        let collector = Collector::new(
            "test-suite",
            &[mgs_client],
            &sled_enum,
            log.clone(),
        );
        let collection = collector
            .collect_all()
            .await
            .expect("failed to carry out collection");
        assert!(collection.errors.is_empty());
        assert_eq!(collection.collector, "test-suite");

        let s = dump_collection(&collection);
        expectorate::assert_contents("tests/output/collector_basic.txt", &s);

        sled1.http_server.close().await.unwrap();
        gwtestctx.teardown().await;
    }

    #[tokio::test]
    async fn test_multi_mgs() {
        // This is the same as the basic test, but we set up two different MGS
        // instances and point the collector at both.  We should get the same
        // result.
        let gwtestctx1 = gateway_test_utils::setup::test_setup(
            "test_multi_mgs_1",
            SpPort::One,
        )
        .await;
        let gwtestctx2 = gateway_test_utils::setup::test_setup(
            "test_multi_mgs_2",
            SpPort::Two,
        )
        .await;
        let log = &gwtestctx1.logctx.log;
        let sled1 = sim_sled_agent(
            log.clone(),
            "9cb9b78f-5614-440c-b66d-e8e81fab69b0".parse().unwrap(),
            "5125277f-0988-490b-ac01-3bba20cc8f07".parse().unwrap(),
        )
        .await;
        let sled2 = sim_sled_agent(
            log.clone(),
            "03265caf-da7d-46c7-b1c2-39fa90ce5c65".parse().unwrap(),
            "8b88a56f-3eb6-4d80-ba42-75d867bc427d".parse().unwrap(),
        )
        .await;
        let sled1_url = format!("http://{}/", sled1.http_server.local_addr());
        let sled2_url = format!("http://{}/", sled2.http_server.local_addr());
        let mgs_clients = [&gwtestctx1, &gwtestctx2]
            .into_iter()
            .map(|g| {
                let url = format!("http://{}/", g.client.bind_address);
                let client = gateway_client::Client::new(&url, log.clone());
                Arc::new(client)
            })
            .collect::<Vec<_>>();
        let sled_enum = StaticSledAgentEnumerator::new([sled1_url, sled2_url]);
        let collector =
            Collector::new("test-suite", &mgs_clients, &sled_enum, log.clone());
        let collection = collector
            .collect_all()
            .await
            .expect("failed to carry out collection");
        assert!(collection.errors.is_empty());
        assert_eq!(collection.collector, "test-suite");

        let s = dump_collection(&collection);
        expectorate::assert_contents("tests/output/collector_basic.txt", &s);

        sled1.http_server.close().await.unwrap();
        gwtestctx1.teardown().await;
        gwtestctx2.teardown().await;
    }

    #[tokio::test]
    async fn test_multi_mgs_failure() {
        // This is similar to the multi-MGS test, but we don't actually set up
        // the second MGS.  To the collector, it should look offline or
        // otherwise non-functional.
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "test_multi_mgs_2",
            SpPort::Two,
        )
        .await;
        let log = &gwtestctx.logctx.log;
        let real_client = {
            let url = format!("http://{}/", gwtestctx.client.bind_address);
            let client = gateway_client::Client::new(&url, log.clone());
            Arc::new(client)
        };
        let bad_client = {
            // This IP range is guaranteed by RFC 6666 to discard traffic.
            let url = "http://[100::1]:12345";
            let client = gateway_client::Client::new(url, log.clone());
            Arc::new(client)
        };
        let mgs_clients = &[bad_client, real_client];
        let sled_enum = StaticSledAgentEnumerator::empty();
        let collector =
            Collector::new("test-suite", mgs_clients, &sled_enum, log.clone());
        let collection = collector
            .collect_all()
            .await
            .expect("failed to carry out collection");
        assert_eq!(collection.collector, "test-suite");

        let s = dump_collection(&collection);
        expectorate::assert_contents("tests/output/collector_errors.txt", &s);

        gwtestctx.teardown().await;
    }

    #[tokio::test]
    async fn test_sled_agent_failure() {
        // Similar to the basic test, but use multiple sled agents, one of which
        // is non-functional.
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "test_sled_agent_failure",
            SpPort::One,
        )
        .await;
        let log = &gwtestctx.logctx.log;
        let sled1 = sim_sled_agent(
            log.clone(),
            "9cb9b78f-5614-440c-b66d-e8e81fab69b0".parse().unwrap(),
            "5125277f-0988-490b-ac01-3bba20cc8f07".parse().unwrap(),
        )
        .await;
        let sled1_url = format!("http://{}/", sled1.http_server.local_addr());
        let sledbogus_url = String::from("http://[100::1]:45678");
        let mgs_url = format!("http://{}/", gwtestctx.client.bind_address);
        let mgs_client =
            Arc::new(gateway_client::Client::new(&mgs_url, log.clone()));
        let sled_enum =
            StaticSledAgentEnumerator::new([sled1_url, sledbogus_url]);
        let collector = Collector::new(
            "test-suite",
            &[mgs_client],
            &sled_enum,
            log.clone(),
        );
        let collection = collector
            .collect_all()
            .await
            .expect("failed to carry out collection");
        assert!(!collection.errors.is_empty());
        assert_eq!(collection.collector, "test-suite");

        let s = dump_collection(&collection);
        expectorate::assert_contents(
            "tests/output/collector_sled_agent_errors.txt",
            &s,
        );

        sled1.http_server.close().await.unwrap();
        gwtestctx.teardown().await;
    }
}

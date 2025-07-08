// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collection of inventory from Omicron components

use crate::SledAgentEnumerator;
use crate::builder::CollectionBuilder;
use crate::builder::InventoryError;
use anyhow::Context;
use gateway_client::types::GetCfpaParams;
use gateway_client::types::RotCfpaSlot;
use gateway_messages::SpComponent;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use nexus_types::inventory::RotPage;
use nexus_types::inventory::RotPageWhich;
use slog::Logger;
use slog::o;
use slog::{debug, error};
use std::time::Duration;
use strum::IntoEnumIterator;

/// connection and request timeout used for Sled Agent HTTP client
const SLED_AGENT_TIMEOUT: Duration = Duration::from_secs(60);

/// Collect all inventory data from an Oxide system
pub struct Collector<'a> {
    log: slog::Logger,
    mgs_clients: Vec<gateway_client::Client>,
    keeper_admin_clients: Vec<clickhouse_admin_keeper_client::Client>,
    sled_agent_lister: &'a (dyn SledAgentEnumerator + Send + Sync),
    in_progress: CollectionBuilder,
}

impl<'a> Collector<'a> {
    pub fn new(
        creator: &str,
        mgs_clients: Vec<gateway_client::Client>,
        keeper_admin_clients: Vec<clickhouse_admin_keeper_client::Client>,
        sled_agent_lister: &'a (dyn SledAgentEnumerator + Send + Sync),
        log: slog::Logger,
    ) -> Self {
        Collector {
            log,
            mgs_clients,
            keeper_admin_clients,
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
        self.collect_all_keepers().await;

        debug!(&self.log, "finished collection");

        Ok(self.in_progress.build())
    }

    /// Collect inventory from all MGS instances
    async fn collect_all_mgs(&mut self) {
        for client in &self.mgs_clients {
            Self::collect_one_mgs(client, &self.log, &mut self.in_progress)
                .await;
        }
    }

    async fn collect_one_mgs(
        client: &gateway_client::Client,
        log: &Logger,
        in_progress: &mut CollectionBuilder,
    ) {
        debug!(log, "begin collection from MGS";
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
                in_progress.found_error(InventoryError::from(error));
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
                    in_progress.found_error(InventoryError::from(error));
                    continue;
                }
                Ok(response) => response.into_inner(),
            };

            // Record the state that we found.
            let Some(baseboard_id) = in_progress.found_sp_state(
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
                if in_progress.found_caboose_already(&baseboard_id, which) {
                    continue;
                }

                let (component, slot) = match which {
                    CabooseWhich::SpSlot0 => ("sp", 0),
                    CabooseWhich::SpSlot1 => ("sp", 1),
                    CabooseWhich::RotSlotA => ("rot", 0),
                    CabooseWhich::RotSlotB => ("rot", 1),
                    CabooseWhich::Stage0 => ("stage0", 0),
                    CabooseWhich::Stage0Next => ("stage0", 1),
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
                        in_progress.found_error(InventoryError::from(error));
                        continue;
                    }
                    Ok(response) => response.into_inner(),
                };
                if let Err(error) = in_progress.found_caboose(
                    &baseboard_id,
                    which,
                    client.baseurl(),
                    caboose,
                ) {
                    error!(
                        log,
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
                if in_progress.found_rot_page_already(&baseboard_id, which) {
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
                        in_progress.found_error(InventoryError::from(error));
                        continue;
                    }
                    Ok(data_base64) => RotPage { data_base64 },
                };
                if let Err(error) = in_progress.found_rot_page(
                    &baseboard_id,
                    which,
                    client.baseurl(),
                    page,
                ) {
                    error!(
                        log,
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
            let client = sled_agent_client::Client::new_with_client(
                &url,
                reqwest_client,
                log,
            );

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

        self.in_progress.found_sled_inventory(&sled_agent_url, inventory)
    }

    /// Collect inventory from about keepers from all `ClickhouseAdminKeeper`
    /// clients
    async fn collect_all_keepers(&mut self) {
        debug!(self.log, "begin collecting all keepers";
            "nkeeper_admin_clients" => self.keeper_admin_clients.len());

        for client in &self.keeper_admin_clients {
            Self::collect_one_keeper(&client, &self.log, &mut self.in_progress)
                .await;
        }

        debug!(self.log, "end collecting all keepers";
            "nkeeper_admin_clients" => self.keeper_admin_clients.len());
    }

    /// Collect inventory about one keeper from one `ClickhouseAdminKeeper`
    async fn collect_one_keeper(
        client: &clickhouse_admin_keeper_client::Client,
        log: &slog::Logger,
        in_progress: &mut CollectionBuilder,
    ) {
        debug!(log, "begin collection from clickhouse-admin-keeper";
            "keeper_admin_url" => client.baseurl()
        );

        let res = client.keeper_cluster_membership().await.with_context(|| {
            format!("Clickhouse Keeper {:?}: inventory", &client.baseurl())
        });

        match res {
            Err(error) => {
                in_progress.found_error(InventoryError::from(error));
            }
            Ok(membership) => {
                let membership = membership.into_inner();
                debug!(log, "found keeper membership";
                    "keeper_admin_url" => client.baseurl(),
                    "leader_committed_log_index" =>
                    membership.leader_committed_log_index
                );
                in_progress
                    .found_clickhouse_keeper_cluster_membership(membership);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::Collector;
    use crate::StaticSledAgentEnumerator;
    use gateway_messages::SpPort;
    use id_map::IdMap;
    use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
    use nexus_sled_agent_shared::inventory::HostPhase2DesiredSlots;
    use nexus_sled_agent_shared::inventory::OmicronSledConfig;
    use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
    use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
    use nexus_sled_agent_shared::inventory::OmicronZoneType;
    use nexus_types::inventory::Collection;
    use omicron_common::api::external::Generation;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_sled_agent::sim;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use slog::o;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use std::sync::Arc;
    use swrite::SWrite as _;
    use swrite::swrite;
    use swrite::swriteln;

    fn dump_sled_config(s: &mut String, config: &OmicronSledConfig) {
        let OmicronSledConfig {
            generation,
            disks,
            datasets,
            zones,
            remove_mupdate_override,
            host_phase_2,
        } = config;

        swriteln!(s, "        generation: {generation}");
        swriteln!(
            s,
            "        remove_mupdate_override: {remove_mupdate_override:?}"
        );
        {
            let HostPhase2DesiredSlots { slot_a, slot_b } = host_phase_2;
            swriteln!(s, "        host_phase_2.slot_a: {slot_a:?}");
            swriteln!(s, "        host_phase_2.slot_b: {slot_b:?}");
        }
        for disk in disks {
            swriteln!(
                s,
                "        disk {}: {} / {} / {}",
                disk.id,
                disk.identity.vendor,
                disk.identity.model,
                disk.identity.serial
            );
        }
        for dataset in datasets {
            swriteln!(
                s,
                "        dataset {}: {}",
                dataset.id,
                dataset.name.full_name()
            );
        }
        for zone in zones {
            swriteln!(
                s,
                "        zone {} type {}",
                zone.id,
                zone.zone_type.kind().report_str(),
            );
        }
    }

    fn dump_collection(collection: &Collection) -> String {
        // Construct a stable, human-readable summary of the Collection
        // contents.  We could use a `Debug` impl for this, but that's not quite
        // right: when debugging, for example, we want fields like the ids, but
        // these change each time and we don't want to include them here.
        // `Serialize` has the same problem -- the set of fields to include
        // depends on what the serialization is for.  It's easy enough to just
        // print what we want here.
        let mut s = String::new();
        swrite!(s, "baseboards:\n");
        for b in &collection.baseboards {
            swrite!(
                s,
                "    part {:?} serial {:?}\n",
                b.part_number,
                b.serial_number
            );
        }

        swrite!(s, "\ncabooses:\n");
        for c in &collection.cabooses {
            swrite!(
                s,
                "    board {:?} name {:?} version {:?} git_commit {:?} sign {:?}\n",
                c.board,
                c.name,
                c.version,
                c.git_commit,
                c.sign,
            );
        }

        swrite!(s, "\nrot pages:\n");
        for p in &collection.rot_pages {
            swrite!(s, "    data_base64 {:?}\n", p.data_base64);
        }

        // All we really need to check here is that we're reporting the right
        // SPs, RoTs, and cabooses.  The actual SP data, RoT data, and caboose
        // data comes straight from MGS.  And proper handling of that data is
        // tested in the builder.
        swrite!(s, "\nSPs:\n");
        for (bb, _) in &collection.sps {
            swrite!(
                s,
                "    baseboard part {:?} serial {:?}\n",
                bb.part_number,
                bb.serial_number,
            );
        }

        swrite!(s, "\nRoTs:\n");
        for (bb, _) in &collection.rots {
            swrite!(
                s,
                "    baseboard part {:?} serial {:?}\n",
                bb.part_number,
                bb.serial_number,
            );
        }

        swrite!(s, "\ncabooses found:\n");
        for (kind, bb_to_found) in &collection.cabooses_found {
            for (bb, found) in bb_to_found {
                swrite!(
                    s,
                    "    {:?} baseboard part {:?} serial {:?}: board {:?}\n",
                    kind,
                    bb.part_number,
                    bb.serial_number,
                    found.caboose.board,
                );
            }
        }

        swrite!(s, "\nrot pages found:\n");
        for (kind, bb_to_found) in &collection.rot_pages_found {
            for (bb, found) in bb_to_found {
                swrite!(
                    s,
                    "    {:?} baseboard part {:?} serial {:?}: \
                              data_base64 {:?}\n",
                    kind,
                    bb.part_number,
                    bb.serial_number,
                    found.page.data_base64
                );
            }
        }

        swrite!(s, "\nsled agents found:\n");
        for sled_info in &collection.sled_agents {
            swrite!(
                s,
                "  sled {} ({:?})\n",
                sled_info.sled_id,
                sled_info.sled_role
            );
            swrite!(s, "    baseboard {:?}\n", sled_info.baseboard_id);

            if let Some(config) = &sled_info.ledgered_sled_config {
                swriteln!(s, "    ledgered sled config:");
                dump_sled_config(&mut s, config);
            } else {
                swriteln!(s, "    no ledgered sled config");
            }

            if let Some(last_reconciliation) = &sled_info.last_reconciliation {
                swriteln!(s, "    last reconciled config:");
                dump_sled_config(
                    &mut s,
                    &last_reconciliation.last_reconciled_config,
                );
                for (id, result) in &last_reconciliation.external_disks {
                    swriteln!(s, "    result for disk {id}: {result:?}");
                }
                for (id, result) in &last_reconciliation.datasets {
                    swriteln!(s, "    result for dataset {id}: {result:?}");
                }
                for (id, result) in &last_reconciliation.zones {
                    swriteln!(s, "    result for zone {id}: {result:?}");
                }
            } else {
                swriteln!(s, "    no completed reconciliation");
            }

            match &sled_info.reconciler_status {
                ConfigReconcilerInventoryStatus::NotYetRun => {
                    swriteln!(s, "    reconciler task not yet run");
                }
                ConfigReconcilerInventoryStatus::Running { config, .. } => {
                    swriteln!(s, "    reconciler task running with config:");
                    dump_sled_config(&mut s, config);
                }
                ConfigReconcilerInventoryStatus::Idle { .. } => {
                    swriteln!(s, "    reconciler task idle");
                }
            }
        }

        swrite!(s, "\nerrors:\n");
        let os_error_re = regex::Regex::new(r"os error \d+").unwrap();
        let comm_error_re =
            regex::Regex::new(r"Communication Error.*").unwrap();
        for e in &collection.errors {
            // Some error strings have OS error numbers in them.  We want to
            // ignore those, particularly for CI, which runs these tests on
            // multiple OSes.
            let message = os_error_re.replace_all(&e, "os error <<redacted>>");
            // Communication errors differ based on the configuration of the
            // machine running the test. For example whether or not the machine
            // has IPv6 configured will determine if an error is network
            // unreachable or a timeout due to sending a packet to a known
            // discard prefix. So just key in on the communication error in a
            // general sense.
            let message = comm_error_re
                .replace_all(&message, "Communication Error <<redacted>>");
            swrite!(s, "error: {}\n", message);
        }

        s
    }

    async fn sim_sled_agent(
        log: slog::Logger,
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
        simulated_upstairs: &Arc<sim::SimulatedUpstairs>,
    ) -> sim::Server {
        // Start a simulated sled agent.
        let config = sim::Config::for_testing(
            sled_id,
            sim::SimMode::Auto,
            None,
            None,
            sim::ZpoolConfig::None,
        );

        let agent =
            sim::Server::start(&config, &log, false, &simulated_upstairs, 0)
                .await
                .unwrap();

        // Pretend to put some zones onto this sled.  We don't need to test this
        // exhaustively here because there are builder tests that exercise a
        // variety of different data.  We just want to make sure that if the
        // sled agent reports something specific (some non-degenerate case),
        // then it shows up in the resulting collection.
        let sled_url = format!("http://{}/", agent.http_server.local_addr());
        let client = sled_agent_client::Client::new(&sled_url, log);

        let filesystem_pool = ZpoolName::new_external(ZpoolUuid::new_v4());
        let zone_address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0);
        client
            .omicron_config_put(&OmicronSledConfig {
                generation: Generation::from(3),
                disks: IdMap::default(),
                datasets: IdMap::default(),
                zones: [OmicronZoneConfig {
                    id: zone_id,
                    zone_type: OmicronZoneType::Oximeter {
                        address: zone_address,
                    },
                    filesystem_pool: Some(filesystem_pool),
                    image_source: OmicronZoneImageSource::InstallDataset,
                }]
                .into_iter()
                .collect(),
                remove_mupdate_override: None,
                host_phase_2: HostPhase2DesiredSlots::current_contents(),
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

        let simulated_upstairs =
            Arc::new(sim::SimulatedUpstairs::new(log.new(o!(
                "component" => "omicron_sled_agent::sim::SimulatedUpstairs",
            ))));

        let sled1 = sim_sled_agent(
            log.clone(),
            "9cb9b78f-5614-440c-b66d-e8e81fab69b0".parse().unwrap(),
            "5125277f-0988-490b-ac01-3bba20cc8f07".parse().unwrap(),
            &simulated_upstairs,
        )
        .await;

        let sled2 = sim_sled_agent(
            log.clone(),
            "03265caf-da7d-46c7-b1c2-39fa90ce5c65".parse().unwrap(),
            "8b88a56f-3eb6-4d80-ba42-75d867bc427d".parse().unwrap(),
            &simulated_upstairs,
        )
        .await;

        let sled1_url = format!("http://{}/", sled1.http_server.local_addr());
        let sled2_url = format!("http://{}/", sled2.http_server.local_addr());
        let mgs_url = format!("http://{}/", gwtestctx.client.bind_address);
        let mgs_client = gateway_client::Client::new(&mgs_url, log.clone());
        let sled_enum = StaticSledAgentEnumerator::new([sled1_url, sled2_url]);
        // We don't have any mocks for this, and it's unclear how much value
        // there would be in providing them at this juncture.
        let keeper_clients = Vec::new();
        let collector = Collector::new(
            "test-suite",
            vec![mgs_client],
            keeper_clients,
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

        let simulated_upstairs =
            Arc::new(sim::SimulatedUpstairs::new(log.new(o!(
                "component" => "omicron_sled_agent::sim::SimulatedUpstairs",
            ))));

        let sled1 = sim_sled_agent(
            log.clone(),
            "9cb9b78f-5614-440c-b66d-e8e81fab69b0".parse().unwrap(),
            "5125277f-0988-490b-ac01-3bba20cc8f07".parse().unwrap(),
            &simulated_upstairs,
        )
        .await;

        let sled2 = sim_sled_agent(
            log.clone(),
            "03265caf-da7d-46c7-b1c2-39fa90ce5c65".parse().unwrap(),
            "8b88a56f-3eb6-4d80-ba42-75d867bc427d".parse().unwrap(),
            &simulated_upstairs,
        )
        .await;

        let sled1_url = format!("http://{}/", sled1.http_server.local_addr());
        let sled2_url = format!("http://{}/", sled2.http_server.local_addr());
        let mgs_clients = [&gwtestctx1, &gwtestctx2]
            .into_iter()
            .map(|g| {
                let url = format!("http://{}/", g.client.bind_address);
                gateway_client::Client::new(&url, log.clone())
            })
            .collect::<Vec<_>>();
        let sled_enum = StaticSledAgentEnumerator::new([sled1_url, sled2_url]);
        // We don't have any mocks for this, and it's unclear how much value
        // there would be in providing them at this juncture.
        let keeper_clients = Vec::new();
        let collector = Collector::new(
            "test-suite",
            mgs_clients,
            keeper_clients,
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
            gateway_client::Client::new(&url, log.clone())
        };
        let bad_client = {
            // This IP range is guaranteed by RFC 6666 to discard traffic.
            let url = "http://[100::1]:12345";
            gateway_client::Client::new(url, log.clone())
        };
        let mgs_clients = vec![bad_client, real_client];
        let sled_enum = StaticSledAgentEnumerator::empty();
        // We don't have any mocks for this, and it's unclear how much value
        // there would be in providing them at this juncture.
        let keeper_clients = Vec::new();
        let collector = Collector::new(
            "test-suite",
            mgs_clients,
            keeper_clients,
            &sled_enum,
            log.clone(),
        );
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

        let simulated_upstairs =
            Arc::new(sim::SimulatedUpstairs::new(log.new(o!(
                "component" => "omicron_sled_agent::sim::SimulatedUpstairs",
            ))));

        let sled1 = sim_sled_agent(
            log.clone(),
            "9cb9b78f-5614-440c-b66d-e8e81fab69b0".parse().unwrap(),
            "5125277f-0988-490b-ac01-3bba20cc8f07".parse().unwrap(),
            &simulated_upstairs,
        )
        .await;

        let sled1_url = format!("http://{}/", sled1.http_server.local_addr());
        let sledbogus_url = String::from("http://[100::1]:45678");
        let mgs_url = format!("http://{}/", gwtestctx.client.bind_address);
        let mgs_client = gateway_client::Client::new(&mgs_url, log.clone());
        let sled_enum =
            StaticSledAgentEnumerator::new([sled1_url, sledbogus_url]);
        // We don't have any mocks for this, and it's unclear how much value
        // there would be in providing them at this juncture.
        let keeper_clients = Vec::new();
        let collector = Collector::new(
            "test-suite",
            vec![mgs_client],
            keeper_clients,
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

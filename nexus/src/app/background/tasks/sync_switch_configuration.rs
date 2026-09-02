// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating user provided switch configurations
//! to the bootstore via sled-agent

use crate::app::background::LoadedTargetBlueprint;

use nexus_db_model::{BootstoreConfig, NETWORK_KEY};
use nexus_types::deployment::SledFilter;
use tokio::sync::watch;

use crate::app::background::BackgroundTask;
use display_error_chain::DisplayErrorChain;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_queries::{context::OpContext, db::DataStore};
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::IncompleteBootstoreConfigReport;
use nexus_types::internal_api::background::SwitchPortSettingsManagerStatus;
use omicron_common::api::external::DataPageParams;
use omicron_uuid_kinds::GenericUuid;
use serde_json::json;
use sled_agent_types::early_networking::EarlyNetworkConfigEnvelope;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::system_networking::BlueprintExternalNetworkingConfig;
use sled_agent_types::router_config::{
    RouterConfigListEntry, SwitchRouterConfigs, default_router_list,
};

use super::router_config_render;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use sled_agent_types::system_networking::WriteNetworkConfigRequest;
use slog_error_chain::InlineErrorChain;
use std::{collections::HashSet, hash::Hash, sync::Arc};

pub struct SwitchPortSettingsManager {
    datastore: Arc<DataStore>,
    rx_blueprint: watch::Receiver<Option<LoadedTargetBlueprint>>,
}

impl SwitchPortSettingsManager {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_blueprint: watch::Receiver<Option<LoadedTargetBlueprint>>,
    ) -> Self {
        Self { datastore, rx_blueprint }
    }
}

impl BackgroundTask for SwitchPortSettingsManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            let log = opctx.log.clone();

            let racks = match self.datastore.rack_list_initialized(opctx, &DataPageParams::max_page()).await {
                Ok(racks) => racks,
                Err(e) => {
                    error!(log, "failed to retrieve racks from database";
                        "error" => %DisplayErrorChain::new(&e)
                    );
                    return json!({
                        "error":
                            format!(
                                "failed to retrieve racks from database : {}",
                                DisplayErrorChain::new(&e)
                            )
                    });
                },
            };


            let mut status = SwitchPortSettingsManagerStatus::default();

            // TODO: https://github.com/oxidecomputer/omicron/issues/3090
            // Here we're iterating over racks because that's technically the correct thing to do,
            // but our logic for pulling switch ports and their related configurations
            // *isn't* per-rack, so that's something we'll need to revisit in the future.
            for rack in &racks {
                let rack_id = rack.id().to_string();
                let log = log.new(slog::o!("rack_id" => rack_id));

                let sleds = match self
                    .datastore
                    .sled_list_all_batched(opctx, SledFilter::Commissioned)
                    .await
                {
                    Ok(sleds) => sleds,
                    Err(e) => {
                        error!(log, "failed to retrieve sleds from database";
                               "error" => %DisplayErrorChain::new(&e)
                        );
                        return json!({
                            "error":
                            format!(
                                "failed to retrieve sleds from database : {}",
                                DisplayErrorChain::new(&e)
                            )
                        });
                    }
                };

                let scrimlet_clients = sleds
                    .into_iter()
                    .filter(|s| s.is_scrimlet())
                    .map(|s| {
                        sled_agent_client::Client::new(
                            &format!("http://{}", s.address()),
                            log.clone(),
                        )
                    });

                //
                // calculate and apply bootstore changes
                //

                let input = match nexus_switch_config_preparation::read_and_assemble(
                    &self.datastore,
                    opctx,
                    &log,
                    rack.rack_subnet,
                )
                .await
                {
                    Ok(input) => input,
                    Err(e) => {
                        error!(
                            log,
                            "failed to read bootstore network config; \
                             skipping rack";
                            "error" => InlineErrorChain::new(&e),
                        );
                        continue;
                    }
                };

                let rack_network_config =
                    match nexus_switch_config::build_rack_network_config(
                        input,
                    ) {
                        Ok(rack_network_config) => rack_network_config,
                        Err(report) => {
                            error!(
                                log,
                                "incomplete bootstore network config; \
                                 skipping rack";
                                "problems" => %report,
                            );
                            status.incomplete_bootstore_configs.push(
                                IncompleteBootstoreConfigReport {
                                    rack_id: rack.id(),
                                    problems: report
                                        .problems
                                        .iter()
                                        .map(|problem| problem.to_string())
                                        .collect(),
                                },
                            );
                            continue;
                        }
                    };

                let (
                    blueprint_external_networking_generation,
                    service_zone_nat_entries,
                ) = match self
                    .rx_blueprint
                    .borrow_and_update()
                    .clone()
                    .map(|bp| bp.blueprint.to_service_zone_nat_entries().map(
                        |entries| (
                            bp.blueprint.external_networking_generation,
                            entries,
                        )
                    ))
                {
                    Some(Ok((generation, entries))) => (generation, entries),
                    Some(Err(err)) => {
                        error!(
                            log,
                            "cannot construct service zone NAT entries \
                             from blueprint";
                            InlineErrorChain::new(&err),
                        );
                        continue;
                    }
                    None => {
                        warn!(log, "blueprint not yet loaded - skipping sync");
                        continue;
                    }
                };

                // Load the most-recently-written bootstore contents from the
                // db cache. They are needed twice below: to carry the current
                // router-config specs forward if rendering fails, and to
                // decide whether the bootstore needs an update at all.
                //
                // * If the cached config cannot be deserialized into our
                //   current bootstore format, we assume that it is an older
                //   format; we treat it like a missing config (update
                //   unconditionally).
                // * If there is no cached config, this is the first time this
                //   rpw has run for the given rack; update unconditionally.
                // * If we cannot fetch it due to a db error, something is
                //   broken so we don't do anything.
                let current_contents: Option<SystemNetworkingConfig> = match self
                    .datastore
                    .get_latest_bootstore_config(opctx, NETWORK_KEY.into())
                    .await
                {
                    Ok(Some(BootstoreConfig { data, .. })) => {
                        match EarlyNetworkConfigEnvelope::deserialize_from_value(data.clone())
                            .and_then(|envelope| envelope.deserialize_body())
                        {
                            Ok(config) => Some(config),
                            Err(e) => {
                                error!(
                                    log,
                                    "bootstore config failed to deserialize \
                                     to current EarlyNetworkConfig format";
                                    "key" => %NETWORK_KEY,
                                    "value" => %data,
                                    "error" => %e,
                                );
                                None
                            },
                        }
                    },
                    Ok(None) => {
                        warn!(
                            log,
                            "no bootstore config found in db";
                            "key" => %NETWORK_KEY,
                        );
                        None
                    },
                    Err(e) => {
                        error!(
                            log,
                            "error while fetching last applied bootstore config";
                            "key" => %NETWORK_KEY,
                            "error" => %e,
                        );
                        continue;
                    },
                };

                // Render the router configurations into bootstore specs. If
                // the render fails, keep the specs already in the bootstore:
                // the downstream mgd apply is total, so writing a partial
                // render would tear down routers that merely failed to load.
                let switch_router_configs =
                    match router_config_render::render_switch_router_configs(
                        &self.datastore,
                        opctx,
                        &mut status.router_config_render_errors,
                    )
                    .await
                    {
                        Ok(rendered) => rendered,
                        Err(e) => {
                            error!(
                                log,
                                "failed to render router configurations; \
                                 keeping the current bootstore specs";
                                "error" => %e,
                            );
                            status.router_config_render_errors.push(e);
                            current_contents
                                .as_ref()
                                .map(|c| c.switch_router_configs.clone())
                                .unwrap_or_default()
                        }
                    };

                // The control-plane (service port) router list rides the
                // bootstore so sled-agents can create service ports before
                // nexus is reachable (boundary NTP). Never configured → the
                // default list; the NULL-id marker row → explicitly empty.
                let control_plane_router_list = match self
                    .datastore
                    .control_plane_router_configurations_list(opctx)
                    .await
                {
                    Ok(entries) if entries.is_empty() => default_router_list(),
                    Ok(entries) => {
                        crate::app::router_configuration::router_list_from_links(
                            entries.into_iter().filter_map(|e| {
                                e.router_configuration_id.map(|id| {
                                    (*e.priority, id.into_untyped_uuid())
                                })
                            }),
                        )
                        .into_iter()
                        .map(|e| RouterConfigListEntry {
                            priority: e.priority,
                            router_id: e.router_id,
                        })
                        .collect()
                    }
                    Err(e) => {
                        error!(
                            log,
                            "failed to list control-plane router \
                             configurations; keeping the current bootstore \
                             list";
                            "error" => %DisplayErrorChain::new(&e),
                        );
                        current_contents
                            .as_ref()
                            .map(|c| c.control_plane_router_list.clone())
                            .unwrap_or_else(default_router_list)
                    }
                };

                // The construction here is slightly weird - we start with
                // `blueprint_external_networking_config: None` and then
                // immediately fill it in. This gives us a non-optional
                // reference to the config we supplied, which we need below to
                // call `does_bootstore_need_update()`.
                let mut desired_config = SystemNetworkingConfig {
                    rack_network_config,
                    blueprint_external_networking_config: None,
                    switch_router_configs,
                    control_plane_router_list,
                };
                let desired_blueprint_networking_config = &*desired_config
                    .blueprint_external_networking_config
                    .insert(
                        BlueprintExternalNetworkingConfig {
                            blueprint_external_networking_generation,
                            service_zone_nat_entries,
                        },
                    );

                // Decide whether we need to increment the bootstore version
                // and push a new config to the sled agents: only if the config
                // we've built differs from the last config we've cached in the
                // db (or there is no usable cached config at all).
                let bootstore_needs_update = match &current_contents {
                    Some(config) => does_bootstore_need_update(
                        config,
                        &desired_config.rack_network_config,
                        desired_blueprint_networking_config,
                        &desired_config.switch_router_configs,
                        &desired_config.control_plane_router_list,
                        &log,
                    ),
                    None => true,
                };

                // The following code is designed to give us the following
                // properties
                // * We only push updates to the bootstore (sled-agents) if
                //   configuration on our side (nexus) has relevant changes.
                // * If the RPW encounters a critical error or crashes at any
                //   point of the operation, it will retry the configuration
                //   again during the next run
                // * We are able to accomplish the above without inspecting
                //   the bootstore on the sled-agents
                //
                // For example, in the event that we crash after pushing to
                // the sled-agents successfully, but before writing the
                // results to the db
                // 1. RPW will restart
                // 2. RPW will build a new network config
                // 3. RPW will compare against the last version stored in the db
                // 4. RPW will decide to apply the config (again)
                // 5. RPW will bump the version (again)
                // 6. RPW will send a new bootstore update to the agents (with
                //    the same info as last time, but with a new version)
                // 7. RPW will record the update in the db
                // 8. We are now back on the happy path
                if bootstore_needs_update {
                    let generation = match self.datastore
                        .bump_bootstore_generation(opctx, NETWORK_KEY.into())
                        .await
                    {
                        Ok(value) => value,
                        Err(e) => {
                            error!(
                                log,
                                "error while fetching next bootstore generation from db";
                                "key" => %NETWORK_KEY,
                                "error" => %e,
                            );
                            continue;
                        },
                    };

                    info!(
                        &log,
                        "updating bootstore config";
                        "config" => ?desired_config,
                    );

                    let write_request = {
                        let generation = match u64::try_from(generation) {
                            Ok(generation) => generation,
                            Err(_) => {
                                error!(
                                    log,
                                    "got negative generation from db";
                                    "generation" => generation,
                                );
                                continue;
                            }
                        };
                        WriteNetworkConfigRequest {
                            generation,
                            body: desired_config,
                        }
                    };

                    // Update the bootstore. We eagerly push updates to both scrimlets.
                    let mut one_succeeded = false;
                    for client in scrimlet_clients {
                        if let Err(e) = client.write_network_bootstore_config(&write_request).await {
                            error!(
                                log,
                                "error updating bootstore";
                                "scrimlet_client" => ?client,
                                "request" => ?write_request,
                                "error" => %e,
                            )
                        } else {
                            one_succeeded = true;
                        }
                    }

                    // if at least one succeeded, record this update in the db
                    if one_succeeded {
                        // Wrap the new config in an envelope to attach the
                        // current body schema version.
                        let envelope = EarlyNetworkConfigEnvelope::from(
                            &write_request.body,
                        );
                        let config = BootstoreConfig {
                            key: NETWORK_KEY.into(),
                            generation,
                            // We're serializing an envelope (guaranteed to be
                            // representable as JSON) to JSON in memory, so this
                            // can't fail.
                            data: serde_json::to_value(&envelope).expect(
                                "EarlyNetworkConfigEnvelope can be serialized \
                                 as JSON",
                            ),
                            time_created: chrono::Utc::now(),
                            time_deleted: None,
                        };
                        if let Err(e) = self.datastore.ensure_bootstore_config(opctx, config.clone()).await {
                            // if this fails, worst case scenario is that we will send the bootstore
                            // information it already has on the next run
                            error!(
                                log,
                                "error while caching bootstore config in db";
                                "config" => ?config,
                                "error" => %e,
                            );
                        }
                    }
                }
            }
            // TODO: we have some early returns in this task. We should instead
            // collect all problems and return them in the response.
            //
            // As part of that, we should move the body into an
            // `activate_impl(...)` function which returns a
            // `SwitchPortManagerStatus`. That'll force us to not do early
            // returns.
            json!(status)
        }
        .boxed()
    }
}

fn hashset_eq<T>(left: &[T], right: &[T]) -> bool
where
    T: Hash + Eq,
{
    let left = left.iter().collect::<HashSet<&T>>();
    let right = right.iter().collect::<HashSet<&T>>();
    left == right
}

// Helper to decide whether we should update the replicated bootstore.
//
// `current_contents` are the most-recently-written bootstore contents; it
// contains both a rack network config and a blueprint external networking
// config.
//
// `desired_rack_network_config` and `desired_blueprint_networking_config` are
// what this activation of this background task believes the current
// configuration should be.
//
// At a high level, there are three general possibilities:
//
// 1. Our desired configs match `current_contents`. (Easy and common case: we
//    return `false`.)
// 2. Our desired configs are different from `current_contents`, but we're
//    operating on stale data (i.e., data older than what was used to produce
//    `current_contents`). This can occur if another Nexus executed this same
//    task with a different and slightly-newer view of the world; e.g., if the
//    target blueprint recently changed, that Nexus had loaded the change, and
//    we haven't yet. We must return `false` in this case to avoid overwriting
//    new data with our stale data.
// 3. Our desired config is different from `current_contents` and we are not
//    operating on stale data. We return `true`.
//
// Today, we only partially handle case 2. We store generation numbers that
// allow us to detect a stale `desired_blueprint_networking_config`, but we have
// no way of detecting a stale `desired_rack_network_config`. If
// `desired_blueprint_networking_config` is not stale and either desired config
// is different from `current_contents`, we'll return true.
fn does_bootstore_need_update(
    current_contents: &SystemNetworkingConfig,
    desired_rack_network_config: &RackNetworkConfig,
    desired_blueprint_networking_config: &BlueprintExternalNetworkingConfig,
    desired_switch_router_configs: &SwitchRouterConfigs,
    desired_control_plane_router_list: &[RouterConfigListEntry],
    log: &slog::Logger,
) -> bool {
    // We should make our decision based on four boolean values: "is the config
    // different" and "is our desired config based on out of date information"
    // for each of our two desired configs. Define a couple of enums here to use
    // instead of `bool` for clarity distinguishing between "are we looking at
    // staleness" or "are we looking at whether there have been changes".
    macro_rules! named_bool_yes_no {
        ($newtype:ident) => {
            #[derive(Clone, Copy)]
            enum $newtype {
                Yes,
                No,
            }
            impl $newtype {
                fn as_bool(self) -> bool {
                    match self {
                        Self::Yes => true,
                        Self::No => false,
                    }
                }
            }
        };
    }
    named_bool_yes_no!(DesiredConfigOutOfDate);
    named_bool_yes_no!(ConfigChanged);

    // Compute staleness and "are there changes" for
    // `desired_blueprint_networking_config`.
    let (is_blueprint_out_of_date, is_blueprint_different) =
        if let Some(current_blueprint_networking_config) =
            current_contents.blueprint_external_networking_config.as_ref()
        {
            let BlueprintExternalNetworkingConfig {
                blueprint_external_networking_generation: current_gen,
                service_zone_nat_entries: current_nat,
            } = current_blueprint_networking_config;

            let BlueprintExternalNetworkingConfig {
                blueprint_external_networking_generation: desired_gen,
                service_zone_nat_entries: desired_nat,
            } = desired_blueprint_networking_config;

            // This check must be "strictly less than", not "<=". It's very
            // possible the blueprint config has not changed (i.e., we'd expect
            // equal generation numbers) but the rack network config (checked
            // below) has. We're only out of date if we know we're strictly
            // older than what's in the bootstore.
            let is_blueprint_out_of_date = if desired_gen < current_gen {
                warn!(
                    log, "our loaded blueprint generation is out of date";
                    "bootstore-gen" => current_gen,
                    "our-blueprint-gen" => desired_gen,
                );
                DesiredConfigOutOfDate::Yes
            } else {
                DesiredConfigOutOfDate::No
            };

            let is_blueprint_different = if current_nat != desired_nat {
                ConfigChanged::Yes
            } else {
                ConfigChanged::No
            };

            (is_blueprint_out_of_date, is_blueprint_different)
        } else {
            // If the bootstore has no blueprint config, we have no way of
            // detecting stale data; we have to assume it's not stale, because
            // there's definitely been a change we need to write!
            (DesiredConfigOutOfDate::No, ConfigChanged::Yes)
        };

    // Compute staleness and "are there changes" for
    // `desired_rack_network_config`.
    //
    // TODO-correctness We have no way of computing staleness! We must always
    // assume `desired_rack_network_config` is not out of date.
    let is_network_config_out_of_date = DesiredConfigOutOfDate::No;
    let is_network_config_different = {
        let RackNetworkConfig {
            rack_subnet: current_subnet,
            infra_ip_first: current_infra_ip_first,
            infra_ip_last: current_infra_ip_last,
            ports: current_ports,
            bgp: current_bgp,
            bfd: current_bfd,
        } = &current_contents.rack_network_config;

        let RackNetworkConfig {
            rack_subnet: desired_subnet,
            infra_ip_first: desired_infra_ip_first,
            infra_ip_last: desired_infra_ip_last,
            ports: desired_ports,
            bgp: desired_bgp,
            bfd: desired_bfd,
        } = desired_rack_network_config;

        let rnc_differs = !hashset_eq(current_bgp, desired_bgp)
            || !hashset_eq(current_bfd, desired_bfd)
            || !hashset_eq(current_ports.as_slice(), desired_ports.as_slice())
            || current_subnet != desired_subnet
            || current_infra_ip_first != desired_infra_ip_first
            || current_infra_ip_last != desired_infra_ip_last;

        if rnc_differs { ConfigChanged::Yes } else { ConfigChanged::No }
    };

    // Compute "are there changes" for the router-config specs and the
    // control-plane router list. Like the rack network config, these carry no
    // generation number, so we have no way of computing staleness and must
    // assume they are not out of date.
    let is_router_config_different = if current_contents.switch_router_configs
        != *desired_switch_router_configs
        || current_contents.control_plane_router_list
            != *desired_control_plane_router_list
    {
        ConfigChanged::Yes
    } else {
        ConfigChanged::No
    };

    match (
        is_blueprint_out_of_date,
        is_network_config_out_of_date,
        is_blueprint_different,
        is_network_config_different,
        is_router_config_different,
    ) {
        // If either config is out of date, we must not make changes to avoid
        // overwriting newer data. A future task activation will load a
        // different (and newer) set of desired config.
        (DesiredConfigOutOfDate::Yes, _, _, _, _)
        | (_, DesiredConfigOutOfDate::Yes, _, _, _) => {
            warn!(
                log, "skipping bootstore update due to stale data";
                "is_blueprint_out_of_date" =>
                    is_blueprint_out_of_date.as_bool(),
                "is_blueprint_different" =>
                    is_blueprint_different.as_bool(),
                "is_network_config_out_of_date" =>
                    is_network_config_out_of_date.as_bool(),
                "is_network_config_different" =>
                    is_network_config_different.as_bool(),
                "is_router_config_different" =>
                    is_router_config_different.as_bool(),
            );
            false
        }

        // If neither config is out of date, has any changed? If so, we do
        // need to write new bootstore contents.
        (
            DesiredConfigOutOfDate::No,
            DesiredConfigOutOfDate::No,
            ConfigChanged::Yes,
            _,
            _,
        )
        | (
            DesiredConfigOutOfDate::No,
            DesiredConfigOutOfDate::No,
            _,
            ConfigChanged::Yes,
            _,
        )
        | (
            DesiredConfigOutOfDate::No,
            DesiredConfigOutOfDate::No,
            _,
            _,
            ConfigChanged::Yes,
        ) => {
            info!(
                log, "will update bootstore with new contents";
                "is_network_config_out_of_date" =>
                    is_network_config_out_of_date.as_bool(),
                "is_network_config_different" =>
                    is_network_config_different.as_bool(),
                "is_router_config_different" =>
                    is_router_config_different.as_bool(),
            );
            true
        }

        // The most common case in practice: our desired config is not out of
        // date, but also hasn't changed since the last task activation. We
        // don't need to write anything to the bootstore; it's up to date.
        (
            DesiredConfigOutOfDate::No,
            DesiredConfigOutOfDate::No,
            ConfigChanged::No,
            ConfigChanged::No,
            ConfigChanged::No,
        ) => {
            info!(log, "will not update bootstore: it is up to date");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iddqd::IdOrdMap;
    use omicron_common::api::external::Vni;
    use omicron_generation_kinds::Generation;
    use omicron_test_utils::dev::test_setup_log;
    use sled_agent_types::early_networking::PortConfig;
    use sled_agent_types::early_networking::SwitchSlot;
    use sled_agent_types::early_networking::UplinkPorts;
    use sled_agent_types::inventory::SourceNatConfigGeneric;
    use sled_agent_types::system_networking::ServiceZoneNatEntries;
    use sled_agent_types::system_networking::ServiceZoneNatEntry;
    use sled_agent_types::system_networking::ServiceZoneNatKind;

    fn make_rack_network_config(rack_subnet: &str) -> RackNetworkConfig {
        RackNetworkConfig {
            rack_subnet: rack_subnet.parse().unwrap(),
            infra_ip_first: "172.20.15.21".parse().unwrap(),
            infra_ip_last: "172.20.15.22".parse().unwrap(),
            // `UplinkPorts` must be non-empty -- use a single placeholder port.
            ports: UplinkPorts::new(vec![PortConfig::empty_for_tests("qsfp0")])
                .expect("placeholder port list is non-empty"),
            bgp: vec![],
            bfd: vec![],
        }
    }

    fn make_nat_entries(nexus_external_ip: &str) -> ServiceZoneNatEntries {
        ServiceZoneNatEntries::try_from(
            [
                ServiceZoneNatEntry {
                    zone_id: "00000000-0000-0000-0000-000000000001"
                        .parse()
                        .unwrap(),
                    sled_underlay_ip: "fd00:1122:3344:101::1".parse().unwrap(),
                    nic_mac: "A8:40:25:FF:80:00".parse().unwrap(),
                    vni: Vni::SERVICES_VNI,
                    kind: ServiceZoneNatKind::BoundaryNtp {
                        snat_cfg: SourceNatConfigGeneric::new(
                            "172.20.26.1".parse().unwrap(),
                            0,
                            16383,
                        )
                        .expect("valid snat cfg"),
                    },
                },
                ServiceZoneNatEntry {
                    zone_id: "00000000-0000-0000-0000-000000000002"
                        .parse()
                        .unwrap(),
                    sled_underlay_ip: "fd00:1122:3344:102::1".parse().unwrap(),
                    nic_mac: "A8:40:25:FF:80:01".parse().unwrap(),
                    vni: Vni::SERVICES_VNI,
                    kind: ServiceZoneNatKind::ExternalDns {
                        external_ip: "172.20.26.2".parse().unwrap(),
                    },
                },
                ServiceZoneNatEntry {
                    zone_id: "00000000-0000-0000-0000-000000000003"
                        .parse()
                        .unwrap(),
                    sled_underlay_ip: "fd00:1122:3344:103::1".parse().unwrap(),
                    nic_mac: "A8:40:25:FF:80:02".parse().unwrap(),
                    vni: Vni::SERVICES_VNI,
                    kind: ServiceZoneNatKind::Nexus {
                        external_ip: nexus_external_ip.parse().unwrap(),
                    },
                },
            ]
            .into_iter()
            .collect::<IdOrdMap<_>>(),
        )
        .expect("valid service zone NAT entries")
    }

    fn make_blueprint_config(
        generation: u32,
        service_zone_nat_entries: ServiceZoneNatEntries,
    ) -> BlueprintExternalNetworkingConfig {
        BlueprintExternalNetworkingConfig {
            blueprint_external_networking_generation: Generation::from_u32(
                generation,
            ),
            service_zone_nat_entries,
        }
    }

    fn make_system_networking_config(
        rnc: RackNetworkConfig,
        blueprint: Option<BlueprintExternalNetworkingConfig>,
    ) -> SystemNetworkingConfig {
        SystemNetworkingConfig {
            rack_network_config: rnc,
            blueprint_external_networking_config: blueprint,
            switch_router_configs: SwitchRouterConfigs::new(),
            control_plane_router_list: default_router_list(),
        }
    }

    #[test]
    fn bootstore_update_when_current_has_no_blueprint_config() {
        let logctx = test_setup_log(
            "bootstore_update_when_current_has_no_blueprint_config",
        );

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let current = make_system_networking_config(rnc.clone(), None);
        let desired_blueprint =
            make_blueprint_config(1, make_nat_entries("172.20.26.3"));

        assert!(does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &current.switch_router_configs,
            &current.control_plane_router_list,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn bootstore_no_update_when_desired_blueprint_is_strictly_older() {
        let logctx = test_setup_log(
            "bootstore_no_update_when_desired_blueprint_is_strictly_older",
        );

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let current = make_system_networking_config(
            rnc.clone(),
            Some(make_blueprint_config(5, make_nat_entries("172.20.26.3"))),
        );

        // Intentionally use different NAT entries here; confirm that we do not
        // report needing an update because the generation here (2) is stale
        // (current is 5).
        let desired_blueprint =
            make_blueprint_config(2, make_nat_entries("172.20.26.4"));

        assert!(!does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &current.switch_router_configs,
            &current.control_plane_router_list,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn bootstore_update_when_desired_blueprint_is_newer_and_nat_differs() {
        let logctx = test_setup_log(
            "bootstore_update_when_desired_blueprint_is_newer_and_nat_differs",
        );

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let current = make_system_networking_config(
            rnc.clone(),
            Some(make_blueprint_config(2, make_nat_entries("172.20.26.3"))),
        );
        let desired_blueprint =
            make_blueprint_config(5, make_nat_entries("172.20.26.4"));

        assert!(does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &current.switch_router_configs,
            &current.control_plane_router_list,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    // Pins the "just-transitioned to tracking generation" case explicitly
    // noted in the comment inside `does_bootstore_need_update()`: at gen=1,
    // the bootstore may have been written with stale NAT entries by a Nexus
    // that pre-dates this generation field. Equal gens with different NATs
    // must still trigger an update so the correct gen=1 value gets written.
    //
    // With the current implementation the test would still pass with any
    // generation (1 isn't special), but we only need to test that we handle
    // this case for generation 1. We never expect a blueprint to have different
    // NAT entries without bumping the associated generation number.
    #[test]
    fn bootstore_update_when_blueprints_equal_and_nat_differs_at_gen_1() {
        let logctx = test_setup_log(
            "bootstore_update_when_blueprints_equal_and_nat_differs_at_gen_1",
        );

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let current = make_system_networking_config(
            rnc.clone(),
            Some(make_blueprint_config(1, make_nat_entries("172.20.26.3"))),
        );
        let desired_blueprint =
            make_blueprint_config(1, make_nat_entries("172.20.26.4"));

        assert!(does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &current.switch_router_configs,
            &current.control_plane_router_list,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn bootstore_update_when_nat_matches_but_rnc_differs() {
        let logctx =
            test_setup_log("bootstore_update_when_nat_matches_but_rnc_differs");

        let nat = make_nat_entries("172.20.26.3");
        let current_rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let desired_rnc = make_rack_network_config("fd00:1122:3344:200::/56");
        let desired_blueprint = make_blueprint_config(3, nat);

        let current = make_system_networking_config(
            current_rnc,
            Some(desired_blueprint.clone()),
        );

        assert!(does_bootstore_need_update(
            &current,
            &desired_rnc,
            &desired_blueprint,
            &current.switch_router_configs,
            &current.control_plane_router_list,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn bootstore_no_update_when_everything_matches() {
        let logctx =
            test_setup_log("bootstore_no_update_when_everything_matches");

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let nat = make_nat_entries("172.20.26.3");
        let desired_blueprint = make_blueprint_config(3, nat);

        let current = make_system_networking_config(
            rnc.clone(),
            Some(desired_blueprint.clone()),
        );

        assert!(!does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &current.switch_router_configs,
            &current.control_plane_router_list,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn bootstore_update_when_router_configs_differ() {
        let logctx =
            test_setup_log("bootstore_update_when_router_configs_differ");

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let desired_blueprint =
            make_blueprint_config(3, make_nat_entries("172.20.26.3"));
        let current = make_system_networking_config(
            rnc.clone(),
            Some(desired_blueprint.clone()),
        );

        let mut desired_router_configs = SwitchRouterConfigs::new();
        desired_router_configs.insert(SwitchSlot::Switch0, Vec::new());
        assert_ne!(current.switch_router_configs, desired_router_configs);

        assert!(does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &desired_router_configs,
            &current.control_plane_router_list,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn bootstore_update_when_control_plane_router_list_differs() {
        let logctx = test_setup_log(
            "bootstore_update_when_control_plane_router_list_differs",
        );

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let desired_blueprint =
            make_blueprint_config(3, make_nat_entries("172.20.26.3"));
        let current = make_system_networking_config(
            rnc.clone(),
            Some(desired_blueprint.clone()),
        );

        let desired_list = vec![
            RouterConfigListEntry {
                priority: 500,
                router_id: Some(
                    "4f9c2ea1-53a3-4b76-9cf8-7f2be4e3e6c1".parse().unwrap(),
                ),
            },
            RouterConfigListEntry { priority: 1000, router_id: None },
        ];
        assert_ne!(current.control_plane_router_list, desired_list);

        assert!(does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &current.switch_router_configs,
            &desired_list,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }
}

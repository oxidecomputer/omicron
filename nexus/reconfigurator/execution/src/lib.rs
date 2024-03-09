// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execution of Nexus blueprints
//!
//! See `nexus_reconfigurator_planning` crate-level docs for background.

use anyhow::{anyhow, Context};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::identity::Asset;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::MGD_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::address::SLED_PREFIX;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use uuid::Uuid;

mod datasets;
mod dns;
mod omicron_zones;
mod resource_allocation;

// XXX-dap
#[derive(Debug, Default)]
pub struct ExecutionOverrides {
    pub dendrite_ports: BTreeMap<Uuid, u16>,
    pub mgs_ports: BTreeMap<Uuid, u16>,
    pub mgd_ports: BTreeMap<Uuid, u16>,
    pub switch_zone_ips: BTreeMap<Uuid, Ipv6Addr>,
}

impl ExecutionOverrides {
    pub fn override_dendrite_port(&mut self, sled_id: Uuid, port: u16) {
        self.dendrite_ports.insert(sled_id, port);
    }

    fn dendrite_port(&self, sled_id: Uuid) -> u16 {
        self.dendrite_ports.get(&sled_id).copied().unwrap_or(DENDRITE_PORT)
    }

    pub fn override_mgs_port(&mut self, sled_id: Uuid, port: u16) {
        self.mgs_ports.insert(sled_id, port);
    }

    fn mgs_port(&self, sled_id: Uuid) -> u16 {
        self.mgs_ports.get(&sled_id).copied().unwrap_or(MGS_PORT)
    }

    pub fn override_mgd_port(&mut self, sled_id: Uuid, port: u16) {
        self.mgd_ports.insert(sled_id, port);
    }

    fn mgd_port(&self, sled_id: Uuid) -> u16 {
        self.mgd_ports.get(&sled_id).copied().unwrap_or(MGD_PORT)
    }

    pub fn override_switch_zone_ip(&mut self, sled_id: Uuid, addr: Ipv6Addr) {
        self.switch_zone_ips.insert(sled_id, addr);
    }

    fn switch_zone_ip(
        &self,
        sled_id: Uuid,
        sled_subnet: Ipv6Subnet<SLED_PREFIX>,
    ) -> Ipv6Addr {
        self.switch_zone_ips
            .get(&sled_id)
            .copied()
            .unwrap_or_else(|| get_switch_zone_address(sled_subnet))
    }
}

struct Sled {
    id: Uuid,
    sled_agent_address: SocketAddrV6,
    is_scrimlet: bool,
}

impl Sled {
    pub fn subnet(&self) -> Ipv6Subnet<SLED_PREFIX> {
        Ipv6Subnet::<SLED_PREFIX>::new(*self.sled_agent_address.ip())
    }
}

impl From<nexus_db_model::Sled> for Sled {
    fn from(value: nexus_db_model::Sled) -> Self {
        Sled {
            id: value.id(),
            sled_agent_address: value.address(),
            is_scrimlet: value.is_scrimlet(),
        }
    }
}

/// Make one attempt to realize the given blueprint, meaning to take actions to
/// alter the real system to match the blueprint
///
/// The assumption is that callers are running this periodically or in a loop to
/// deal with transient errors or changes in the underlying system state.
pub async fn realize_blueprint<S>(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus_label: S,
    overrides: &ExecutionOverrides,
) -> Result<(), Vec<anyhow::Error>>
where
    String: From<S>,
{
    let opctx = opctx.child(BTreeMap::from([(
        "comment".to_string(),
        blueprint.comment.clone(),
    )]));

    info!(
        opctx.log,
        "attempting to realize blueprint";
        "blueprint_id" => ?blueprint.id
    );

    resource_allocation::ensure_zone_resources_allocated(
        &opctx,
        datastore,
        &blueprint.omicron_zones,
    )
    .await
    .map_err(|err| vec![err])?;

    let sleds_by_id: BTreeMap<Uuid, _> = datastore
        .sled_list_all_batched(&opctx)
        .await
        .context("listing all sleds")
        .map_err(|e| vec![e])?
        .into_iter()
        .map(|db_sled| (db_sled.id(), Sled::from(db_sled)))
        .collect();
    omicron_zones::deploy_zones(&opctx, &sleds_by_id, &blueprint.omicron_zones)
        .await?;

    datasets::ensure_crucible_dataset_records_exist(
        &opctx,
        datastore,
        blueprint.all_omicron_zones().map(|(_sled_id, zone)| zone),
    )
    .await
    .map_err(|err| vec![err])?;

    dns::deploy_dns(
        &opctx,
        datastore,
        String::from(nexus_label),
        blueprint,
        &sleds_by_id,
        &overrides,
    )
    .await
    .map_err(|e| vec![anyhow!("{}", InlineErrorChain::new(&e))])?;

    Ok(())
}

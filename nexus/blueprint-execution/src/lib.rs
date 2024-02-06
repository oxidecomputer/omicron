// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execution of Nexus blueprints
//!
//! See `nexus_deployment` crate-level docs for background.

use anyhow::{anyhow, Context};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::identity::Asset;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;
use uuid::Uuid;

mod dns;
mod omicron_zones;

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

    dns::deploy_dns(&opctx, &datastore, nexus_label, &blueprint, &sleds_by_id)
        .await
        .map_err(|e| vec![anyhow!("{}", InlineErrorChain::new(&e))])
}

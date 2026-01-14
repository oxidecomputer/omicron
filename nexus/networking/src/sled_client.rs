// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functionality for constructing sled-agent clients.

use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use sled_agent_client::Client as SledAgentClient;
use sled_hardware_types::BaseboardId;
use slog::Logger;
use slog::o;
use std::net::SocketAddrV6;

pub fn sled_lookup<'a>(
    datastore: &'a DataStore,
    opctx: &'a OpContext,
    sled_id: SledUuid,
) -> LookupResult<lookup::Sled<'a>> {
    let sled = LookupPath::new(opctx, datastore).sled_id(sled_id);
    Ok(sled)
}

pub fn default_reqwest_client_builder() -> reqwest::ClientBuilder {
    let dur = std::time::Duration::from_secs(60);
    reqwest::ClientBuilder::new().connect_timeout(dur).timeout(dur)
}

pub async fn sled_client(
    datastore: &DataStore,
    lookup_opctx: &OpContext,
    sled_id: SledUuid,
    log: &Logger,
) -> Result<SledAgentClient, Error> {
    let client = default_reqwest_client_builder().build().unwrap();
    sled_client_ext(datastore, lookup_opctx, sled_id, log, client).await
}

pub async fn sled_client_ext(
    datastore: &DataStore,
    lookup_opctx: &OpContext,
    sled_id: SledUuid,
    log: &Logger,
    client: reqwest::Client,
) -> Result<SledAgentClient, Error> {
    let (.., sled) =
        sled_lookup(datastore, lookup_opctx, sled_id)?.fetch().await?;

    Ok(sled_client_from_address_ext(sled_id, sled.address(), log, client))
}

/// Lookup a sled agent by its `BaseboardId` only if it is commmissioned and part
/// of a rack given by `rack_id`.
///
/// In some cases we want to lookup a sled agent by its `BaseboardId`. However,
/// because a `BaseboardId` can map to multiple sled-agents, but only one is
/// currently commissioned, we want to find the commissioned one. We also want
/// to ensure that the baseboard is still inserted in the rack we expect, as
/// given by `rack_id`.
pub async fn sled_client_by_baseboard_id_and_rack_id_if_commissioned(
    datastore: &DataStore,
    opctx: &OpContext,
    baseboard_id: &BaseboardId,
    rack_id: RackUuid,
    log: &Logger,
) -> Result<Option<SledAgentClient>, Error> {
    let client = default_reqwest_client_builder().build().unwrap();
    sled_client_by_baseboard_id_and_rack_id_if_commissioned_ext(
        datastore,
        opctx,
        baseboard_id,
        rack_id,
        log,
        client,
    )
    .await
}

pub async fn sled_client_by_baseboard_id_and_rack_id_if_commissioned_ext(
    datastore: &DataStore,
    opctx: &OpContext,
    baseboard_id: &BaseboardId,
    rack_id: RackUuid,
    log: &Logger,
    client: reqwest::Client,
) -> Result<Option<SledAgentClient>, Error> {
    let Some(sled) = datastore
        .sled_get_commissioned_by_baseboard_and_rack_id(
            opctx,
            rack_id,
            baseboard_id,
        )
        .await?
    else {
        return Ok(None);
    };

    Ok(Some(sled_client_from_address_ext(
        sled.identity.id.into(),
        sled.address(),
        log,
        client,
    )))
}

pub fn sled_client_from_address(
    sled_id: SledUuid,
    address: SocketAddrV6,
    log: &Logger,
) -> SledAgentClient {
    let client = default_reqwest_client_builder().build().unwrap();
    sled_client_from_address_ext(sled_id, address, log, client)
}

pub fn sled_client_from_address_ext(
    sled_id: SledUuid,
    address: SocketAddrV6,
    log: &Logger,
    client: reqwest::Client,
) -> SledAgentClient {
    let log = log.new(o!("SledAgent" => sled_id.to_string()));
    SledAgentClient::new_with_client(&format!("http://{address}"), client, log)
}

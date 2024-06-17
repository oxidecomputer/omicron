// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functionality for constructing sled-agent clients.

use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use sled_agent_client::Client as SledAgentClient;
use slog::o;
use slog::Logger;
use std::net::SocketAddrV6;
use uuid::Uuid;

pub fn sled_lookup<'a>(
    datastore: &'a DataStore,
    opctx: &'a OpContext,
    sled_id: Uuid,
) -> LookupResult<lookup::Sled<'a>> {
    let sled = LookupPath::new(opctx, datastore).sled_id(sled_id);
    Ok(sled)
}

pub async fn sled_client(
    datastore: &DataStore,
    lookup_opctx: &OpContext,
    sled_id: Uuid,
    log: &Logger,
) -> Result<SledAgentClient, Error> {
    let (.., sled) =
        sled_lookup(datastore, lookup_opctx, sled_id)?.fetch().await?;

    Ok(sled_client_from_address(sled_id, sled.address(), log))
}

pub fn sled_client_from_address(
    sled_id: Uuid,
    address: SocketAddrV6,
    log: &Logger,
) -> SledAgentClient {
    let log = log.new(o!("SledAgent" => sled_id.to_string()));
    SledAgentClient::new_with_client(
        &format!("http://{address}"),
        shared_client::timeout::<60>(),
        log,
    )
}

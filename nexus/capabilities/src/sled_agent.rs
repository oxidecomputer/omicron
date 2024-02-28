// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus capabilities for creating sled-agent clients

use crate::Base;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use sled_agent_client::Client;
use slog::o;
use std::net::SocketAddrV6;
use std::sync::Arc;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait SledAgent: Base {
    /// Operational context used for sled lookups to create sled agent clients
    fn opctx_sled_client(&self) -> &OpContext;

    fn sled_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        sled_id: Uuid,
    ) -> lookup::Sled<'a> {
        LookupPath::new(opctx, self.datastore()).sled_id(sled_id)
    }

    fn sled_client(&self, id: Uuid, address: SocketAddrV6) -> Client {
        let log = self.log().new(o!("SledAgent" => id.to_string()));
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .unwrap();
        Client::new_with_client(&format!("http://{address}"), client, log)
    }

    async fn sled_client_by_id(
        &self,
        id: Uuid,
    ) -> Result<Arc<Client>, omicron_common::api::external::Error> {
        // TODO: We should consider injecting connection pooling here,
        // but for now, connections to sled agents are constructed
        // on an "as requested" basis.
        //
        // Frankly, returning an "Arc" here without a connection pool is a
        // little silly; it's not actually used if each client connection exists
        // as a one-shot.
        let (.., sled) =
            self.sled_lookup(self.opctx_sled_client(), id).fetch().await?;

        Ok(Arc::new(self.sled_client(id, sled.address())))
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::SocketAddr;

use crate::CockroachCli;
use anyhow::Context;
use anyhow::bail;
use dropshot::HttpError;
use omicron_uuid_kinds::OmicronZoneUuid;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use tokio::sync::OnceCell;

pub struct ServerContext {
    zone_id: OmicronZoneUuid,
    cockroach_cli: CockroachCli,
    // Cockroach node IDs never change; we defer contacting our local node to
    // ask for its ID until we need to, but once we have it we don't need to ask
    // again.
    node_id: OnceCell<String>,
    log: Logger,
}

impl ServerContext {
    pub fn new(
        zone_id: OmicronZoneUuid,
        cockroach_cli: CockroachCli,
        log: Logger,
    ) -> Self {
        Self { zone_id, cockroach_cli, node_id: OnceCell::new(), log }
    }

    pub fn log(&self) -> &Logger {
        &self.log
    }

    pub fn cockroach_cli(&self) -> &CockroachCli {
        &self.cockroach_cli
    }

    pub fn zone_id(&self) -> OmicronZoneUuid {
        self.zone_id
    }

    pub async fn node_id(&self) -> Result<&str, HttpError> {
        match self
            .node_id
            .get_or_try_init(|| self.read_node_id_from_cockroach())
            .await
        {
            Ok(id) => Ok(id.as_str()),
            Err(err) => {
                let message = format!(
                    "failed to read node ID from local cockroach instance: \
                    {err:#}",
                );
                Err(HttpError {
                    status_code: dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                    error_code: None,
                    external_message: message.clone(),
                    internal_message: message,
                    headers: None,
                })
            }
        }
    }

    async fn read_node_id_from_cockroach(&self) -> anyhow::Result<String> {
        let cockroach_address = self.cockroach_cli().cockroach_address();
        // TODO-cleanup This connection string is duplicated in Nexus - maybe we
        // should centralize it? I'm not sure where we could put it;
        // omicron_common, perhaps?
        let connect_url = format!(
            "postgresql://root@{cockroach_address}/omicron?sslmode=disable",
        );
        let (client, connection) =
            tokio_postgres::connect(&connect_url, tokio_postgres::NoTls)
                .await
                .with_context(|| {
                    format!("failed to connect to {connect_url}")
                })?;

        let log = self.log.clone();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                slog::warn!(
                    log, "connection error reading node ID";
                    "err" => InlineErrorChain::new(&e),
                );
            }
        });

        // This uses an undocumented internal function - not awesome, but we're
        // told this is "unlikely to change for some time".
        // https://github.com/cockroachdb/cockroach/issues/124988 requests that
        // this be documented (or an alternative be documented / provided).
        let row = client
            .query_one("SELECT crdb_internal.node_id()::TEXT", &[])
            .await
            .context("failed to send node ID query")?;

        let node_id = row
            .try_get(0)
            .context("failed to read results of node ID query")?;

        // We'll be paranoid: While it seems unlikely we could ever get an
        // incorrect node ID from the internal builtin, since it's not
        // documented, we don't know for sure if it's possible for our query to
        // be forwarded to a different node. Let's also run `NodeStatus`, and
        // ensure that this node ID's address matches the address of our local
        // crdb instance.
        let node_statuses = self
            .cockroach_cli()
            .node_status()
            .await
            .context("failed to get node status")?;

        let our_node_status = node_statuses
            .iter()
            .find(|status| status.node_id == node_id)
            .with_context(|| {
                format!(
                    "node status did not include information for our node ID \
                    ({node_id}): {node_statuses:?}"
                )
            })?;

        if our_node_status.address != SocketAddr::V6(cockroach_address) {
            bail!(
                "node ID / address mismatch: we fetched node ID {node_id} \
                from our local cockroach at {cockroach_address}, but \
                `node status` reported this node ID at address {}",
                our_node_status.address
            )
        }

        Ok(node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_test_utils::db::TestDatabase;
    use omicron_test_utils::dev;
    use std::net::SocketAddrV6;
    use url::Url;

    #[tokio::test]
    async fn test_node_id() {
        let logctx = dev::test_setup_log("test_node_id");
        let db = TestDatabase::new_populate_nothing(&logctx.log).await;
        let crdb = db.crdb();

        // Construct a `ServerContext`.
        let db_url = crdb.listen_url().to_string();
        let url: Url = db_url.parse().expect("valid url");
        let cockroach_address: SocketAddrV6 = format!(
            "{}:{}",
            url.host().expect("url has host"),
            url.port().expect("url has port")
        )
        .parse()
        .expect("valid SocketAddrV6");
        let cli = CockroachCli::new(
            "cockroach".into(),
            cockroach_address,
            SocketAddr::V6(cockroach_address),
        );
        let context = ServerContext::new(
            OmicronZoneUuid::new_v4(),
            cli,
            logctx.log.clone(),
        );

        // We should be able to fetch a node id, and it should be `1` (since we
        // have a single-node test cockroach instance).
        let node_id =
            context.node_id().await.expect("successfully read node ID");
        assert_eq!(node_id, "1");

        // The `OnceCell` should be populated now; even if we shut down the DB,
        // we can still fetch the node ID.
        db.terminate().await;
        let node_id =
            context.node_id().await.expect("successfully read node ID");
        assert_eq!(node_id, "1");

        logctx.cleanup_successful();
    }
}

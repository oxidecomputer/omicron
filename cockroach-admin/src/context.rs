// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::CockroachCli;
use anyhow::Context;
use dropshot::HttpError;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use tokio::sync::OnceCell;

pub struct ServerContext {
    pub cockroach_cli: CockroachCli,
    // Cockroach node IDs never change; we defer contacting our local node to
    // ask for its ID until we need to, but once we have it we don't need to ask
    // again.
    node_id: OnceCell<String>,
    log: Logger,
}

impl ServerContext {
    pub fn new(cockroach_cli: CockroachCli, log: Logger) -> Self {
        Self { cockroach_cli, node_id: OnceCell::new(), log }
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
                    status_code: http::StatusCode::SERVICE_UNAVAILABLE,
                    error_code: None,
                    external_message: message.clone(),
                    internal_message: message,
                })
            }
        }
    }

    async fn read_node_id_from_cockroach(&self) -> anyhow::Result<String> {
        // TODO-cleanup This connection string is duplicated in Nexus - maybe we
        // should centralize it? I'm not sure where we could put it;
        // omicron_common, perhaps?
        let connect_url = format!(
            "postgresql://root@{}/omicron?sslmode=disable",
            self.cockroach_cli.cockroach_address()
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

        row.try_get(0).context("failed to read results of node ID query")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use std::net::SocketAddrV6;
    use url::Url;

    #[tokio::test]
    async fn test_node_id() {
        let logctx = dev::test_setup_log("test_node_id");
        let mut db = test_setup_database(&logctx.log).await;

        // Construct a `ServerContext`.
        let db_url = db.listen_url().to_string();
        let url: Url = db_url.parse().expect("valid url");
        let cockroach_address: SocketAddrV6 = format!(
            "{}:{}",
            url.host().expect("url has host"),
            url.port().expect("url has port")
        )
        .parse()
        .expect("valid SocketAddrV6");
        let cli = CockroachCli::new("cockroach".into(), cockroach_address);
        let context = ServerContext::new(cli, logctx.log.clone());

        // We should be able to fetch a node id, and it should be `1` (since we
        // have a single-node test cockroach instance).
        let node_id =
            context.node_id().await.expect("successfully read node ID");
        assert_eq!(node_id, "1");

        // The `OnceCell` should be populated now; even if we shut down the DB,
        // we can still fetch the node ID.
        db.cleanup().await.unwrap();
        let node_id =
            context.node_id().await.expect("successfully read node ID");
        assert_eq!(node_id, "1");

        logctx.cleanup_successful();
    }
}

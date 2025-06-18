// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `qorb` support for Crucible pantry connection pooling.

use anyhow::Context;
use anyhow::anyhow;
use internal_dns_resolver::QorbResolver;
use internal_dns_types::names::ServiceName;
use qorb::backend;
use qorb::pool;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;

/// Wrapper around a Crucible pantry client that also remembers its address.
///
/// In most cases when Nexus wants to pick a pantry, it doesn't actually want a
/// client right then, but instead wants to write down its address for subsequent
/// use (and reuse) later. This type carries around a `client` only to perform
/// health checks as supported by `qorb`; the rest of Nexus only accesses its
/// `address`.
#[derive(Debug)]
pub(crate) struct PooledPantryClient {
    client: crucible_pantry_client::Client,
    address: SocketAddrV6,
}

impl PooledPantryClient {
    pub(crate) fn address(&self) -> SocketAddrV6 {
        self.address
    }
}

/// A [`backend::Connector`] for [`PooledPantryClient`]s.
#[derive(Debug)]
struct PantryConnector;

#[async_trait::async_trait]
impl backend::Connector for PantryConnector {
    type Connection = PooledPantryClient;

    async fn connect(
        &self,
        backend: &backend::Backend,
    ) -> Result<Self::Connection, backend::Error> {
        let address = match backend.address {
            SocketAddr::V6(addr) => addr,
            SocketAddr::V4(addr) => {
                return Err(backend::Error::Other(anyhow!(
                    "unexpected IPv4 address for Crucible pantry: {addr}"
                )));
            }
        };
        let client =
            crucible_pantry_client::Client::new(&format!("http://{address}"));
        let mut conn = PooledPantryClient { client, address };
        self.is_valid(&mut conn).await?;
        Ok(conn)
    }

    async fn is_valid(
        &self,
        conn: &mut Self::Connection,
    ) -> Result<(), backend::Error> {
        conn.client
            .pantry_status()
            .await
            .with_context(|| {
                format!("failed to fetch pantry status from {}", conn.address())
            })
            .map_err(backend::Error::Other)?;

        Ok(())
    }
}

pub(crate) fn make_pantry_connection_pool(
    qorb_resolver: &QorbResolver,
) -> pool::Pool<PooledPantryClient> {
    match pool::Pool::new(
        "crucible-pantry".to_string(),
        qorb_resolver.for_service(ServiceName::CruciblePantry),
        Arc::new(PantryConnector),
        qorb::policy::Policy::default(),
    ) {
        Ok(pool) => pool,
        Err(e) => e.into_inner(),
    }
}

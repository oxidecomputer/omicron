// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`ExternalIp`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::ExternalIp;
use crate::db::model::IncompleteExternalIp;
use crate::db::model::IpKind;
use crate::db::model::Name;
use crate::db::pool::DbConnection;
use crate::db::queries::external_ip::NextExternalIp;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::{AsyncRunQueryDsl, PoolError};
use chrono::Utc;
use diesel::prelude::*;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::Name as ExternalName;
use std::net::IpAddr;
use std::str::FromStr;
use uuid::Uuid;

impl DataStore {
    /// Create an external IP address for source NAT for an instance.
    pub async fn allocate_instance_snat_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        instance_id: Uuid,
        pool_id: Uuid,
    ) -> CreateResult<ExternalIp> {
        let data = IncompleteExternalIp::for_instance_source_nat(
            ip_id,
            instance_id,
            pool_id,
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Create an Ephemeral IP address for an instance.
    pub async fn allocate_instance_ephemeral_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        instance_id: Uuid,
        pool_name: Option<Name>,
    ) -> CreateResult<ExternalIp> {
        let name = pool_name.unwrap_or_else(|| {
            Name(ExternalName::from_str("default").unwrap())
        });

        // TODO: this is the whole thing -- update this call to EITHER look up
        // the pool by name (only allowing access to pools matching the scope of
        // the current project, i.e., you can pick a pool by name only if it's
        // scoped to your project, silo, or the whole fleet) or use the default
        // logic in ip_pool_fetch_default_for

        let (.., pool) = self
            .ip_pools_fetch_for(opctx, authz::Action::CreateChild, &name)
            .await?;
        let pool_id = pool.identity.id;

        let data =
            IncompleteExternalIp::for_ephemeral(ip_id, instance_id, pool_id);
        self.allocate_external_ip(opctx, data).await
    }

    /// Allocates an IP address for internal service usage.
    pub async fn allocate_service_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        name: &Name,
        description: &str,
        service_id: Uuid,
    ) -> CreateResult<ExternalIp> {
        let (.., pool) = self.ip_pools_service_lookup(opctx).await?;

        let data = IncompleteExternalIp::for_service(
            ip_id,
            name,
            description,
            service_id,
            pool.id(),
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Allocates an SNAT IP address for internal service usage.
    pub async fn allocate_service_snat_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        service_id: Uuid,
    ) -> CreateResult<ExternalIp> {
        let (.., pool) = self.ip_pools_service_lookup(opctx).await?;

        let data = IncompleteExternalIp::for_service_snat(
            ip_id,
            service_id,
            pool.id(),
        );
        self.allocate_external_ip(opctx, data).await
    }

    async fn allocate_external_ip(
        &self,
        opctx: &OpContext,
        data: IncompleteExternalIp,
    ) -> CreateResult<ExternalIp> {
        let conn = self.pool_authorized(opctx).await?;
        Self::allocate_external_ip_on_connection(conn, data).await
    }

    /// Variant of [Self::allocate_external_ip] which may be called from a
    /// transaction context.
    pub(crate) async fn allocate_external_ip_on_connection<ConnErr>(
        conn: &(impl async_bb8_diesel::AsyncConnection<DbConnection, ConnErr>
              + Sync),
        data: IncompleteExternalIp,
    ) -> CreateResult<ExternalIp>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        let explicit_ip = data.explicit_ip().is_some();
        NextExternalIp::new(data).get_result_async(conn).await.map_err(|e| {
            use async_bb8_diesel::ConnectionError::Query;
            use async_bb8_diesel::PoolError::Connection;
            use diesel::result::Error::NotFound;
            let e = PoolError::from(e);
            match e {
                Connection(Query(NotFound)) => {
                    if explicit_ip {
                        Error::invalid_request(
                            "Requested external IP address not available",
                        )
                    } else {
                        Error::invalid_request(
                            "No external IP addresses available",
                        )
                    }
                }
                _ => crate::db::queries::external_ip::from_pool(e),
            }
        })
    }

    /// Allocates an explicit Floating IP address for an internal service.
    ///
    /// Unlike the other IP allocation requests, this does not search for an
    /// available IP address, it asks for one explicitly.
    pub async fn allocate_explicit_service_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        name: &Name,
        description: &str,
        service_id: Uuid,
        ip: IpAddr,
    ) -> CreateResult<ExternalIp> {
        let (.., pool) = self.ip_pools_service_lookup(opctx).await?;
        let data = IncompleteExternalIp::for_service_explicit(
            ip_id,
            name,
            description,
            service_id,
            pool.id(),
            ip,
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Allocates an explicit SNAT IP address for an internal service.
    ///
    /// Unlike the other IP allocation requests, this does not search for an
    /// available IP address, it asks for one explicitly.
    pub async fn allocate_explicit_service_snat_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        service_id: Uuid,
        ip: IpAddr,
        port_range: (u16, u16),
    ) -> CreateResult<ExternalIp> {
        let (.., pool) = self.ip_pools_service_lookup(opctx).await?;
        let data = IncompleteExternalIp::for_service_explicit_snat(
            ip_id,
            service_id,
            pool.id(),
            ip,
            port_range,
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Deallocate the external IP address with the provided ID.
    ///
    /// To support idempotency, such as in saga operations, this method returns
    /// an extra boolean, rather than the usual `DeleteResult`. The meaning of
    /// return values are:
    /// - `Ok(true)`: The record was deleted during this call
    /// - `Ok(false)`: The record was already deleted, such as by a previous
    /// call
    /// - `Err(_)`: Any other condition, including a non-existent record.
    pub async fn deallocate_external_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
    ) -> Result<bool, Error> {
        use db::schema::external_ip::dsl;
        let now = Utc::now();
        diesel::update(dsl::external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(ip_id))
            .set(dsl::time_deleted.eq(now))
            .check_if_exists::<ExternalIp>(ip_id)
            .execute_and_check(self.pool_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Delete all external IP addresses associated with the provided instance
    /// ID.
    ///
    /// This method returns the number of records deleted, rather than the usual
    /// `DeleteResult`. That's mostly useful for tests, but could be important
    /// if callers have some invariants they'd like to check.
    // TODO-correctness: This can't be used for Floating IPs, we'll need a
    // _detatch_ method for that.
    pub async fn deallocate_external_ip_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error> {
        use db::schema::external_ip::dsl;
        let now = Utc::now();
        diesel::update(dsl::external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::is_service.eq(false))
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::kind.ne(IpKind::Floating))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Fetch all external IP addresses of any kind for the provided instance
    pub async fn instance_lookup_external_ips(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> LookupResult<Vec<ExternalIp>> {
        use db::schema::external_ip::dsl;
        dsl::external_ip
            .filter(dsl::is_service.eq(false))
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::time_deleted.is_null())
            .select(ExternalIp::as_select())
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }
}

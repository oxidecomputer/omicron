// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`InstanceExternalIp`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::IncompleteInstanceExternalIp;
use crate::db::model::InstanceExternalIp;
use crate::db::model::IpKind;
use crate::db::model::Name;
use crate::db::queries::external_ip::NextExternalIp;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use uuid::Uuid;

impl DataStore {
    /// Create an external IP address for source NAT for an instance.
    // TODO-correctness: This should be made idempotent.
    pub async fn allocate_instance_snat_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        project_id: Uuid,
        instance_id: Uuid,
    ) -> CreateResult<InstanceExternalIp> {
        let data = IncompleteInstanceExternalIp::for_instance_source_nat(
            ip_id,
            project_id,
            instance_id,
            /* pool_id = */ None,
        );
        self.allocate_instance_external_ip(opctx, data).await
    }

    /// Create an Ephemeral IP address for an instance.
    pub async fn allocate_instance_ephemeral_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        project_id: Uuid,
        instance_id: Uuid,
        _pool_name: Option<Name>,
    ) -> CreateResult<InstanceExternalIp> {
        let data = IncompleteInstanceExternalIp::for_ephemeral(
            ip_id,
            project_id,
            instance_id,
            /* pool_id = */ None,
        );
        self.allocate_instance_external_ip(opctx, data).await
    }

    async fn allocate_instance_external_ip(
        &self,
        opctx: &OpContext,
        data: IncompleteInstanceExternalIp,
    ) -> CreateResult<InstanceExternalIp> {
        NextExternalIp::new(data)
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                use async_bb8_diesel::ConnectionError::Query;
                use async_bb8_diesel::PoolError::Connection;
                use diesel::result::Error::NotFound;
                match e {
                    Connection(Query(NotFound)) => Error::invalid_request(
                        "No external IP addresses available for new instance",
                    ),
                    _ => public_error_from_diesel_pool(e, ErrorHandler::Server),
                }
            })
    }

    /// Deallocate the external IP address with the provided ID.
    ///
    /// To support idempotency, such as in saga operations, this method returns
    /// an extra boolean, rather than the usual `DeleteResult`. The meaning of
    /// return values are:
    ///
    /// - `Ok(true)`: The record was deleted during this call
    /// - `Ok(false)`: The record was already deleted, such as by a previous
    /// call
    /// - `Err(_)`: Any other condition, including a non-existent record.
    pub async fn deallocate_instance_external_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
    ) -> Result<bool, Error> {
        use db::schema::instance_external_ip::dsl;
        let now = Utc::now();
        diesel::update(dsl::instance_external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(ip_id))
            .set(dsl::time_deleted.eq(now))
            .check_if_exists::<InstanceExternalIp>(ip_id)
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
    /// To support idempotency, such as in saga operations, this method returns
    /// the number of records deleted, rather than the usual `DeleteResult`.
    /// Callers should check this to verify their expectations about how many
    /// records _should_ have been deleted.
    // TODO-correctness: This can't be used for Floating IPs, we'll need a
    // _detatch_ method for that.
    pub async fn deallocate_instance_external_ip_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error> {
        use db::schema::instance_external_ip::dsl;
        let now = Utc::now();
        diesel::update(dsl::instance_external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::instance_id.eq(instance_id))
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
    ) -> LookupResult<Vec<InstanceExternalIp>> {
        use db::schema::instance_external_ip::dsl;
        dsl::instance_external_ip
            .filter(dsl::instance_id.eq(instance_id))
            .filter(dsl::time_deleted.is_null())
            .select(InstanceExternalIp::as_select())
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }
}

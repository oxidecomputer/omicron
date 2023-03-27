// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::DnsGroup;
use crate::db::model::DnsName;
use crate::db::model::DnsVersion;
use crate::db::model::DnsZone;
use crate::db::model::Generation;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::PoolError;
use diesel::prelude::*;
use nexus_db_model::InitialDnsGroup;
use nexus_types::internal_api::params::DnsKv;
use nexus_types::internal_api::params::DnsRecordKey;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::bail_unless;
use uuid::Uuid;

impl DataStore {
    pub async fn dns_zones_list(
        &self,
        opctx: &OpContext,
        dns_group: DnsGroup,
        pagparams: &DataPageParams<'_, String>,
    ) -> ListResultVec<DnsZone> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::dns_zone::dsl;
        paginated(dsl::dns_zone, dsl::zone_name, pagparams)
            .filter(dsl::dns_group.eq(dns_group))
            .select(DnsZone::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn dns_group_latest_version(
        &self,
        opctx: &OpContext,
        dns_group: DnsGroup,
    ) -> LookupResult<DnsVersion> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::dns_version::dsl;
        let versions = dsl::dns_version
            .filter(dsl::dns_group.eq(dns_group))
            .order_by(dsl::version.desc())
            .limit(1)
            .select(DnsVersion::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

        bail_unless!(
            versions.len() == 1,
            "expected exactly one latest version for DNS group {:?}, found {}",
            dns_group,
            versions.len()
        );

        Ok(versions.into_iter().next().unwrap())
    }

    pub async fn dns_names_list(
        &self,
        opctx: &OpContext,
        dns_zone_id: Uuid,
        version: Generation,
        pagparams: &DataPageParams<'_, String>,
    ) -> ListResultVec<DnsKv> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::dns_name::dsl;
        Ok(paginated(dsl::dns_name, dsl::name, pagparams)
            .filter(dsl::dns_zone_id.eq(dns_zone_id))
            .filter(dsl::version_added.le(version))
            .filter(
                dsl::version_removed
                    .is_null()
                    .or(dsl::version_removed.gt(version)),
            )
            .select(DnsName::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?
            .into_iter()
            .filter_map(|n: DnsName| {
                match serde_json::from_value(n.dns_record_data) {
                    Ok(records) => Some(DnsKv {
                        key: DnsRecordKey { name: n.name },
                        records,
                    }),
                    Err(error) => {
                        warn!(
                            opctx.log,
                            "failed to deserialize dns_name records: {:#}",
                            error;
                            "dns_zone_id" => n.dns_zone_id.to_string(),
                            "name" => n.name.to_string(),
                            "version_added" => n.version_added.to_string(),
                        );
                        None
                    }
                }
            })
            .collect())
    }

    pub async fn load_dns_data<ConnErr>(
        conn: &(impl async_bb8_diesel::AsyncConnection<
            crate::db::pool::DbConnection,
            ConnErr,
        > + Sync),
        dns: InitialDnsGroup,
    ) -> Result<(), Error>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        ConnErr: Into<PoolError>,
    {
        {
            use db::schema::dns_zone::dsl;
            diesel::insert_into(dsl::dns_zone)
                .values(dns.row_for_zone())
                .on_conflict((dsl::dns_group, dsl::zone_name))
                .do_nothing()
                .execute_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(
                        e.into(),
                        ErrorHandler::Server,
                    )
                })?;
        }

        {
            use db::schema::dns_version::dsl;
            diesel::insert_into(dsl::dns_version)
                .values(dns.row_for_version())
                .on_conflict((dsl::dns_group, dsl::version))
                .do_nothing()
                .execute_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(
                        e.into(),
                        ErrorHandler::Server,
                    )
                })?;
        }

        {
            use db::schema::dns_name::dsl;
            diesel::insert_into(dsl::dns_name)
                .values(dns.rows_for_names().map_err(|error| {
                    Error::internal_error(&format!(
                        "error serializing initial DNS data: {:#}",
                        error
                    ))
                })?)
                .on_conflict((dsl::dns_zone_id, dsl::version_added, dsl::name))
                .do_nothing()
                .execute_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(
                        e.into(),
                        ErrorHandler::Server,
                    )
                })?;
        }

        Ok(())
    }
}

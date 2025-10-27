// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::DataStoreConnection;
use crate::db::model::Name;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use nexus_db_model::IncompleteNetworkInterface;
use nexus_db_model::IpConfig;
use nexus_db_model::Probe;
use nexus_db_model::VpcSubnet;
use nexus_db_model::to_db_typed_uuid;
use nexus_types::external_api::shared::ProbeInfo;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_uuid_kinds::SledUuid;
use ref_cast::RefCast;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IpKind {
    Snat,
    Floating,
    Ephemeral,
}

impl From<nexus_db_model::IpKind> for IpKind {
    fn from(value: nexus_db_model::IpKind) -> Self {
        match value {
            nexus_db_model::IpKind::SNat => Self::Snat,
            nexus_db_model::IpKind::Ephemeral => Self::Ephemeral,
            nexus_db_model::IpKind::Floating => Self::Floating,
        }
    }
}

impl super::DataStore {
    /// List the probes for the given project.
    pub async fn probe_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<ProbeInfo> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use nexus_db_schema::schema::probe::dsl;
        use nexus_db_schema::schema::vpc_subnet::dsl as vpc_subnet_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let probes = match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::probe, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::probe,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::project_id.eq(authz_project.id()))
        .filter(dsl::time_deleted.is_null())
        .select(Probe::as_select())
        .load_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let mut result = Vec::with_capacity(probes.len());

        for probe in probes.into_iter() {
            let external_ips = self
                .probe_lookup_external_ips(opctx, probe.id())
                .await?
                .into_iter()
                .map(Into::into)
                .collect();

            let interface =
                self.probe_get_network_interface(opctx, probe.id()).await?;

            let vni = self.resolve_vpc_to_vni(opctx, interface.vpc_id).await?;

            let db_subnet = vpc_subnet_dsl::vpc_subnet
                .filter(vpc_subnet_dsl::id.eq(interface.subnet_id))
                .select(VpcSubnet::as_select())
                .first_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            let mut interface: NetworkInterface =
                interface.into_internal(db_subnet.ipv4_block.0.into());

            interface.vni = vni.0;

            result.push(ProbeInfo {
                id: probe.id(),
                name: probe.name().clone(),
                sled: probe.sled(),
                interface,
                external_ips,
            })
        }

        Ok(result)
    }

    async fn resolve_probe_info(
        &self,
        opctx: &OpContext,
        probe: &Probe,
        conn: &DataStoreConnection,
    ) -> LookupResult<ProbeInfo> {
        use nexus_db_schema::schema::vpc_subnet::dsl as vpc_subnet_dsl;

        let external_ips = self
            .probe_lookup_external_ips(opctx, probe.id())
            .await?
            .into_iter()
            .map(Into::into)
            .collect();

        let interface =
            self.probe_get_network_interface(opctx, probe.id()).await?;

        let db_subnet = vpc_subnet_dsl::vpc_subnet
            .filter(vpc_subnet_dsl::id.eq(interface.subnet_id))
            .select(VpcSubnet::as_select())
            .first_async(&**conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let vni = self.resolve_vpc_to_vni(opctx, interface.vpc_id).await?;

        let mut interface: NetworkInterface =
            interface.into_internal(db_subnet.ipv4_block.0.into());
        interface.vni = vni.0;

        Ok(ProbeInfo {
            id: probe.id(),
            name: probe.name().clone(),
            sled: probe.sled(),
            interface,
            external_ips,
        })
    }

    /// List the probes for a given sled. This is used by sled agents for
    /// determining what probes they should be running.
    pub async fn probe_list_for_sled(
        &self,
        sled_id: SledUuid,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<ProbeInfo> {
        use nexus_db_schema::schema::probe::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let probes = paginated(dsl::probe, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::sled.eq(to_db_typed_uuid(sled_id)))
            .select(Probe::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let mut result = Vec::with_capacity(probes.len());

        for probe in probes.into_iter() {
            result.push(self.resolve_probe_info(opctx, &probe, &conn).await?);
        }

        Ok(result)
    }

    /// Get information about a particular probe given its name or id.
    pub async fn probe_get(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        name_or_id: &NameOrId,
    ) -> LookupResult<ProbeInfo> {
        use nexus_db_schema::schema::probe;
        use nexus_db_schema::schema::probe::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;

        let name_or_id = name_or_id.clone();

        let probe = match name_or_id {
            NameOrId::Name(name) => dsl::probe
                .filter(probe::name.eq(name.to_string()))
                .filter(probe::time_deleted.is_null())
                .filter(probe::project_id.eq(authz_project.id()))
                .select(Probe::as_select())
                .limit(1)
                .first_async::<Probe>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByLookup(
                            ResourceType::Probe,
                            LookupType::ByName(name.to_string()),
                        ),
                    )
                }),
            NameOrId::Id(id) => dsl::probe
                .filter(probe::id.eq(id))
                .filter(probe::project_id.eq(authz_project.id()))
                .select(Probe::as_select())
                .limit(1)
                .first_async::<Probe>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByLookup(
                            ResourceType::Probe,
                            LookupType::ById(id),
                        ),
                    )
                }),
        }?;

        self.resolve_probe_info(opctx, &probe, &conn).await
    }

    /// Add a probe to the data store.
    pub async fn probe_create(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        probe: &Probe,
        ip_pool: Option<authz::IpPool>,
    ) -> CreateResult<Probe> {
        //TODO in transaction
        use nexus_db_schema::schema::probe::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;

        let _eip = self
            .allocate_probe_ephemeral_ip(
                opctx,
                Uuid::new_v4(),
                probe.id(),
                ip_pool,
            )
            .await?;

        let default_name = omicron_common::api::external::Name::try_from(
            "default".to_string(),
        )
        .unwrap();
        let internal_default_name = db::model::Name::from(default_name.clone());

        let (.., db_subnet) = LookupPath::new(opctx, self)
            .project_id(authz_project.id())
            .vpc_name(&internal_default_name)
            .vpc_subnet_name(&internal_default_name)
            .fetch()
            .await?;

        let incomplete = IncompleteNetworkInterface::new_probe(
            Uuid::new_v4(),
            probe.id(),
            db_subnet,
            IdentityMetadataCreateParams {
                name: probe.name().clone(),
                description: format!(
                    "default primary interface for {}",
                    probe.name(),
                ),
            },
            IpConfig::auto_ipv4(),
            None, //Request MAC address assignment
        )?;

        let _ifx = self
            .probe_create_network_interface(opctx, incomplete)
            .await
            .map_err(|e| {
                omicron_common::api::external::Error::InternalError {
                    internal_message: format!(
                        "create network interface: {e:?}"
                    ),
                }
            })?;

        let result = diesel::insert_into(dsl::probe)
            .values(probe.clone())
            .returning(Probe::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(result)
    }

    /// Remove a probe from the data store.
    pub async fn probe_delete(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        name_or_id: &NameOrId,
    ) -> DeleteResult {
        use nexus_db_schema::schema::probe;
        use nexus_db_schema::schema::probe::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;

        let name_or_id = name_or_id.clone();

        //TODO in transaction
        let id = match name_or_id {
            NameOrId::Name(name) => dsl::probe
                .filter(probe::name.eq(name.to_string()))
                .filter(probe::time_deleted.is_null())
                .filter(probe::project_id.eq(authz_project.id()))
                .select(probe::id)
                .limit(1)
                .first_async::<Uuid>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?,
            NameOrId::Id(id) => id,
        };

        self.deallocate_external_ip_by_probe_id(opctx, id).await?;

        self.probe_delete_all_network_interfaces(opctx, id).await?;

        diesel::update(dsl::probe)
            .filter(dsl::id.eq(id))
            .filter(dsl::project_id.eq(authz_project.id()))
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }
}

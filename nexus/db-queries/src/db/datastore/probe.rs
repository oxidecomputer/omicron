// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::DataStoreConnection;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::model::Name;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::JoinOnDsl as _;
use diesel::NullableExpressionMethods as _;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use nexus_db_model::IncompleteNetworkInterface;
use nexus_db_model::IpConfig;
use nexus_db_model::Probe;
use nexus_db_model::VpcSubnet;
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
use omicron_uuid_kinds::GenericUuid as _;
use omicron_uuid_kinds::SledUuid;
use ref_cast::RefCast;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_client::types::ProbeCreate;
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
    ///
    /// This also returns the create-params needed to notify the sled-agent
    /// about this new probe. This is a little awkward, but that type is built
    /// from a number of different tables, and doing so avoids another lookup,
    /// one which requires a few joins.
    pub async fn probe_create(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        probe: &Probe,
        ip_pool: Option<authz::IpPool>,
    ) -> CreateResult<(Probe, ProbeCreate)> {
        // TODO-correctness: These need to be in a transaction.
        // See https://github.com/oxidecomputer/omicron/issues/9340.
        use nexus_db_schema::schema::probe::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;

        let eip = self
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
        let ipv4_subnet = db_subnet.ipv4_block.0.into();

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

        let ifx = self
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

        // Construct create-time parameters for the sled-agent
        //
        // The `vpc_subnet` table has the VPC ID, but we always need the VNI to
        // provide to OPTE. Look that up and store it now.
        let vni = self.resolve_vpc_to_vni(opctx, ifx.vpc_id).await?.0;
        let interface =
            NetworkInterface { vni, ..ifx.into_internal(ipv4_subnet) };
        let params = ProbeCreate {
            id: result.id(),
            external_ips: vec![sled_agent_client::types::ExternalIp {
                first_port: eip.first_port.into(),
                ip: eip.ip.ip(),
                kind: match eip.kind {
                    nexus_db_model::IpKind::SNat => {
                        sled_agent_client::types::IpKind::Snat
                    }
                    nexus_db_model::IpKind::Ephemeral => {
                        sled_agent_client::types::IpKind::Ephemeral
                    }
                    nexus_db_model::IpKind::Floating => {
                        sled_agent_client::types::IpKind::Floating
                    }
                },
                last_port: eip.last_port.into(),
            }],
            interface,
        };

        Ok((result, params))
    }

    /// List the ID and create-time parameters for probes on a seld.
    ///
    /// The probe-create API in the sled-agent requires data built from a number
    /// of tables (probes, network interfaces, vpcs, etc). This method queries
    /// all of them (in batches) and constructs the creation parameters. It
    /// makes enough queries to get all records, so should be used in
    /// latency-insensitive contexts like background tasks.
    pub async fn list_all_probe_create_params_for_sled_batched(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> ListResultVec<ProbeCreate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        // NOTE: This paginator _must_ use ascending order. See
        // `list_probe_create_params_for_sled` for details.
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let mut all_probes_for_sled = Vec::new();
        while let Some(p) = paginator.next() {
            let mut batch = self
                .list_probe_create_params_for_sled(
                    opctx,
                    sled_id,
                    &p.current_pagparams(),
                )
                .await?;
            paginator = p.found_batch(&batch, &|probe| probe.id);
            all_probes_for_sled.append(&mut batch);
        }
        Ok(all_probes_for_sled)
    }

    // Construct a page of probe-create parameters for a specific sled.
    //
    // # Panics
    //
    // This panics if the `pagparams` is not in ascending order. This method
    // paginates a big join of all the tables needed to build these parameters.
    // We can't use the existing `paginated` method for that.
    async fn list_probe_create_params_for_sled(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<ProbeCreate> {
        use nexus_db_schema::schema::{
            external_ip, network_interface, probe, vpc, vpc_subnet,
        };

        /*
        assert_eq!(pagparams.direction, dropshot::PaginationOrder::Ascending);
        let marker = pagparams.marker.cloned().unwrap_or_default();
        */

        // TODO-correctness: This inner join below assumes exactly one external
        // IP for each probe. That's true today because of how we specify the
        // create-params in the external API. That has one IP Pool, and we
        // create exactly one external IP for the probe from that pool. If we
        // allow multiple IPs, then we'll need to change this to add group the
        // external IPs by the probe ID.
        //
        // In that case, we'd get rows like:
        //
        // probe_id, vpc stuff, vpc_subnet stuff, [eip0, eip1, ...]
        paginated(probe::dsl::probe, probe::dsl::id, pagparams)
            .inner_join(
                external_ip::dsl::external_ip
                    .on(external_ip::dsl::parent_id
                        .eq(probe::dsl::id.nullable())),
            )
            .inner_join(
                network_interface::dsl::network_interface
                    .on(network_interface::dsl::parent_id.eq(probe::dsl::id)),
            )
            .inner_join(
                vpc_subnet::dsl::vpc_subnet
                    .on(vpc_subnet::dsl::id
                        .eq(network_interface::dsl::subnet_id)),
            )
            .inner_join(
                vpc::dsl::vpc.on(vpc::dsl::id.eq(vpc_subnet::dsl::vpc_id)),
            )
            //.filter(probe::dsl::id.gt(marker))
            .filter(probe::dsl::sled.eq(sled_id.into_untyped_uuid()))
            .filter(probe::dsl::time_deleted.is_null())
            .filter(external_ip::dsl::time_deleted.is_null())
            .filter(network_interface::dsl::time_deleted.is_null())
            .filter(vpc_subnet::dsl::time_deleted.is_null())
            .filter(vpc::dsl::time_deleted.is_null())
            //.order(probe::dsl::id.asc())
            //.limit(pagparams.limit.get().into())
            .select((
                probe::dsl::id,
                external_ip::dsl::ip,
                external_ip::dsl::first_port,
                external_ip::dsl::last_port,
                external_ip::dsl::kind,
                nexus_db_model::NetworkInterface::as_select(),
                vpc_subnet::dsl::ipv4_block,
                vpc::dsl::vni,
            ))
            .get_results_async::<(
                Uuid,
                ipnetwork::IpNetwork,
                nexus_db_model::SqlU16,
                nexus_db_model::SqlU16,
                nexus_db_model::IpKind,
                nexus_db_model::NetworkInterface,
                // TODO: Need to extract IPv6 block when we support dual-stack
                // external IP addresses. See
                // https://github.com/oxidecomputer/omicron/issues/9318.
                nexus_db_model::Ipv4Net,
                nexus_db_model::Vni,
            )>(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(
                        |(
                            probe_id,
                            ip,
                            first_port,
                            last_port,
                            kind,
                            nic,
                            ipv4_block,
                            vni,
                        )| {
                            let kind = match kind {
                                nexus_db_model::IpKind::SNat => {
                                    sled_agent_client::types::IpKind::Snat
                                }
                                nexus_db_model::IpKind::Ephemeral => {
                                    sled_agent_client::types::IpKind::Ephemeral
                                }
                                nexus_db_model::IpKind::Floating => {
                                    sled_agent_client::types::IpKind::Floating
                                }
                            };
                            let external_ips =
                                vec![sled_agent_client::types::ExternalIp {
                                    first_port: first_port.into(),
                                    ip: ip.ip(),
                                    kind,
                                    last_port: last_port.into(),
                                }];
                            let interface = NetworkInterface {
                                vni: vni.0,
                                ..nic.into_internal((*ipv4_block).into())
                            };
                            ProbeCreate {
                                id: probe_id,
                                external_ips,
                                interface,
                            }
                        },
                    )
                    .collect()
            })
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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

        // TODO-correctness: This also needs to be in a transaction. See
        // https://github.com/oxidecomputer/omicron/issues/9340.
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

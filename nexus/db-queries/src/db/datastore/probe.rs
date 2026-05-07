// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::DataStoreConnection;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::datastore::multicast::ops;
use crate::db::model::Name;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::BoolExpressionMethods as _;
use diesel::JoinOnDsl as _;
use diesel::NullableExpressionMethods as _;
use diesel::OptionalExtension as _;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use ipnetwork::IpNetwork;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use nexus_db_model::IncompleteNetworkInterface;
use nexus_db_model::IpVersion;
use nexus_db_model::MemberParentRef;
use nexus_db_model::MulticastGroupMember;
use nexus_db_model::MulticastGroupMemberParentKind;
use nexus_db_model::MulticastGroupMemberState;
use nexus_db_model::Probe;
use nexus_db_model::VpcSubnet;
use nexus_types::external_api::instance::PrivateIpStackCreate;
use nexus_types::external_api::probe::ProbeInfo;
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
use omicron_uuid_kinds::GenericUuid as _;
use omicron_uuid_kinds::MulticastGroupUuid;
use omicron_uuid_kinds::ProbeUuid;
use omicron_uuid_kinds::SledUuid;
use ref_cast::RefCast;
use sled_agent_client::types::ProbeCreate;
use sled_agent_types::inventory::NetworkInterface;
use slog::warn;
use std::collections::HashMap;
use std::net::IpAddr;
use uuid::Uuid;

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

            let interface = NetworkInterface {
                vni: vni.0,
                ..interface.into_internal(
                    db_subnet.ipv4_block.0,
                    db_subnet.ipv6_block.0,
                )?
            };

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

        let interface = NetworkInterface {
            vni: vni.0,
            ..interface
                .into_internal(db_subnet.ipv4_block.0, db_subnet.ipv6_block.0)?
        };

        Ok(ProbeInfo {
            id: probe.id(),
            name: probe.name().clone(),
            sled: probe.sled(),
            interface,
            external_ips,
        })
    }

    /// Look up the hosting sled for a probe.
    ///
    /// Returns `Some(sled_id)` when the probe exists and is not
    /// soft-deleted, `None` otherwise. Probes carry `sled` directly on
    /// the row, so a single scalar lookup is sufficient (no VMM
    /// indirection like instances).
    ///
    /// This is used by the multicast RPW reconciler to advance probe-typed
    /// member rows through their lifecycle state machine without having to
    /// re-run authz on each pass. The reconciler owns only the member-row
    /// lifecycle (the underlay multicast membership itself is programmed by
    /// mg-lower).
    pub async fn probe_get_sled_for_multicast(
        &self,
        opctx: &OpContext,
        probe_id: ProbeUuid,
    ) -> Result<Option<SledUuid>, omicron_common::api::external::Error> {
        use nexus_db_schema::schema::probe::dsl;

        let sled = dsl::probe
            .filter(dsl::id.eq(probe_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .select(dsl::sled)
            .first_async::<Uuid>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| {
                nexus_db_errors::public_error_from_diesel(
                    e,
                    nexus_db_errors::ErrorHandler::Server,
                )
            })?;

        Ok(sled.map(SledUuid::from_untyped_uuid))
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
    /// The probe-row insert and any multicast member-attach operations run
    /// inside a single transaction so the probe distributor never observes a
    /// committed probe row without its committed member rows.
    ///
    /// `multicast_memberships` pairs each resolved group with its optional
    /// source IPs (already validated and capped by the caller).
    pub async fn probe_create(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        probe: &Probe,
        ip_pool: Option<authz::IpPool>,
        ip_version: Option<IpVersion>,
        multicast_memberships: &[(MulticastGroupUuid, Option<&[IpAddr]>)],
    ) -> CreateResult<Probe> {
        // TODO-correctness: The EIP and NIC allocation below remain outside the
        // transaction. They are pool-allocation operations that acquire their
        // own connections and run internal CTEs. Re-running them on a
        // transaction retry would carve out a fresh address or MAC off the pool
        // on each pass, leaking the prior one, so they cannot easily move into
        // the retry closure.
        //
        // A failure after allocation is instead reconciled by the best-effort
        // cleanup in `cleanup_failed_probe_create_allocations_on_error`.
        //
        // See https://github.com/oxidecomputer/omicron/issues/9340.
        use nexus_db_schema::schema::probe::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;

        self.allocate_probe_ephemeral_ip(
            opctx,
            Uuid::new_v4(),
            probe.id(),
            ip_pool,
            ip_version,
        )
        .await?;

        // Everything after the EIP allocation funnels its error through the
        // cleanup wrapper so a failed subnet lookup or NIC create does not
        // leak the EIP allocated just above.
        let res = async {
            let default_name = omicron_common::api::external::Name::try_from(
                "default".to_string(),
            )
            .unwrap();
            let internal_default_name =
                db::model::Name::from(default_name.clone());

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
                PrivateIpStackCreate::auto_dual_stack(),
                None, //Request MAC address assignment
            )?;

            self.probe_create_network_interface(opctx, incomplete)
                .await
                .map_err(|e| {
                    omicron_common::api::external::Error::InternalError {
                        internal_message: format!(
                            "create network interface: {e:?}"
                        ),
                    }
                })?;

            // Pre-convert the source IPs so the transaction closure (which may
            // run more than once on retry) does not (re-)borrow multicast memberships
            let probe_id = ProbeUuid::from_untyped_uuid(probe.id());
            let member_attach_specs: Vec<(
                MulticastGroupUuid,
                Option<Vec<IpNetwork>>,
            )> = multicast_memberships
                    .iter()
                    .map(|(group_id, source_ips)| {
                        let source_networks = source_ips.map(|ips| {
                            ips.iter().copied().map(IpNetwork::from).collect()
                        });
                        (*group_id, source_networks)
                    })
                    .collect();

            let err = OptionalError::new();
            self.transaction_retry_wrapper("probe_create")
                .transaction(&conn, |conn| {
                    let err = err.clone();
                    let probe = probe.clone();
                    let member_attach_specs = member_attach_specs.clone();
                    async move {
                        let probe = diesel::insert_into(dsl::probe)
                            .values(probe)
                            .returning(Probe::as_returning())
                            .get_result_async(&conn)
                            .await
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                })
                            })?;

                        for (group_id, source_networks) in member_attach_specs {
                            ops::member_attach::AttachMemberToGroupStatement::new(
                                group_id.into_untyped_uuid(),
                                MemberParentRef::Probe(probe_id),
                                Uuid::new_v4(),
                                source_networks,
                            )
                            .get_result_async::<MulticastGroupMember>(&conn)
                            .await
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    ops::member_attach::AttachMemberError::
                                        from_diesel(e)
                                        .into()
                                })
                            })?;
                        }

                        Ok(probe)
                    }
                })
                .await
                .map_err(|e| {
                    if let Some(err) = err.take() {
                        return err;
                    }
                    public_error_from_diesel(e, ErrorHandler::Server)
                })
        }
        .await;

        self.cleanup_failed_probe_create_allocations_on_error(
            opctx,
            probe.id(),
            res,
        )
        .await
    }

    async fn cleanup_failed_probe_create_allocations_on_error<T>(
        &self,
        opctx: &OpContext,
        probe_id: Uuid,
        res: CreateResult<T>,
    ) -> CreateResult<T> {
        if res.is_err() {
            self.cleanup_failed_probe_create_allocations(opctx, probe_id).await;
        }
        res
    }

    /// Best-effort cleanup for resources allocated before the probe row is
    /// committed. The caller returns the original create error even if cleanup
    /// has trouble.
    async fn cleanup_failed_probe_create_allocations(
        &self,
        opctx: &OpContext,
        probe_id: Uuid,
    ) {
        use nexus_db_model::IpKind;
        use nexus_db_model::NetworkInterfaceKind;
        use nexus_db_schema::schema::external_ip;
        use nexus_db_schema::schema::network_interface;

        let now = Utc::now();
        let conn = match self.pool_connection_authorized(opctx).await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(
                    opctx.log,
                    "failed to get connection for failed probe-create cleanup";
                    "probe_id" => %probe_id,
                    "error" => ?e,
                );
                return;
            }
        };

        if let Err(e) = diesel::update(external_ip::dsl::external_ip)
            .filter(external_ip::dsl::time_deleted.is_null())
            .filter(external_ip::dsl::is_probe.eq(true))
            .filter(external_ip::dsl::parent_id.eq(probe_id))
            .filter(external_ip::dsl::kind.eq(IpKind::Ephemeral))
            .set(external_ip::dsl::time_deleted.eq(now))
            .execute_async(&*conn)
            .await
        {
            warn!(
                opctx.log,
                "failed to clean up external IP after failed probe create";
                "probe_id" => %probe_id,
                "error" => ?e,
            );
        }

        if let Err(e) =
            diesel::update(network_interface::dsl::network_interface)
                .filter(network_interface::dsl::time_deleted.is_null())
                .filter(network_interface::dsl::parent_id.eq(probe_id))
                .filter(
                    network_interface::dsl::kind
                        .eq(NetworkInterfaceKind::Probe),
                )
                .set(network_interface::dsl::time_deleted.eq(now))
                .execute_async(&*conn)
                .await
        {
            warn!(
                opctx.log,
                "failed to clean up network interface after failed probe create";
                "probe_id" => %probe_id,
                "error" => ?e,
            );
        }
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
        opctx.check_complex_operations_allowed()?;
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
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
            paginator =
                p.found_batch(&batch, &|probe| probe.id.into_untyped_uuid());
            all_probes_for_sled.append(&mut batch);
        }
        Ok(all_probes_for_sled)
    }

    // Construct a page of probe-create parameters for a specific sled.
    async fn list_probe_create_params_for_sled(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<ProbeCreate> {
        use nexus_db_schema::schema::{
            external_ip, network_interface, probe, vpc, vpc_subnet,
        };

        let conn = self.pool_connection_authorized(opctx).await?;

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
        let rows = paginated(probe::dsl::probe, probe::dsl::id, pagparams)
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
            .filter(probe::dsl::sled.eq(sled_id.into_untyped_uuid()))
            .filter(probe::dsl::time_deleted.is_null())
            .filter(external_ip::dsl::time_deleted.is_null())
            .filter(network_interface::dsl::time_deleted.is_null())
            .filter(vpc_subnet::dsl::time_deleted.is_null())
            .filter(vpc::dsl::time_deleted.is_null())
            .select((
                probe::dsl::id,
                external_ip::dsl::ip,
                external_ip::dsl::first_port,
                external_ip::dsl::last_port,
                external_ip::dsl::kind,
                nexus_db_model::NetworkInterface::as_select(),
                vpc_subnet::dsl::ipv4_block,
                vpc_subnet::dsl::ipv6_block,
                vpc::dsl::vni,
            ))
            .get_results_async::<(
                Uuid,
                ipnetwork::IpNetwork,
                nexus_db_model::SqlU16,
                nexus_db_model::SqlU16,
                nexus_db_model::IpKind,
                nexus_db_model::NetworkInterface,
                nexus_db_model::Ipv4Net,
                nexus_db_model::Ipv6Net,
                nexus_db_model::Vni,
            )>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let mut probes: Vec<ProbeCreate> = rows
            .into_iter()
            .map(
                |(
                    probe_id,
                    ip,
                    first_port,
                    last_port,
                    kind,
                    nic,
                    ipv4_block,
                    ipv6_block,
                    vni,
                )| {
                    let kind = db_ip_kind_to_sled(kind);
                    let external_ips =
                        vec![sled_agent_client::types::ExternalIp {
                            first_port: first_port.into(),
                            ip: ip.ip(),
                            kind,
                            last_port: last_port.into(),
                        }];
                    let interface = NetworkInterface {
                        vni: vni.0,
                        ..nic.into_internal(*ipv4_block, *ipv6_block)?
                    };
                    Ok(ProbeCreate {
                        id: ProbeUuid::from_untyped_uuid(probe_id),
                        external_ips,
                        interface,
                        multicast_groups: Vec::new(),
                    })
                },
            )
            .collect::<Result<_, omicron_common::api::external::Error>>()?;

        if probes.is_empty() {
            return Ok(probes);
        }

        // Backfill `multicast_groups` with a single batched lookup against
        // `multicast_group_member` rather than a per-probe (N+1) query.
        // Membership rows carry `multicast_ip` and `source_ips` directly, so
        // no `multicast_group` join is needed to build the wire type.
        use nexus_db_schema::schema::multicast_group_member::dsl as mgm;

        let probe_ids: Vec<Uuid> =
            probes.iter().map(|p| p.id.into_untyped_uuid()).collect();

        let memberships: Vec<(
            Uuid,
            ipnetwork::IpNetwork,
            Vec<ipnetwork::IpNetwork>,
        )> = mgm::multicast_group_member
            .filter(mgm::parent_id.eq_any(probe_ids))
            .filter(mgm::parent_kind.eq(MulticastGroupMemberParentKind::Probe))
            .filter(mgm::time_deleted.is_null())
            .filter(
                mgm::state
                    .eq(MulticastGroupMemberState::Joining)
                    .or(mgm::state.eq(MulticastGroupMemberState::Joined)),
            )
            .select((mgm::parent_id, mgm::multicast_ip, mgm::source_ips))
            .get_results_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let mut by_probe: HashMap<
            Uuid,
            Vec<sled_agent_client::types::InstanceMulticastMembership>,
        > = HashMap::new();
        for (parent_id, group_ip, source_ips) in memberships {
            by_probe.entry(parent_id).or_default().push(
                sled_agent_client::types::InstanceMulticastMembership {
                    group_ip: group_ip.ip(),
                    sources: source_ips.into_iter().map(|s| s.ip()).collect(),
                },
            );
        }

        for probe in probes.iter_mut() {
            probe.multicast_groups =
                by_probe.remove(probe.id.as_untyped_uuid()).unwrap_or_default();
        }

        Ok(probes)
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

const fn db_ip_kind_to_sled(
    kind: nexus_db_model::IpKind,
) -> sled_agent_client::types::IpKind {
    match kind {
        nexus_db_model::IpKind::SNat => sled_agent_client::types::IpKind::Snat,
        nexus_db_model::IpKind::Ephemeral => {
            sled_agent_client::types::IpKind::Ephemeral
        }
        nexus_db_model::IpKind::Floating => {
            sled_agent_client::types::IpKind::Floating
        }
    }
}

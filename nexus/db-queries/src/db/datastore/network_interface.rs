// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`NetworkInterface`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::cte_utils::BoxedQuery;
use crate::db::model::IncompleteNetworkInterface;
use crate::db::model::Instance;
use crate::db::model::InstanceNetworkInterface;
use crate::db::model::Name;
use crate::db::model::NetworkInterface;
use crate::db::model::NetworkInterfaceKind;
use crate::db::model::NetworkInterfaceUpdate;
use crate::db::model::SqlU8;
use crate::db::model::VpcSubnet;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::queries::network_interface;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::IpVersion;
use nexus_db_model::Ipv4Addr;
use nexus_db_model::Ipv6Addr;
use nexus_db_model::ServiceNetworkInterface;
use nexus_types::identity::Resource;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_common::api::internal::shared::PrivateIpv4Config;
use omicron_common::api::internal::shared::PrivateIpv6Config;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use ref_cast::RefCast;
use uuid::Uuid;

/// OPTE requires information that's currently split across the network
/// interface and VPC subnet tables.
#[derive(Debug, diesel::Queryable, diesel::Selectable)]
struct NicInfo {
    #[diesel(select_expression = nexus_db_schema::schema::network_interface::id)]
    id: Uuid,
    #[diesel(select_expression = nexus_db_schema::schema::network_interface::parent_id)]
    parent_id: Uuid,
    #[diesel(select_expression = nexus_db_schema::schema::network_interface::kind)]
    kind: NetworkInterfaceKind,
    #[diesel(select_expression = nexus_db_schema::schema::network_interface::name)]
    name: db::model::Name,
    #[diesel(select_expression = nexus_db_schema::schema::network_interface::ip)]
    ipv4: Option<Ipv4Addr>,
    #[diesel(select_expression = nexus_db_schema::schema::network_interface::ipv6)]
    ipv6: Option<Ipv6Addr>,
    #[diesel(select_expression = nexus_db_schema::schema::network_interface::mac)]
    mac: db::model::MacAddr,
    #[diesel(select_expression = nexus_db_schema::schema::vpc_subnet::ipv4_block)]
    ipv4_block: db::model::Ipv4Net,
    #[diesel(select_expression = nexus_db_schema::schema::vpc_subnet::ipv6_block)]
    ipv6_block: db::model::Ipv6Net,
    #[diesel(select_expression = nexus_db_schema::schema::vpc::vni)]
    vni: db::model::Vni,
    #[diesel(select_expression = nexus_db_schema::schema::network_interface::is_primary)]
    primary: bool,
    #[diesel(select_expression = nexus_db_schema::schema::network_interface::slot)]
    slot: SqlU8,
    #[diesel(select_expression = nexus_db_schema::schema::network_interface::transit_ips)]
    transit_ips: Vec<ipnetwork::IpNetwork>,
}

impl TryFrom<NicInfo>
    for omicron_common::api::internal::shared::NetworkInterface
{
    type Error = Error;

    fn try_from(
        nic: NicInfo,
    ) -> Result<
        omicron_common::api::internal::shared::NetworkInterface,
        Self::Error,
    > {
        let ip = match (nic.ipv4, nic.ipv6) {
            (None, None) => {
                return Err(Error::internal_error(
                    "Found NIC with no VPC-private IP addresses at all",
                ));
            }
            (Some(ipv4), None) => {
                let transit_ips = nic
                    .transit_ips
                    .iter()
                    .map(|net| {
                        let ipnetwork::IpNetwork::V4(net) = net else {
                            return Err(Error::internal_error(
                                "Found NIC with IPv4 address only, but \
                                which has IPv6 transit IPs",
                            ));
                        };
                        Ok(Ipv4Net::from(*net))
                    })
                    .collect::<Result<_, _>>()?;
                PrivateIpConfig::V4(PrivateIpv4Config::new_with_transit_ips(
                    *ipv4,
                    *nic.ipv4_block,
                    transit_ips,
                )?)
            }
            (None, Some(ipv6)) => {
                let transit_ips = nic
                    .transit_ips
                    .iter()
                    .map(|net| {
                        let ipnetwork::IpNetwork::V6(net) = net else {
                            return Err(Error::internal_error(
                                "Found NIC with IPv6 address only, but \
                                which has IPv4 transit IPs",
                            ));
                        };
                        Ok(Ipv6Net::from(*net))
                    })
                    .collect::<Result<_, _>>()?;
                PrivateIpConfig::V6(PrivateIpv6Config::new_with_transit_ips(
                    *ipv6,
                    *nic.ipv6_block,
                    transit_ips,
                )?)
            }
            (Some(ipv4), Some(ipv6)) => {
                let mut ipv4_transit_ips = Vec::new();
                let mut ipv6_transit_ips = Vec::new();
                for net in nic.transit_ips.iter() {
                    match net {
                        ipnetwork::IpNetwork::V4(net) => {
                            ipv4_transit_ips.push(Ipv4Net::from(*net))
                        }
                        ipnetwork::IpNetwork::V6(net) => {
                            ipv6_transit_ips.push(Ipv6Net::from(*net))
                        }
                    }
                }
                let v4 = PrivateIpv4Config::new_with_transit_ips(
                    *ipv4,
                    *nic.ipv4_block,
                    ipv4_transit_ips,
                )?;
                let v6 = PrivateIpv6Config::new_with_transit_ips(
                    *ipv6,
                    *nic.ipv6_block,
                    ipv6_transit_ips,
                )?;
                PrivateIpConfig::DualStack { v4, v6 }
            }
        };
        let kind = match nic.kind {
            NetworkInterfaceKind::Instance => {
                omicron_common::api::internal::shared::NetworkInterfaceKind::Instance{ id: nic.parent_id }
            }
            NetworkInterfaceKind::Service => {
                omicron_common::api::internal::shared::NetworkInterfaceKind::Service{ id: nic.parent_id }
            }
            NetworkInterfaceKind::Probe => {
                omicron_common::api::internal::shared::NetworkInterfaceKind::Probe{ id: nic.parent_id }
            }
        };
        Ok(omicron_common::api::internal::shared::NetworkInterface {
            id: nic.id,
            kind,
            name: nic.name.into(),
            ip_config: ip,
            mac: nic.mac.0,
            vni: nic.vni.0,
            primary: nic.primary,
            slot: u8::try_from(nic.slot).unwrap(),
        })
    }
}

impl DataStore {
    /// Create a network interface attached to the provided instance.
    pub async fn instance_create_network_interface(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        authz_instance: &authz::Instance,
        interface: IncompleteNetworkInterface,
    ) -> Result<InstanceNetworkInterface, network_interface::InsertError> {
        opctx
            .authorize(authz::Action::CreateChild, authz_instance)
            .await
            .map_err(network_interface::InsertError::External)?;
        // Creating a NIC doesn't create a child resource of the subnet itself;
        // it creates a child of the Instance. We only need Read permission on
        // the subnet to reference it. This allows limited-collaborators to
        // create instances while still blocking them from modifying networking
        // infrastructure.
        opctx
            .authorize(authz::Action::Read, authz_subnet)
            .await
            .map_err(network_interface::InsertError::External)?;
        self.instance_create_network_interface_raw(&opctx, interface).await
    }

    pub async fn probe_create_network_interface(
        &self,
        opctx: &OpContext,
        interface: IncompleteNetworkInterface,
    ) -> Result<NetworkInterface, network_interface::InsertError> {
        self.create_network_interface_raw(&opctx, interface).await
    }

    pub(crate) async fn instance_create_network_interface_raw(
        &self,
        opctx: &OpContext,
        interface: IncompleteNetworkInterface,
    ) -> Result<InstanceNetworkInterface, network_interface::InsertError> {
        if interface.kind != NetworkInterfaceKind::Instance {
            return Err(network_interface::InsertError::External(
                Error::invalid_request(
                    "expected instance type network interface",
                ),
            ));
        }

        let out = self
            .create_network_interface_raw(opctx, interface)
            .await
            // Convert to `InstanceNetworkInterface` before returning; we know
            // this is valid as we've checked the condition on-entry.
            .map(NetworkInterface::as_instance)?;

        // `instance:xxx` targets in router rules resolve to the primary
        // NIC of that instance. Accordingly, NIC create may cause dangling
        // entries to re-resolve to a valid instance (even if it is not yet
        // started).
        // This will not trigger the route RPW directly, we still need to do
        // so in e.g. the instance watcher task.
        if out.primary {
            self.vpc_increment_rpw_version(opctx, out.vpc_id)
                .await
                .map_err(|e| network_interface::InsertError::External(e))?;
        }

        Ok(out)
    }

    /// List network interfaces associated with a given service.
    pub async fn service_list_network_interfaces_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        service_id: Uuid,
    ) -> ListResultVec<ServiceNetworkInterface> {
        use nexus_db_schema::schema::service_network_interface::dsl;
        dsl::service_network_interface
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::service_id.eq(service_id))
            .select(ServiceNetworkInterface::as_select())
            .get_results_async::<ServiceNetworkInterface>(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List one page of all network interfaces associated with internal services
    pub async fn service_network_interfaces_all_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<ServiceNetworkInterface> {
        use nexus_db_schema::schema::service_network_interface::dsl;

        // See the comment in `service_create_network_interface`. There's no
        // obvious parent for a service network interface (as opposed to
        // instance network interfaces, which require ListChildren on the
        // instance to list). As a logical proxy, we check for listing children
        // of the service IP pool.
        //
        // Note that the IP version doesn't matter here, both pools have the
        // same permissions.
        let (authz_pool, _pool) =
            self.ip_pools_service_lookup(opctx, IpVersion::V4).await?;
        opctx.authorize(authz::Action::ListChildren, &authz_pool).await?;

        paginated(dsl::service_network_interface, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(ServiceNetworkInterface::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all network interfaces associated with internal services, making as
    /// many queries as needed to get them all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn service_network_interfaces_all_list_batched(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<ServiceNetworkInterface> {
        opctx.check_complex_operations_allowed()?;

        let mut all_ips = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .service_network_interfaces_all_list(
                    opctx,
                    &p.current_pagparams(),
                )
                .await?;
            paginator = p
                .found_batch(&batch, &|nic: &ServiceNetworkInterface| nic.id());
            all_ips.extend(batch);
        }
        Ok(all_ips)
    }

    /// Create a network interface attached to the provided service zone.
    pub async fn service_create_network_interface(
        &self,
        opctx: &OpContext,
        interface: IncompleteNetworkInterface,
    ) -> Result<ServiceNetworkInterface, network_interface::InsertError> {
        // In `instance_create_network_interface`, the authz checks are for
        // creating children of the VpcSubnet and the instance. We don't have an
        // instance. We do have a VpcSubet, but for services these are all
        // fixed data subnets.
        //
        // As a proxy auth check that isn't really guarding the right resource
        // but should logically be equivalent, we can insert a authz check for
        // creating children of the service IP pool. For any service zone with
        // external networking, we create an external IP (in the service IP
        // pool) and a network interface (in the relevant VpcSubnet). Putting
        // this check here ensures that the caller can't proceed if they also
        // couldn't proceed with creating the corresponding external IP.
        //
        // Note that the IP version here doesn't matter, both IPv4 and IPv6
        // service pools have the same permissions.
        let (authz_service_ip_pool, _) = self
            .ip_pools_service_lookup(opctx, IpVersion::V4)
            .await
            .map_err(network_interface::InsertError::External)?;
        opctx
            .authorize(authz::Action::CreateChild, &authz_service_ip_pool)
            .await
            .map_err(network_interface::InsertError::External)?;
        self.service_create_network_interface_raw(opctx, interface).await
    }

    pub(crate) async fn service_create_network_interface_raw(
        &self,
        opctx: &OpContext,
        interface: IncompleteNetworkInterface,
    ) -> Result<ServiceNetworkInterface, network_interface::InsertError> {
        if interface.kind != NetworkInterfaceKind::Service {
            return Err(network_interface::InsertError::External(
                Error::invalid_request(
                    "expected service type network interface",
                ),
            ));
        }
        self.create_network_interface_raw(opctx, interface)
            .await
            // Convert to `ServiceNetworkInterface` before returning; we know
            // this is valid as we've checked the condition on-entry.
            .map(NetworkInterface::as_service)
    }

    async fn create_network_interface_raw(
        &self,
        opctx: &OpContext,
        interface: IncompleteNetworkInterface,
    ) -> Result<NetworkInterface, network_interface::InsertError> {
        let conn = self
            .pool_connection_authorized(opctx)
            .await
            .map_err(network_interface::InsertError::External)?;
        self.create_network_interface_raw_conn(&conn, interface).await
    }

    pub(crate) async fn create_network_interface_raw_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        interface: IncompleteNetworkInterface,
    ) -> Result<NetworkInterface, network_interface::InsertError> {
        use nexus_db_schema::schema::network_interface::dsl;
        let subnet_id = interface.subnet.identity.id;
        let query = network_interface::InsertQuery::new(interface.clone());
        VpcSubnet::insert_resource(
            subnet_id,
            diesel::insert_into(dsl::network_interface).values(query),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => {
                network_interface::InsertError::External(
                    Error::ObjectNotFound {
                        type_name: ResourceType::VpcSubnet,
                        lookup_type: LookupType::ById(subnet_id),
                    },
                )
            }
            AsyncInsertError::DatabaseError(e) => {
                network_interface::InsertError::from_diesel(e, &interface)
            }
        })
    }

    /// Delete all network interfaces attached to the given instance.
    pub async fn instance_delete_all_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;

        use nexus_db_schema::schema::network_interface::dsl;
        let now = Utc::now();
        diesel::update(dsl::network_interface)
            .filter(dsl::parent_id.eq(authz_instance.id()))
            .filter(dsl::kind.eq(NetworkInterfaceKind::Instance))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_instance),
                )
            })?;
        Ok(())
    }

    /// Delete all network interfaces attached to the given probe.
    pub async fn probe_delete_all_network_interfaces(
        &self,
        opctx: &OpContext,
        probe_id: Uuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::network_interface::dsl;
        let now = Utc::now();
        diesel::update(dsl::network_interface)
            .filter(dsl::parent_id.eq(probe_id))
            .filter(dsl::kind.eq(NetworkInterfaceKind::Probe))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Probe,
                        LookupType::ById(probe_id),
                    ),
                )
            })?;
        Ok(())
    }

    /// Delete an `InstanceNetworkInterface` attached to a provided instance.
    ///
    /// Note that the primary interface for an instance cannot be deleted if
    /// there are any secondary interfaces.
    ///
    /// To support idempotency, such as in saga operations, this method returns
    /// an extra boolean. The meaning of return values are:
    /// - `Ok(true)`: The record was deleted during this call
    /// - `Ok(false)`: The record was already deleted, such as by a previous
    /// call
    /// - `Err(_)`: Any other condition, including a non-existent record.
    pub async fn instance_delete_network_interface(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        authz_interface: &authz::InstanceNetworkInterface,
    ) -> Result<bool, network_interface::DeleteError> {
        opctx
            .authorize(authz::Action::Delete, authz_interface)
            .await
            .map_err(network_interface::DeleteError::External)?;
        let query = network_interface::DeleteQuery::new(
            NetworkInterfaceKind::Instance,
            authz_instance.id(),
            authz_interface.id(),
        );
        query
            .clone()
            .execute_and_check(
                &*self
                    .pool_connection_authorized(opctx)
                    .await
                    .map_err(network_interface::DeleteError::External)?,
            )
            .await
            .map_err(|e| network_interface::DeleteError::from_diesel(e, &query))
    }

    /// Delete a `ServiceNetworkInterface` attached to a provided service.
    ///
    /// To support idempotency, such as in saga operations, this method returns
    /// an extra boolean. The meaning of return values are:
    /// - `Ok(true)`: The record was deleted during this call
    /// - `Ok(false)`: The record was already deleted, such as by a previous
    /// call
    /// - `Err(_)`: Any other condition, including a non-existent record.
    pub async fn service_delete_network_interface(
        &self,
        opctx: &OpContext,
        service_id: Uuid,
        network_interface_id: Uuid,
    ) -> Result<bool, network_interface::DeleteError> {
        // See the comment in `service_create_network_interface`. There's no
        // obvious parent for a service network interface (as opposed to
        // instance network interfaces, which require permissions on the
        // instance). As a logical proxy, we check for listing children of the
        // service IP pool.
        //
        // Note that the IP version here doesn't matter, both pools have the
        // same permissions.
        let (authz_service_ip_pool, _) = self
            .ip_pools_service_lookup(opctx, IpVersion::V4)
            .await
            .map_err(network_interface::DeleteError::External)?;
        opctx
            .authorize(authz::Action::Delete, &authz_service_ip_pool)
            .await
            .map_err(network_interface::DeleteError::External)?;

        let conn = self
            .pool_connection_authorized(opctx)
            .await
            .map_err(network_interface::DeleteError::External)?;
        self.service_delete_network_interface_on_connection(
            &conn,
            service_id,
            network_interface_id,
        )
        .await
    }

    /// Variant of [Self::service_delete_network_interface] which may be called
    /// from a transaction context.
    pub async fn service_delete_network_interface_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        service_id: Uuid,
        network_interface_id: Uuid,
    ) -> Result<bool, network_interface::DeleteError> {
        let query = network_interface::DeleteQuery::new(
            NetworkInterfaceKind::Service,
            service_id,
            network_interface_id,
        );
        query
            .clone()
            .execute_and_check(conn)
            .await
            .map_err(|e| network_interface::DeleteError::from_diesel(e, &query))
    }

    /// Return information about network interfaces required for the sled
    /// agent to instantiate or modify them via OPTE. This function takes
    /// a partially constructed query over the network interface table so
    /// that we can use it for instances, services, VPCs, and subnets.
    async fn derive_network_interface_info(
        &self,
        opctx: &OpContext,
        partial_query: BoxedQuery<
            nexus_db_schema::schema::network_interface::table,
        >,
    ) -> ListResultVec<omicron_common::api::internal::shared::NetworkInterface>
    {
        use nexus_db_schema::schema::network_interface;
        use nexus_db_schema::schema::vpc;
        use nexus_db_schema::schema::vpc_subnet;

        let rows = partial_query
            .filter(network_interface::time_deleted.is_null())
            .inner_join(
                vpc_subnet::table
                    .on(network_interface::subnet_id.eq(vpc_subnet::id)),
            )
            .inner_join(vpc::table.on(vpc_subnet::vpc_id.eq(vpc::id)))
            .order_by(network_interface::slot)
            .select(NicInfo::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        rows
            .into_iter()
            .map(omicron_common::api::internal::shared::NetworkInterface::try_from)
            .collect::<Result<_, _>>()
    }

    /// Return the information about an instance's network interfaces required
    /// for the sled agent to instantiate them via OPTE.
    pub async fn derive_guest_network_interface_info(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> ListResultVec<omicron_common::api::internal::shared::NetworkInterface>
    {
        opctx.authorize(authz::Action::ListChildren, authz_instance).await?;

        use nexus_db_schema::schema::network_interface;
        self.derive_network_interface_info(
            opctx,
            network_interface::table
                .filter(network_interface::parent_id.eq(authz_instance.id()))
                .filter(
                    network_interface::kind.eq(NetworkInterfaceKind::Instance),
                )
                .into_boxed(),
        )
        .await
    }

    pub async fn derive_probe_network_interface_info(
        &self,
        opctx: &OpContext,
        probe_id: Uuid,
    ) -> ListResultVec<omicron_common::api::internal::shared::NetworkInterface>
    {
        use nexus_db_schema::schema::network_interface;
        self.derive_network_interface_info(
            opctx,
            network_interface::table
                .filter(network_interface::parent_id.eq(probe_id))
                .filter(network_interface::kind.eq(NetworkInterfaceKind::Probe))
                .into_boxed(),
        )
        .await
    }

    /// Return information about all VNICs connected to a VPC required
    /// for the sled agent to instantiate firewall rules via OPTE.
    pub async fn derive_vpc_network_interface_info(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
    ) -> ListResultVec<omicron_common::api::internal::shared::NetworkInterface>
    {
        opctx.authorize(authz::Action::ListChildren, authz_vpc).await?;

        use nexus_db_schema::schema::network_interface;
        self.derive_network_interface_info(
            opctx,
            network_interface::table
                .filter(network_interface::vpc_id.eq(authz_vpc.id()))
                .into_boxed(),
        )
        .await
    }

    /// Return information about all VNICs connected to a VpcSubnet required
    /// for the sled agent to instantiate firewall rules via OPTE.
    pub async fn derive_subnet_network_interface_info(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
    ) -> ListResultVec<omicron_common::api::internal::shared::NetworkInterface>
    {
        opctx.authorize(authz::Action::ListChildren, authz_subnet).await?;

        use nexus_db_schema::schema::network_interface;
        self.derive_network_interface_info(
            opctx,
            network_interface::table
                .filter(network_interface::subnet_id.eq(authz_subnet.id()))
                .into_boxed(),
        )
        .await
    }

    /// List network interfaces associated with a given instance.
    pub async fn instance_list_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<InstanceNetworkInterface> {
        opctx.authorize(authz::Action::ListChildren, authz_instance).await?;

        use nexus_db_schema::schema::instance_network_interface::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::instance_network_interface, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::instance_network_interface,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::instance_id.eq(authz_instance.id()))
        .select(InstanceNetworkInterface::as_select())
        .load_async::<InstanceNetworkInterface>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Retrieve the primary network interface for a given instance.
    pub async fn instance_get_primary_network_interface(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> LookupResult<InstanceNetworkInterface> {
        opctx.authorize(authz::Action::ListChildren, authz_instance).await?;

        use nexus_db_schema::schema::instance_network_interface::dsl;
        dsl::instance_network_interface
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::instance_id.eq(authz_instance.id()))
            .filter(dsl::is_primary.eq(true))
            .select(InstanceNetworkInterface::as_select())
            .limit(1)
            .first_async::<InstanceNetworkInterface>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| match e {
                DieselError::NotFound => Error::non_resourcetype_not_found(
                    "instance has no primary NIC",
                ),
                e => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// Get network interface associated with a given probe.
    pub async fn probe_get_network_interface(
        &self,
        opctx: &OpContext,
        probe_id: Uuid,
    ) -> LookupResult<NetworkInterface> {
        use nexus_db_schema::schema::network_interface::dsl;

        dsl::network_interface
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::parent_id.eq(probe_id))
            .select(NetworkInterface::as_select())
            .first_async::<NetworkInterface>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Update a network interface associated with a given instance.
    pub async fn instance_update_network_interface(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        authz_interface: &authz::InstanceNetworkInterface,
        updates: NetworkInterfaceUpdate,
    ) -> UpdateResult<InstanceNetworkInterface> {
        // This database operation is surprisingly subtle. It's possible to
        // express this in a single query, with multiple common-table
        // expressions for the updated rows. For example, if we're setting a new
        // primary interface, we need to set the `is_primary` column to false
        // for the current primary, and then set it to true, along with any
        // other updates, for the new primary.
        //
        // That's feasible, but there's a CRDB bug that affects some queries
        // with multiple update statements. It's possible that this query isn't
        // in that bucket, but we'll still avoid it for now. Instead, we'll bite
        // the bullet and use a transaction.
        //
        // See https://github.com/oxidecomputer/omicron/issues/1204 for the
        // issue tracking the work to move this into a CTE.
        //
        // It's also further complicated by the fact we're updating a row
        // in the `network_interface` table but will return a `InstanceNetworkInterface`
        // which represents a row in the `instance_network_interface`. This is
        // fine because `instance_network_interface` is just a view over
        // `network_interface` constrained to rows of `kind = 'instance'`.

        // Build up some of the queries first, outside the transaction.
        //
        // This selects the existing primary interface.
        // Note we consult only `kind = 'instance'` rows by querying from
        // `instance_network_interface`.
        let instance_id = authz_instance.id();
        let interface_id = authz_interface.id();
        let find_primary_query = {
            use nexus_db_schema::schema::instance_network_interface::dsl;
            dsl::instance_network_interface
                .filter(dsl::instance_id.eq(instance_id))
                .filter(dsl::is_primary.eq(true))
                .filter(dsl::time_deleted.is_null())
                .select(InstanceNetworkInterface::as_select())
        };

        // This returns the state of the associated instance.
        let instance_query = {
            use nexus_db_schema::schema::instance::dsl;
            dsl::instance
                .filter(dsl::id.eq(instance_id))
                .filter(dsl::time_deleted.is_null())
                .select(Instance::as_select())
        };
        let stopped = db::model::InstanceState::NoVmm;

        // This is the actual query to update the target interface.
        // Unlike Postgres, CockroachDB doesn't support inserting or updating a view
        // so we need to use the underlying table, taking care to constrain
        // the update to rows of `kind = 'instance'`.
        let primary = matches!(updates.primary, Some(true));
        let update_target_query = {
            use nexus_db_schema::schema::network_interface::dsl;
            diesel::update(dsl::network_interface)
                .filter(dsl::id.eq(interface_id))
                .filter(dsl::kind.eq(NetworkInterfaceKind::Instance))
                .filter(dsl::time_deleted.is_null())
                .set(updates)
                .returning(NetworkInterface::as_returning())
        };

        // Errors returned from the below transactions.
        #[derive(Debug)]
        enum NetworkInterfaceUpdateError {
            InstanceNotStopped,
            FailedToUnsetPrimary(DieselError),
        }

        let err = OptionalError::new();

        let conn = self.pool_connection_authorized(opctx).await?;
        if primary {
            self.transaction_retry_wrapper("instance_update_network_interface")
                .transaction(&conn, |conn| {
                    let err = err.clone();
                    let update_target_query = update_target_query.clone();
                    async move {
                        let instance_runtime =
                            instance_query.get_result_async(&conn).await?.runtime_state;
                        if instance_runtime.propolis_id.is_some()
                            || instance_runtime.nexus_state != stopped
                        {
                            return Err(err.bail(NetworkInterfaceUpdateError::InstanceNotStopped));
                        }

                        // First, get the primary interface
                        let primary_interface =
                            find_primary_query.get_result_async(&conn).await?;
                        // If the target and primary are different, we need to toggle
                        // the primary into a secondary.
                        if primary_interface.identity.id != interface_id {
                            use nexus_db_schema::schema::network_interface::dsl;
                            if let Err(e) = diesel::update(dsl::network_interface)
                                .filter(dsl::id.eq(primary_interface.identity.id))
                                .filter(dsl::kind.eq(NetworkInterfaceKind::Instance))
                                .filter(dsl::time_deleted.is_null())
                                .set(dsl::is_primary.eq(false))
                                .execute_async(&conn)
                                .await
                            {
                                return Err(err.bail_retryable_or_else(
                                    e,
                                    |e| NetworkInterfaceUpdateError::FailedToUnsetPrimary(e)
                                ));
                            }
                        }

                        // In any case, update the actual target
                        update_target_query.get_result_async(&conn).await
                    }
                }).await
        } else {
            // In this case, we can just directly apply the updates. By
            // construction, `updates.primary` is `None`, so nothing will
            // be done there. The other columns always need to be updated, and
            // we're only hitting a single row. Note that we still need to
            // verify the instance is stopped.
            self.transaction_retry_wrapper("instance_update_network_interface")
                .transaction(&conn, |conn| {
                    let err = err.clone();
                    let update_target_query = update_target_query.clone();
                    async move {
                        let instance_state =
                            instance_query.get_result_async(&conn).await?.runtime_state;
                        if instance_state.propolis_id.is_some()
                            || instance_state.nexus_state != stopped
                        {
                            return Err(err.bail(NetworkInterfaceUpdateError::InstanceNotStopped));
                        }
                        update_target_query.get_result_async(&conn).await
                    }
                }).await
        }
        // Convert to `InstanceNetworkInterface` before returning, we know
        // this is valid as we've filtered appropriately above.
        .map(NetworkInterface::as_instance)
        .map_err(|e| {
            if let Some(err) = err.take() {
                match err {
                    NetworkInterfaceUpdateError::InstanceNotStopped => {
                        return Error::invalid_request(
                            "Instance must be stopped to update its network interfaces",
                        );
                    },
                    NetworkInterfaceUpdateError::FailedToUnsetPrimary(err) => {
                        return public_error_from_diesel(err, ErrorHandler::Server);
                    },
                }
            }
            public_error_from_diesel(e, ErrorHandler::Server)
        })
    }

    /// List all network interfaces associated with all instances, making as
    /// many queries as needed to get them all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    ///
    /// This particular method was added for propagating v2p mappings via RPWs
    pub async fn instance_network_interfaces_all_list_batched(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<InstanceNetworkInterface> {
        opctx.check_complex_operations_allowed()?;

        let mut all_interfaces = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .instance_network_interfaces_all_list(
                    opctx,
                    &p.current_pagparams(),
                )
                .await?;
            paginator = p
                .found_batch(&batch, &|nic: &InstanceNetworkInterface| {
                    nic.id()
                });
            all_interfaces.extend(batch);
        }
        Ok(all_interfaces)
    }

    /// List one page of all network interfaces associated with instances
    pub async fn instance_network_interfaces_all_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<InstanceNetworkInterface> {
        use nexus_db_schema::schema::instance_network_interface::dsl;

        // See the comment in `service_create_network_interface`. There's no
        // obvious parent for a service network interface (as opposed to
        // instance network interfaces, which require ListChildren on the
        // instance to list). As a logical proxy, we check for listing children
        // of the service IP pool.
        //
        // TODO-cleanup: https://github.com/oxidecomputer/omicron/issues/8873.
        // This authz check looks at the builtin services IP Pool, but we're
        // listing _instance_ NICs. That seems to be authorizing against the
        // wrong resource.
        //
        // But assuming this check is correct, both service pools have the same
        // permissions, so the actual IP version here doesn't matter.
        let (authz_pool, _pool) =
            self.ip_pools_service_lookup(opctx, IpVersion::V4).await?;
        opctx.authorize(authz::Action::ListChildren, &authz_pool).await?;

        paginated(dsl::instance_network_interface, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(InstanceNetworkInterface::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
    use nexus_db_fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
    use nexus_db_model::IpConfig;
    use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
    use omicron_test_utils::dev;
    use std::collections::BTreeSet;

    async fn read_all_service_nics(
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> Vec<ServiceNetworkInterface> {
        let all_batched = datastore
            .service_network_interfaces_all_list_batched(opctx)
            .await
            .expect("failed to fetch all service NICs batched");
        let all_paginated = datastore
            .service_network_interfaces_all_list(
                opctx,
                &DataPageParams::max_page(),
            )
            .await
            .expect("failed to fetch all service NICs paginated");
        assert_eq!(all_batched, all_paginated);
        all_batched
    }

    #[tokio::test]
    async fn test_service_network_interfaces_list() {
        let logctx =
            dev::test_setup_log("test_service_network_interfaces_list");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // No IPs, to start
        let nics = read_all_service_nics(&datastore, &opctx).await;
        assert_eq!(nics, vec![]);

        // Insert 10 Nexus NICs
        let ip_range = NEXUS_OPTE_IPV4_SUBNET
            .addr_iter()
            .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES)
            .take(10);
        let mut macs = omicron_common::api::external::MacAddr::iter_system();
        let mut service_nics = Vec::new();
        for (i, ip) in ip_range.enumerate() {
            let name = format!("service-nic-{i}");
            let interface = IncompleteNetworkInterface::new_service(
                Uuid::new_v4(),
                Uuid::new_v4(),
                NEXUS_VPC_SUBNET.clone(),
                omicron_common::api::external::IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: name,
                },
                IpConfig::from_ipv4(ip),
                macs.next().unwrap(),
                0,
            )
            .unwrap();
            let nic = datastore
                .service_create_network_interface(&opctx, interface)
                .await
                .expect("failed to insert service nic");
            service_nics.push(nic);
        }
        service_nics.sort_by_key(|nic| nic.id());

        // Ensure we see them all.
        let nics = read_all_service_nics(&datastore, &opctx).await;
        assert_eq!(nics, service_nics);

        // Delete a few, and ensure we don't see them anymore.
        let mut removed_nic_ids = BTreeSet::new();
        for (i, nic) in service_nics.iter().enumerate() {
            if i % 3 == 0 {
                let id = nic.id();
                datastore
                    .service_delete_network_interface(
                        &opctx,
                        nic.service_id,
                        id,
                    )
                    .await
                    .expect("failed to delete NIC");
                removed_nic_ids.insert(id);
            }
        }

        // Check that we removed at least one, then prune them from our list of
        // expected IPs.
        assert!(!removed_nic_ids.is_empty());
        service_nics.retain(|nic| !removed_nic_ids.contains(&nic.id()));

        // Ensure we see them all remaining IPs.
        let nics = read_all_service_nics(&datastore, &opctx).await;
        assert_eq!(nics, service_nics);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}

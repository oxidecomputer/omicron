// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`NetworkInterface`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::cte_utils::BoxedQuery;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::IncompleteNetworkInterface;
use crate::db::model::Instance;
use crate::db::model::InstanceNetworkInterface;
use crate::db::model::Name;
use crate::db::model::NetworkInterface;
use crate::db::model::NetworkInterfaceKind;
use crate::db::model::NetworkInterfaceUpdate;
use crate::db::model::VpcSubnet;
use crate::db::pagination::paginated;
use crate::db::pool::DbConnection;
use crate::db::queries::network_interface;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use nexus_db_model::ServiceNetworkInterface;
use omicron_common::api::external;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;
use uuid::Uuid;

/// OPTE requires information that's currently split across the network
/// interface and VPC subnet tables.
#[derive(Debug, diesel::Queryable)]
struct NicInfo {
    id: Uuid,
    parent_id: Uuid,
    kind: NetworkInterfaceKind,
    name: db::model::Name,
    ip: ipnetwork::IpNetwork,
    mac: db::model::MacAddr,
    ipv4_block: db::model::Ipv4Net,
    ipv6_block: db::model::Ipv6Net,
    vni: db::model::Vni,
    primary: bool,
    slot: i16,
}

impl From<NicInfo> for omicron_common::api::internal::shared::NetworkInterface {
    fn from(
        nic: NicInfo,
    ) -> omicron_common::api::internal::shared::NetworkInterface {
        let ip_subnet = if nic.ip.is_ipv4() {
            external::IpNet::V4(nic.ipv4_block.0)
        } else {
            external::IpNet::V6(nic.ipv6_block.0)
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
        omicron_common::api::internal::shared::NetworkInterface {
            id: nic.id,
            kind,
            name: nic.name.into(),
            ip: nic.ip.ip(),
            mac: nic.mac.0,
            subnet: ip_subnet,
            vni: nic.vni.0,
            primary: nic.primary,
            slot: u8::try_from(nic.slot).unwrap(),
        }
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
        opctx
            .authorize(authz::Action::CreateChild, authz_subnet)
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
        self.create_network_interface_raw(opctx, interface)
            .await
            // Convert to `InstanceNetworkInterface` before returning; we know
            // this is valid as we've checked the condition on-entry.
            .map(NetworkInterface::as_instance)
    }

    /// List network interfaces associated with a given service.
    pub async fn service_list_network_interfaces(
        &self,
        opctx: &OpContext,
        service_id: Uuid,
    ) -> ListResultVec<ServiceNetworkInterface> {
        // See the comment in `service_create_network_interface`. There's no
        // obvious parent for a service network interface (as opposed to
        // instance network interfaces, which require ListChildren on the
        // instance to list). As a logical proxy, we check for listing children
        // of the service IP pool.
        let (authz_service_ip_pool, _) =
            self.ip_pools_service_lookup(opctx).await?;
        opctx
            .authorize(authz::Action::ListChildren, &authz_service_ip_pool)
            .await?;

        use db::schema::service_network_interface::dsl;
        dsl::service_network_interface
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::service_id.eq(service_id))
            .select(ServiceNetworkInterface::as_select())
            .get_results_async::<ServiceNetworkInterface>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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
        let (authz_service_ip_pool, _) = self
            .ip_pools_service_lookup(opctx)
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
        use db::schema::network_interface::dsl;
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

        use db::schema::network_interface::dsl;
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
        use db::schema::network_interface::dsl;
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
    pub async fn instance_delete_network_interface(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        authz_interface: &authz::InstanceNetworkInterface,
    ) -> Result<(), network_interface::DeleteError> {
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
            .execute_async(
                &*self
                    .pool_connection_authorized(opctx)
                    .await
                    .map_err(network_interface::DeleteError::External)?,
            )
            .await
            .map_err(|e| {
                network_interface::DeleteError::from_diesel(e, &query)
            })?;
        Ok(())
    }

    /// Return information about network interfaces required for the sled
    /// agent to instantiate or modify them via OPTE. This function takes
    /// a partially constructed query over the network interface table so
    /// that we can use it for instances, services, VPCs, and subnets.
    async fn derive_network_interface_info(
        &self,
        opctx: &OpContext,
        partial_query: BoxedQuery<db::schema::network_interface::table>,
    ) -> ListResultVec<omicron_common::api::internal::shared::NetworkInterface>
    {
        use db::schema::network_interface;
        use db::schema::vpc;
        use db::schema::vpc_subnet;

        let rows = partial_query
            .filter(network_interface::time_deleted.is_null())
            .inner_join(
                vpc_subnet::table
                    .on(network_interface::subnet_id.eq(vpc_subnet::id)),
            )
            .inner_join(vpc::table.on(vpc_subnet::vpc_id.eq(vpc::id)))
            .order_by(network_interface::slot)
            // TODO-cleanup: Having to specify each column again is less than
            // ideal, but we can't derive `Selectable` since this is the result
            // of a JOIN and not from a single table. DRY this out if possible.
            .select((
                network_interface::id,
                network_interface::parent_id,
                network_interface::kind,
                network_interface::name,
                network_interface::ip,
                network_interface::mac,
                vpc_subnet::ipv4_block,
                vpc_subnet::ipv6_block,
                vpc::vni,
                network_interface::is_primary,
                network_interface::slot,
            ))
            .get_results_async::<NicInfo>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(rows
            .into_iter()
            .map(omicron_common::api::internal::shared::NetworkInterface::from)
            .collect())
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

        use db::schema::network_interface;
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
        use db::schema::network_interface;
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

        use db::schema::network_interface;
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

        use db::schema::network_interface;
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

        use db::schema::instance_network_interface::dsl;
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

    /// Get network interface associated with a given probe.
    pub async fn probe_get_network_interface(
        &self,
        opctx: &OpContext,
        probe_id: Uuid,
    ) -> LookupResult<NetworkInterface> {
        use db::schema::network_interface::dsl;

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
            use crate::db::schema::instance_network_interface::dsl;
            dsl::instance_network_interface
                .filter(dsl::instance_id.eq(instance_id))
                .filter(dsl::is_primary.eq(true))
                .filter(dsl::time_deleted.is_null())
                .select(InstanceNetworkInterface::as_select())
        };

        // This returns the state of the associated instance.
        let instance_query = {
            use crate::db::schema::instance::dsl;
            dsl::instance
                .filter(dsl::id.eq(instance_id))
                .filter(dsl::time_deleted.is_null())
                .select(Instance::as_select())
        };
        let stopped =
            db::model::InstanceState::new(external::InstanceState::Stopped);

        // This is the actual query to update the target interface.
        // Unlike Postgres, CockroachDB doesn't support inserting or updating a view
        // so we need to use the underlying table, taking care to constrain
        // the update to rows of `kind = 'instance'`.
        let primary = matches!(updates.primary, Some(true));
        let update_target_query = {
            use crate::db::schema::network_interface::dsl;
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
                    let stopped = stopped.clone();
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
                            use crate::db::schema::network_interface::dsl;
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
                    let stopped = stopped.clone();
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
}

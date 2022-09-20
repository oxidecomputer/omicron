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
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::model::IncompleteNetworkInterface;
use crate::db::model::Instance;
use crate::db::model::Name;
use crate::db::model::NetworkInterface;
use crate::db::model::NetworkInterfaceUpdate;
use crate::db::model::VpcSubnet;
use crate::db::pagination::paginated;
use crate::db::queries::network_interface;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use sled_agent_client::types as sled_client_types;

impl DataStore {
    /// Create a network interface attached to the provided instance.
    pub async fn instance_create_network_interface(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        authz_instance: &authz::Instance,
        interface: IncompleteNetworkInterface,
    ) -> Result<NetworkInterface, network_interface::InsertError> {
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

    pub(crate) async fn instance_create_network_interface_raw(
        &self,
        opctx: &OpContext,
        interface: IncompleteNetworkInterface,
    ) -> Result<NetworkInterface, network_interface::InsertError> {
        use db::schema::network_interface::dsl;
        let subnet_id = interface.subnet.identity.id;
        let query = network_interface::InsertQuery::new(interface.clone());
        VpcSubnet::insert_resource(
            subnet_id,
            diesel::insert_into(dsl::network_interface).values(query),
        )
        .insert_and_get_result_async(
            self.pool_authorized(opctx)
                .await
                .map_err(network_interface::InsertError::External)?,
        )
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
                network_interface::InsertError::from_pool(e, &interface)
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
            .filter(dsl::instance_id.eq(authz_instance.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_instance),
                )
            })?;
        Ok(())
    }

    /// Delete a `NetworkInterface` attached to a provided instance.
    ///
    /// Note that the primary interface for an instance cannot be deleted if
    /// there are any secondary interfaces.
    pub async fn instance_delete_network_interface(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        authz_interface: &authz::NetworkInterface,
    ) -> Result<(), network_interface::DeleteError> {
        opctx
            .authorize(authz::Action::Delete, authz_interface)
            .await
            .map_err(network_interface::DeleteError::External)?;
        let query = network_interface::DeleteQuery::new(
            authz_instance.id(),
            authz_interface.id(),
        );
        query
            .clone()
            .execute_async(
                self.pool_authorized(opctx)
                    .await
                    .map_err(network_interface::DeleteError::External)?,
            )
            .await
            .map_err(|e| {
                network_interface::DeleteError::from_pool(e, &query)
            })?;
        Ok(())
    }

    /// Return the information about an instance's network interfaces required
    /// for the sled agent to instantiate them via OPTE.
    ///
    /// OPTE requires information that's currently split across the network
    /// interface and VPC subnet tables. This query just joins those for each
    /// NIC in the given instance.
    pub(crate) async fn derive_guest_network_interface_info(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> ListResultVec<sled_client_types::NetworkInterface> {
        opctx.authorize(authz::Action::ListChildren, authz_instance).await?;

        use db::schema::network_interface;
        use db::schema::vpc;
        use db::schema::vpc_subnet;

        // The record type for the results of the below JOIN query
        #[derive(Debug, diesel::Queryable)]
        struct NicInfo {
            name: db::model::Name,
            ip: ipnetwork::IpNetwork,
            mac: db::model::MacAddr,
            ipv4_block: db::model::Ipv4Net,
            ipv6_block: db::model::Ipv6Net,
            vni: db::model::Vni,
            primary: bool,
            slot: i16,
        }

        impl From<NicInfo> for sled_client_types::NetworkInterface {
            fn from(nic: NicInfo) -> sled_client_types::NetworkInterface {
                let ip_subnet = if nic.ip.is_ipv4() {
                    external::IpNet::V4(nic.ipv4_block.0)
                } else {
                    external::IpNet::V6(nic.ipv6_block.0)
                };
                sled_client_types::NetworkInterface {
                    name: sled_client_types::Name::from(&nic.name.0),
                    ip: nic.ip.ip(),
                    mac: sled_client_types::MacAddr::from(nic.mac.0),
                    subnet: sled_client_types::IpNet::from(ip_subnet),
                    vni: sled_client_types::Vni::from(nic.vni.0),
                    primary: nic.primary,
                    slot: u8::try_from(nic.slot).unwrap(),
                }
            }
        }

        let rows = network_interface::table
            .filter(network_interface::instance_id.eq(authz_instance.id()))
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
                network_interface::name,
                network_interface::ip,
                network_interface::mac,
                vpc_subnet::ipv4_block,
                vpc_subnet::ipv6_block,
                vpc::vni,
                network_interface::is_primary,
                network_interface::slot,
            ))
            .get_results_async::<NicInfo>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        Ok(rows
            .into_iter()
            .map(sled_client_types::NetworkInterface::from)
            .collect())
    }

    /// List network interfaces associated with a given instance.
    pub async fn instance_list_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<NetworkInterface> {
        opctx.authorize(authz::Action::ListChildren, authz_instance).await?;

        use db::schema::network_interface::dsl;
        paginated(dsl::network_interface, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::instance_id.eq(authz_instance.id()))
            .select(NetworkInterface::as_select())
            .load_async::<NetworkInterface>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Update a network interface associated with a given instance.
    pub async fn instance_update_network_interface(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        authz_interface: &authz::NetworkInterface,
        updates: NetworkInterfaceUpdate,
    ) -> UpdateResult<NetworkInterface> {
        use crate::db::schema::network_interface::dsl;

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

        // Build up some of the queries first, outside the transaction.
        //
        // This selects the existing primary interface.
        let instance_id = authz_instance.id();
        let interface_id = authz_interface.id();
        let find_primary_query = dsl::network_interface
            .filter(dsl::instance_id.eq(instance_id))
            .filter(dsl::is_primary.eq(true))
            .filter(dsl::time_deleted.is_null())
            .select(NetworkInterface::as_select());

        // This returns the state of the associated instance.
        let instance_query = db::schema::instance::dsl::instance
            .filter(db::schema::instance::dsl::id.eq(instance_id))
            .filter(db::schema::instance::dsl::time_deleted.is_null())
            .select(Instance::as_select());
        let stopped =
            db::model::InstanceState::new(external::InstanceState::Stopped);

        // This is the actual query to update the target interface.
        let primary = matches!(updates.primary, Some(true));
        let update_target_query = diesel::update(dsl::network_interface)
            .filter(dsl::id.eq(interface_id))
            .filter(dsl::time_deleted.is_null())
            .set(updates)
            .returning(NetworkInterface::as_returning());

        // Errors returned from the below transactions.
        #[derive(Debug)]
        enum NetworkInterfaceUpdateError {
            InstanceNotStopped,
            FailedToUnsetPrimary(async_bb8_diesel::ConnectionError),
        }
        type TxnError = TransactionError<NetworkInterfaceUpdateError>;

        let pool = self.pool_authorized(opctx).await?;
        if primary {
            pool.transaction_async(|conn| async move {
                let instance_state = instance_query
                    .get_result_async(&conn)
                    .await?
                    .runtime_state
                    .state;
                if instance_state != stopped {
                    return Err(TxnError::CustomError(
                        NetworkInterfaceUpdateError::InstanceNotStopped,
                    ));
                }

                // First, get the primary interface
                let primary_interface =
                    find_primary_query.get_result_async(&conn).await?;
                // If the target and primary are different, we need to toggle
                // the primary into a secondary.
                if primary_interface.identity.id != interface_id {
                    if let Err(e) = diesel::update(dsl::network_interface)
                        .filter(dsl::id.eq(primary_interface.identity.id))
                        .filter(dsl::time_deleted.is_null())
                        .set(dsl::is_primary.eq(false))
                        .execute_async(&conn)
                        .await
                    {
                        return Err(TxnError::CustomError(
                            NetworkInterfaceUpdateError::FailedToUnsetPrimary(
                                e,
                            ),
                        ));
                    }
                }

                // In any case, update the actual target
                Ok(update_target_query.get_result_async(&conn).await?)
            })
        } else {
            // In this case, we can just directly apply the updates. By
            // construction, `updates.primary` is `None`, so nothing will
            // be done there. The other columns always need to be updated, and
            // we're only hitting a single row. Note that we still need to
            // verify the instance is stopped.
            pool.transaction_async(|conn| async move {
                let instance_state = instance_query
                    .get_result_async(&conn)
                    .await?
                    .runtime_state
                    .state;
                if instance_state != stopped {
                    return Err(TxnError::CustomError(
                        NetworkInterfaceUpdateError::InstanceNotStopped,
                    ));
                }
                Ok(update_target_query.get_result_async(&conn).await?)
            })
        }
        .await
        .map_err(|e| match e {
            TxnError::CustomError(
                NetworkInterfaceUpdateError::InstanceNotStopped,
            ) => Error::invalid_request(
                "Instance must be stopped to update its network interfaces",
            ),
            _ => Error::internal_error(&format!("Transaction error: {:?}", e)),
        })
    }
}

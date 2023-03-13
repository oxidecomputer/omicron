use crate::authz::ApiResource;
use crate::db::queries::network_interface;
use nexus_db_model::Name;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;

use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;
use uuid::Uuid;

use crate::authz;
use crate::db;
use crate::db::lookup::{self, LookupPath};

impl super::Nexus {
    pub fn network_interface_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        network_interface_selector: &'a params::NetworkInterfaceSelector,
    ) -> LookupResult<lookup::NetworkInterface<'a>> {
        match network_interface_selector {
            params::NetworkInterfaceSelector {
                instance_selector: None,
                network_interface: NameOrId::Id(id),
            } => {
                let network_interface =
                    LookupPath::new(opctx, &self.db_datastore)
                        .network_interface_id(*id);
                Ok(network_interface)
            }
            params::NetworkInterfaceSelector {
                instance_selector: Some(instance_selector),
                network_interface: NameOrId::Name(name),
            } => {
                let network_interface = self
                    .instance_lookup(opctx, instance_selector)?
                    .network_interface_name(Name::ref_cast(name));
                Ok(network_interface)
            }
            params::NetworkInterfaceSelector {
              instance_selector: Some(_),
              network_interface: NameOrId::Id(_),
            } => {
              Err(Error::invalid_request(
                "when providing network_interface as an id, instance_selector should not be specified"
              ))
            }
            _ => {
              Err(Error::invalid_request(
                "network_interface should either be a UUID or instance should be specified"
              ))
            }
        }
    }

    /// Create a network interface attached to the provided instance.
    // TODO-performance: Add a version of this that accepts the instance ID
    // directly. This will avoid all the internal database lookups in the event
    // that we create many NICs for the same instance, such as in a saga.
    pub async fn network_interface_create(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        params: &params::NetworkInterfaceCreate,
    ) -> CreateResult<db::model::NetworkInterface> {
        let (.., authz_project, authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        // NOTE: We need to lookup the VPC and VPC Subnet, since we need both
        // IDs for creating the network interface.
        //
        // TODO-correctness: There are additional races here. The VPC and VPC
        // Subnet could both be deleted between the time we fetch them and
        // actually insert the record for the interface. The solution is likely
        // to make both objects implement `DatastoreCollection` for their
        // children, and then use `VpcSubnet::insert_resource` inside the
        // `instance_create_network_interface` method. See
        // https://github.com/oxidecomputer/omicron/issues/738.
        let vpc_name = db::model::Name(params.vpc_name.clone());
        let subnet_name = db::model::Name(params.subnet_name.clone());
        let (.., authz_vpc, authz_subnet, db_subnet) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .vpc_name(&vpc_name)
                .vpc_subnet_name(&subnet_name)
                .fetch()
                .await?;
        let interface_id = Uuid::new_v4();
        let interface = db::model::IncompleteNetworkInterface::new(
            interface_id,
            authz_instance.id(),
            authz_vpc.id(),
            db_subnet,
            params.identity.clone(),
            params.ip,
        )?;
        self.db_datastore
            .instance_create_network_interface(
                opctx,
                &authz_subnet,
                &authz_instance,
                interface,
            )
            .await
            .map_err(|e| {
                debug!(
                    self.log,
                    "failed to create network interface";
                    "instance_id" => ?authz_instance.id(),
                    "interface_id" => ?interface_id,
                    "error" => ?e,
                );
                if matches!(
                    e,
                    network_interface::InsertError::InstanceNotFound(_)
                ) {
                    // Return the not-found message of the authz interface
                    // object, so that the message reflects how the caller
                    // originally looked it up
                    authz_instance.not_found()
                } else {
                    // Convert other errors into an appropriate client error
                    network_interface::InsertError::into_external(e)
                }
            })
    }

    /// Lists network interfaces attached to the instance.
    pub async fn network_interface_list(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::NetworkInterface> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .instance_list_network_interfaces(opctx, &authz_instance, pagparams)
            .await
    }

    /// Update a network interface for the given instance.
    pub async fn network_interface_update(
        &self,
        opctx: &OpContext,
        network_interface_lookup: &lookup::NetworkInterface<'_>,
        updates: params::NetworkInterfaceUpdate,
    ) -> UpdateResult<db::model::NetworkInterface> {
        let (.., authz_instance, authz_interface) =
            network_interface_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .instance_update_network_interface(
                opctx,
                &authz_instance,
                &authz_interface,
                db::model::NetworkInterfaceUpdate::from(updates),
            )
            .await
    }

    /// Delete a network interface from the provided instance.
    ///
    /// Note that the primary interface for an instance cannot be deleted if
    /// there are any secondary interfaces.
    pub async fn network_interface_delete(
        &self,
        opctx: &OpContext,
        network_interface_lookup: &lookup::NetworkInterface<'_>,
    ) -> DeleteResult {
        let (.., authz_instance, authz_interface) =
            network_interface_lookup.lookup_for(authz::Action::Delete).await?;
        self.db_datastore
            .instance_delete_network_interface(
                opctx,
                &authz_instance,
                &authz_interface,
            )
            .await
            .map_err(|e| {
                debug!(
                    self.log,
                    "failed to delete network interface";
                    "instance_id" => ?authz_instance.id(),
                    "interface_id" => ?authz_interface.id(),
                    "error" => ?e,
                );
                if matches!(
                    e,
                    network_interface::DeleteError::InstanceNotFound(_)
                ) {
                    // Return the not-found message of the authz interface
                    // object, so that the message reflects how the caller
                    // originally looked it up
                    authz_instance.not_found()
                } else {
                    // Convert other errors into an appropriate client error
                    network_interface::DeleteError::into_external(e)
                }
            })
    }
}

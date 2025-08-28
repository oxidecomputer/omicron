use nexus_db_lookup::lookup;
use nexus_db_queries::authz::ApiResource;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::queries::network_interface;
use nexus_types::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use oxnet::IpNet;
use uuid::Uuid;

use nexus_db_lookup::{self, LookupPath};
use nexus_db_queries::authz;
use nexus_db_queries::db;

impl super::Nexus {
    pub fn instance_network_interface_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        network_interface_selector: params::InstanceNetworkInterfaceSelector,
    ) -> LookupResult<lookup::InstanceNetworkInterface<'a>> {
        match network_interface_selector {
            params::InstanceNetworkInterfaceSelector {
                network_interface: NameOrId::Id(id),
                instance: None,
                project: None,
            } => {
                let network_interface =
                    LookupPath::new(opctx, &self.db_datastore)
                        .instance_network_interface_id(id);
                Ok(network_interface)
            }
            params::InstanceNetworkInterfaceSelector {
                network_interface: NameOrId::Name(name),
                instance: Some(instance),
                project,
            } => {
                let network_interface = self
                    .instance_lookup(
                        opctx,
                        params::InstanceSelector { project, instance },
                    )?
                    .instance_network_interface_name_owned(name.into());
                Ok(network_interface)
            }
            params::InstanceNetworkInterfaceSelector {
                network_interface: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing network_interface as an id instance and project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "network_interface should either be a UUID or instance should be specified",
            )),
        }
    }

    /// Create a network interface attached to the provided instance.
    // TODO-performance: Add a version of this that accepts the instance ID
    // directly. This will avoid all the internal database lookups in the event
    // that we create many NICs for the same instance, such as in a saga.
    pub(crate) async fn network_interface_create(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        params: &params::InstanceNetworkInterfaceCreate,
    ) -> CreateResult<db::model::InstanceNetworkInterface> {
        let (.., authz_project, authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        // NOTE: We need to lookup the VPC and VPC Subnet, since we need both
        // IDs for creating the network interface.
        let vpc_name = db::model::Name(params.vpc_name.clone());
        let subnet_name = db::model::Name(params.subnet_name.clone());
        let (.., authz_subnet, db_subnet) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .vpc_name(&vpc_name)
                .vpc_subnet_name(&subnet_name)
                .fetch()
                .await?;
        let interface_id = Uuid::new_v4();
        let interface = db::model::IncompleteNetworkInterface::new_instance(
            interface_id,
            InstanceUuid::from_untyped_uuid(authz_instance.id()),
            db_subnet,
            params.identity.clone(),
            params.ip,
            params.transit_ips.iter().map(|ip| (*ip).into()).collect(),
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
    pub(crate) async fn instance_network_interface_list(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::InstanceNetworkInterface> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .instance_list_network_interfaces(opctx, &authz_instance, pagparams)
            .await
    }

    /// Update a network interface for the given instance.
    pub(crate) async fn instance_network_interface_update(
        &self,
        opctx: &OpContext,
        network_interface_lookup: &lookup::InstanceNetworkInterface<'_>,
        updates: params::InstanceNetworkInterfaceUpdate,
    ) -> UpdateResult<db::model::InstanceNetworkInterface> {
        let (.., authz_instance, authz_interface) =
            network_interface_lookup.lookup_for(authz::Action::Modify).await?;
        validate_transit_ips(updates.transit_ips.as_slice())?;
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
    pub(crate) async fn instance_network_interface_delete(
        &self,
        opctx: &OpContext,
        network_interface_lookup: &lookup::InstanceNetworkInterface<'_>,
    ) -> DeleteResult {
        let (.., authz_instance, authz_interface) =
            network_interface_lookup.lookup_for(authz::Action::Delete).await?;
        let interface_was_deleted = self
            .db_datastore
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
            })?;

        // If the interface was already deleted, in general we'd expect to
        // return an error on the `lookup_for(Delete)` above. However, we have a
        // TOCTOU race here; if multiple simultaneous calls to delete the same
        // interface arrive, all will pass the `lookup_for`, then one will get
        // `interface_was_deleted=true` and the rest will get
        // `interface_was_deleted=false`. Convert those falses into 404s to
        // match what subsequent delete requests will see.
        if interface_was_deleted {
            Ok(())
        } else {
            Err(authz_interface.not_found())
        }
    }
}

fn validate_transit_ips(ips: &[IpNet]) -> Result<(), Error> {
    for (i, ip) in ips.iter().enumerate() {
        let (count, ty) = if ip.is_host_net() {
            ("", "address")
        } else {
            (" block", "network")
        };

        if !ip.is_network_address() {
            return Err(Error::invalid_request(format!(
                "transit IP{count} {ip} has a non-zero host identifier"
            )));
        }

        if ip.is_multicast() {
            return Err(Error::invalid_request(format!(
                "transit IP{count} {ip} is a multicast {ty}"
            )));
        }

        if ip.is_loopback() {
            return Err(Error::invalid_request(format!(
                "transit IP{count} {ip} is a loopback {ty}"
            )));
        }

        // Checking for overlapping CIDRs using all prior ips is O(n^2). This
        // is an infrequent check, and we can bound n if desired.
        // The fastest way to catch overlaps would be to make use of a trie for
        // representing past `IpNet`s per address family.
        let overlap = &ips[..i].iter().find(|el| ip.overlaps(el));

        if let Some(past) = overlap {
            return Err(Error::invalid_request(format!(
                "transit IP{count} {ip} overlaps with {past}"
            )));
        }
    }

    Ok(())
}

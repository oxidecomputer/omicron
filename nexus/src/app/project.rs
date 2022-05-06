// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Unimpl, MAX_DISKS_PER_INSTANCE};
use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::db::model::VpcRouterKind;
use crate::db::subnet_allocation::SubnetError;
use crate::defaults;
use crate::external_api::params;
use crate::sagas;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::RouterRouteCreateParams;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::UpdateResult;
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    pub async fn project_create(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        new_project: &params::ProjectCreate,
    ) -> CreateResult<db::model::Project> {
        let (.., authz_org) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        // Create a project.
        let db_project =
            db::model::Project::new(authz_org.id(), new_project.clone());
        let db_project = self
            .db_datastore
            .project_create(opctx, &authz_org, db_project)
            .await?;

        // TODO: We probably want to have "project creation" and "default VPC
        // creation" co-located within a saga for atomicity.
        //
        // Until then, we just perform the operations sequentially.

        // Create a default VPC associated with the project.
        // TODO-correctness We need to be using the project_id we just created.
        // project_create() should return authz::Project and we should use that
        // here.
        let _ = self
            .project_create_vpc(
                opctx,
                &organization_name,
                &new_project.identity.name.clone().into(),
                &params::VpcCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "default".parse().unwrap(),
                        description: "Default VPC".to_string(),
                    },
                    ipv6_prefix: Some(defaults::random_vpc_ipv6_prefix()?),
                    // TODO-robustness this will need to be None if we decide to
                    // handle the logic around name and dns_name by making
                    // dns_name optional
                    dns_name: "default".parse().unwrap(),
                },
            )
            .await?;

        Ok(db_project)
    }

    pub async fn project_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
    ) -> LookupResult<db::model::Project> {
        let (.., db_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .fetch()
            .await?;
        Ok(db_project)
    }

    pub async fn projects_list_by_name(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Project> {
        let (.., authz_org) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        self.db_datastore
            .projects_list_by_name(opctx, &authz_org, pagparams)
            .await
    }

    pub async fn projects_list_by_id(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Project> {
        let (.., authz_org) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        self.db_datastore
            .projects_list_by_id(opctx, &authz_org, pagparams)
            .await
    }

    pub async fn project_list_instances(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Instance> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .project_list_instances(opctx, &authz_project, pagparams)
            .await
    }

    pub async fn project_delete(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
    ) -> DeleteResult {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::Delete)
            .await?;
        self.db_datastore.project_delete(opctx, &authz_project).await
    }

    pub async fn project_update(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        new_params: &params::ProjectUpdate,
    ) -> UpdateResult<db::model::Project> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .project_update(opctx, &authz_project, new_params.clone().into())
            .await
    }

    // Instances

    pub async fn project_create_instance(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::InstanceCreate,
    ) -> CreateResult<db::model::Instance> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        // Validate parameters
        if params.disks.len() > MAX_DISKS_PER_INSTANCE as usize {
            return Err(Error::invalid_request(&format!(
                "cannot attach more than {} disks to instance!",
                MAX_DISKS_PER_INSTANCE
            )));
        }

        let saga_params = Arc::new(sagas::ParamsInstanceCreate {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            organization_name: organization_name.clone().into(),
            project_name: project_name.clone().into(),
            project_id: authz_project.id(),
            create_params: params.clone(),
        });

        let saga_outputs = self
            .execute_saga(
                Arc::clone(&sagas::SAGA_INSTANCE_CREATE_TEMPLATE),
                sagas::SAGA_INSTANCE_CREATE_NAME,
                saga_params,
            )
            .await?;
        // TODO-error more context would be useful
        let instance_id =
            saga_outputs.lookup_output::<Uuid>("instance_id").map_err(|e| {
                Error::InternalError { internal_message: e.to_string() }
            })?;
        // TODO-correctness TODO-robustness TODO-design It's not quite correct
        // to take this instance id and look it up again.  It's possible that
        // it's been modified or even deleted since the saga executed.  In that
        // case, we might return a different state of the Instance than the one
        // that the user created or even fail with a 404!  Both of those are
        // wrong behavior -- we should be returning the very instance that the
        // user created.
        //
        // How can we fix this?  Right now we have internal representations like
        // Instance and analaogous end-user-facing representations like
        // Instance.  The former is not even serializable.  The saga
        // _could_ emit the View version, but that's not great for two (related)
        // reasons: (1) other sagas might want to provision instances and get
        // back the internal representation to do other things with the
        // newly-created instance, and (2) even within a saga, it would be
        // useful to pass a single Instance representation along the saga,
        // but they probably would want the internal representation, not the
        // view.
        //
        // The saga could emit an Instance directly.  Today, Instance
        // etc. aren't supposed to even be serializable -- we wanted to be able
        // to have other datastore state there if needed.  We could have a third
        // InstanceInternalView...but that's starting to feel pedantic.  We
        // could just make Instance serializable, store that, and call it a
        // day.  Does it matter that we might have many copies of the same
        // objects in memory?
        //
        // If we make these serializable, it would be nice if we could leverage
        // the type system to ensure that we never accidentally send them out a
        // dropshot endpoint.  (On the other hand, maybe we _do_ want to do
        // that, for internal interfaces!  Can we do this on a
        // per-dropshot-server-basis?)
        //
        // TODO Even worse, post-authz, we do two lookups here instead of one.
        // Maybe sagas should be able to emit `authz::Instance`-type objects.
        let (.., db_instance) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(instance_id)
            .fetch()
            .await?;
        Ok(db_instance)
    }

    pub async fn project_instance_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> LookupResult<db::model::Instance> {
        let (.., db_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .fetch()
            .await?;
        Ok(db_instance)
    }

    // TODO-correctness It's not totally clear what the semantics and behavior
    // should be here.  It might be nice to say that you can only do this
    // operation if the Instance is already stopped, in which case we can
    // execute this immediately by just removing it from the database, with the
    // same race we have with disk delete (i.e., if someone else is requesting
    // an instance boot, we may wind up in an inconsistent state).  On the other
    // hand, we could always allow this operation, issue the request to the SA
    // to destroy the instance (not just stop it), and proceed with deletion
    // when that finishes.  But in that case, although the HTTP DELETE request
    // completed, the object will still appear for a little while, which kind of
    // sucks.
    pub async fn project_destroy_instance(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> DeleteResult {
        // TODO-robustness We need to figure out what to do with Destroyed
        // instances?  Presumably we need to clean them up at some point, but
        // not right away so that callers can see that they've been destroyed.
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::Delete)
            .await?;
        self.db_datastore.project_delete_instance(opctx, &authz_instance).await
    }

    // Disks

    pub async fn project_list_disks(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Disk> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .project_list_disks(opctx, &authz_project, pagparams)
            .await
    }

    pub async fn project_create_disk(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::DiskCreate,
    ) -> CreateResult<db::model::Disk> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        match &params.disk_source {
            params::DiskSource::Blank { block_size } => {
                // Reject disks where the block size doesn't evenly divide the
                // total size
                if (params.size.to_bytes() % block_size.0 as u64) != 0 {
                    return Err(Error::InvalidValue {
                        label: String::from("size and block_size"),
                        message: String::from(
                            "total size must be a multiple of block size",
                        ),
                    });
                }
            }
            params::DiskSource::Snapshot { snapshot_id: _ } => {
                // Until we implement snapshots, do not allow disks to be
                // created from a snapshot.
                return Err(Error::InvalidValue {
                    label: String::from("snapshot"),
                    message: String::from("snapshots are not yet supported"),
                });
            }
            params::DiskSource::Image { image_id: _ } => {
                // Until we implement project images, do not allow disks to be
                // created from a project image.
                return Err(Error::InvalidValue {
                    label: String::from("image"),
                    message: String::from(
                        "project image are not yet supported",
                    ),
                });
            }
            params::DiskSource::GlobalImage { image_id } => {
                let (.., db_global_image) =
                    LookupPath::new(opctx, &self.db_datastore)
                        .global_image_id(*image_id)
                        .fetch()
                        .await?;

                // Reject disks where the block size doesn't evenly divide the
                // total size
                if (params.size.to_bytes()
                    % db_global_image.block_size.to_bytes() as u64)
                    != 0
                {
                    return Err(Error::InvalidValue {
                        label: String::from("size and block_size"),
                        message: String::from(
                            "total size must be a multiple of global image's block size",
                        ),
                    });
                }

                // If the size of the image is greater than the size of the
                // disk, return an error.
                if db_global_image.size.to_bytes() > params.size.to_bytes() {
                    return Err(Error::invalid_request(
                        &format!(
                            "disk size {} must be greater than or equal to image size {}",
                            params.size.to_bytes(),
                            db_global_image.size.to_bytes(),
                        ),
                    ));
                }
            }
        }

        let saga_params = Arc::new(sagas::ParamsDiskCreate {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: authz_project.id(),
            create_params: params.clone(),
        });
        let saga_outputs = self
            .execute_saga(
                Arc::clone(&sagas::SAGA_DISK_CREATE_TEMPLATE),
                sagas::SAGA_DISK_CREATE_NAME,
                saga_params,
            )
            .await?;
        let disk_created = saga_outputs
            .lookup_output::<db::model::Disk>("created_disk")
            .map_err(|e| Error::InternalError {
                internal_message: e.to_string(),
            })?;
        Ok(disk_created)
    }

    pub async fn project_disk_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        disk_name: &Name,
    ) -> LookupResult<db::model::Disk> {
        let (.., db_disk) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .disk_name(disk_name)
            .fetch()
            .await?;
        Ok(db_disk)
    }

    pub async fn project_delete_disk(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        disk_name: &Name,
    ) -> DeleteResult {
        let (.., authz_disk) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .disk_name(disk_name)
            .lookup_for(authz::Action::Delete)
            .await?;

        let saga_params =
            Arc::new(sagas::ParamsDiskDelete { disk_id: authz_disk.id() });
        self.execute_saga(
            Arc::clone(&sagas::SAGA_DISK_DELETE_TEMPLATE),
            sagas::SAGA_DISK_DELETE_NAME,
            saga_params,
        )
        .await?;

        Ok(())
    }

    // Images

    pub async fn project_list_images(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        _pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Image> {
        let _ = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn project_create_image(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        _params: &params::ImageCreate,
    ) -> CreateResult<db::model::Image> {
        let _ = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn project_image_fetch(
        &self,
        opctx: &OpContext,
        _organization_name: &Name,
        _project_name: &Name,
        image_name: &Name,
    ) -> LookupResult<db::model::Image> {
        let lookup_type = LookupType::ByName(image_name.to_string());
        let not_found_error = lookup_type.into_not_found(ResourceType::Image);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub async fn project_delete_image(
        self: &Arc<Self>,
        opctx: &OpContext,
        _organization_name: &Name,
        _project_name: &Name,
        image_name: &Name,
    ) -> DeleteResult {
        let lookup_type = LookupType::ByName(image_name.to_string());
        let not_found_error = lookup_type.into_not_found(ResourceType::Image);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    // Snapshots

    pub async fn project_create_snapshot(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        _params: &params::SnapshotCreate,
    ) -> CreateResult<db::model::Snapshot> {
        let _ = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn project_list_snapshots(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        _pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Snapshot> {
        let _ = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn project_snapshot_fetch(
        &self,
        opctx: &OpContext,
        _organization_name: &Name,
        _project_name: &Name,
        snapshot_name: &Name,
    ) -> LookupResult<db::model::Snapshot> {
        let lookup_type = LookupType::ByName(snapshot_name.to_string());
        let not_found_error =
            lookup_type.into_not_found(ResourceType::Snapshot);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub async fn project_delete_snapshot(
        self: &Arc<Self>,
        opctx: &OpContext,
        _organization_name: &Name,
        _project_name: &Name,
        snapshot_name: &Name,
    ) -> DeleteResult {
        let lookup_type = LookupType::ByName(snapshot_name.to_string());
        let not_found_error =
            lookup_type.into_not_found(ResourceType::Snapshot);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    // VPCs

    pub async fn project_list_vpcs(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Vpc> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .project_list_vpcs(&opctx, &authz_project, pagparams)
            .await
    }

    pub async fn project_create_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::VpcCreate,
    ) -> CreateResult<db::model::Vpc> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        let vpc_id = Uuid::new_v4();
        let system_router_id = Uuid::new_v4();
        let default_route_id = Uuid::new_v4();
        let default_subnet_id = Uuid::new_v4();

        // TODO: This is both fake and utter nonsense. It should be eventually
        // replaced with the proper behavior for creating the default route
        // which may not even happen here. Creating the vpc, its system router,
        // and that routers default route should all be a part of the same
        // transaction.
        let vpc = db::model::Vpc::new(
            vpc_id,
            authz_project.id(),
            system_router_id,
            params.clone(),
        )?;
        let (authz_vpc, db_vpc) = self
            .db_datastore
            .project_create_vpc(opctx, &authz_project, vpc)
            .await?;

        // TODO: Ultimately when the VPC is created a system router w/ an
        // appropriate setup should also be created.  Given that the underlying
        // systems aren't wired up yet this is a naive implementation to
        // populate the database with a starting router. Eventually this code
        // should be replaced with a saga that'll handle creating the VPC and
        // its underlying system
        let router = db::model::VpcRouter::new(
            system_router_id,
            vpc_id,
            VpcRouterKind::System,
            params::VpcRouterCreate {
                identity: IdentityMetadataCreateParams {
                    name: "system".parse().unwrap(),
                    description: "Routes are automatically added to this \
                        router as vpc subnets are created"
                        .into(),
                },
            },
        );
        let (authz_router, _) = self
            .db_datastore
            .vpc_create_router(&opctx, &authz_vpc, router)
            .await?;
        let route = db::model::RouterRoute::new(
            default_route_id,
            system_router_id,
            RouterRouteKind::Default,
            RouterRouteCreateParams {
                identity: IdentityMetadataCreateParams {
                    name: "default".parse().unwrap(),
                    description: "The default route of a vpc".to_string(),
                },
                target: RouteTarget::InternetGateway(
                    "outbound".parse().unwrap(),
                ),
                destination: RouteDestination::Vpc(
                    params.identity.name.clone(),
                ),
            },
        );

        self.db_datastore
            .router_create_route(opctx, &authz_router, route)
            .await?;

        // Allocate the first /64 sub-range from the requested or created
        // prefix.
        let ipv6_block = external::Ipv6Net(
            ipnetwork::Ipv6Network::new(db_vpc.ipv6_prefix.network(), 64)
                .map_err(|_| {
                    external::Error::internal_error(
                        "Failed to allocate default IPv6 subnet",
                    )
                })?,
        );

        // TODO: batch this up with everything above
        let subnet = db::model::VpcSubnet::new(
            default_subnet_id,
            vpc_id,
            IdentityMetadataCreateParams {
                name: "default".parse().unwrap(),
                description: format!(
                    "The default subnet for {}",
                    params.identity.name
                ),
            },
            *defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK,
            ipv6_block,
        );

        // Create the subnet record in the database. Overlapping IP ranges
        // should be translated into an internal error. That implies that
        // there's already an existing VPC Subnet, but we're explicitly creating
        // the _first_ VPC in the project. Something is wrong, and likely a bug
        // in our code.
        self.db_datastore
            .vpc_create_subnet(opctx, &authz_vpc, subnet)
            .await
            .map_err(|err| match err {
                SubnetError::OverlappingIpRange(ip) => {
                    let ipv4_block = &defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK;
                    error!(
                        self.log,
                        concat!(
                            "failed to create default VPC Subnet, IP address ",
                            "range '{}' overlaps with existing",
                        ),
                        ip;
                        "vpc_id" => ?vpc_id,
                        "subnet_id" => ?default_subnet_id,
                        "ipv4_block" => ?**ipv4_block,
                        "ipv6_block" => ?ipv6_block,
                    );
                    external::Error::internal_error(
                        "Failed to create default VPC Subnet, \
                            found overlapping IP address ranges",
                    )
                }
                SubnetError::External(e) => e,
            })?;
        let rules = db::model::VpcFirewallRule::vec_from_params(
            authz_vpc.id(),
            defaults::DEFAULT_FIREWALL_RULES.clone(),
        );
        self.db_datastore
            .vpc_update_firewall_rules(opctx, &authz_vpc, rules)
            .await?;
        Ok(db_vpc)
    }

    pub async fn project_vpc_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> LookupResult<db::model::Vpc> {
        let (.., db_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .fetch()
            .await?;
        Ok(db_vpc)
    }

    pub async fn project_update_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &params::VpcUpdate,
    ) -> UpdateResult<db::model::Vpc> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .project_update_vpc(opctx, &authz_vpc, params.clone().into())
            .await
    }

    pub async fn project_delete_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> DeleteResult {
        let (.., authz_vpc, db_vpc) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .fetch()
                .await?;

        let authz_vpc_router = authz::VpcRouter::new(
            authz_vpc.clone(),
            db_vpc.system_router_id,
            LookupType::ById(db_vpc.system_router_id),
        );

        // TODO: This should eventually use a saga to call the
        // networking subsystem to have it clean up the networking resources
        self.db_datastore.vpc_delete_router(&opctx, &authz_vpc_router).await?;
        self.db_datastore.project_delete_vpc(opctx, &authz_vpc).await?;

        // Delete all firewall rules after deleting the VPC, to ensure no
        // firewall rules get added between rules deletion and VPC deletion.
        self.db_datastore
            .vpc_delete_all_firewall_rules(&opctx, &authz_vpc)
            .await
    }

}

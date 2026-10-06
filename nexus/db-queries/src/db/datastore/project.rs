// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Project`]s.

use super::DataStore;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::identity::Resource;
use crate::db::model::CollectionTypeProvisioned;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::model::ProjectUpdate;
use crate::db::model::Silo;
use crate::db::model::VirtualProvisioningCollection;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_fixed_data::project::SERVICES_PROJECT;
use nexus_types::silo::INTERNAL_SILO_ID;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use ref_cast::RefCast;

// Generates internal functions used for validation during project deletion.
// Used simply to reduce boilerplate.
//
// It assumes:
//
// - $i is an identifier for a type of resource.
// - $i has a corresponding "db::schema::$i", which has a project_id,
// time_deleted, and $label field.
// - If $label is supplied, it must be a mandatory column of the table
// which is (1) looked up, and (2) used in an error message, if the resource.
// exists in the project. Otherwise, it is assumbed to be a "Uuid" named "id".
macro_rules! generate_fn_to_ensure_none_in_project {
    ($i:ident, $label:ident, $label_ty:ty) => {
        ::paste::paste! {
            async fn [<ensure_no_ $i s_in_project>](
                &self,
                opctx: &OpContext,
                authz_project: &authz::Project,
            ) -> DeleteResult {
                use nexus_db_schema::schema::$i;

                let maybe_label = $i::dsl::$i
                    .filter($i::dsl::project_id.eq(authz_project.id()))
                    .filter($i::dsl::time_deleted.is_null())
                    .select($i::dsl::$label)
                    .limit(1)
                    .first_async::<$label_ty>(&*self.pool_connection_authorized(opctx).await?)
                    .await
                    .optional()
                    .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

                if let Some(label) = maybe_label {
                    let object = stringify!($i).replace('_', " ");
                    const VOWELS: [char; 5] = ['a', 'e', 'i', 'o', 'u'];
                    let article = if VOWELS.iter().any(|&v| object.starts_with(v)) {
                        "an"
                    } else {
                        "a"
                    };

                    return Err(Error::invalid_request(
                        format!("project to be deleted contains {article} {object}: {label}")
                    ));
                }

                Ok(())
            }
        }
    };
    ($i:ident) => {
        generate_fn_to_ensure_none_in_project!($i, id, Uuid);
    };
}

impl DataStore {
    /// Load built-in projects into the database
    pub async fn load_builtin_projects(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        debug!(opctx.log, "attempting to create built-in projects");

        let (authz_silo,) = nexus_db_lookup::LookupPath::new(&opctx, self)
            .silo_id(INTERNAL_SILO_ID)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        self.project_create_in_silo(
            opctx,
            SERVICES_PROJECT.clone(),
            &authz_silo,
        )
        .await
        .map(|_| ())
        .or_else(|e| match e {
            Error::ObjectAlreadyExists { .. } => Ok(()),
            _ => Err(e),
        })?;

        info!(opctx.log, "created built-in services project");

        Ok(())
    }

    /// Create a project
    pub async fn project_create(
        &self,
        opctx: &OpContext,
        project: Project,
    ) -> CreateResult<(authz::Project, Project)> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("creating a Project")?;
        self.project_create_in_silo(opctx, project, &authz_silo).await
    }

    /// Create a project in the given silo.
    async fn project_create_in_silo(
        &self,
        opctx: &OpContext,
        project: Project,
        authz_silo: &authz::Silo,
    ) -> CreateResult<(authz::Project, Project)> {
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;

        let silo_id = authz_silo.id();
        let authz_silo_inner = authz_silo.clone();

        use nexus_db_schema::schema::project::dsl;

        let err = OptionalError::new();
        let name = project.name().as_str().to_string();
        let conn = self.pool_connection_authorized(opctx).await?;

        let db_project = self
            .transaction_retry_wrapper("project_create_in_silo")
            .transaction(&conn, |conn| {
                let err = err.clone();

                let authz_silo_inner = authz_silo_inner.clone();
                let name = name.clone();
                let project = project.clone();
                async move {
                    let project: Project = Silo::insert_resource(
                        silo_id,
                        diesel::insert_into(dsl::project).values(project),
                    )
                    .insert_and_get_result_async(&conn)
                    .await
                    .map_err(|e| match e {
                        AsyncInsertError::CollectionNotFound => {
                            err.bail(authz_silo_inner.not_found())
                        }
                        AsyncInsertError::DatabaseError(diesel_error) => err
                            .bail_retryable_or_else(
                                diesel_error,
                                |diesel_error| {
                                    public_error_from_diesel(
                                        diesel_error,
                                        ErrorHandler::Conflict(
                                            ResourceType::Project,
                                            &name,
                                        ),
                                    )
                                },
                            ),
                    })?;

                    // Create resource provisioning for the project.
                    self.virtual_provisioning_collection_create_on_connection(
                        &conn,
                        VirtualProvisioningCollection::new(
                            project.id(),
                            CollectionTypeProvisioned::Project,
                        ),
                    )
                    .await?;
                    Ok(project)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;

        Ok((
            authz::Project::new(
                authz_silo.clone(),
                db_project.id(),
                LookupType::ByName(db_project.name().to_string()),
            ),
            db_project,
        ))
    }

    generate_fn_to_ensure_none_in_project!(instance, name, String);
    generate_fn_to_ensure_none_in_project!(disk, name, String);
    generate_fn_to_ensure_none_in_project!(floating_ip, name, String);
    generate_fn_to_ensure_none_in_project!(project_image, name, String);
    generate_fn_to_ensure_none_in_project!(snapshot, name, String);
    generate_fn_to_ensure_none_in_project!(vpc, name, String);
    generate_fn_to_ensure_none_in_project!(affinity_group, name, String);
    generate_fn_to_ensure_none_in_project!(anti_affinity_group, name, String);

    /// Delete a project
    pub async fn project_delete(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        db_project: &db::model::Project,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_project).await?;

        // Verify that child resources do not exist.
        self.ensure_no_instances_in_project(opctx, authz_project).await?;
        self.ensure_no_disks_in_project(opctx, authz_project).await?;
        self.ensure_no_floating_ips_in_project(opctx, authz_project).await?;
        self.ensure_no_project_images_in_project(opctx, authz_project).await?;
        self.ensure_no_snapshots_in_project(opctx, authz_project).await?;
        self.ensure_no_vpcs_in_project(opctx, authz_project).await?;
        self.ensure_no_affinity_groups_in_project(opctx, authz_project).await?;
        self.ensure_no_anti_affinity_groups_in_project(opctx, authz_project)
            .await?;

        use nexus_db_schema::schema::project::dsl;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("project_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    let now = Utc::now();
                    let updated_rows = diesel::update(dsl::project)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(authz_project.id()))
                        .filter(dsl::rcgen.eq(db_project.rcgen))
                        .set(dsl::time_deleted.eq(now))
                        .returning(Project::as_returning())
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::NotFoundByResource(
                                        authz_project,
                                    ),
                                )
                            })
                        })?;

                    if updated_rows == 0 {
                        return Err(err.bail(Error::invalid_request(
                            "deletion failed due to concurrent modification",
                        )));
                    }

                    self.virtual_provisioning_collection_delete_on_connection(
                        &opctx.log,
                        &conn,
                        db_project.id(),
                    )
                    .await?;
                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    pub async fn projects_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Project> {
        let authz_silo =
            opctx.authn.silo_required().internal_context("listing Projects")?;
        opctx.authorize(authz::Action::ListChildren, &authz_silo).await?;

        use nexus_db_schema::schema::project::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::project, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::project,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::silo_id.eq(authz_silo.id()))
        .filter(dsl::time_deleted.is_null())
        .select(Project::as_select())
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Updates a project (clobbering update -- no etag)
    pub async fn project_update(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        updates: ProjectUpdate,
    ) -> UpdateResult<Project> {
        opctx.authorize(authz::Action::Modify, authz_project).await?;

        use nexus_db_schema::schema::project::dsl;
        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_project.id()))
            .set(updates)
            .returning(Project::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_project),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::Ipv4Addr;

    use crate::db::DataStore;
    use crate::db::datastore::DnsVersionUpdateBuilder;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_auth::authz;
    use nexus_db_model::DnsGroup;
    use nexus_db_model::InitialDnsGroup;
    use nexus_db_model::Project;
    use nexus_types::external_api::external_subnet::ExternalSubnetAllocator;
    use nexus_types::external_api::external_subnet::ExternalSubnetCreate;
    use nexus_types::external_api::ip_pool::PoolSelector;
    use nexus_types::external_api::project::ProjectCreate;
    use nexus_types::external_api::silo::SiloCreate;
    use nexus_types::external_api::silo::SiloIdentityMode;
    use nexus_types::external_api::silo::SiloQuotasCreate;
    use nexus_types::external_api::subnet_pool::SubnetPoolCreate;
    use nexus_types::external_api::subnet_pool::SubnetPoolMemberAdd;
    use nexus_types::identity::Resource as _;
    use omicron_common::address::IpVersion;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::LookupType;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid as _;
    use oxnet::IpNet;
    use oxnet::Ipv4Net;

    #[tokio::test]
    async fn cannot_delete_project_with_outstanding_external_subnet() {
        let logctx = dev::test_setup_log(
            "cannot_delete_project_with_outstanding_external_subnet",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let opctx = db.opctx();

        // Create the resource hierarchy. This starts with some dummy DNS data,
        // and then the Silo itself.
        let initial = InitialDnsGroup::new(
            DnsGroup::External,
            "dummy.oxide.test",
            "test suite",
            "test suite",
            HashMap::new(),
        );
        DataStore::load_dns_data(
            &db.datastore().pool_connection_for_tests().await.unwrap(),
            initial,
        )
        .await
        .expect("failed to load initial DNS zone");
        let silo = db
            .datastore()
            .silo_create(
                opctx,
                opctx,
                SiloCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "silo".parse().unwrap(),
                        description: String::new(),
                    },
                    identity_mode: SiloIdentityMode::LocalOnly,
                    admin_group_name: None,
                    tls_certificates: vec![],
                    quotas: SiloQuotasCreate {
                        cpus: i64::MAX,
                        memory: ByteCount::try_from(u64::from(u32::MAX))
                            .unwrap(),
                        storage: ByteCount::try_from(u64::from(u32::MAX))
                            .unwrap(),
                    },
                    mapped_fleet_roles: Default::default(),
                },
                &[],
                DnsVersionUpdateBuilder::new(
                    DnsGroup::External,
                    String::new(),
                    String::new(),
                ),
            )
            .await
            .expect("able to create silo");
        let authz_silo = authz::Silo::new(
            authz::FLEET,
            silo.id(),
            LookupType::ById(silo.id()),
        );

        // Then the project
        let params = ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: "proj".parse().unwrap(),
                description: String::new(),
            },
            defaults: None,
        };
        let project = Project::new(silo.id(), params);
        let (authz_project, db_project) = db
            .datastore()
            .project_create(opctx, project)
            .await
            .expect("able to create project");

        // Then the subnet pool, linked to the silo as a default pool.
        let subnet_pool = db
            .datastore()
            .create_subnet_pool(
                opctx,
                SubnetPoolCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "subnet-pool".parse().unwrap(),
                        description: String::new(),
                    },
                    ip_version: IpVersion::V4,
                },
            )
            .await
            .expect("able to create subnet pool");
        let authz_pool = authz::SubnetPool::new(
            authz::FLEET,
            subnet_pool.id(),
            LookupType::ById(subnet_pool.id().into_untyped_uuid()),
        );
        db.datastore()
            .link_subnet_pool_to_silo(opctx, &authz_pool, &authz_silo, true)
            .await
            .expect("able to link subnet pool to silo");

        // Next the subnet pool member.
        let _pool_member = db
            .datastore()
            .add_subnet_pool_member(
                opctx,
                &authz_pool,
                &subnet_pool,
                &SubnetPoolMemberAdd {
                    subnet: IpNet::V4(
                        Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 0), 24).unwrap(),
                    ),
                    min_prefix_length: Some(24),
                    max_prefix_length: Some(28),
                },
            )
            .await
            .expect("able to create subnet pool member");

        // Finally the actual subnet, in the project.
        let subnet_name = "subnet";
        let subnet = db
            .datastore()
            .create_external_subnet(
                opctx,
                &silo.id(),
                &authz_project,
                ExternalSubnetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: subnet_name.parse().unwrap(),
                        description: String::new(),
                    },
                    allocator: ExternalSubnetAllocator::Auto {
                        prefix_length: 26,
                        pool_selector: PoolSelector::Auto { ip_version: None },
                    },
                },
            )
            .await
            .expect("able to create subnet in project");
        let authz_subnet = authz::ExternalSubnet::new(
            authz_project.clone(),
            subnet.id(),
            LookupType::ById(subnet.id().into_untyped_uuid()),
        );

        // We should not be able to delete the project now.
        let err = db
            .datastore()
            .project_delete(opctx, &authz_project, &db_project)
            .await
            .expect_err("should not be able to delete project");
        let Error::InvalidRequest { message } = &err else {
            panic!(
                "Expected an InvalidRequest when deleting \
                a project while it still has an outstanding \
                external subnet, but found: {err:#?}",
            );
        };
        assert_eq!(
            message.external_message(),
            &format!(
                "project to be deleted contains an external subnet: \
                {subnet_name}"
            ),
        );

        // Delete the subnet and try again, which should work.
        db.datastore()
            .delete_external_subnet(opctx, &authz_subnet)
            .await
            .expect("able to delete external subnet from project");
        db.datastore()
            .project_delete(opctx, &authz_project, &db_project)
            .await
            .expect("should be able to delete project after deleting subnet");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}

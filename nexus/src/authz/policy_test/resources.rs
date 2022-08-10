// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Structures and functions related to the resources created for the IAM role
//! policy test

use super::coverage::Coverage;
use super::make_uuid;
use crate::authz;
use crate::authz::ApiResourceWithRolesType;
use crate::authz::AuthorizedResource;
use crate::context::OpContext;
use crate::db;
use crate::db::fixed_data::FLEET_ID;
use authz::ApiResource;
use futures::future::BoxFuture;
use futures::FutureExt;
use lazy_static::lazy_static;
use nexus_db_model::DatabaseString;
use nexus_types::external_api::shared;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use std::sync::Arc;
use strum::IntoEnumIterator;
use uuid::Uuid;

lazy_static! {
    pub static ref SILO1_ID: Uuid = make_uuid();
}

/// Manages the construction of the resource hierarchy used in the test, plus
/// associated users and role assignments
struct ResourceBuilder<'a> {
    opctx: &'a OpContext,
    coverage: &'a mut Coverage,
    datastore: &'a db::DataStore,
    // XXX-dap Arc?
    resources: Vec<Arc<dyn Authorizable>>,
    main_silo_id: Uuid,
    users: Vec<(String, Uuid)>,
}

// XXX-dap TODO-doc
impl<'a> ResourceBuilder<'a> {
    fn new(
        opctx: &'a OpContext,
        datastore: &'a db::DataStore,
        coverage: &'a mut Coverage,
        main_silo_id: Uuid,
    ) -> ResourceBuilder<'a> {
        ResourceBuilder {
            opctx,
            coverage,
            datastore,
            resources: Vec::new(),
            main_silo_id,
            users: Vec::new(),
        }
    }

    fn new_resource<T: Authorizable>(&mut self, resource: T) {
        self.coverage.covered(&resource);
        self.resources.push(Arc::new(resource));
    }

    async fn new_resource_with_roles<T>(&mut self, resource: T)
    where
        T: Authorizable + ApiResourceWithRolesType + AuthorizedResource + Clone,
        T::AllowedRoles: IntoEnumIterator,
    {
        self.new_resource(resource.clone());
        let resource_name = match resource.lookup_type() {
            LookupType::ByName(name) => name,
            LookupType::ById(id) if *id == *FLEET_ID => "fleet",
            LookupType::ById(_)
            | LookupType::BySessionToken(_)
            | LookupType::ByCompositeId(_) => {
                panic!("test resources must be given names");
            }
        };
        let silo_id = self.main_silo_id;

        let opctx = self.opctx;
        let datastore = self.datastore;
        for role in T::AllowedRoles::iter() {
            let role_name = role.to_database_string();
            let username = format!("{}-{}", resource_name, role_name);
            let user_id = make_uuid();
            println!("creating user: {}", &username);
            self.users.push((username.clone(), user_id));

            let silo_user =
                db::model::SiloUser::new(silo_id, user_id, username);
            datastore
                .silo_user_create(silo_user)
                .await
                .expect("failed to create silo user");

            let old_role_assignments = datastore
                .role_assignment_fetch_visible(opctx, &resource)
                .await
                .expect("fetching policy");
            let new_role_assignments = old_role_assignments
                .into_iter()
                .map(|r| r.try_into().unwrap())
                .chain(std::iter::once(shared::RoleAssignment {
                    identity_type: shared::IdentityType::SiloUser,
                    identity_id: user_id,
                    role_name: role,
                }))
                .collect::<Vec<_>>();
            datastore
                .role_assignment_replace_visible(
                    opctx,
                    &resource,
                    &new_role_assignments,
                )
                .await
                .expect("failed to assign role");
        }
    }

    fn build(self) -> Resources {
        Resources { resources: self.resources, users: self.users }
    }
}

/// Describes the hierarchy of resources used in our RBAC test
// The hierarchy looks like this:
// fleet
// fleet/s1
// fleet/s1/o1
// fleet/s1/o1/p1
// fleet/s1/o1/p1/vpc1
// fleet/s1/o1/p2
// fleet/s1/o1/p2/vpc1
// fleet/s1/o2
// fleet/s1/o2/p1
// fleet/s1/o2/p1/vpc1
// fleet/s2
// fleet/s2/o1
// fleet/s2/o1/p1
// fleet/s2/o1/p1/vpc1
pub struct Resources {
    resources: Vec<Arc<dyn Authorizable>>,
    users: Vec<(String, Uuid)>,
}

impl Resources {
    pub async fn new<'a>(
        opctx: &'a OpContext,
        datastore: &'a db::DataStore,
        coverage: &'a mut Coverage,
        main_silo_id: Uuid,
    ) -> Resources {
        let mut builder =
            ResourceBuilder::new(opctx, datastore, coverage, main_silo_id);
        // XXX-dap consider moving this back into mod.rs
        Self::init(&mut builder).await;
        builder.build()
    }

    async fn init<'a>(builder: &mut ResourceBuilder<'a>) {
        builder.new_resource(authz::DATABASE.clone());
        builder.new_resource_with_roles(authz::FLEET.clone()).await;

        let silo1 = authz::Silo::new(
            authz::FLEET,
            *SILO1_ID,
            LookupType::ByName(String::from("silo1")),
        );
        builder.new_resource_with_roles(silo1.clone()).await;

        let silo1_org1 = authz::Organization::new(
            silo1.clone(),
            make_uuid(),
            LookupType::ByName(String::from("silo1-org1")),
        );
        builder.new_resource_with_roles(silo1_org1.clone()).await;

        Self::make_project(builder, &silo1_org1, "silo1-org1-proj1", true)
            .await;
        Self::make_project(builder, &silo1_org1, "silo1-org1-proj2", false)
            .await;

        let silo1_org2 = authz::Organization::new(
            silo1.clone(),
            make_uuid(),
            LookupType::ByName(String::from("silo1-org2")),
        );
        builder.new_resource(silo1_org2.clone());
        Self::make_project(builder, &silo1_org2, "silo1-org2-proj1", false)
            .await;

        let silo2 = authz::Silo::new(
            authz::FLEET,
            make_uuid(),
            LookupType::ByName(String::from("silo2")),
        );
        builder.new_resource(silo2.clone());
        let silo2_org1 = authz::Organization::new(
            silo2.clone(),
            make_uuid(),
            LookupType::ByName(String::from("silo2-org1")),
        );
        builder.new_resource(silo2_org1.clone());
        Self::make_project(builder, &silo2_org1, "silo2-org1-proj1", false)
            .await;
    }

    async fn make_project(
        builder: &mut ResourceBuilder<'_>,
        organization: &authz::Organization,
        project_name: &str,
        with_roles: bool,
    ) {
        let project = authz::Project::new(
            organization.clone(),
            make_uuid(),
            LookupType::ByName(project_name.to_string()),
        );
        if with_roles {
            builder.new_resource_with_roles(project.clone()).await;
        } else {
            builder.new_resource(project.clone());
        }

        let vpc1_name = format!("{}-vpc1", project_name);
        let vpc1 = authz::Vpc::new(
            project.clone(),
            make_uuid(),
            LookupType::ByName(vpc1_name.clone()),
        );

        // XXX-dap TODO-coverage add more different kinds of children
        builder.new_resource(authz::Disk::new(
            project.clone(),
            make_uuid(),
            LookupType::ByName(format!("{}-disk1", project_name)),
        ));
        builder.new_resource(authz::Instance::new(
            project.clone(),
            make_uuid(),
            LookupType::ByName(format!("{}-instance1", project_name)),
        ));
        builder.new_resource(vpc1.clone());
        // Test a resource nested two levels below Project
        builder.new_resource(authz::VpcSubnet::new(
            vpc1,
            make_uuid(),
            LookupType::ByName(format!("{}-subnet1", vpc1_name)),
        ));
    }

    pub fn resources(
        &self,
    ) -> impl std::iter::Iterator<Item = Arc<dyn Authorizable>> + '_ {
        self.resources.iter().cloned()
    }

    pub fn users(
        &self,
    ) -> impl std::iter::Iterator<Item = &(String, Uuid)> + '_ {
        self.users.iter()
    }
}

pub trait Authorizable: AuthorizedResource + std::fmt::Debug {
    fn resource_name(&self) -> String;

    fn do_authorize<'a, 'b>(
        &'a self,
        opctx: &'b OpContext,
        action: authz::Action,
    ) -> BoxFuture<'a, Result<(), Error>>
    where
        'b: 'a;
}

impl<T> Authorizable for T
where
    T: ApiResource + AuthorizedResource + oso::PolarClass + Clone,
{
    fn do_authorize<'a, 'b>(
        &'a self,
        opctx: &'b OpContext,
        action: authz::Action,
    ) -> BoxFuture<'a, Result<(), Error>>
    where
        'b: 'a,
    {
        opctx.authorize(action, self).boxed()
    }

    fn resource_name(&self) -> String {
        let my_ident = match self.lookup_type() {
            LookupType::ByName(name) => format!("{:?}", name),
            LookupType::ById(id) => format!("id {:?}", id.to_string()),
            LookupType::BySessionToken(_) | LookupType::ByCompositeId(_) => {
                unimplemented!()
            }
        };

        format!("{:?} {}", self.resource_type(), my_ident)
    }
}

impl Authorizable for authz::oso_generic::Database {
    fn resource_name(&self) -> String {
        String::from("DATABASE")
    }

    fn do_authorize<'a, 'b>(
        &'a self,
        opctx: &'b OpContext,
        action: authz::Action,
    ) -> BoxFuture<'a, Result<(), Error>>
    where
        'b: 'a,
    {
        opctx.authorize(action, self).boxed()
    }
}

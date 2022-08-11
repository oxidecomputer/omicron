// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Structures and functions related to the resources created for the IAM role
//! policy test

use super::coverage::Coverage;
use crate::authz;
use crate::authz::ApiResourceWithRolesType;
use crate::authz::AuthorizedResource;
use crate::context::OpContext;
use crate::db;
use crate::db::fixed_data::FLEET_ID;
use authz::ApiResource;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::DatabaseString;
use nexus_types::external_api::shared;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use std::sync::Arc;
use strum::IntoEnumIterator;
use uuid::Uuid;

/// Manages the construction of the resource hierarchy used in the test, plus
/// associated users and role assignments
pub struct ResourceBuilder<'a> {
    opctx: &'a OpContext,
    coverage: &'a mut Coverage,
    datastore: &'a db::DataStore,
    resources: Vec<Arc<dyn Authorizable>>,
    main_silo_id: Uuid,
    users: Vec<(String, Uuid)>,
}

// XXX-dap TODO-doc
impl<'a> ResourceBuilder<'a> {
    pub fn new(
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

    pub fn new_resource<T: Authorizable>(&mut self, resource: T) {
        self.coverage.covered(&resource);
        self.resources.push(Arc::new(resource));
    }

    pub async fn new_resource_with_roles<T>(&mut self, resource: T)
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
            let user_id = Uuid::new_v4();
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

    pub fn build(self) -> Resources {
        Resources { resources: self.resources, users: self.users }
    }
}

/// Describes the hierarchy of resources used in our RBAC test
pub struct Resources {
    resources: Vec<Arc<dyn Authorizable>>,
    users: Vec<(String, Uuid)>,
}

impl Resources {
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

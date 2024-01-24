// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Structures and functions for creating resources and associated users for the
//! IAM policy test

use super::coverage::Coverage;
use crate::authz;
use crate::authz::ApiResourceWithRolesType;
use crate::authz::AuthorizedResource;
use crate::context::OpContext;
use crate::db;
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
    // Inputs
    /// opcontext used for creating users and role assignments
    opctx: &'a OpContext,
    /// datastore used for creating users and role assignments
    datastore: &'a db::DataStore,
    /// used to verify test coverage of all authz resources
    coverage: &'a mut Coverage,
    /// id of the "main" silo -- this is the one that users are created in
    main_silo_id: Uuid,

    // Outputs
    /// list of resources created so far
    resources: Vec<Arc<dyn DynAuthorizedResource>>,
    /// list of users created so far
    users: Vec<(String, Uuid)>,
}

impl<'a> ResourceBuilder<'a> {
    /// Begin constructing a resource hierarchy and associated users and role
    /// assignments
    ///
    /// The users and role assignments will be created in silo `main_silo_id`
    /// using OpContext `opctx` and datastore `datastore`.  `coverage` is used
    /// to verify test coverage of authz resource types.
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

    /// Register a new resource for later testing, with no associated users or
    /// role assignments
    pub fn new_resource<T: DynAuthorizedResource>(&mut self, resource: T) {
        self.coverage.covered(&resource);
        self.resources.push(Arc::new(resource));
    }

    /// Register a new resource for later testing and also: for each supported
    /// role on this resource, create a user that has that role on this resource
    pub async fn new_resource_with_users<T>(&mut self, resource: T)
    where
        T: DynAuthorizedResource
            + ApiResourceWithRolesType
            + AuthorizedResource
            + Clone,
        T::AllowedRoles: IntoEnumIterator,
    {
        self.new_resource(resource.clone());

        let resource_name = match resource.lookup_type() {
            LookupType::ByName(name) => name.clone(),
            LookupType::ById(_) => {
                // For resources identified only by id, we only have one of them
                // in our test suite and it's more convenient to omit the id
                // (e.g., "fleet").
                resource.resource_type().to_string().to_lowercase()
            }
            LookupType::ByCompositeId(_) | LookupType::ByOther(_) => {
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

            let authz_silo = authz::Silo::new(
                authz::FLEET,
                silo_id,
                LookupType::ById(silo_id),
            );
            let silo_user =
                db::model::SiloUser::new(silo_id, user_id, username);
            datastore
                .silo_user_create(&authz_silo, silo_user)
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

    /// Returns an immutable view of the resources and users created
    pub fn build(self) -> ResourceSet {
        ResourceSet { resources: self.resources, users: self.users }
    }
}

/// Describes the hierarchy of resources that were registered and the users that
/// were created with specific roles on those resources
pub struct ResourceSet {
    resources: Vec<Arc<dyn DynAuthorizedResource>>,
    users: Vec<(String, Uuid)>,
}

impl ResourceSet {
    /// Iterate the resources to be tested
    pub fn resources(
        &self,
    ) -> impl std::iter::Iterator<Item = Arc<dyn DynAuthorizedResource>> + '_
    {
        self.resources.iter().cloned()
    }

    /// Iterate the users that were created as `(username, user_id)` pairs
    pub fn users(
        &self,
    ) -> impl std::iter::Iterator<Item = &(String, Uuid)> + '_ {
        self.users.iter()
    }
}

/// Dynamically-dispatched version of `AuthorizedResource`
///
/// This is needed because calling [`OpContext::authorize()`] requires knowing
/// at compile time exactly which resource you're authorizing.  But we want to
/// put many different resource types into a collection and do authz checks on
/// all of them.  (We could also change `authorize()` to be dynamically-
/// dispatched.  This would be a much more sprawling change.  And it's not clear
/// that our use case has much application outside of a test like this.)
pub trait DynAuthorizedResource: AuthorizedResource + std::fmt::Debug {
    fn do_authorize<'a, 'b>(
        &'a self,
        opctx: &'b OpContext,
        action: authz::Action,
    ) -> BoxFuture<'a, Result<(), Error>>
    where
        'b: 'a;

    fn resource_name(&self) -> String;
}

impl<T> DynAuthorizedResource for T
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
            LookupType::ByCompositeId(id) => format!("id {:?}", id),
            LookupType::ByOther(_) => {
                unimplemented!()
            }
        };

        format!("{:?} {}", self.resource_type(), my_ident)
    }
}

macro_rules! impl_dyn_authorized_resource_for_global {
    ($t:ty) => {
        impl DynAuthorizedResource for $t {
            fn resource_name(&self) -> String {
                String::from(stringify!($t))
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
    };
}

impl_dyn_authorized_resource_for_global!(authz::oso_generic::Database);
impl_dyn_authorized_resource_for_global!(authz::BlueprintConfig);
impl_dyn_authorized_resource_for_global!(authz::ConsoleSessionList);
impl_dyn_authorized_resource_for_global!(authz::DeviceAuthRequestList);
impl_dyn_authorized_resource_for_global!(authz::DnsConfig);
impl_dyn_authorized_resource_for_global!(authz::IpPoolList);
impl_dyn_authorized_resource_for_global!(authz::Inventory);

impl DynAuthorizedResource for authz::SiloCertificateList {
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
        format!("{}: certificate list", self.silo().resource_name())
    }
}

impl DynAuthorizedResource for authz::SiloIdentityProviderList {
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
        format!("{}: identity provider list", self.silo().resource_name())
    }
}

impl DynAuthorizedResource for authz::SiloUserList {
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
        format!("{}: user list", self.silo().resource_name())
    }
}

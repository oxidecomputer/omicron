// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Look up API resources from the database

use super::datastore::DataStore;
use super::identity::Asset;
use super::identity::Resource;
use crate::{
    authz,
    context::OpContext,
    db,
    db::error::{public_error_from_diesel_pool, ErrorHandler},
};
use async_bb8_diesel::AsyncRunQueryDsl;
use db_macros::lookup_resource;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use nexus_db_model::Name;
use nexus_db_model::UpdateArtifactKind;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::{LookupResult, LookupType, ResourceType};
use uuid::Uuid;

/// Look up an API resource in the database
///
/// `LookupPath` provides a builder-like interface for identifying a resource by
/// id or a path of names.  Once you've selected a resource, you can use one of
/// a few different functions to get information about it from the database:
///
/// * `fetch()`: fetches the database record and `authz` objects for all parents
///   in the path to this object.  This function checks that the caller has
///   permission to `authz::Action::Read` the resoure.
/// * `fetch_for(authz::Action)`: like `fetch()`, but allows you to specify some
///   other action that will be checked rather than `authz::Action::Read`.
/// * `lookup_for(authz::Action)`: fetch just the `authz` objects for a resource
///   and its parents.  This function checks that the caller has permissions to
///   perform the specified action.
///
/// # Examples
///
/// ```
/// # use omicron_nexus::authz;
/// # use omicron_nexus::context::OpContext;
/// # use omicron_nexus::db;
/// # use omicron_nexus::db::DataStore;
/// # use omicron_nexus::db::lookup::LookupPath;
/// # use uuid::Uuid;
/// # async fn foo(opctx: &OpContext, datastore: &DataStore)
/// # -> Result<(), omicron_common::api::external::Error> {
///
/// // Fetch an organization by name
/// let organization_name = db::model::Name("engineering".parse().unwrap());
/// let (authz_silo, authz_org, db_org):
///     (authz::Silo, authz::Organization, db::model::Organization) =
///     LookupPath::new(opctx, datastore)
///         .organization_name(&organization_name)
///         .fetch()
///         .await?;
///
/// // Fetch an organization by id
/// let id: Uuid = todo!();
/// let (authz_silo, authz_org, db_org):
///     (authz::Silo, authz::Organization, db::model::Organization) =
///     LookupPath::new(opctx, datastore)
///         .organization_id(id)
///         .fetch()
///         .await?;
///
/// // Lookup a Project with the intent of creating an Instance inside it.  For
/// // this purpose, we don't need the database row for the Project, so we use
/// // `lookup_for()`.
/// let project_name = db::model::Name("omicron".parse().unwrap());
/// let (authz_silo, authz_org, authz_project) =
///     LookupPath::new(opctx, datastore)
///         .organization_name(&organization_name)
///         .project_name(&project_name)
///         .lookup_for(authz::Action::CreateChild)
///         .await?;
///
/// // Fetch an Instance by a path of names (Organization name, Project name,
/// // Instance name)
/// let instance_name = db::model::Name("test-server".parse().unwrap());
/// let (authz_silo, authz_org, authz_project, authz_instance, db_instance) =
///     LookupPath::new(opctx, datastore)
///         .organization_name(&organization_name)
///         .project_name(&project_name)
///         .instance_name(&instance_name)
///         .fetch()
///         .await?;
///
/// // Having looked up the Instance, you have the `authz::Project`.  Use this
/// // to look up a Disk that you expect is in the same Project.
/// let disk_name = db::model::Name("my-disk".parse().unwrap());
/// let (.., authz_disk, db_disk) =
///     LookupPath::new(opctx, datastore)
///         .project_id(authz_project.id())
///         .disk_name(&disk_name)
///         .fetch()
///         .await?;
/// # }
/// ```
// Implementation notes
//
// We say that a caller using `LookupPath` is building a _selection path_ for a
// resource.  They use this builder interface to _select_ a specific resource.
// Example selection paths:
//
// - From the root, select Organization with name "org1", then Project with name
//   "proj1", then Instance with name "instance1".
//
// - From the root, select Project with id 123, then Instance "instance1".
//
// A selection path always starts at the root, then _may_ contain a lookup-by-id
// node, and then _may_ contain any number of lookup-by-name nodes.  It must
// include at least one lookup-by-id or lookup-by-name node.
//
// Once constructed, it looks like this:
//
//        Instance::Name(p, "instance1")
//                       |
//            +----------+
//            |
//            v
//          Project::Name(o, "proj")
//                        |
//                  +-----+
//                  |
//                  v
//              Organization::Name(r, "org1")
//                                 |
//                      +----------+
//                      |
//                      v
//                  Silo::PrimaryKey(r, id)
//                                   |
//                      +------------+
//                      |
//                      v
//                  Root
//                      lookup_root: LookupPath (references OpContext and
//                                               DataStore)
//
// This is essentially a singly-linked list, except that each node _owns_
// (rather than references) the previous node.  This is important: the caller's
// going to do something like this:
//
//     let (authz_silo, authz_org, authz_project, authz_instance, db_instance) =
//         LookupPath::new(opctx, datastore)   // returns LookupPath
//             .organization_name("org1")      // consumes LookupPath,
//                                             //   returns Organization
//             .project_name("proj1")          // consumes Organization,
//                                                  returns Project
//             .instance_name("instance1")     // consumes Project,
//                                                  returns Instance
//             .fetch().await?;
//
// As you can see, at each step, a selection function (like "organization_name")
// consumes the current tail of the list and returns a new tail.  We don't want
// the caller to have to keep track of multiple objects, so that implies that
// the tail must own all the state that we're building up as we go.
pub struct LookupPath<'a> {
    opctx: &'a OpContext,
    datastore: &'a DataStore,
}

impl<'a> LookupPath<'a> {
    /// Begin selecting a resource for lookup
    ///
    /// Authorization checks will be applied to the caller in `opctx`.
    pub fn new<'b, 'c>(
        opctx: &'b OpContext,
        datastore: &'c DataStore,
    ) -> LookupPath<'a>
    where
        'b: 'a,
        'c: 'a,
    {
        LookupPath { opctx, datastore }
    }

    // The top-level selection functions are implemented by hand because the
    // macro is not in a great position to do this.

    /// Select a resource of type Organization, identified by its name
    pub fn organization_name<'b, 'c>(self, name: &'b Name) -> Organization<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        match self
            .opctx
            .authn
            .silo_required()
            .internal_context("looking up Organization by name")
        {
            Ok(authz_silo) => {
                let root = Root { lookup_root: self };
                let silo_key = Silo::PrimaryKey(root, authz_silo.id());
                Organization::Name(silo_key, name)
            }
            Err(error) => {
                let root = Root { lookup_root: self };
                Organization::Error(root, error)
            }
        }
    }

    /// Select a resource of type Organization, identified by its id
    pub fn organization_id(self, id: Uuid) -> Organization<'a> {
        Organization::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Project, identified by its id
    pub fn project_id(self, id: Uuid) -> Project<'a> {
        Project::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Instance, identified by its id
    pub fn instance_id(self, id: Uuid) -> Instance<'a> {
        Instance::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type IpPool, identified by its name
    pub fn ip_pool_name<'b, 'c>(self, name: &'b Name) -> IpPool<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        IpPool::Name(Root { lookup_root: self }, name)
    }

    /// Select a resource of type IpPool, identified by its id
    pub fn ip_pool_id(self, id: Uuid) -> IpPool<'a> {
        IpPool::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Disk, identified by its id
    pub fn disk_id(self, id: Uuid) -> Disk<'a> {
        Disk::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Image, identified by its id
    pub fn image_id(self, id: Uuid) -> Image<'a> {
        Image::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Snapshot, identified by its id
    pub fn snapshot_id(self, id: Uuid) -> Snapshot<'a> {
        Snapshot::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type NetworkInterface, identified by its id
    pub fn network_interface_id(self, id: Uuid) -> NetworkInterface<'a> {
        NetworkInterface::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Vpc, identified by its id
    pub fn vpc_id(self, id: Uuid) -> Vpc<'a> {
        Vpc::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type VpcSubnet, identified by its id
    pub fn vpc_subnet_id(self, id: Uuid) -> VpcSubnet<'a> {
        VpcSubnet::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type VpcRouter, identified by its id
    pub fn vpc_router_id(self, id: Uuid) -> VpcRouter<'a> {
        VpcRouter::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type RouterRoute, identified by its id
    pub fn router_route_id(self, id: Uuid) -> RouterRoute<'a> {
        RouterRoute::PrimaryKey(Root { lookup_root: self }, id)
    }

    // Fleet-level resources

    /// Select a resource of type ConsoleSession, identified by its `token`
    pub fn console_session_token<'b, 'c>(
        self,
        token: &'b str,
    ) -> ConsoleSession<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        ConsoleSession::PrimaryKey(
            Root { lookup_root: self },
            token.to_string(),
        )
    }

    /// Select a resource of type DeviceAuthRequest, identified by its `user_code`
    pub fn device_auth_request<'b, 'c>(
        self,
        user_code: &'b str,
    ) -> DeviceAuthRequest<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        DeviceAuthRequest::PrimaryKey(
            Root { lookup_root: self },
            user_code.to_string(),
        )
    }

    /// Select a resource of type DeviceAccessToken, identified by its `token`
    pub fn device_access_token<'b, 'c>(
        self,
        token: &'b str,
    ) -> DeviceAccessToken<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        DeviceAccessToken::PrimaryKey(
            Root { lookup_root: self },
            token.to_string(),
        )
    }

    /// Select a resource of type RoleBuiltin, identified by its `name`
    pub fn role_builtin_name(self, name: &str) -> RoleBuiltin<'a> {
        let parts = name.split_once('.');
        if let Some((resource_type, role_name)) = parts {
            RoleBuiltin::PrimaryKey(
                Root { lookup_root: self },
                resource_type.to_string(),
                role_name.to_string(),
            )
        } else {
            let root = Root { lookup_root: self };
            RoleBuiltin::Error(
                root,
                Error::ObjectNotFound {
                    type_name: ResourceType::RoleBuiltin,
                    lookup_type: LookupType::ByName(String::from(name)),
                },
            )
        }
    }

    /// Select a resource of type Silo, identified by its id
    pub fn silo_id(self, id: Uuid) -> Silo<'a> {
        Silo::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Silo, identified by its name
    pub fn silo_name<'b, 'c>(self, name: &'b Name) -> Silo<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Silo::Name(Root { lookup_root: self }, name)
    }

    /// Select a resource of type SiloUser, identified by its id
    pub fn silo_user_id(self, id: Uuid) -> SiloUser<'a> {
        SiloUser::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type SiloGroup, identified by its id
    pub fn silo_group_id(self, id: Uuid) -> SiloGroup<'a> {
        SiloGroup::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Rack, identified by its id
    pub fn rack_id(self, id: Uuid) -> Rack<'a> {
        Rack::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Sled, identified by its id
    pub fn sled_id(self, id: Uuid) -> Sled<'a> {
        Sled::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type UpdateAvailableArtifact, identified by its
    /// `(name, version, kind)` tuple
    pub fn update_available_artifact_tuple(
        self,
        name: &str,
        version: i64,
        kind: UpdateArtifactKind,
    ) -> UpdateAvailableArtifact<'a> {
        UpdateAvailableArtifact::PrimaryKey(
            Root { lookup_root: self },
            name.to_string(),
            version,
            kind,
        )
    }

    /// Select a resource of type UserBuiltin, identified by its `name`
    pub fn user_builtin_name<'b, 'c>(self, name: &'b Name) -> UserBuiltin<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        UserBuiltin::Name(Root { lookup_root: self }, name)
    }

    /// Select a resource of type GlobalImage, identified by its name
    pub fn global_image_name<'b, 'c>(self, name: &'b Name) -> GlobalImage<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        GlobalImage::Name(Root { lookup_root: self }, name)
    }

    /// Select a resource of type GlobalImage, identified by its id
    pub fn global_image_id<'b>(self, id: Uuid) -> GlobalImage<'b>
    where
        'a: 'b,
    {
        GlobalImage::PrimaryKey(Root { lookup_root: self }, id)
    }
}

/// Represents the head of the selection path for a resource
pub struct Root<'a> {
    lookup_root: LookupPath<'a>,
}

impl<'a> Root<'a> {
    fn lookup_root(&self) -> &LookupPath<'a> {
        &self.lookup_root
    }
}

// Define the specific builder types for each resource.  The `lookup_resource`
// macro defines a struct for the resource, helper functions for selecting child
// resources, and the publicly-exposed fetch functions (fetch(), fetch_for(),
// and lookup_for()).

// Main resource hierarchy: Organizations, Projects, and their resources

lookup_resource! {
    name = "Silo",
    ancestors = [],
    children = [ "Organization", "IdentityProvider", "SamlIdentityProvider"],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "SiloUser",
    ancestors = [ "Silo" ],
    children = [ "SshKey" ],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "SiloGroup",
    ancestors = [ "Silo" ],
    children = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "IdentityProvider",
    ancestors = [ "Silo" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [
        { column_name = "silo_id", rust_type = Uuid },
        { column_name = "id", rust_type = Uuid }
    ]
}

lookup_resource! {
    name = "IpPool",
    ancestors = [],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid }]
}

lookup_resource! {
    name = "SamlIdentityProvider",
    ancestors = [ "Silo" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [
        { column_name = "id", rust_type = Uuid },
    ],
    visible_outside_silo = true
}

lookup_resource! {
    name = "SshKey",
    ancestors = [ "Silo", "SiloUser" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Organization",
    ancestors = [ "Silo" ],
    children = [ "Project" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Project",
    ancestors = [ "Silo", "Organization" ],
    children = [ "Disk", "Instance", "Vpc", "Snapshot" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Disk",
    ancestors = [ "Silo", "Organization", "Project" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Image",
    ancestors = [ "Silo", "Organization", "Project" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Snapshot",
    ancestors = [ "Silo", "Organization", "Project" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Instance",
    ancestors = [ "Silo", "Organization", "Project" ],
    children = [ "NetworkInterface" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "NetworkInterface",
    ancestors = [ "Silo", "Organization", "Project", "Instance" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Vpc",
    ancestors = [ "Silo", "Organization", "Project" ],
    children = [ "VpcRouter", "VpcSubnet" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "VpcRouter",
    ancestors = [ "Silo", "Organization", "Project", "Vpc" ],
    children = [ "RouterRoute" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "RouterRoute",
    ancestors = [ "Silo", "Organization", "Project", "Vpc", "VpcRouter" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "VpcSubnet",
    ancestors = [ "Silo", "Organization", "Project", "Vpc" ],
    children = [ ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

// Miscellaneous resources nested directly below "Fleet"

lookup_resource! {
    name = "ConsoleSession",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "token", rust_type = String },
    ]
}

lookup_resource! {
    name = "DeviceAuthRequest",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "user_code", rust_type = String },
    ]
}

lookup_resource! {
    name = "DeviceAccessToken",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "token", rust_type = String },
    ]
}

lookup_resource! {
    name = "RoleBuiltin",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "resource_type", rust_type = String },
        { column_name = "role_name", rust_type = String },
    ]
}

lookup_resource! {
    name = "Rack",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Sled",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "UpdateAvailableArtifact",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "name", rust_type = String },
        { column_name = "version", rust_type = i64 },
        { column_name = "kind", rust_type = UpdateArtifactKind }
    ]
}

lookup_resource! {
    name = "UserBuiltin",
    ancestors = [],
    children = [],
    lookup_by_name = true,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "GlobalImage",
    ancestors = [],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

#[cfg(test)]
mod test {
    use super::Instance;
    use super::LookupPath;
    use super::Organization;
    use super::Project;
    use crate::context::OpContext;
    use crate::db::model::Name;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use std::sync::Arc;

    /* This is a smoke test that things basically appear to work. */
    #[tokio::test]
    async fn test_lookup() {
        let logctx = dev::test_setup_log("test_lookup");
        let mut db = test_setup_database(&logctx.log).await;
        let (_, datastore) =
            crate::db::datastore::datastore_test(&logctx, &db).await;
        let opctx =
            OpContext::for_tests(logctx.log.new(o!()), Arc::clone(&datastore));
        let org_name: Name = Name("my-org".parse().unwrap());
        let project_name: Name = Name("my-project".parse().unwrap());
        let instance_name: Name = Name("my-instance".parse().unwrap());

        let leaf = LookupPath::new(&opctx, &datastore)
            .organization_name(&org_name)
            .project_name(&project_name)
            .instance_name(&instance_name);
        assert!(matches!(&leaf,
            Instance::Name(Project::Name(Organization::Name(_, o) , p) , i)
            if **o == org_name && **p == project_name && **i == instance_name));

        let org_id = "006f29d9-0ff0-e2d2-a022-87e152440122".parse().unwrap();
        let leaf = LookupPath::new(&opctx, &datastore)
            .organization_id(org_id)
            .project_name(&project_name);
        assert!(matches!(&leaf,
            Project::Name(Organization::PrimaryKey(_, o), p)
            if *o == org_id && **p == project_name));

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}

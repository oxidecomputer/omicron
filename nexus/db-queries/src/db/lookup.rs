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
    db::error::{public_error_from_diesel, ErrorHandler},
};
use async_bb8_diesel::AsyncRunQueryDsl;
use db_macros::lookup_resource;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use ipnetwork::IpNetwork;
use nexus_db_model::Name;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::{LookupResult, LookupType, ResourceType};
use omicron_uuid_kinds::TufRepoKind;
use omicron_uuid_kinds::TypedUuid;
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
// Implementation notes
//
// We say that a caller using `LookupPath` is building a _selection path_ for a
// resource.  They use this builder interface to _select_ a specific resource.
// Example selection paths:
//
// - From the root, select Project with name "proj1", then Instance with name
//   "instance1".
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
//               Silo::PrimaryKey(r, id)
//                                |
//                   +------------+
//                   |
//                   v
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
//             .project_name("proj1")          // consumes LookupPath,
//                                                  returns Project
//             .instance_name("instance1")     // consumes Project,
//                                                  returns Instance
//             .fetch().await?;
//
// As you can see, at each step, a selection function (like "project_name")
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

    /// Select a resource of type Project, identified by its name
    pub fn project_name<'b, 'c>(self, name: &'b Name) -> Project<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        match self
            .opctx
            .authn
            .silo_required()
            .internal_context("looking up Project by name")
        {
            Ok(authz_silo) => {
                let root = Root { lookup_root: self };
                let silo_key = Silo::PrimaryKey(root, authz_silo.id());
                Project::Name(silo_key, name)
            }
            Err(error) => {
                let root = Root { lookup_root: self };
                Project::Error(root, error)
            }
        }
    }

    /// Select a resource of type Project, identified by its owned name
    pub fn project_name_owned<'b, 'c>(self, name: Name) -> Project<'c>
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
                Project::OwnedName(silo_key, name)
            }
            Err(error) => {
                let root = Root { lookup_root: self };
                Project::Error(root, error)
            }
        }
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

    pub fn image_id(self, id: Uuid) -> Image<'a> {
        Image::PrimaryKey(Root { lookup_root: self }, id)
    }

    pub fn project_image_id(self, id: Uuid) -> ProjectImage<'a> {
        ProjectImage::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Snapshot, identified by its id
    pub fn snapshot_id(self, id: Uuid) -> Snapshot<'a> {
        Snapshot::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type InstanceNetworkInterface, identified by its id
    pub fn instance_network_interface_id(
        self,
        id: Uuid,
    ) -> InstanceNetworkInterface<'a> {
        InstanceNetworkInterface::PrimaryKey(Root { lookup_root: self }, id)
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

    /// Select a resource of type InternetGateway, identified by its id
    pub fn internet_gateway_id(self, id: Uuid) -> InternetGateway<'a> {
        InternetGateway::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type RouterRoute, identified by its id
    pub fn router_route_id(self, id: Uuid) -> RouterRoute<'a> {
        RouterRoute::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type InternetGatewayIpPool, identified by its id
    pub fn internet_gateway_ip_pool_id(
        self,
        id: Uuid,
    ) -> InternetGatewayIpPool<'a> {
        InternetGatewayIpPool::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type InternetGatewayIpAddress, identified by its id
    pub fn internet_gateway_ip_address_id(
        self,
        id: Uuid,
    ) -> InternetGatewayIpAddress<'a> {
        InternetGatewayIpAddress::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type FloatingIp, identified by its id
    pub fn floating_ip_id(self, id: Uuid) -> FloatingIp<'a> {
        FloatingIp::PrimaryKey(Root { lookup_root: self }, id)
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

    /// Select a resource of type Silo, identified by its owned name
    pub fn silo_name_owned<'b, 'c>(self, name: Name) -> Silo<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Silo::OwnedName(Root { lookup_root: self }, name)
    }

    /// Select a resource of type SiloUser, identified by its id
    pub fn silo_user_id(self, id: Uuid) -> SiloUser<'a> {
        SiloUser::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type SiloGroup, identified by its id
    pub fn silo_group_id(self, id: Uuid) -> SiloGroup<'a> {
        SiloGroup::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type SshKey, identified by its id
    pub fn ssh_key_id(self, id: Uuid) -> SshKey<'a> {
        SshKey::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Rack, identified by its id
    pub fn rack_id(self, id: Uuid) -> Rack<'a> {
        Rack::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Sled, identified by its id
    pub fn sled_id(self, id: Uuid) -> Sled<'a> {
        Sled::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Zpool, identified by its id
    pub fn zpool_id(self, id: Uuid) -> Zpool<'a> {
        Zpool::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Switch, identified by its id
    pub fn switch_id(self, id: Uuid) -> Switch<'a> {
        Switch::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type PhysicalDisk, identified by its id
    pub fn physical_disk(self, id: Uuid) -> PhysicalDisk<'a> {
        PhysicalDisk::PrimaryKey(Root { lookup_root: self }, id)
    }

    pub fn silo_image_id(self, id: Uuid) -> SiloImage<'a> {
        SiloImage::PrimaryKey(Root { lookup_root: self }, id)
    }

    pub fn silo_image_name<'b, 'c>(self, name: &'b Name) -> SiloImage<'c>
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
                SiloImage::Name(silo_key, name)
            }
            Err(error) => {
                let root = Root { lookup_root: self };
                SiloImage::Error(root, error)
            }
        }
    }

    pub fn address_lot_id(self, id: Uuid) -> AddressLot<'a> {
        AddressLot::PrimaryKey(Root { lookup_root: self }, id)
    }

    pub fn address_lot_name_owned(self, name: Name) -> AddressLot<'a> {
        AddressLot::OwnedName(Root { lookup_root: self }, name)
    }

    pub fn loopback_address(
        self,
        rack_id: Uuid,
        switch_location: Name,
        address: IpNetwork,
    ) -> LoopbackAddress<'a> {
        LoopbackAddress::PrimaryKey(
            Root { lookup_root: self },
            address,
            rack_id,
            switch_location.to_string(),
        )
    }

    /// Select a resource of type TufRepo, identified by its UUID.
    pub fn tuf_repo_id(self, id: TypedUuid<TufRepoKind>) -> TufRepo<'a> {
        TufRepo::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type UpdateArtifact, identified by its
    /// `(name, version, kind)` tuple
    pub fn tuf_artifact_tuple(
        self,
        name: impl Into<String>,
        version: db::model::SemverVersion,
        kind: impl Into<String>,
    ) -> TufArtifact<'a> {
        TufArtifact::PrimaryKey(
            Root { lookup_root: self },
            name.into(),
            version,
            kind.into(),
        )
    }

    /// Select a resource of type UserBuiltin, identified by its `name`
    pub fn user_builtin_id<'b>(self, id: Uuid) -> UserBuiltin<'b>
    where
        'a: 'b,
    {
        UserBuiltin::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type UserBuiltin, identified by its `name`
    pub fn user_builtin_name<'b, 'c>(self, name: &'b Name) -> UserBuiltin<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        UserBuiltin::Name(Root { lookup_root: self }, name)
    }

    /// Select a resource of type Certificate, identified by its id
    pub fn certificate_id<'b>(self, id: Uuid) -> Certificate<'b>
    where
        'a: 'b,
    {
        Certificate::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Certificate, identified by its name
    pub fn certificate_name<'b, 'c>(self, name: &'b Name) -> Certificate<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        match self
            .opctx
            .authn
            .silo_required()
            .internal_context("looking up Certificate by name")
        {
            Ok(authz_silo) => {
                let root = Root { lookup_root: self };
                let silo_key = Silo::PrimaryKey(root, authz_silo.id());
                Certificate::Name(silo_key, name)
            }
            Err(error) => {
                let root = Root { lookup_root: self };
                Certificate::Error(root, error)
            }
        }
    }

    /// Select a resource of type Certificate, identified by its owned name
    pub fn certificate_name_owned<'b, 'c>(self, name: Name) -> Certificate<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        match self
            .opctx
            .authn
            .silo_required()
            .internal_context("looking up Certificate by name")
        {
            Ok(authz_silo) => {
                let root = Root { lookup_root: self };
                let silo_key = Silo::PrimaryKey(root, authz_silo.id());
                Certificate::OwnedName(silo_key, name)
            }
            Err(error) => {
                let root = Root { lookup_root: self };
                Certificate::Error(root, error)
            }
        }
    }

    /// Select a resource of type SamlIdentityProvider, identified by its id
    pub fn saml_identity_provider_id<'b>(
        self,
        id: Uuid,
    ) -> SamlIdentityProvider<'b>
    where
        'a: 'b,
    {
        SamlIdentityProvider::PrimaryKey(Root { lookup_root: self }, id)
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
    children = [ "IdentityProvider", "SamlIdentityProvider", "Project", "SiloImage", "Certificate" ],
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
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ],
    visible_outside_silo = true
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
    name = "SiloImage",
    ancestors = [ "Silo" ],
    children = [],
    lookup_by_name = true,
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
    name = "Project",
    ancestors = [ "Silo" ],
    children = [ "Disk", "Instance", "Vpc", "Snapshot", "ProjectImage", "FloatingIp" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Disk",
    ancestors = [ "Silo", "Project" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Image",
    ancestors = ["Silo"],
    children = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "ProjectImage",
    ancestors = [ "Silo", "Project" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Snapshot",
    ancestors = [ "Silo", "Project" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Instance",
    ancestors = [ "Silo", "Project" ],
    children = [ "InstanceNetworkInterface" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "InstanceNetworkInterface",
    ancestors = [ "Silo", "Project", "Instance" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Vpc",
    ancestors = [ "Silo", "Project" ],
    children = [ "VpcRouter", "VpcSubnet", "InternetGateway" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "VpcRouter",
    ancestors = [ "Silo", "Project", "Vpc" ],
    children = [ "RouterRoute" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "RouterRoute",
    ancestors = [ "Silo", "Project", "Vpc", "VpcRouter" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "VpcSubnet",
    ancestors = [ "Silo", "Project", "Vpc" ],
    children = [ ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "InternetGateway",
    ancestors = [ "Silo", "Project", "Vpc" ],
    children = [ "InternetGatewayIpPool", "InternetGatewayIpAddress" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "InternetGatewayIpPool",
    ancestors = [ "Silo", "Project", "Vpc", "InternetGateway" ],
    children = [ ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "InternetGatewayIpAddress",
    ancestors = [ "Silo", "Project", "Vpc", "InternetGateway" ],
    children = [ ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "FloatingIp",
    ancestors = [ "Silo", "Project" ],
    children = [],
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
    name = "Zpool",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "SledInstance",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ],
}

lookup_resource! {
    name = "Switch",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "PhysicalDisk",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "TufRepo",
    ancestors = [],
    // TODO: should this have TufArtifact as a child? This is a many-many
    // relationship.
    children = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", uuid_kind = TufRepoKind } ]
}

lookup_resource! {
    name = "TufArtifact",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "name", rust_type = String },
        { column_name = "version", rust_type = db::model::SemverVersion },
        { column_name = "kind", rust_type = String },
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
    name = "Certificate",
    ancestors = [ "Silo" ],
    children = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "AddressLot",
    ancestors = [],
    children = [], // TODO: Should this include AddressLotBlock?
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "LoopbackAddress",
    ancestors = [],
    children = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "address", rust_type = IpNetwork },
        { column_name = "rack_id", rust_type = Uuid },
        { column_name = "switch_location", rust_type = String }
    ]
}

// Helpers for unifying the interfaces around images

pub enum ImageLookup<'a> {
    ProjectImage(ProjectImage<'a>),
    SiloImage(SiloImage<'a>),
}

pub enum ImageParentLookup<'a> {
    Project(Project<'a>),
    Silo(Silo<'a>),
}

#[cfg(test)]
mod test {
    use super::Instance;
    use super::LookupPath;
    use super::Project;
    use crate::db::datastore::pub_test_utils::TestDatabase;
    use crate::db::model::Name;
    use omicron_test_utils::dev;

    /* This is a smoke test that things basically appear to work. */
    #[tokio::test]
    async fn test_lookup() {
        let logctx = dev::test_setup_log("test_lookup");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let project_name: Name = Name("my-project".parse().unwrap());
        let instance_name: Name = Name("my-instance".parse().unwrap());

        let leaf = LookupPath::new(&opctx, &datastore)
            .project_name(&project_name)
            .instance_name(&instance_name);
        assert!(matches!(&leaf,
            Instance::Name(Project::Name(_, p), i)
            if **p == project_name && **i == instance_name));

        let leaf =
            LookupPath::new(&opctx, &datastore).project_name(&project_name);
        assert!(matches!(&leaf,
            Project::Name(_, p)
            if **p == project_name));

        let project_id =
            "006f29d9-0ff0-e2d2-a022-87e152440122".parse().unwrap();
        let leaf = LookupPath::new(&opctx, &datastore).project_id(project_id);
        assert!(matches!(&leaf,
            Project::PrimaryKey(_, p)
            if *p == project_id));

        db.terminate().await;
        logctx.cleanup_successful();
    }
}

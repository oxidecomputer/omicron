// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authz types for resources in the API hierarchy
//!
//! The general pattern in Nexus for working with an object is to look it up
//! (see [`crate::db::lookup::LookupPath`]) and get back a so-called `authz`
//! type.  This type uniquely identifies the resource regardless of any other
//! changes (e.g., name change or moving it to a different parent collection).
//! The various datastore functions that modify API resources accept these
//! `authz` types.
//!
//! The `authz` types can be passed to
//! [`crate::context::OpContext::authorize()`] to do an authorization check --
//! is the caller allowed to perform some action on the resource?  This is the
//! primary way of doing authz checks in Nexus.
//!
//! `authz` types also retain information about how the resource was looked-up
//! in the first place so that if it turns out the caller is not even allowed to
//! know if the resource exists, we can produce an appropriate 404 error.  For
//! example, if they look up organization "foo", and we get back one with id
//! 123, but they're not allowed to see it, then the user should get back a 404
//! that organization "foo" doesn't exist (and definitely not that organization
//! 123 doesn't exist, since that would tell the user that it _does_ exist!).
//!
//! Most `authz` types are generated by the `authz_resource!` macro.

use super::actor::AnyActor;
use super::context::AuthorizedResource;
use super::oso_generic::Init;
use super::roles::{
    load_roles_for_resource, load_roles_for_resource_tree, RoleSet,
};
use super::Action;
use super::{actor::AuthenticatedActor, Authz};
use crate::authn;
use crate::context::OpContext;
use crate::db;
use crate::db::fixed_data::FLEET_ID;
use crate::db::model::KnownArtifactKind;
use crate::db::model::SemverVersion;
use crate::db::DataStore;
use authz_macros::authz_resource;
use futures::future::BoxFuture;
use futures::FutureExt;
use lazy_static::lazy_static;
use nexus_types::external_api::shared::{FleetRole, ProjectRole, SiloRole};
use omicron_common::api::external::{Error, LookupType, ResourceType};
use oso::PolarClass;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Describes an authz resource that corresponds to an API resource that has a
/// corresponding ResourceType and is stored in the database
pub trait ApiResource:
    std::fmt::Debug + oso::ToPolar + Send + Sync + 'static
{
    /// If roles can be assigned to this resource, return this object as a
    /// [`ApiResourceWithRoles`]
    ///
    /// If roles cannot be assigned to this resource, returns `None`.
    fn as_resource_with_roles(&self) -> Option<&dyn ApiResourceWithRoles>;

    /// If this resource has a parent in the API hierarchy whose assigned roles
    /// can affect access to this resource, return the parent resource.
    /// Otherwise, returns `None`.
    fn parent(&self) -> Option<&dyn AuthorizedResource>;

    fn resource_type(&self) -> ResourceType;
    fn lookup_type(&self) -> &LookupType;

    /// Returns an error as though this resource were not found, suitable for
    /// use when an actor should not be able to see that this resource exists
    fn not_found(&self) -> Error {
        self.lookup_type().clone().into_not_found(self.resource_type())
    }
}

/// Describes an authz resource on which we allow users to assign roles
pub trait ApiResourceWithRoles: ApiResource {
    fn resource_id(&self) -> Uuid;

    /// Returns an optional other resource whose roles should be fetched along
    /// with this resource
    ///
    /// This exists to support the behavior that Silo-level roles can confer
    /// Fleet-level roles.  That is, it's possible to set configuration on the
    /// Silo that means "if a person has the 'admin' role on this Silo, then
    /// they also get the 'admin' role on the Fleet."  In order to implement
    /// this, if such a policy exists on the user's Silo, then we have to load a
    /// user's roles on that Silo whenever we would load the roles for the
    /// Fleet.
    ///
    /// Note this differs from "parent" in that it's not recursive.  With
    /// "parent", all of the roles that might affect the parent will be fetched,
    /// which include all of _its_ parents.  With this function, we only fetch
    /// this one resource's directly-attached roles.
    fn conferred_roles_by(
        &self,
        authn: &authn::Context,
    ) -> Result<Option<(ResourceType, Uuid)>, Error>;
}

/// Describes the specific roles for an `ApiResourceWithRoles`
pub trait ApiResourceWithRolesType: ApiResourceWithRoles {
    type AllowedRoles: serde::Serialize
        + serde::de::DeserializeOwned
        + db::model::DatabaseString
        + Clone;
}

impl<T: ApiResource + oso::PolarClass + Clone> AuthorizedResource for T {
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        load_roles_for_resource_tree(self, opctx, datastore, authn, roleset)
            .boxed()
    }

    fn on_unauthorized(
        &self,
        authz: &Authz,
        error: Error,
        actor: AnyActor,
        action: Action,
    ) -> Error {
        if action == Action::Read {
            return self.not_found();
        }

        // If the user failed an authz check, and they can't even read this
        // resource, then we should produce a 404 rather than a 401/403.
        match authz.is_allowed(&actor, Action::Read, self) {
            Err(error) => Error::internal_error(&format!(
                "failed to compute read authorization to determine visibility: \
                {:#}",
                error
            )),
            Ok(false) => self.not_found(),
            Ok(true) => error,
        }
    }

    fn polar_class(&self) -> oso::Class {
        Self::get_polar_class()
    }
}

/// Represents the Oxide fleet for authz purposes
///
/// Fleet-level resources are essentially global.  See RFD 24 for more on
/// Fleets.
///
/// This object is used for authorization checks on a Fleet by passing it as the
/// `resource` argument to [`crate::context::OpContext::authorize()`].  You
/// don't construct a `Fleet` yourself -- use the global [`FLEET`].
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Fleet;
/// Singleton representing the [`Fleet`] itself for authz purposes
pub const FLEET: Fleet = Fleet;

lazy_static! {
    pub static ref FLEET_LOOKUP: LookupType = LookupType::ById(*FLEET_ID);
}

impl Eq for Fleet {}
impl PartialEq for Fleet {
    fn eq(&self, _: &Self) -> bool {
        // There is only one Fleet.
        true
    }
}

impl oso::PolarClass for Fleet {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder().with_equality_check().add_method(
            "has_role",
            |_: &Fleet, actor: AuthenticatedActor, role: String| {
                actor.has_role_resource(ResourceType::Fleet, *FLEET_ID, &role)
            },
        )
    }
}

impl ApiResource for Fleet {
    fn as_resource_with_roles(&self) -> Option<&dyn ApiResourceWithRoles> {
        Some(self)
    }

    fn parent(&self) -> Option<&dyn AuthorizedResource> {
        None
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::Fleet
    }

    fn lookup_type(&self) -> &LookupType {
        &FLEET_LOOKUP
    }

    fn not_found(&self) -> Error {
        // The Fleet is always visible.
        Error::Forbidden
    }
}

impl ApiResourceWithRoles for Fleet {
    fn resource_id(&self) -> Uuid {
        *FLEET_ID
    }

    fn conferred_roles_by(
        &self,
        authn: &authn::Context,
    ) -> Result<Option<(ResourceType, Uuid)>, Error> {
        // If the actor is associated with a Silo, and if that Silo has a policy
        // that grants fleet-level roles, then we must look up the actor's
        // Silo-level roles when looking up their roles on the Fleet.
        let Some(silo_id) = authn.actor().and_then(|actor| actor.silo_id())
        else {
            return Ok(None);
        };
        let silo_authn_policy = authn.silo_authn_policy().ok_or_else(|| {
            Error::internal_error(&format!(
                "actor had a Silo ({}) but no SiloAuthnPolicy",
                silo_id
            ))
        })?;
        Ok(if silo_authn_policy.mapped_fleet_roles().is_empty() {
            None
        } else {
            Some((ResourceType::Silo, silo_id))
        })
    }
}

impl ApiResourceWithRolesType for Fleet {
    type AllowedRoles = FleetRole;
}

// TODO: refactor synthetic resources below

/// ConsoleSessionList is a synthetic resource used for modeling who has access
/// to create sessions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsoleSessionList;

pub const CONSOLE_SESSION_LIST: ConsoleSessionList = ConsoleSessionList {};

impl oso::PolarClass for ConsoleSessionList {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        // Roles are not directly attached to ConsoleSessionList.
        oso::Class::builder()
            .with_equality_check()
            .add_method(
                "has_role",
                |_: &ConsoleSessionList,
                 _actor: AuthenticatedActor,
                 _role: String| false,
            )
            .add_attribute_getter("fleet", |_| FLEET)
    }
}

impl AuthorizedResource for ConsoleSessionList {
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> futures::future::BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        load_roles_for_resource(
            opctx,
            datastore,
            authn,
            ResourceType::Fleet,
            *FLEET_ID,
            roleset,
        )
        .boxed()
    }

    fn on_unauthorized(
        &self,
        _: &Authz,
        error: Error,
        _: AnyActor,
        _: Action,
    ) -> Error {
        error
    }

    fn polar_class(&self) -> oso::Class {
        Self::get_polar_class()
    }
}

/// DnsConfig is a synthetic resource used for modeling access to the internal
/// and external DNS configuration
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DnsConfig;

pub const DNS_CONFIG: DnsConfig = DnsConfig {};

impl oso::PolarClass for DnsConfig {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        // Roles are not directly attached to DnsConfig
        oso::Class::builder()
            .with_equality_check()
            .add_method(
                "has_role",
                |_: &DnsConfig, _actor: AuthenticatedActor, _role: String| {
                    false
                },
            )
            .add_attribute_getter("fleet", |_| FLEET)
    }
}

impl AuthorizedResource for DnsConfig {
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> futures::future::BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        load_roles_for_resource(
            opctx,
            datastore,
            authn,
            ResourceType::Fleet,
            *FLEET_ID,
            roleset,
        )
        .boxed()
    }

    fn on_unauthorized(
        &self,
        _: &Authz,
        error: Error,
        _: AnyActor,
        _: Action,
    ) -> Error {
        error
    }

    fn polar_class(&self) -> oso::Class {
        Self::get_polar_class()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct IpPoolList;

/// Singleton representing the [`IpPoolList`] itself for authz purposes
pub const IP_POOL_LIST: IpPoolList = IpPoolList;

impl Eq for IpPoolList {}

impl PartialEq for IpPoolList {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl oso::PolarClass for IpPoolList {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_attribute_getter("fleet", |_: &IpPoolList| FLEET)
    }
}

impl AuthorizedResource for IpPoolList {
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> futures::future::BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        // There are no roles on the IpPoolList, only permissions. But we still
        // need to load the Fleet-related roles to verify that the actor has the
        // "admin" role on the Fleet.
        load_roles_for_resource(
            opctx,
            datastore,
            authn,
            ResourceType::Fleet,
            *FLEET_ID,
            roleset,
        )
        .boxed()
    }

    fn on_unauthorized(
        &self,
        _: &Authz,
        error: Error,
        _: AnyActor,
        _: Action,
    ) -> Error {
        error
    }

    fn polar_class(&self) -> oso::Class {
        Self::get_polar_class()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeviceAuthRequestList;
/// Singleton representing the [`DeviceAuthRequestList`] itself for authz purposes
pub const DEVICE_AUTH_REQUEST_LIST: DeviceAuthRequestList =
    DeviceAuthRequestList;

impl oso::PolarClass for DeviceAuthRequestList {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_attribute_getter("fleet", |_| FLEET)
    }
}

impl AuthorizedResource for DeviceAuthRequestList {
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> futures::future::BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        // There are no roles on the DeviceAuthRequestList, only permissions. But we
        // still need to load the Fleet-related roles to verify that the actor has the
        // "admin" role on the Fleet.
        load_roles_for_resource(
            opctx,
            datastore,
            authn,
            ResourceType::Fleet,
            *FLEET_ID,
            roleset,
        )
        .boxed()
    }

    fn on_unauthorized(
        &self,
        _: &Authz,
        error: Error,
        _: AnyActor,
        _: Action,
    ) -> Error {
        error
    }

    fn polar_class(&self) -> oso::Class {
        Self::get_polar_class()
    }
}

/// Synthetic resource describing the list of Certificates associated with a
/// Silo
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SiloCertificateList(Silo);

impl SiloCertificateList {
    pub fn new(silo: Silo) -> SiloCertificateList {
        SiloCertificateList(silo)
    }

    pub fn silo(&self) -> &Silo {
        &self.0
    }
}

impl oso::PolarClass for SiloCertificateList {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_attribute_getter("silo", |list: &SiloCertificateList| {
                list.0.clone()
            })
    }
}

impl AuthorizedResource for SiloCertificateList {
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> futures::future::BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        // There are no roles on this resource, but we still need to load the
        // Silo-related roles.
        self.silo().load_roles(opctx, datastore, authn, roleset)
    }

    fn on_unauthorized(
        &self,
        _: &Authz,
        error: Error,
        _: AnyActor,
        _: Action,
    ) -> Error {
        error
    }

    fn polar_class(&self) -> oso::Class {
        Self::get_polar_class()
    }
}

/// Synthetic resource describing the list of Identity Providers associated with
/// a Silo
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SiloIdentityProviderList(Silo);

impl SiloIdentityProviderList {
    pub fn new(silo: Silo) -> SiloIdentityProviderList {
        SiloIdentityProviderList(silo)
    }

    pub fn silo(&self) -> &Silo {
        &self.0
    }
}

impl oso::PolarClass for SiloIdentityProviderList {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_attribute_getter("silo", |list: &SiloIdentityProviderList| {
                list.0.clone()
            })
    }
}

impl AuthorizedResource for SiloIdentityProviderList {
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> futures::future::BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        // There are no roles on this resource, but we still need to load the
        // Silo-related roles.
        self.silo().load_roles(opctx, datastore, authn, roleset)
    }

    fn on_unauthorized(
        &self,
        _: &Authz,
        error: Error,
        _: AnyActor,
        _: Action,
    ) -> Error {
        error
    }

    fn polar_class(&self) -> oso::Class {
        Self::get_polar_class()
    }
}

/// Synthetic resource describing the list of Users in a Silo
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SiloUserList(Silo);

impl SiloUserList {
    pub fn new(silo: Silo) -> SiloUserList {
        SiloUserList(silo)
    }

    pub fn silo(&self) -> &Silo {
        &self.0
    }
}

impl oso::PolarClass for SiloUserList {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_attribute_getter("silo", |list: &SiloUserList| list.0.clone())
    }
}

impl AuthorizedResource for SiloUserList {
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> futures::future::BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        // There are no roles on this resource, but we still need to load the
        // Silo-related roles.
        self.silo().load_roles(opctx, datastore, authn, roleset)
    }

    fn on_unauthorized(
        &self,
        _: &Authz,
        error: Error,
        _: AnyActor,
        _: Action,
    ) -> Error {
        error
    }

    fn polar_class(&self) -> oso::Class {
        Self::get_polar_class()
    }
}

// Main resource hierarchy: Projects and their resources

authz_resource! {
    name = "Project",
    parent = "Silo",
    primary_key = Uuid,
    roles_allowed = true,
    polar_snippet = Custom,
}

impl ApiResourceWithRolesType for Project {
    type AllowedRoles = ProjectRole;
}

authz_resource! {
    name = "Disk",
    parent = "Project",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InProject,
}

authz_resource! {
    name = "ProjectImage",
    parent = "Project",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InProject,
}

authz_resource! {
    name = "Snapshot",
    parent = "Project",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InProject,
}

authz_resource! {
    name = "Instance",
    parent = "Project",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InProject,
}

authz_resource! {
    name = "InstanceNetworkInterface",
    parent = "Instance",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InProject,
}

authz_resource! {
    name = "Vpc",
    parent = "Project",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InProject,
}

authz_resource! {
    name = "VpcRouter",
    parent = "Vpc",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InProject,
}

authz_resource! {
    name = "RouterRoute",
    parent = "VpcRouter",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InProject,
}

authz_resource! {
    name = "VpcSubnet",
    parent = "Vpc",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InProject,
}

// Customer network integration resources nested below "Fleet"

authz_resource! {
    name = "AddressLot",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "AddressLotBlock",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "LoopbackAddress",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "SwitchPort",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "SwitchPortSettings",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

// Miscellaneous resources nested directly below "Fleet"

authz_resource! {
    name = "ConsoleSession",
    parent = "Fleet",
    primary_key = String,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "DeviceAuthRequest",
    parent = "Fleet",
    primary_key = String, // user_code
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "DeviceAccessToken",
    parent = "Fleet",
    primary_key = String, // token
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "RoleBuiltin",
    parent = "Fleet",
    primary_key = (String, String),
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "UserBuiltin",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "Rack",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "Silo",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = true,
    polar_snippet = Custom,
}

impl ApiResourceWithRolesType for Silo {
    type AllowedRoles = SiloRole;
}

authz_resource! {
    name = "SiloUser",
    parent = "Silo",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = Custom,
}

authz_resource! {
    name = "SiloGroup",
    parent = "Silo",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = Custom,
}

authz_resource! {
    name = "SiloImage",
    parent = "Silo",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InSilo,
}

// This resource is a collection of _all_ images in a silo, including project images.
authz_resource! {
    name = "Image",
    parent = "Silo",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = InSilo,
}

authz_resource! {
    name = "IdentityProvider",
    parent = "Silo",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = Custom,
}

authz_resource! {
    name = "SamlIdentityProvider",
    parent = "Silo",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = Custom,
}

authz_resource! {
    name = "SshKey",
    parent = "SiloUser",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = Custom,
}

authz_resource! {
    name = "Sled",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "SledInstance",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "Service",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "Switch",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "PhysicalDisk",
    parent = "Fleet",
    primary_key = (String, String, String),
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "UpdateArtifact",
    parent = "Fleet",
    primary_key = (String, SemverVersion, KnownArtifactKind),
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "Certificate",
    parent = "Silo",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = Custom,
}

authz_resource! {
    name = "SystemUpdate",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "UpdateDeployment",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

authz_resource! {
    name = "IpPool",
    parent = "Fleet",
    primary_key = Uuid,
    roles_allowed = false,
    polar_snippet = FleetChild,
}

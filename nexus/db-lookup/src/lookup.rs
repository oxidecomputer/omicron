// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Defines the specific builder types for each resource.
//!
//! The `lookup_resource` macro defines a struct for the resource, helper
//! functions for selecting child resources, and the publicly-exposed fetch
//! functions (fetch(), fetch_for(), and lookup_for()).
//!
//! This module is called `lookup` rather than something like `resource` so
//! callers can customarily refer to types in here via `lookup::Instance`,
//! `lookup::Project` etc without needing to say `use nexus_db_lookup::resource
//! as lookup`.

use async_bb8_diesel::AsyncRunQueryDsl;
use db_macros::lookup_resource;
use diesel::{ExpressionMethods, JoinOnDsl, QueryDsl, SelectableHelper};
use ipnetwork::IpNetwork;
use nexus_auth::authn;
use nexus_auth::authz;
use nexus_auth::context::OpContext;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_model::Name;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::{LookupResult, LookupType, ResourceType};
use omicron_uuid_kinds::*;
use slog::{error, trace};
use uuid::Uuid;

use crate::{LookupDataStore, LookupPath};

impl<'a> LookupPath<'a> {
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

    /// Select a resource of type AffinityGroup, identified by its id
    pub fn affinity_group_id(self, id: Uuid) -> AffinityGroup<'a> {
        AffinityGroup::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type AntiAffinityGroup, identified by its id
    pub fn anti_affinity_group_id(self, id: Uuid) -> AntiAffinityGroup<'a> {
        AntiAffinityGroup::PrimaryKey(Root { lookup_root: self }, id)
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

    /// Select a resource of type ConsoleSession, identified by its `id`
    pub fn console_session_id(
        self,
        id: ConsoleSessionUuid,
    ) -> ConsoleSession<'a> {
        ConsoleSession::PrimaryKey(Root { lookup_root: self }, id)
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

    /// Select a resource of type DeviceAccessToken, identified by its `id`
    pub fn device_access_token_id(
        self,
        id: TypedUuid<AccessTokenKind>,
    ) -> DeviceAccessToken<'a> {
        DeviceAccessToken::PrimaryKey(Root { lookup_root: self }, id)
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
    pub fn silo_user_id(self, id: SiloUserUuid) -> SiloUser<'a> {
        SiloUser::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type SiloUser that matches an authenticated Actor
    pub fn silo_user_actor(
        self,
        actor: &'a authn::Actor,
    ) -> Result<SiloUser<'a>, Error> {
        match actor {
            authn::Actor::SiloUser { silo_user_id, .. } => Ok(
                SiloUser::PrimaryKey(Root { lookup_root: self }, *silo_user_id),
            ),

            authn::Actor::UserBuiltin { .. } => Err(
                Error::non_resourcetype_not_found("could not find silo user"),
            ),
        }
    }

    /// Select a resource of type SiloGroup, identified by its id
    pub fn silo_group_id(self, id: SiloGroupUuid) -> SiloGroup<'a> {
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
    pub fn sled_id(self, id: SledUuid) -> Sled<'a> {
        Sled::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Zpool, identified by its id
    pub fn zpool_id(self, id: ZpoolUuid) -> Zpool<'a> {
        Zpool::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type Switch, identified by its id
    pub fn switch_id(self, id: Uuid) -> Switch<'a> {
        Switch::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type PhysicalDisk, identified by its id
    pub fn physical_disk(self, id: PhysicalDiskUuid) -> PhysicalDisk<'a> {
        PhysicalDisk::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type SupportBundle, identified by its id
    pub fn support_bundle(self, id: SupportBundleUuid) -> SupportBundle<'a> {
        SupportBundle::PrimaryKey(Root { lookup_root: self }, id)
    }

    pub fn tuf_trust_root(self, id: TufTrustRootUuid) -> TufTrustRoot<'a> {
        TufTrustRoot::PrimaryKey(Root { lookup_root: self }, id)
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
    pub fn tuf_artifact_id(
        self,
        id: TypedUuid<TufArtifactKind>,
    ) -> TufArtifact<'a> {
        TufArtifact::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type UserBuiltin, identified by its `id`
    pub fn user_builtin_id<'b>(self, id: BuiltInUserUuid) -> UserBuiltin<'b>
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

    pub fn alert_receiver_id<'b>(
        self,
        id: AlertReceiverUuid,
    ) -> AlertReceiver<'b>
    where
        'a: 'b,
    {
        AlertReceiver::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type [`AlertReceiver`], identified by its name
    pub fn alert_receiver_name<'b, 'c>(
        self,
        name: &'b Name,
    ) -> AlertReceiver<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        AlertReceiver::Name(Root { lookup_root: self }, name)
    }

    /// Select a resource of type [`AlertReceiver`], identified by its owned name
    pub fn alert_receiver_name_owned<'b, 'c>(
        self,
        name: Name,
    ) -> AlertReceiver<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        AlertReceiver::OwnedName(Root { lookup_root: self }, name)
    }

    /// Select a resource of type [`WebhookSecret`], identified by its UUID.
    pub fn webhook_secret_id<'b>(
        self,
        id: WebhookSecretUuid,
    ) -> WebhookSecret<'b>
    where
        'a: 'b,
    {
        WebhookSecret::PrimaryKey(Root { lookup_root: self }, id)
    }

    /// Select a resource of type [`Alert`], identified by its UUID.
    pub fn alert_id<'b>(self, id: AlertUuid) -> Alert<'b>
    where
        'a: 'b,
    {
        Alert::PrimaryKey(Root { lookup_root: self }, id)
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

// Main resource hierarchy: Organizations, Projects, and their resources

lookup_resource! {
    name = "Silo",
    ancestors = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "SiloUser",
    ancestors = [ "Silo" ],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", uuid_kind = SiloUserKind } ],
    visible_outside_silo = true
}

lookup_resource! {
    name = "SiloGroup",
    ancestors = [ "Silo" ],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", uuid_kind = SiloGroupKind } ]
}

lookup_resource! {
    name = "SiloImage",
    ancestors = [ "Silo" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "IdentityProvider",
    ancestors = [ "Silo" ],
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
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid }]
}

lookup_resource! {
    name = "SamlIdentityProvider",
    ancestors = [ "Silo" ],
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
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

// Project lookup is hand-written (instead of using the macro) so we can fetch
// restrict_network_actions from the Silo table during lookup
pub enum Project<'a> {
    Error(Root<'a>, Error),
    Name(Silo<'a>, &'a Name),
    OwnedName(Silo<'a>, Name),
    PrimaryKey(Root<'a>, Uuid),
}

impl<'a> Project<'a> {
    pub async fn fetch(
        &self,
    ) -> LookupResult<(authz::Silo, authz::Project, nexus_db_model::Project)> {
        self.fetch_for(authz::Action::Read).await
    }

    pub async fn optional_fetch(
        &self,
    ) -> LookupResult<Option<(authz::Silo, authz::Project, nexus_db_model::Project)>> {
        self.optional_fetch_for(authz::Action::Read).await
    }

    pub async fn fetch_for(
        &self,
        action: authz::Action,
    ) -> LookupResult<(authz::Silo, authz::Project, nexus_db_model::Project)> {
        let lookup = self.lookup_root();
        let opctx = &lookup.opctx;
        let datastore = lookup.datastore;
        match &self {
            Project::Error(_, error) => Err(error.clone()),
            Project::Name(parent, &ref name) | Project::OwnedName(parent, ref name) => {
                let (authz_silo,) = parent.lookup().await?;
                let (authz_project, db_row) = Self::fetch_by_name_for(
                        opctx,
                        datastore,
                        &authz_silo,
                        name,
                        action,
                    )
                    .await?;
                Ok((authz_silo, authz_project, db_row))
            }
            Project::PrimaryKey(_, v0) => {
                Self::fetch_by_id_for(opctx, datastore, v0, action).await
            }
        }
            .and_then(|input| {
                let (ref authz_silo, .., ref authz_project, ref _db_row) = &input;
                Self::silo_check(opctx, authz_silo, authz_project)?;
                Ok(input)
            })
    }

    pub async fn optional_fetch_for(
        &self,
        action: authz::Action,
    ) -> LookupResult<Option<(authz::Silo, authz::Project, nexus_db_model::Project)>> {
        let result = self.fetch_for(action).await;
        match result {
            Err(Error::ObjectNotFound { type_name: _, lookup_type: _ }) => Ok(None),
            _ => Ok(Some(result?)),
        }
    }

    pub async fn lookup_for(
        &self,
        action: authz::Action,
    ) -> LookupResult<(authz::Silo, authz::Project)> {
        let lookup = self.lookup_root();
        let opctx = &lookup.opctx;
        let (authz_silo, authz_project) = self.lookup().await?;
        opctx.authorize(action, &authz_project).await?;
        Ok((authz_silo, authz_project))
            .and_then(|input| {
                let (ref authz_silo, .., ref authz_project) = &input;
                Self::silo_check(opctx, authz_silo, authz_project)?;
                Ok(input)
            })
    }

    pub async fn optional_lookup_for(
        &self,
        action: authz::Action,
    ) -> LookupResult<Option<(authz::Silo, authz::Project)>> {
        let result = self.lookup_for(action).await;
        match result {
            Err(Error::ObjectNotFound { type_name: _, lookup_type: _ }) => Ok(None),
            _ => Ok(Some(result?)),
        }
    }

    async fn lookup(&self) -> LookupResult<(authz::Silo, authz::Project)> {
        let lookup = self.lookup_root();
        let opctx = &lookup.opctx;
        let datastore = lookup.datastore;
        match &self {
            Project::Error(_, error) => Err(error.clone()),
            Project::Name(parent, &ref name) | Project::OwnedName(parent, ref name) => {
                let (authz_silo,) = parent.lookup().await?;
                let (authz_project, _) = Self::lookup_by_name_no_authz(
                        opctx,
                        datastore,
                        &authz_silo,
                        name,
                    )
                    .await?;
                Ok((authz_silo, authz_project))
            }
            Project::PrimaryKey(_, v0) => {
                let (authz_silo, authz_project, _) = Self::lookup_by_id_no_authz(
                        opctx,
                        datastore,
                        v0,
                    )
                    .await?;
                Ok((authz_silo, authz_project))
            }
        }
    }

    fn lookup_root(&self) -> &LookupPath<'a> {
        match &self {
            Project::Error(root, ..) => root.lookup_root(),
            Project::Name(parent, _) | Project::OwnedName(parent, _) => {
                parent.lookup_root()
            }
            Project::PrimaryKey(root, ..) => root.lookup_root(),
        }
    }

    fn silo_check(
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        authz_project: &authz::Project,
    ) -> Result<(), Error> {
        let log = &opctx.log;
        let actor_silo_id = match opctx
            .authn
            .silo_or_builtin()
            .internal_context("siloed resource check")
        {
            Ok(Some(silo)) => silo.id(),
            Ok(None) => {
                trace!(
                    log,
                    "successful lookup of siloed resource {:?} \
                            using built-in user",
                    "Project",
                );
                return Ok(());
            }
            Err(error) => {
                error!(
                    log,
                    "unexpected successful lookup of siloed resource \
                            {:?} with no actor in OpContext",
                    "Project",
                );
                return Err(error);
            }
        };
        let resource_silo_id = authz_silo.id();
        if resource_silo_id != actor_silo_id {
            use nexus_auth::authz::ApiResource;
            error!(
                log,
                "unexpected successful lookup of siloed resource \
                        {:?} in a different Silo from current actor (resource \
                        Silo {}, actor Silo {})",
                "Project", resource_silo_id, actor_silo_id,
            );
            Err(authz_project.not_found())
        } else {
            Ok(())
        }
    }

    async fn fetch_by_name_for(
        opctx: &OpContext,
        datastore: &dyn LookupDataStore,
        authz_silo: &authz::Silo,
        name: &Name,
        action: authz::Action,
    ) -> LookupResult<(authz::Project, nexus_db_model::Project)> {
        let (authz_project, db_row) = Self::lookup_by_name_no_authz(
                opctx,
                datastore,
                authz_silo,
                name,
            )
            .await?;
        opctx.authorize(action, &authz_project).await?;
        Ok((authz_project, db_row))
    }

    // CUSTOM: This function is customized to JOIN with the silo table to fetch restrict_network_actions
    async fn lookup_by_name_no_authz(
        opctx: &OpContext,
        datastore: &dyn LookupDataStore,
        authz_silo: &authz::Silo,
        name: &Name,
    ) -> LookupResult<(authz::Project, nexus_db_model::Project)> {
        use nexus_db_schema::schema::project::dsl as project_dsl;
        use nexus_db_schema::schema::silo::dsl as silo_dsl;

        let (db_row, restrict_network_actions): (nexus_db_model::Project, bool) = project_dsl::project
            .filter(project_dsl::time_deleted.is_null())
            .filter(project_dsl::name.eq(name.clone()))
            .filter(project_dsl::silo_id.eq(authz_silo.id()))
            .inner_join(silo_dsl::silo.on(project_dsl::silo_id.eq(silo_dsl::id)))
            .select((
                nexus_db_model::Project::as_select(),
                silo_dsl::restrict_network_actions,
            ))
            .get_result_async(&*datastore.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Project,
                        LookupType::ByName(name.as_str().to_string()),
                    ),
                )
            })?;

        let authz_project = authz::Project::with_primary_key(
            authz_silo.clone(),
            db_row.id(),
            LookupType::ByName(name.as_str().to_string())
        )
        .with_network_restrictions(restrict_network_actions);

        Ok((authz_project, db_row))
    }

    async fn fetch_by_id_for(
        opctx: &OpContext,
        datastore: &dyn LookupDataStore,
        v0: &Uuid,
        action: authz::Action,
    ) -> LookupResult<(authz::Silo, authz::Project, nexus_db_model::Project)> {
        let (authz_silo, authz_project, db_row) = Self::lookup_by_id_no_authz(
                opctx,
                datastore,
                v0,
            )
            .await?;
        opctx.authorize(action, &authz_project).await?;
        Ok((authz_silo, authz_project, db_row))
    }

    // CUSTOM: This function is customized to JOIN with the silo table to fetch restrict_network_actions
    async fn lookup_by_id_no_authz(
        opctx: &OpContext,
        datastore: &dyn LookupDataStore,
        v0: &Uuid,
    ) -> LookupResult<(authz::Silo, authz::Project, nexus_db_model::Project)> {
        use nexus_db_schema::schema::project::dsl as project_dsl;
        use nexus_db_schema::schema::silo::dsl as silo_dsl;

        let (db_row, restrict_network_actions): (nexus_db_model::Project, bool) = project_dsl::project
            .filter(project_dsl::time_deleted.is_null())
            .filter(project_dsl::id.eq(v0.clone()))
            .inner_join(silo_dsl::silo.on(project_dsl::silo_id.eq(silo_dsl::id)))
            .select((
                nexus_db_model::Project::as_select(),
                silo_dsl::restrict_network_actions,
            ))
            .get_result_async(&*datastore.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Project,
                        LookupType::ById(
                            ::omicron_uuid_kinds::GenericUuid::into_untyped_uuid(*v0),
                        ),
                    ),
                )
            })?;

        let (authz_silo, _) = Silo::lookup_by_id_no_authz(
                opctx,
                datastore,
                &db_row.silo_id.into(),
            )
            .await?;
        let authz_project = authz::Project::with_primary_key(
            authz_silo.clone(),
            db_row.id(),
            LookupType::ById(::omicron_uuid_kinds::GenericUuid::into_untyped_uuid(*v0)),
        )
        .with_network_restrictions(restrict_network_actions);

        Ok((authz_silo, authz_project, db_row))
    }
}

// Child selector functions for Silo
impl<'a> Silo<'a> {
    pub fn project_name<'b, 'c>(self, name: &'b Name) -> Project<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Project::Name(self, name)
    }

    pub fn project_name_owned<'c>(self, name: Name) -> Project<'c>
    where
        'a: 'c,
    {
        Project::OwnedName(self, name)
    }
}

lookup_resource! {
    name = "Disk",
    ancestors = [ "Silo", "Project" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Image",
    ancestors = ["Silo"],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "ProjectImage",
    ancestors = [ "Silo", "Project" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Snapshot",
    ancestors = [ "Silo", "Project" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Instance",
    ancestors = [ "Silo", "Project" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "AffinityGroup",
    ancestors = [ "Silo", "Project" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "AntiAffinityGroup",
    ancestors = [ "Silo", "Project" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "InstanceNetworkInterface",
    ancestors = [ "Silo", "Project", "Instance" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Vpc",
    ancestors = [ "Silo", "Project" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "VpcRouter",
    ancestors = [ "Silo", "Project", "Vpc" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "RouterRoute",
    ancestors = [ "Silo", "Project", "Vpc", "VpcRouter" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "VpcSubnet",
    ancestors = [ "Silo", "Project", "Vpc" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "InternetGateway",
    ancestors = [ "Silo", "Project", "Vpc" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "InternetGatewayIpPool",
    ancestors = [ "Silo", "Project", "Vpc", "InternetGateway" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "InternetGatewayIpAddress",
    ancestors = [ "Silo", "Project", "Vpc", "InternetGateway" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "FloatingIp",
    ancestors = [ "Silo", "Project" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

// Miscellaneous resources nested directly below "Fleet"

lookup_resource! {
    name = "ConsoleSession",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", uuid_kind = ConsoleSessionKind } ]
}

lookup_resource! {
    name = "DeviceAuthRequest",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "user_code", rust_type = String },
    ]
}

lookup_resource! {
    name = "DeviceAccessToken",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", uuid_kind = AccessTokenKind } ]
}

lookup_resource! {
    name = "Rack",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "Sled",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", uuid_kind = SledKind } ]
}

lookup_resource! {
    name = "Zpool",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", uuid_kind = ZpoolKind } ]
}

lookup_resource! {
    name = "SledInstance",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ],
}

lookup_resource! {
    name = "Switch",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "PhysicalDisk",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", uuid_kind = PhysicalDiskKind } ]
}

lookup_resource! {
    name = "SupportBundle",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", uuid_kind = SupportBundleKind } ]
}

lookup_resource! {
    name = "TufRepo",
    ancestors = [],
    // TODO: should this have TufArtifact as a child? This is a many-many
    // relationship.
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", uuid_kind = TufRepoKind } ]
}

lookup_resource! {
    name = "TufArtifact",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", uuid_kind = TufArtifactKind } ]
}

lookup_resource! {
    name = "TufTrustRoot",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", uuid_kind = TufTrustRootKind } ]
}

lookup_resource! {
    name = "UserBuiltin",
    ancestors = [],
    lookup_by_name = true,
    soft_deletes = false,
    primary_key_columns = [ { column_name = "id", uuid_kind = BuiltInUserKind } ]
}

lookup_resource! {
    name = "Certificate",
    ancestors = [ "Silo" ],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "AddressLot",
    ancestors = [], // TODO: Should this include AddressLotBlock?
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
}

lookup_resource! {
    name = "LoopbackAddress",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "address", rust_type = IpNetwork },
        { column_name = "rack_id", rust_type = Uuid },
        { column_name = "switch_location", rust_type = String }
    ]
}

lookup_resource! {
    name = "AlertReceiver",
    ancestors = [],
    lookup_by_name = true,
    soft_deletes = true,
    primary_key_columns = [
        { column_name = "id", uuid_kind = AlertReceiverKind }
    ]
}

lookup_resource! {
    name = "WebhookSecret",
    ancestors = ["AlertReceiver"],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "id", uuid_kind = WebhookSecretKind }
    ]
}

lookup_resource! {
    name = "Alert",
    ancestors = [],
    lookup_by_name = false,
    soft_deletes = false,
    primary_key_columns = [
        { column_name = "id", uuid_kind = AlertKind }
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

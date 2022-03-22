// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for looking up API resources from the database

use super::datastore::DataStore;
use super::identity::Resource;
use super::model;
use crate::{
    authz::{self, AuthorizedResource},
    context::OpContext,
    db,
    db::error::{public_error_from_diesel_pool, ErrorHandler},
    db::model::Name,
};
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use futures::future::BoxFuture;
use futures::FutureExt;
use omicron_common::api::external::{LookupResult, LookupType, ResourceType};
use uuid::Uuid;

// TODO-dap XXX Neither "fetcH" nor "lookup" needs to be a trait now that the
// macro is defining the impls and the structs

pub trait Fetch {
    type FetchType;
    fn fetch(&self) -> BoxFuture<'_, LookupResult<Self::FetchType>>;
}

// This private module exist solely to implement the "Sealed trait" pattern.
// This isn't about future-proofing ourselves.  Rather, we don't want to expose
// an interface that accesses database objects without an authz check.
mod private {
    use super::LookupPath;
    use futures::future::BoxFuture;
    use omicron_common::api::external::LookupResult;

    // TODO-dap XXX-dap we could probably get rid of this trait and its impls
    // because we're now calling `lookup_root()` from a macro that can
    // essentially inline the impl of GetLookupRoot for Key.
    pub trait GetLookupRoot {
        fn lookup_root(&self) -> &LookupPath<'_>;
    }

    pub trait LookupNoauthz {
        type LookupType;
        fn lookup(&self) -> BoxFuture<'_, LookupResult<Self::LookupType>>;
    }
}

use private::GetLookupRoot;
use private::LookupNoauthz;

pub trait LookupFor {
    type LookupType;
    fn lookup_for(
        &self,
        action: authz::Action,
    ) -> BoxFuture<'_, LookupResult<Self::LookupType>>;
}

enum Key<'a, P> {
    Name(P, &'a Name),
    Id(LookupPath<'a>, Uuid),
}

impl<'a, T> GetLookupRoot for Key<'a, T>
where
    T: GetLookupRoot,
{
    fn lookup_root(&self) -> &LookupPath<'_> {
        match self {
            Key::Name(parent, _) => parent.lookup_root(),
            Key::Id(lookup, _) => lookup,
        }
    }
}

impl<'a, P> Key<'a, P> {
    fn lookup_type(&self) -> LookupType {
        match self {
            Key::Name(_, name) => LookupType::ByName(name.as_str().to_string()),
            Key::Id(_, id) => LookupType::ById(*id),
        }
    }
}

pub struct LookupPath<'a> {
    opctx: &'a OpContext,
    datastore: &'a DataStore,
}

impl<'a> LookupPath<'a> {
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

    pub fn organization_name<'b, 'c>(self, name: &'b Name) -> Organization<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Organization { key: Key::Name(self, name) }
    }

    pub fn organization_id(self, id: Uuid) -> Organization<'a> {
        Organization { key: Key::Id(self, id) }
    }

    pub fn project_id(self, id: Uuid) -> Project<'a> {
        Project { key: Key::Id(self, id) }
    }

    pub fn instance_id(self, id: Uuid) -> Instance<'a> {
        Instance { key: Key::Id(self, id) }
    }

    pub fn disk_id(self, id: Uuid) -> Disk<'a> {
        Disk { key: Key::Id(self, id) }
    }
}

impl<'a> GetLookupRoot for LookupPath<'a> {
    fn lookup_root(&self) -> &LookupPath<'_> {
        self
    }
}

impl<'a> Organization<'a> {
    pub fn project_name<'b, 'c>(self, name: &'b Name) -> Project<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Project { key: Key::Name(self, name) }
    }
}

impl<'a> Project<'a> {
    pub fn disk_name<'b, 'c>(self, name: &'b Name) -> Disk<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Disk { key: Key::Name(self, name) }
    }

    pub fn instance_name<'b, 'c>(self, name: &'b Name) -> Instance<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Instance { key: Key::Name(self, name) }
    }
}

macro_rules! define_lookup {
    ($pc:ident) => {
        paste::paste! {
            pub struct $pc<'a> {
                key: Key<'a, LookupPath<'a>>,
            }

            impl<'a> GetLookupRoot for $pc<'a> {
                fn lookup_root(&self) -> &LookupPath<'_> {
                    self.key.lookup_root()
                }
            }

            // Do NOT make these functions public.  They should instead be
            // wrapped by functions that perform authz checks.
            async fn [<$pc:lower _lookup_by_id_no_authz>](
                _opctx: &OpContext,
                datastore: &DataStore,
                id: Uuid,
            ) -> LookupResult<(authz::$pc, model::$pc)> {
                use db::schema::[<$pc:lower>]::dsl;
                // TODO-security This could use pool_authorized() instead.
                // However, it will change the response code for this case:
                // unauthenticated users will get a 401 rather than a 404
                // because we'll kick them out sooner than we used to -- they
                // won't even be able to make this database query.  That's a
                // good thing but this change can be deferred to a follow-up PR.
                let conn = datastore.pool();
                dsl::[<$pc:lower>]
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::id.eq(id))
                    .select(model::$pc::as_select())
                    .get_result_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::NotFoundByLookup(
                                ResourceType::$pc,
                                LookupType::ById(id)
                            )
                        )
                    })
                    .map(|o| {(
                        authz::FLEET.[<$pc:lower>](o.id(), LookupType::ById(id)),
                        o
                        )}
                    )
            }

            // Do NOT make these functions public.  They should instead be
            // wrapped by functions that perform authz checks.
            async fn [<$pc:lower _lookup_by_name_no_authz>](
                _opctx: &OpContext,
                datastore: &DataStore,
                name: &Name,
            ) -> LookupResult<(authz::$pc, model::$pc)> {
                use db::schema::[<$pc:lower>]::dsl;
                // TODO-security See the note about pool_authorized() above.
                let conn = datastore.pool();
                dsl::[<$pc:lower>]
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::name.eq(name.clone()))
                    .select(model::$pc::as_select())
                    .get_result_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::NotFoundByLookup(
                                ResourceType::$pc,
                                LookupType::ByName(name.as_str().to_string())
                            )
                        )
                    })
                    .map(|o| {(
                        authz::FLEET.[<$pc:lower>](
                            o.id(),
                            LookupType::ByName(name.as_str().to_string())
                        ),
                        o
                        )}
                    )
            }

            async fn [<$pc:lower _fetch_by_id>](
                opctx: &OpContext,
                datastore: &DataStore,
                id: Uuid,
            ) -> LookupResult<(authz::$pc, model::$pc)> {
                let (authz_child, db_child) =
                    [<$pc:lower _lookup_by_id_no_authz>](
                        opctx,
                        datastore,
                        id,
                    ).await?;
                opctx.authorize(authz::Action::Read, &authz_child).await?;
                Ok((authz_child, db_child))
            }

            async fn [<$pc:lower _fetch_by_name>](
                opctx: &OpContext,
                datastore: &DataStore,
                name: &Name,
            ) -> LookupResult<(authz::$pc, model::$pc)> {
                let (authz_child, db_child) =
                    [<$pc:lower _lookup_by_name_no_authz>](
                        opctx,
                        datastore,
                        name
                    ).await?;
                opctx.authorize(authz::Action::Read, &authz_child).await?;
                Ok((authz_child, db_child))
            }

            impl LookupNoauthz for $pc<'_> {
                type LookupType = (authz::$pc,);

                fn lookup(
                    &self,
                ) -> BoxFuture<'_, LookupResult<Self::LookupType>> {
                    async {
                        let lookup = self.lookup_root();
                        let opctx = &lookup.opctx;
                        let datastore = lookup.datastore;
                        match self.key {
                            Key::Name(_, name) => {
                                let (rv, _) =
                                    [<$pc:lower _lookup_by_name_no_authz>](
                                        opctx,
                                        datastore,
                                        name
                                    ).await?;
                                Ok((rv,))
                            }
                            Key::Id(_, id) => {
                                let (rv, _) =
                                    [<$pc:lower _lookup_by_id_no_authz>](
                                        opctx,
                                        datastore,
                                        id
                                    ).await?;
                                Ok((rv,))
                            }
                        }
                    }.boxed()
                }
            }

            impl Fetch for $pc<'_> {
                type FetchType = (authz::$pc, model::$pc);

                fn fetch(&self) -> BoxFuture<'_, LookupResult<Self::FetchType>> {
                    async {
                        let lookup = self.lookup_root();
                        let opctx = &lookup.opctx;
                        let datastore = lookup.datastore;
                        match self.key {
                            Key::Name(_, name) => {
                                [<$pc:lower _fetch_by_name>](
                                    opctx,
                                    datastore,
                                    name
                                ).await
                            }
                            Key::Id(_, id) => {
                                [<$pc:lower _fetch_by_id>](
                                    opctx,
                                    datastore,
                                    id
                                ).await
                            }
                        }
                    }
                    .boxed()
                }
            }

            impl LookupFor for $pc<'_> {
                type LookupType = <Self as LookupNoauthz>::LookupType;

                fn lookup_for(
                    &self,
                    action: authz::Action,
                ) -> BoxFuture<'_, LookupResult<Self::LookupType>> {
                    async move {
                        let lookup = self.lookup_root();
                        let opctx = &lookup.opctx;
                        let (authz_child,) = self.lookup().await?;
                        opctx.authorize(action, &authz_child).await?;
                        Ok((authz_child,))
                    }
                    .boxed()
                }
            }
        }
    };
}

macro_rules! define_lookup_with_parent {
    (
        $pc:ident,              // Pascal-case version of resource name
        $parent_pc:ident,       // Pascal-case version of parent resource name
        ($($ancestor:ident),*), // List of ancestors above parent
        // XXX-dap update comment
        $mkauthz:expr           // Closure to generate resource's authz object
                                //   from parent's
    ) => {
        paste::paste! {
            pub struct $pc<'a> {
                key: Key<'a, $parent_pc<'a>>,
            }

            impl<'a> GetLookupRoot for $pc<'a> {
                fn lookup_root(&self) -> &LookupPath<'_> {
                    self.key.lookup_root()
                }
            }

            // Do NOT make these functions public.  They should instead be
            // wrapped by functions that perform authz checks.
            async fn [<$pc:lower _lookup_by_id_no_authz>](
                opctx: &OpContext,
                datastore: &DataStore,
                id: Uuid,
            ) -> LookupResult<(
                    $(authz::[<$ancestor>],)*
                    authz::$parent_pc,
                    authz::$pc,
                    model::$pc
            )> {
                use db::schema::[<$pc:lower>]::dsl;
                // TODO-security See the note about pool_authorized() above.
                let conn = datastore.pool();
                let db_row = dsl::[<$pc:lower>]
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::id.eq(id))
                    .select(model::$pc::as_select())
                    .get_result_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::NotFoundByLookup(
                                ResourceType::$pc,
                                LookupType::ById(id)
                            )
                        )
                    })?;
                let ($([<_authz_ $ancestor:lower>],)* authz_parent, _) =
                    [< $parent_pc:lower _lookup_by_id_no_authz >](
                        opctx,
                        datastore,
                        db_row.[<$parent_pc:lower _id>]
                    ).await?;
                let authz_list = ($mkauthz)(
                    authz_parent, db_row, LookupType::ById(id)
                );
                Ok(authz_list)
            }

            // Do NOT make these functions public.  They should instead be
            // wrapped by functions that perform authz checks.
            async fn [<$pc:lower _lookup_by_name_no_authz>](
                _opctx: &OpContext,
                datastore: &DataStore,
                authz_parent: &authz::$parent_pc,
                name: &Name,
            ) -> LookupResult<(
                    $(authz::[<$ancestor>],)*
                    authz::$parent_pc,
                    authz::$pc,
                    model::$pc
            )> {
                use db::schema::[<$pc:lower>]::dsl;
                // TODO-security See the note about pool_authorized() above.
                let conn = datastore.pool();
                dsl::[<$pc:lower>]
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::name.eq(name.clone()))
                    .filter(dsl::[<$parent_pc:lower _id>].eq(authz_parent.id()))
                    .select(model::$pc::as_select())
                    .get_result_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::NotFoundByLookup(
                                ResourceType::$pc,
                                LookupType::ByName(name.as_str().to_string())
                            )
                        )
                    })
                    .map(|dbmodel| {
                        ($mkauthz)(
                            authz_parent.clone(),
                            dbmodel,
                            LookupType::ByName(name.as_str().to_string())
                        )
                    })
            }

            async fn [<$pc:lower _fetch_by_id>](
                opctx: &OpContext,
                datastore: &DataStore,
                id: Uuid,
            ) -> LookupResult<(
                    $(authz::[<$ancestor>],)*
                    authz::$parent_pc,
                    authz::$pc,
                    model::$pc
            )> {
                let (
                    $([<authz_ $ancestor:lower>],)*
                    authz_parent,
                    authz_child,
                    db_child
                ) =
                    [<$pc:lower _lookup_by_id_no_authz>](
                        opctx,
                        datastore,
                        id,
                    ).await?;
                opctx.authorize(authz::Action::Read, &authz_child).await?;
                Ok((
                    $([<authz_ $ancestor:lower>],)*
                    authz_parent,
                    authz_child,
                    db_child
                ))
            }

            async fn [<$pc:lower _fetch_by_name>](
                opctx: &OpContext,
                datastore: &DataStore,
                authz_parent: &authz::$parent_pc,
                name: &Name,
            ) -> LookupResult<(
                    $(authz::[<$ancestor>],)*
                    authz::$parent_pc,
                    authz::$pc,
                    model::$pc
            )> {
                let (
                    $([<authz_ $ancestor:lower>],)*
                    authz_parent,
                    authz_child,
                    db_child
                ) =
                    [<$pc:lower _lookup_by_name_no_authz>](
                        opctx,
                        datastore,
                        authz_parent,
                        name
                    ).await?;
                opctx.authorize(authz::Action::Read, &authz_child).await?;
                Ok((
                    $([<authz_ $ancestor:lower>],)*
                    authz_parent,
                    authz_child,
                    db_child
                ))
            }

            impl LookupNoauthz for $pc<'_> {
                type LookupType = (
                    $(authz::[<$ancestor>],)*
                    authz::[<$parent_pc>],
                    authz::$pc,
                );

                fn lookup(
                    &self,
                ) -> BoxFuture<'_, LookupResult<Self::LookupType>> {
                    async {
                        let lookup = self.lookup_root();
                        let opctx = &lookup.opctx;
                        let datastore = lookup.datastore;
                        match &self.key {
                            Key::Name(parent, name) => {
                                let (
                                    $([<authz_ $ancestor:lower>],)*
                                    authz_parent,
                                ) = parent.lookup().await?;
                                let (
                                    $([<_authz_ $ancestor:lower>],)*
                                    _authz_parent,
                                    authz_child, _) =
                                    [< $pc:lower _lookup_by_name_no_authz >](
                                        opctx,
                                        datastore,
                                        &authz_parent,
                                        *name
                                    ).await?;
                                Ok((
                                    $([<authz_ $ancestor:lower>],)*
                                    authz_parent,
                                    authz_child
                                ))
                            }
                            Key::Id(_, id) => {
                                let (
                                    $([<authz_ $ancestor:lower>],)*
                                    authz_parent,
                                    authz_child, _) =
                                    [< $pc:lower _lookup_by_id_no_authz >](
                                        opctx,
                                        datastore,
                                        *id
                                    ).await?;
                                Ok((
                                    $([<authz_ $ancestor:lower>],)*
                                    authz_parent,
                                    authz_child
                                ))
                            }
                        }
                    }
                    .boxed()
                }
            }

            impl Fetch for $pc<'_> {
                type FetchType = (
                    $(authz::[<$ancestor>],)*
                    authz::[<$parent_pc>],
                    authz::$pc,
                    model::$pc
                );

                fn fetch(&self) -> BoxFuture<'_, LookupResult<Self::FetchType>> {
                    async {
                        let lookup = self.lookup_root();
                        let opctx = &lookup.opctx;
                        let datastore = lookup.datastore;
                        match &self.key {
                            Key::Name(parent, name) => {
                                let (
                                    $([<_authz_ $ancestor:lower>],)*
                                    authz_parent,
                                ) = parent.lookup().await?;
                                [< $pc:lower _fetch_by_name >](
                                    opctx,
                                    datastore,
                                    &authz_parent,
                                    *name
                                ).await
                            }
                            Key::Id(_, id) => {
                                [< $pc:lower _fetch_by_id >](
                                    opctx,
                                    datastore,
                                    *id
                                ).await
                            }
                        }
                    }
                    .boxed()
                }
            }

            impl LookupFor for $pc<'_> {
                type LookupType = <Self as LookupNoauthz>::LookupType;

                fn lookup_for(
                    &self,
                    action: authz::Action,
                ) -> BoxFuture<'_, LookupResult<Self::LookupType>> {
                    async move {
                        let lookup = self.lookup_root();
                        let opctx = &lookup.opctx;
                        let (
                                $([<authz_ $ancestor:lower>],)*
                                authz_parent,
                                authz_child
                        ) = self.lookup().await?;
                        opctx.authorize(action, &authz_child).await?;
                        Ok((
                            $([<authz_ $ancestor:lower>],)*
                            authz_parent,
                            authz_child
                        ))
                    }
                    .boxed()
                }
            }
        }
    };
}

define_lookup!(Organization);

define_lookup_with_parent!(
    Project,
    Organization,
    (),
    |authz_org: authz::Organization,
     project: model::Project,
     lookup: LookupType| {
        (authz_org.clone(), authz_org.project(project.id(), lookup), project)
    }
);

define_lookup_with_parent!(
    Instance,
    Project,
    (Organization),
    |authz_project: authz::Project,
     instance: model::Instance,
     lookup: LookupType| {
        (
            authz_project.organization().clone(),
            authz_project.clone(),
            authz_project.child_generic(
                ResourceType::Instance,
                instance.id(),
                lookup,
            ),
            instance,
        )
    }
);

define_lookup_with_parent!(
    Disk,
    Project,
    (Organization),
    |authz_project: authz::Project, disk: model::Disk, lookup: LookupType| {
        (
            authz_project.organization().clone(),
            authz_project.clone(),
            authz_project.child_generic(ResourceType::Disk, disk.id(), lookup),
            disk,
        )
    }
);

// XXX-dap
mod play {
    use super::{Organization, Project};
    use crate::db::identity::Resource as _;
    use crate::{
        authz::{self, AuthorizedResource},
        context::OpContext,
        db::{
            self,
            error::{public_error_from_diesel_pool, ErrorHandler},
            model::{self, Name},
            DataStore,
        },
    };
    use diesel::dsl::Eq;
    use diesel::dsl::IsNull;
    use diesel::query_dsl::methods;
    use diesel::{sql_types, ExpressionMethods, QueryDsl};
    use omicron_common::api::external::{LookupType, ResourceType};
    use uuid::Uuid;

    trait Resource {
        type Authz: AuthorizedResource;
        type LookupPath;

        type Model;
        const RESOURCE_TYPE: ResourceType;

        type Table: diesel::Table
            + diesel::QueryDsl
            + methods::FilterDsl<IsNull<Self::TimeDeletedColumn>>
            + methods::FilterDsl<Eq<Self::NameColumn, Name>>
            + methods::FilterDsl<Eq<Self::IdColumn, Uuid>>;
        const TABLE: Self::Table;

        type TimeDeletedColumn: 'static
            + diesel::Column
            + Copy
            + ExpressionMethods
            + diesel::AppearsOnTable<Self::Table>;
        const TIME_DELETED_COLUMN: Self::TimeDeletedColumn;

        type IdColumn: 'static
            // XXX-dap should be related to Uuid instead?
            + diesel::Column<SqlType = sql_types::Uuid>
            + Copy
            + ExpressionMethods
            + diesel::AppearsOnTable<Self::Table>;
        const ID_COLUMN: Self::IdColumn;

        type NameColumn: 'static
            // XXX-dap should be related to Name instead?
            + diesel::Column<SqlType = sql_types::Text>
            + Copy
            + ExpressionMethods
            + diesel::AppearsOnTable<Self::Table>;
        const NAME_COLUMN: Self::NameColumn;
    }

    trait ResourceWithParent: Resource {
        type Parent: Resource;
        type ParentIdColumn: 'static
            // XXX-dap shoudl be related to Uuid instead?
            + diesel::Column<SqlType = sql_types::Uuid>
            + Copy
            + ExpressionMethods
            + diesel::AppearsOnTable<<Self as Resource>::Table>;
        const PARENT_ID_COLUMN: Self::ParentIdColumn;

        fn authz_path(
            authz_parent: &<Self::Parent as Resource>::Authz,
            db_row: &Self::Model,
            lookup: LookupType,
        ) -> Self::LookupPath;

        fn parent_key(authz_parent: &<Self::Parent as Resource>::Authz)
            -> Uuid;
    }

    impl<'a> Resource for Organization<'a> {
        type Authz = authz::Organization;
        type LookupPath = (authz::Organization,);
        type Model = model::Organization;
        const RESOURCE_TYPE: ResourceType = ResourceType::Organization;
        type Table = db::schema::organization::table;
        const TABLE: Self::Table = db::schema::organization::dsl::organization;
        type TimeDeletedColumn =
            db::schema::organization::columns::time_deleted;
        const TIME_DELETED_COLUMN: Self::TimeDeletedColumn =
            db::schema::organization::dsl::time_deleted;
        type IdColumn = db::schema::organization::columns::id;
        const ID_COLUMN: Self::IdColumn = db::schema::organization::dsl::id;
        type NameColumn = db::schema::organization::columns::name;
        const NAME_COLUMN: Self::NameColumn =
            db::schema::organization::dsl::name;
    }

    impl<'a> Resource for Project<'a> {
        type Authz = authz::Project;
        type LookupPath = (authz::Organization, authz::Project);
        type Model = model::Project;
        const RESOURCE_TYPE: ResourceType = ResourceType::Project;
        type Table = db::schema::project::table;
        const TABLE: Self::Table = db::schema::project::dsl::project;
        type TimeDeletedColumn = db::schema::project::columns::time_deleted;
        const TIME_DELETED_COLUMN: Self::TimeDeletedColumn =
            db::schema::project::dsl::time_deleted;
        type IdColumn = db::schema::project::columns::id;
        const ID_COLUMN: Self::IdColumn = db::schema::project::dsl::id;
        type NameColumn = db::schema::project::columns::name;
        const NAME_COLUMN: Self::NameColumn = db::schema::project::dsl::name;
    }

    impl<'a> ResourceWithParent for Project<'a> {
        type Parent = Organization<'a>;
        type ParentIdColumn = db::schema::project::columns::organization_id;
        const PARENT_ID_COLUMN: Self::ParentIdColumn =
            db::schema::project::dsl::organization_id;

        fn parent_key(
            authz_parent: &<Self::Parent as Resource>::Authz,
        ) -> Uuid {
            authz_parent.id()
        }

        fn authz_path(
            authz_parent: &<Self::Parent as Resource>::Authz,
            db_row: &Self::Model,
            lookup: LookupType,
        ) -> Self::LookupPath {
            (authz_parent.clone(), authz_parent.project(db_row.id(), lookup))
        }
    }

    async fn resource_lookup_by_name_no_authz<T: ResourceWithParent>(
        opctx: &OpContext,
        datastore: &DataStore,
        authz_parent: &<T::Parent as Resource>::Authz,
        name: &Name,
    ) -> Result<T::LookupPath, T::Model>
    where
        // The table itself needs to be filterable by "time_deleted IS NOT NULL"
        T::Table: diesel::Table
            + methods::FilterDsl<IsNull<T::TimeDeletedColumn>>,

        // The result of that needs to be filterable again by "name"
        <T::Table as methods::FilterDsl<
            IsNull<T::TimeDeletedColumn>,
        >>::Output:
            methods::FilterDsl<Eq<T::NameColumn, Name>>,

        // The result of that needs to be filterable again by "parent_id"
        <<T::Table as methods::FilterDsl<
            IsNull<T::TimeDeletedColumn>,
        >>::Output as methods::FilterDsl<Eq<T::NameColumn, Name>>>::Output:
            methods::FilterDsl<Eq<T::ParentIdColumn, Uuid>>,
    {
        // TODO-security XXX-dap copy comment from above.
        let conn = datastore.pool();
        let lookup = LookupType::ByName(name.as_str().to_string());
        T::TABLE
            .filter(T::TIME_DELETED_COLUMN.is_null())
            .filter(T::NAME_COLUMN.eq(name.clone()))
            .filter(T::PARENT_ID_COLUMN.eq(T::parent_key(authz_parent)))
            .select(T::Model::as_select())
            .get_result_async(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByLookup(T::RESOURCE_TYPE, lookup),
                )
            })
            .map(|db_row| T::authz_path(authz_parent, &db_row, lookup))
    }
}

#[cfg(test)]
mod test {
    use super::Instance;
    use super::Key;
    use super::LookupPath;
    use super::Organization;
    use super::Project;
    use crate::context::OpContext;
    use crate::db::model::Name;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use std::sync::Arc;

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
            Instance {
                key: Key::Name(Project {
                    key: Key::Name(Organization {
                        key: Key::Name(_, o)
                    }, p)
                }, i)
            }
            if **o == org_name && **p == project_name && **i == instance_name));

        let org_id = "006f29d9-0ff0-e2d2-a022-87e152440122".parse().unwrap();
        let leaf = LookupPath::new(&opctx, &datastore)
            .organization_id(org_id)
            .project_name(&project_name);
        assert!(matches!(&leaf, Project {
            key: Key::Name(Organization {
                key: Key::Id(LookupPath { .. }, o)
            }, p)
        } if *o == org_id && **p == project_name));

        db.cleanup().await.unwrap();
    }
}

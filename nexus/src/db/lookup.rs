// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Look up API resources from the database

use super::datastore::DataStore;
use super::identity::Resource;
use super::model;
use crate::{
    authz::{self},
    context::OpContext,
    db,
    db::error::{public_error_from_diesel_pool, ErrorHandler},
    db::model::Name,
};
use async_bb8_diesel::AsyncRunQueryDsl;
use db_macros::lookup_resource;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use omicron_common::api::external::{LookupResult, LookupType, ResourceType};
use uuid::Uuid;

enum Key<'a, P> {
    Name(P, &'a Name),
    Id(Root<'a>, Uuid),
}

struct Root<'a> {
    lookup_root: LookupPath<'a>,
}

impl<'a> Root<'a> {
    fn lookup_root(&self) -> &LookupPath<'a> {
        &self.lookup_root
    }
}

#[lookup_resource {
    ancestors = [],
    authz_kind = Typed
}]
struct Organization;

#[lookup_resource {
    ancestors = [ "Organization" ],
    authz_kind = Typed
}]
struct Project;

#[lookup_resource {
    ancestors = [ "Organization", "Project" ],
    authz_kind = Generic
}]
struct Instance;

#[lookup_resource {
    ancestors = [ "Organization", "Project" ],
    authz_kind = Generic
}]
struct Disk;

// TODO XXX-dap remove me -- expanded
// TODO XXX-dap end remove-me -- expanded

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
        Organization { key: Key::Name(Root { lookup_root: self }, name) }
    }

    pub fn organization_id(self, id: Uuid) -> Organization<'a> {
        Organization { key: Key::Id(Root { lookup_root: self }, id) }
    }

    pub fn project_id(self, id: Uuid) -> Project<'a> {
        Project { key: Key::Id(Root { lookup_root: self }, id) }
    }

    pub fn instance_id(self, id: Uuid) -> Instance<'a> {
        Instance { key: Key::Id(Root { lookup_root: self }, id) }
    }

    // pub fn disk_id(self, id: Uuid) -> Disk<'a> {
    //     Disk { key: Key::Id(Root { lookup_root: self }, id) }
    // }
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
    // pub fn disk_name<'b, 'c>(self, name: &'b Name) -> Disk<'c>
    // where
    //     'a: 'c,
    //     'b: 'c,
    // {
    //     Disk { key: Key::Name(self, name) }
    // }

    pub fn instance_name<'b, 'c>(self, name: &'b Name) -> Instance<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Instance { key: Key::Name(self, name) }
    }
}

//macro_rules! define_lookup {
//    ($pc:ident) => {
//        paste::paste! {
//            pub struct $pc<'a> {
//                key: Key<'a, LookupPath<'a>>,
//            }
//
//            impl<'a> GetLookupRoot for $pc<'a> {
//                fn lookup_root(&self) -> &LookupPath<'_> {
//                    self.key.lookup_root()
//                }
//            }
//
//            // Do NOT make these functions public.  They should instead be
//            // wrapped by functions that perform authz checks.
//            async fn [<$pc:lower _lookup_by_id_no_authz>](
//                _opctx: &OpContext,
//                datastore: &DataStore,
//                id: Uuid,
//            ) -> LookupResult<(authz::$pc, model::$pc)> {
//                use db::schema::[<$pc:lower>]::dsl;
//                // TODO-security This could use pool_authorized() instead.
//                // However, it will change the response code for this case:
//                // unauthenticated users will get a 401 rather than a 404
//                // because we'll kick them out sooner than we used to -- they
//                // won't even be able to make this database query.  That's a
//                // good thing but this change can be deferred to a follow-up PR.
//                let conn = datastore.pool();
//                dsl::[<$pc:lower>]
//                    .filter(dsl::time_deleted.is_null())
//                    .filter(dsl::id.eq(id))
//                    .select(model::$pc::as_select())
//                    .get_result_async(conn)
//                    .await
//                    .map_err(|e| {
//                        public_error_from_diesel_pool(
//                            e,
//                            ErrorHandler::NotFoundByLookup(
//                                ResourceType::$pc,
//                                LookupType::ById(id)
//                            )
//                        )
//                    })
//                    .map(|o| {(
//                        authz::FLEET.[<$pc:lower>](o.id(), LookupType::ById(id)),
//                        o
//                        )}
//                    )
//            }
//
//            // Do NOT make these functions public.  They should instead be
//            // wrapped by functions that perform authz checks.
//            async fn [<$pc:lower _lookup_by_name_no_authz>](
//                _opctx: &OpContext,
//                datastore: &DataStore,
//                name: &Name,
//            ) -> LookupResult<(authz::$pc, model::$pc)> {
//                use db::schema::[<$pc:lower>]::dsl;
//                // TODO-security See the note about pool_authorized() above.
//                let conn = datastore.pool();
//                dsl::[<$pc:lower>]
//                    .filter(dsl::time_deleted.is_null())
//                    .filter(dsl::name.eq(name.clone()))
//                    .select(model::$pc::as_select())
//                    .get_result_async(conn)
//                    .await
//                    .map_err(|e| {
//                        public_error_from_diesel_pool(
//                            e,
//                            ErrorHandler::NotFoundByLookup(
//                                ResourceType::$pc,
//                                LookupType::ByName(name.as_str().to_string())
//                            )
//                        )
//                    })
//                    .map(|o| {(
//                        authz::FLEET.[<$pc:lower>](
//                            o.id(),
//                            LookupType::ByName(name.as_str().to_string())
//                        ),
//                        o
//                        )}
//                    )
//            }
//
//            async fn [<$pc:lower _fetch_by_id>](
//                opctx: &OpContext,
//                datastore: &DataStore,
//                id: Uuid,
//            ) -> LookupResult<(authz::$pc, model::$pc)> {
//                let (authz_child, db_child) =
//                    [<$pc:lower _lookup_by_id_no_authz>](
//                        opctx,
//                        datastore,
//                        id,
//                    ).await?;
//                opctx.authorize(authz::Action::Read, &authz_child).await?;
//                Ok((authz_child, db_child))
//            }
//
//            async fn [<$pc:lower _fetch_by_name>](
//                opctx: &OpContext,
//                datastore: &DataStore,
//                name: &Name,
//            ) -> LookupResult<(authz::$pc, model::$pc)> {
//                let (authz_child, db_child) =
//                    [<$pc:lower _lookup_by_name_no_authz>](
//                        opctx,
//                        datastore,
//                        name
//                    ).await?;
//                opctx.authorize(authz::Action::Read, &authz_child).await?;
//                Ok((authz_child, db_child))
//            }
//
//            impl LookupNoauthz for $pc<'_> {
//                type LookupType = (authz::$pc,);
//
//                fn lookup(
//                    &self,
//                ) -> BoxFuture<'_, LookupResult<Self::LookupType>> {
//                    async {
//                        let lookup = self.lookup_root();
//                        let opctx = &lookup.opctx;
//                        let datastore = lookup.datastore;
//                        match self.key {
//                            Key::Name(_, name) => {
//                                let (rv, _) =
//                                    [<$pc:lower _lookup_by_name_no_authz>](
//                                        opctx,
//                                        datastore,
//                                        name
//                                    ).await?;
//                                Ok((rv,))
//                            }
//                            Key::Id(_, id) => {
//                                let (rv, _) =
//                                    [<$pc:lower _lookup_by_id_no_authz>](
//                                        opctx,
//                                        datastore,
//                                        id
//                                    ).await?;
//                                Ok((rv,))
//                            }
//                        }
//                    }.boxed()
//                }
//            }
//
//            impl Fetch for $pc<'_> {
//                type FetchType = (authz::$pc, model::$pc);
//
//                fn fetch(&self) -> BoxFuture<'_, LookupResult<Self::FetchType>> {
//                    async {
//                        let lookup = self.lookup_root();
//                        let opctx = &lookup.opctx;
//                        let datastore = lookup.datastore;
//                        match self.key {
//                            Key::Name(_, name) => {
//                                [<$pc:lower _fetch_by_name>](
//                                    opctx,
//                                    datastore,
//                                    name
//                                ).await
//                            }
//                            Key::Id(_, id) => {
//                                [<$pc:lower _fetch_by_id>](
//                                    opctx,
//                                    datastore,
//                                    id
//                                ).await
//                            }
//                        }
//                    }
//                    .boxed()
//                }
//            }
//
//            impl LookupFor for $pc<'_> {
//                type LookupType = <Self as LookupNoauthz>::LookupType;
//
//                fn lookup_for(
//                    &self,
//                    action: authz::Action,
//                ) -> BoxFuture<'_, LookupResult<Self::LookupType>> {
//                    async move {
//                        let lookup = self.lookup_root();
//                        let opctx = &lookup.opctx;
//                        let (authz_child,) = self.lookup().await?;
//                        opctx.authorize(action, &authz_child).await?;
//                        Ok((authz_child,))
//                    }
//                    .boxed()
//                }
//            }
//        }
//    };
//}
//
//macro_rules! define_lookup_with_parent {
//    (
//        $pc:ident,              // Pascal-case version of resource name
//        $parent_pc:ident,       // Pascal-case version of parent resource name
//        ($($ancestor:ident),*), // List of ancestors above parent
//        // XXX-dap update comment
//        $mkauthz:expr           // Closure to generate resource's authz object
//                                //   from parent's
//    ) => {
//        paste::paste! {
//            pub struct $pc<'a> {
//                key: Key<'a, $parent_pc<'a>>,
//            }
//
//            impl<'a> GetLookupRoot for $pc<'a> {
//                fn lookup_root(&self) -> &LookupPath<'_> {
//                    self.key.lookup_root()
//                }
//            }
//
//            // Do NOT make these functions public.  They should instead be
//            // wrapped by functions that perform authz checks.
//            async fn [<$pc:lower _lookup_by_id_no_authz>](
//                opctx: &OpContext,
//                datastore: &DataStore,
//                id: Uuid,
//            ) -> LookupResult<(
//                    $(authz::[<$ancestor>],)*
//                    authz::$parent_pc,
//                    authz::$pc,
//                    model::$pc
//            )> {
//                use db::schema::[<$pc:lower>]::dsl;
//                // TODO-security See the note about pool_authorized() above.
//                let conn = datastore.pool();
//                let db_row = dsl::[<$pc:lower>]
//                    .filter(dsl::time_deleted.is_null())
//                    .filter(dsl::id.eq(id))
//                    .select(model::$pc::as_select())
//                    .get_result_async(conn)
//                    .await
//                    .map_err(|e| {
//                        public_error_from_diesel_pool(
//                            e,
//                            ErrorHandler::NotFoundByLookup(
//                                ResourceType::$pc,
//                                LookupType::ById(id)
//                            )
//                        )
//                    })?;
//                let ($([<_authz_ $ancestor:lower>],)* authz_parent, _) =
//                    [< $parent_pc:lower _lookup_by_id_no_authz >](
//                        opctx,
//                        datastore,
//                        db_row.[<$parent_pc:lower _id>]
//                    ).await?;
//                let authz_list = ($mkauthz)(
//                    authz_parent, db_row, LookupType::ById(id)
//                );
//                Ok(authz_list)
//            }
//
//            // Do NOT make these functions public.  They should instead be
//            // wrapped by functions that perform authz checks.
//            async fn [<$pc:lower _lookup_by_name_no_authz>](
//                _opctx: &OpContext,
//                datastore: &DataStore,
//                authz_parent: &authz::$parent_pc,
//                name: &Name,
//            ) -> LookupResult<(
//                    $(authz::[<$ancestor>],)*
//                    authz::$parent_pc,
//                    authz::$pc,
//                    model::$pc
//            )> {
//                use db::schema::[<$pc:lower>]::dsl;
//                // TODO-security See the note about pool_authorized() above.
//                let conn = datastore.pool();
//                dsl::[<$pc:lower>]
//                    .filter(dsl::time_deleted.is_null())
//                    .filter(dsl::name.eq(name.clone()))
//                    .filter(dsl::[<$parent_pc:lower _id>].eq(authz_parent.id()))
//                    .select(model::$pc::as_select())
//                    .get_result_async(conn)
//                    .await
//                    .map_err(|e| {
//                        public_error_from_diesel_pool(
//                            e,
//                            ErrorHandler::NotFoundByLookup(
//                                ResourceType::$pc,
//                                LookupType::ByName(name.as_str().to_string())
//                            )
//                        )
//                    })
//                    .map(|dbmodel| {
//                        ($mkauthz)(
//                            authz_parent.clone(),
//                            dbmodel,
//                            LookupType::ByName(name.as_str().to_string())
//                        )
//                    })
//            }
//
//            async fn [<$pc:lower _fetch_by_id>](
//                opctx: &OpContext,
//                datastore: &DataStore,
//                id: Uuid,
//            ) -> LookupResult<(
//                    $(authz::[<$ancestor>],)*
//                    authz::$parent_pc,
//                    authz::$pc,
//                    model::$pc
//            )> {
//                let (
//                    $([<authz_ $ancestor:lower>],)*
//                    authz_parent,
//                    authz_child,
//                    db_child
//                ) =
//                    [<$pc:lower _lookup_by_id_no_authz>](
//                        opctx,
//                        datastore,
//                        id,
//                    ).await?;
//                opctx.authorize(authz::Action::Read, &authz_child).await?;
//                Ok((
//                    $([<authz_ $ancestor:lower>],)*
//                    authz_parent,
//                    authz_child,
//                    db_child
//                ))
//            }
//
//            async fn [<$pc:lower _fetch_by_name>](
//                opctx: &OpContext,
//                datastore: &DataStore,
//                authz_parent: &authz::$parent_pc,
//                name: &Name,
//            ) -> LookupResult<(
//                    $(authz::[<$ancestor>],)*
//                    authz::$parent_pc,
//                    authz::$pc,
//                    model::$pc
//            )> {
//                let (
//                    $([<authz_ $ancestor:lower>],)*
//                    authz_parent,
//                    authz_child,
//                    db_child
//                ) =
//                    [<$pc:lower _lookup_by_name_no_authz>](
//                        opctx,
//                        datastore,
//                        authz_parent,
//                        name
//                    ).await?;
//                opctx.authorize(authz::Action::Read, &authz_child).await?;
//                Ok((
//                    $([<authz_ $ancestor:lower>],)*
//                    authz_parent,
//                    authz_child,
//                    db_child
//                ))
//            }
//
//            impl LookupNoauthz for $pc<'_> {
//                type LookupType = (
//                    $(authz::[<$ancestor>],)*
//                    authz::[<$parent_pc>],
//                    authz::$pc,
//                );
//
//                fn lookup(
//                    &self,
//                ) -> BoxFuture<'_, LookupResult<Self::LookupType>> {
//                    async {
//                        let lookup = self.lookup_root();
//                        let opctx = &lookup.opctx;
//                        let datastore = lookup.datastore;
//                        match &self.key {
//                            Key::Name(parent, name) => {
//                                let (
//                                    $([<authz_ $ancestor:lower>],)*
//                                    authz_parent,
//                                ) = parent.lookup().await?;
//                                let (
//                                    $([<_authz_ $ancestor:lower>],)*
//                                    _authz_parent,
//                                    authz_child, _) =
//                                    [< $pc:lower _lookup_by_name_no_authz >](
//                                        opctx,
//                                        datastore,
//                                        &authz_parent,
//                                        *name
//                                    ).await?;
//                                Ok((
//                                    $([<authz_ $ancestor:lower>],)*
//                                    authz_parent,
//                                    authz_child
//                                ))
//                            }
//                            Key::Id(_, id) => {
//                                let (
//                                    $([<authz_ $ancestor:lower>],)*
//                                    authz_parent,
//                                    authz_child, _) =
//                                    [< $pc:lower _lookup_by_id_no_authz >](
//                                        opctx,
//                                        datastore,
//                                        *id
//                                    ).await?;
//                                Ok((
//                                    $([<authz_ $ancestor:lower>],)*
//                                    authz_parent,
//                                    authz_child
//                                ))
//                            }
//                        }
//                    }
//                    .boxed()
//                }
//            }
//
//            impl Fetch for $pc<'_> {
//                type FetchType = (
//                    $(authz::[<$ancestor>],)*
//                    authz::[<$parent_pc>],
//                    authz::$pc,
//                    model::$pc
//                );
//
//                fn fetch(&self) -> BoxFuture<'_, LookupResult<Self::FetchType>> {
//                    async {
//                        let lookup = self.lookup_root();
//                        let opctx = &lookup.opctx;
//                        let datastore = lookup.datastore;
//                        match &self.key {
//                            Key::Name(parent, name) => {
//                                let (
//                                    $([<_authz_ $ancestor:lower>],)*
//                                    authz_parent,
//                                ) = parent.lookup().await?;
//                                [< $pc:lower _fetch_by_name >](
//                                    opctx,
//                                    datastore,
//                                    &authz_parent,
//                                    *name
//                                ).await
//                            }
//                            Key::Id(_, id) => {
//                                [< $pc:lower _fetch_by_id >](
//                                    opctx,
//                                    datastore,
//                                    *id
//                                ).await
//                            }
//                        }
//                    }
//                    .boxed()
//                }
//            }
//
//            impl LookupFor for $pc<'_> {
//                type LookupType = <Self as LookupNoauthz>::LookupType;
//
//                fn lookup_for(
//                    &self,
//                    action: authz::Action,
//                ) -> BoxFuture<'_, LookupResult<Self::LookupType>> {
//                    async move {
//                        let lookup = self.lookup_root();
//                        let opctx = &lookup.opctx;
//                        let (
//                                $([<authz_ $ancestor:lower>],)*
//                                authz_parent,
//                                authz_child
//                        ) = self.lookup().await?;
//                        opctx.authorize(action, &authz_child).await?;
//                        Ok((
//                            $([<authz_ $ancestor:lower>],)*
//                            authz_parent,
//                            authz_child
//                        ))
//                    }
//                    .boxed()
//                }
//            }
//        }
//    };
//}
//
//define_lookup!(Organization);
//
//define_lookup_with_parent!(
//    Project,
//    Organization,
//    (),
//    |authz_org: authz::Organization,
//     project: model::Project,
//     lookup: LookupType| {
//        (authz_org.clone(), authz_org.project(project.id(), lookup), project)
//    }
//);
//
//define_lookup_with_parent!(
//    Instance,
//    Project,
//    (Organization),
//    |authz_project: authz::Project,
//     instance: model::Instance,
//     lookup: LookupType| {
//        (
//            authz_project.organization().clone(),
//            authz_project.clone(),
//            authz_project.child_generic(
//                ResourceType::Instance,
//                instance.id(),
//                lookup,
//            ),
//            instance,
//        )
//    }
//);
//
//define_lookup_with_parent!(
//    Disk,
//    Project,
//    (Organization),
//    |authz_project: authz::Project, disk: model::Disk, lookup: LookupType| {
//        (
//            authz_project.organization().clone(),
//            authz_project.clone(),
//            authz_project.child_generic(ResourceType::Disk, disk.id(), lookup),
//            disk,
//        )
//    }
//);

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

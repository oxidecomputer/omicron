// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for looking up API resources from the database

// XXX-dap TODO status:
// - I've got the skeleton of an API here that looks pretty promising
// - Next step is probably to implement some of the `Fetch` interfaces.  The
//   challenge I'm facing here is that I need access to the `LookupPath` at the
//   top of the tree in order to use the datastore's methods.
//   - idea: pass the LookupPath down so it's only present at the leaf
//     problem: doesn't that mean every node needs two versions: one as a leaf,
//         and one as a node in the tree?  (maybe every internal node can be the
//         same class?  but probably not because we're going to wind up using
//         their `Fetch` impls recursively).  Maybe instead of two versions,
//         each internal node could be an enum with two variants, one as a leaf
//         (which has the LookupPath) and one as an internal node (which has the
//         rest)?  But then how will its impl of Fetch work -- it will have _no_
//         way to get the datastore out.
//   - idea: use Arc on the LookupPath -- this would probably work but feels
//     cheesy
//   - idea: put a reference to the LookupPath at each node
//     problem: _somebody_ has to own it.  Who will that be?  Root?
//   - idea: have every resource impl a trait that gets its own key out.  Then
//     we can impl `key.lookup()` in terms of the parent key.
//     problem: lots of boilerplate
//   - idea: just use Key instead of structs like Project, etc.  Then we can
//     impl `key.lookup()` more easily?
//     problem: then we can't have different methods (like "instance_name") at
//     each node along the way.
//
// Conclusions:
// - the only thing that can possibly own the LookupPath is the leaf node
//   because that's the only thing the caller actually has.
// - the internal nodes also need to be able to get the LookupPath so that they
//   can impl their own Fetch()
// => The LookupPath should appear at the root, owned (indirectly) by each item
//    in the chain, ending at the leaf.
// => Each item has to traverse the chain above it to get to the LookupPath
//
// NOTE: as I impl the first Fetch, I realize that I want Fetch to do an access
// check.  But I can't do an access check when I'm only doing a fetch as a
// non-leaf node, for the same reason that foo_lookup_noauthz() cannot do the
// access check.  I think this implies there need to be two different traits:
// - Fetch: the public trait that lets you fetch a complete record and the authz
//   objects for the items in the hierarchy
// - Lookup: a trait private to this file that lets callers fetch the record and
//   _does not_ do an access check
// If so, that may simplify things because:
// - I can have the leaf node store the LookupPath directly
// - it can pass the LookupPath (or whatever context is needed) to the lookup()
//   function of the lookup() trait.

use super::datastore::DataStore;
use super::identity::Resource;
use super::model;
use crate::{
    authz,
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

pub trait Fetch {
    type FetchType;
    fn fetch(&self) -> BoxFuture<'_, LookupResult<Self::FetchType>>;
}

trait Lookup {
    type LookupType;
    fn lookup(
        &self,
        lookup: &LookupPath,
    ) -> BoxFuture<'_, LookupResult<Self::LookupType>>;
}

enum Key<'a, P> {
    Name(P, &'a Name),
    Id(LookupPath<'a>, Uuid),
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
}

pub struct Organization<'a> {
    key: Key<'a, LookupPath<'a>>,
}

impl<'a> Organization<'a> {
    fn project_name<'b, 'c>(self, name: &'b Name) -> Project<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Project { key: Key::Name(self, name) }
    }
}

impl Fetch for Organization<'_> {
    type FetchType = (authz::Organization, model::Organization);

    fn fetch(&self) -> BoxFuture<'_, LookupResult<Self::FetchType>> {
        // XXX-dap TODO This is a proof of concept.  Each type in this path is
        // going to need to get to the LookupPath through a sort of convoluted
        // way.  I wanted to implement a recursive function to get to the
        // LookupPath, but then I'd need to create a trait for it that each type
        // would have to impl, etc.  (That might still be the way to go.)
        //
        // But see the note above -- maybe a different approach is better here!
        let lookup = match &self.key {
            Key::Name(lookup, _) => lookup,
            Key::Id(lookup, _) => lookup,
        };

        let opctx = &lookup.opctx;
        let datastore = lookup.datastore;
        async {
            let lookup_type = || match &self.key {
                Key::Name(_, name) => {
                    LookupType::ByName(name.as_str().to_owned())
                }
                Key::Id(_, id) => LookupType::ById(*id),
            };

            use db::schema::organization::dsl;
            let conn = datastore.pool_authorized(opctx).await?;
            // XXX-dap TODO This construction sucks.  What I kind of want is a
            // generic function that takes:
            // - a table (e.g., dsl::organization)
            // - a db model type to return
            // - some information about the LookupType we're trying to do
            // - a closure that can be used to apply filters
            //   (which would be either "name" or "id")
            // and does the whole thing.
            let db_org = match self.key {
                Key::Name(_, name) => dsl::organization
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::name.eq(name.clone()))
                    .select(model::Organization::as_select())
                    .get_result_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::NotFoundByLookup(
                                ResourceType::Organization,
                                lookup_type(),
                            ),
                        )
                    }),
                Key::Id(_, id) => dsl::organization
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::id.eq(id))
                    .select(model::Organization::as_select())
                    .get_result_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::NotFoundByLookup(
                                ResourceType::Organization,
                                lookup_type(),
                            ),
                        )
                    }),
            }?;

            let authz_org =
                authz::FLEET.organization(db_org.id(), lookup_type());
            opctx.authorize(authz::Action::Read, &authz_org).await?;
            Ok((authz_org, db_org))
        }
        .boxed()
    }
}

pub struct Project<'a> {
    key: Key<'a, Organization<'a>>,
}

impl Fetch for Project<'_> {
    type FetchType = (authz::Organization, authz::Project, model::Project);

    fn fetch(&self) -> BoxFuture<'_, LookupResult<Self::FetchType>> {
        todo!()
    }
}

impl<'a> Project<'a> {
    fn instance_name<'b, 'c>(self, name: &'b Name) -> Instance<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Instance { key: Key::Name(self, name) }
    }
}

impl Fetch for Instance<'_> {
    type FetchType =
        (authz::Organization, authz::Project, authz::Instance, model::Instance);

    fn fetch(&self) -> BoxFuture<'_, LookupResult<Self::FetchType>> {
        todo!()
    }
}

pub struct Instance<'a> {
    key: Key<'a, Project<'a>>,
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

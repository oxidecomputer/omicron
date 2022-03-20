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
//   - idea: put a reference to the LookupPath at each node problem: _somebody_
//     has to own it.  Who will that be?  Root?
//   - idea: have every resource impl a trait that gets its own key out.  Then
//     we can impl `key.lookup()` in terms of the parent key.
//     problem: lots of boilerplate
//   - idea: just use Key instead of structs like Project, etc.  Then we can
//     impl `key.lookup()` more easily?
//     problem: then we can't have different methods (like "instance_name") at
//     each node along the way.
//
// Most promising right now looks like putting a reference to the LookupPath in
// every node.

use super::datastore::DataStore;
use super::model;
use crate::{authz, context::OpContext};
use futures::future::BoxFuture;
use omicron_common::api::external::{LookupResult, Name};
use uuid::Uuid;

pub trait Fetch {
    type FetchType;
    fn fetch(&self) -> BoxFuture<'_, LookupResult<Self::FetchType>>;
}

enum Key<'a, P> {
    Name(P, &'a Name),
    Id(Root, Uuid),
}

pub struct LookupPath<'a> {
    opctx: &'a OpContext,
    datastore: &'a DataStore,
}

struct Root<'a> {}

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
        Organization { key: Key::Name(Root { lookup: self }, name) }
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
    key: Key<'a, Root<'a>>,
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
        todo!()
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
    use super::Root;
    use crate::context::OpContext;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::Name;
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
        let org_name: Name = "my-org".parse().unwrap();
        let project_name: Name = "my-project".parse().unwrap();
        let instance_name: Name = "my-instance".parse().unwrap();

        let leaf = LookupPath::new(&opctx, &datastore)
            .organization_name(&org_name)
            .project_name(&project_name)
            .instance_name(&instance_name);
        assert!(matches!(&leaf,
            Instance {
                key: Key::Name(Project {
                    key: Key::Name(Organization {
                        key: Key::Name(Root { .. }, o)
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

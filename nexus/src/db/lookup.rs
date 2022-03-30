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
/// let (authz_org, db_org): (authz::Organization, db::model::Organization) =
///     LookupPath::new(opctx, datastore)
///         .organization_name(&organization_name)
///         .fetch()
///         .await?;
///
/// // Fetch an organization by id
/// let id: Uuid = todo!();
/// let (authz_org, db_org): (authz::Organization, db::model::Organization) =
///     LookupPath::new(opctx, datastore)
///         .organization_id(id)
///         .fetch()
///         .await?;
///
/// // Lookup a Project with the intent of creating an Instance inside it.  For
/// // this purpose, we don't need the database row for the Project, so we use
/// // `lookup_for()`.
/// let project_name = db::model::Name("omicron".parse().unwrap());
/// let (authz_org, authz_project) =
///     LookupPath::new(opctx, datastore)
///         .organization_name(&organization_name)
///         .project_name(&project_name)
///         .lookup_for(authz::Action::CreateChild)
///         .await?;
///
/// // Fetch an Instance by a path of names (Organization name, Project name,
/// // Instance name)
/// let instance_name = db::model::Name("test-server".parse().unwrap());
/// let (authz_org, authz_project, authz_instance, db_instance) =
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
/// let (_, _, authz_disk, db_disk) =
///     LookupPath::new(opctx, datastore)
///         .project_id(authz_project.id())
///         .disk_name(&disk_name)
///         .fetch()
///         .await?;
/// # }
/// ```
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

    /// Select a resource of type Organization, identified by its name
    pub fn organization_name<'b, 'c>(self, name: &'b Name) -> Organization<'c>
    where
        'a: 'c,
        'b: 'c,
    {
        Organization { key: Key::Name(Root { lookup_root: self }, name) }
    }

    /// Select a resource of type Organization, identified by its id
    pub fn organization_id(self, id: Uuid) -> Organization<'a> {
        Organization { key: Key::Id(Root { lookup_root: self }, id) }
    }

    /// Select a resource of type Project, identified by its id
    pub fn project_id(self, id: Uuid) -> Project<'a> {
        Project { key: Key::Id(Root { lookup_root: self }, id) }
    }

    /// Select a resource of type Instance, identified by its id
    pub fn instance_id(self, id: Uuid) -> Instance<'a> {
        Instance { key: Key::Id(Root { lookup_root: self }, id) }
    }

    /// Select a resource of type Disk, identified by its id
    pub fn disk_id(self, id: Uuid) -> Disk<'a> {
        Disk { key: Key::Id(Root { lookup_root: self }, id) }
    }
}

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

lookup_resource! {
    name = "Organization",
    ancestors = [],
    children = [ "Project" ],
    authz_kind = Typed
}

lookup_resource! {
    name = "Project",
    ancestors = [ "Organization" ],
    children = [ "Disk", "Instance" ],
    authz_kind = Typed
}

lookup_resource! {
    name = "Instance",
    ancestors = [ "Organization", "Project" ],
    children = [],
    authz_kind = Generic
}

lookup_resource! {
    name = "Disk",
    ancestors = [ "Organization", "Project" ],
    children = [],
    authz_kind = Generic
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
                key: Key::Id(_, o)
            }, p)
        } if *o == org_id && **p == project_name));

        db.cleanup().await.unwrap();
    }
}

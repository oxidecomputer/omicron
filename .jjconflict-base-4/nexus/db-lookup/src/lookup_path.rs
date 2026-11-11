// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Look up API resources from the database

use nexus_auth::context::OpContext;

use crate::LookupDataStore;

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
    pub(crate) opctx: &'a OpContext,
    pub(crate) datastore: &'a dyn LookupDataStore,
}

impl<'a> LookupPath<'a> {
    /// Begin selecting a resource for lookup
    ///
    /// Authorization checks will be applied to the caller in `opctx`.
    ///
    /// `datastore` is generic to allow a variety of input types to be passed in
    /// (particularly `&T` and `&Arc<T>`). If we just accepted `&'a dyn
    /// LookupDataStore`, callers with a `&Arc<T>` would have to write
    /// `&**datastore`, which is somewhat ugly.
    pub fn new<D>(opctx: &'a OpContext, datastore: D) -> LookupPath<'a>
    where
        D: Into<&'a dyn LookupDataStore>,
    {
        let datastore = datastore.into();
        LookupPath { opctx, datastore }
    }
}

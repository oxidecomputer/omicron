// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;

use diesel::PgConnection;
use diesel_dtrace::DTraceConnection;
use nexus_auth::context::OpContext;
use omicron_common::api::external::Error;

/// The interface between lookups and the Nexus datastore.
#[async_trait::async_trait]
pub trait LookupDataStore: Send + Sync {
    async fn pool_connection_authorized(
        &self,
        opctx: &OpContext,
    ) -> Result<DataStoreConnection, Error>;
}

// Define `From` impls for a couple of types to allow easier coercions from
// `&Arc<T>` and `&T` in `LookupPath::new`.
//
// `AsRef` doesn't work due to lifetime constraints.
impl<'a, D> From<&'a D> for &'a dyn LookupDataStore
where
    D: 'a + LookupDataStore,
{
    #[inline]
    fn from(datastore: &'a D) -> &'a dyn LookupDataStore {
        datastore
    }
}

impl<'a, D> From<&'a Arc<D>> for &'a dyn LookupDataStore
where
    D: 'a + LookupDataStore,
{
    fn from(datastore: &'a Arc<D>) -> &'a dyn LookupDataStore {
        &**datastore
    }
}

// It's a bit funky for these general type aliases to live in something as
// specific as nexus-db-lookup, but there isn't a more obvious place to put them
// that doesn't introduce new, unnecessary dependency edges.
//
// If a more natural location becomes available in the future, consider moving
// these aliases there.
pub type DbConnection = DTraceConnection<PgConnection>;
pub type DataStoreConnection =
    qorb::claim::Handle<async_bb8_diesel::Connection<DbConnection>>;

//! Wrapper around [`diesel::PgConnection`] which adds tracing.

use diesel::connection::{
    AnsiTransactionManager, ConnectionGatWorkaround, SimpleConnection,
};
use diesel::expression::QueryMetadata;
use diesel::pg::{GetPgMetadataCache, Pg, PgMetadataCache};
use diesel::prelude::*;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::PgConnection;

/// A wrapper around [`diesel::PgConnection`] enabling tracing.
///
/// This struct attempts to mimic nearly the entire public API
/// of PgConnection - and should be usable in the same spots -
/// but provides hooks for tracing and logging functions to inspect
/// operations as they are issued to the database.
///
/// NOTE: This function effectively relies on implementation details
/// of Diesel, and as such, is highly unstable. Modifications beyond
/// "changes for instrumentation and observability" are not recommended.
pub(crate) struct TracedPgConnection(PgConnection);

impl SimpleConnection for TracedPgConnection {
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        // TODO: We should trace this. We have a query string here.
        self.0.batch_execute(query)
    }
}

impl<'a> ConnectionGatWorkaround<'a, Pg> for TracedPgConnection {
    type Cursor = <PgConnection as ConnectionGatWorkaround<'a, Pg>>::Cursor;
    type Row = <PgConnection as ConnectionGatWorkaround<'a, Pg>>::Row;
}

impl Connection for TracedPgConnection {
    type Backend = Pg;
    type TransactionManager = AnsiTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<TracedPgConnection> {
        Ok(TracedPgConnection(PgConnection::establish(database_url)?))
    }

    fn execute(&mut self, query: &str) -> QueryResult<usize> {
        // TODO: We should trace this. We have a query string here.
        self.0.execute(query)
    }

    fn load<T>(
        &mut self,
        source: T,
    ) -> QueryResult<<Self as ConnectionGatWorkaround<Pg>>::Cursor>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        // TODO: This is also worth tracing - it appears to issue a call to the
        // underlying DB using the "raw connection" - so it doesn't call
        // 'execute'.
        self.0.load(source)
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Pg> + QueryId,
    {
        self.0.execute_returning_count(source)
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager
    where
        Self: Sized,
    {
        self.0.transaction_state()
    }
}

impl GetPgMetadataCache for TracedPgConnection {
    fn get_metadata_cache(&mut self) -> &mut PgMetadataCache {
        self.0.get_metadata_cache()
    }
}

impl diesel::r2d2::R2D2Connection for TracedPgConnection {
    fn ping(&mut self) -> QueryResult<()> {
        self.0.ping()
    }
}

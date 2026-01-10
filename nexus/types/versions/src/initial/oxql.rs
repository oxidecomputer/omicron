// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OxQL query types for version INITIAL.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// OxQL QUERIES

/// A table represents one or more timeseries with the same schema.
///
/// A table is the result of an OxQL query. It contains a name, usually the name
/// of the timeseries schema from which the data is derived, and any number of
/// timeseries, which contain the actual data.
//
// # Motivation
//
// This struct is derived from [`oxql_types::Table`] but presents timeseries data as a `Vec`
// rather than a map keyed by [`TimeseriesKey`]. This provides a cleaner JSON
// representation for external consumers, as these numeric keys are ephemeral
// identifiers that have no meaning to API consumers. Key ordering is retained
// as this is contructed from the already sorted values present in [`Table`].
//
// When serializing a [`Table`] to JSON, the `BTreeMap<TimeseriesKey, Timeseries>`
// structure produces output with numeric keys like:
// ```json
// {
//   "timeseries": {
//     "2352746367989923131": { ... },
//     "3940108470521992408": { ... }
//   }
// }
// ```
//
// The `Table` view instead serializes timeseries as an array:
// ```json
// {
//   "timeseries": [ { ... }, { ... } ]
// }
// ```
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct OxqlTable {
    /// The name of the table.
    pub name: String,
    /// The set of timeseries in the table, ordered by key.
    pub timeseries: Vec<oxql_types::Timeseries>,
}

impl From<oxql_types::Table> for OxqlTable {
    fn from(table: oxql_types::Table) -> Self {
        OxqlTable {
            name: table.name.clone(),
            timeseries: table.into_iter().collect(),
        }
    }
}

/// Basic metadata about the resource usage of a query.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct OxqlQuerySummary {
    /// The database-assigned query ID.
    pub id: Uuid,
    /// The raw query.
    pub query: String,
    /// The total duration of the query (network plus execution).
    pub elapsed_ms: usize,
    /// Summary of the data read and written.
    pub io_summary: oxql_types::IoSummary,
}

impl From<oxql_types::QuerySummary> for OxqlQuerySummary {
    fn from(query_summary: oxql_types::QuerySummary) -> Self {
        OxqlQuerySummary {
            id: query_summary.id,
            query: query_summary.query,
            elapsed_ms: query_summary.elapsed.as_millis() as usize,
            io_summary: query_summary.io_summary,
        }
    }
}

/// The result of a successful OxQL query.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct OxqlQueryResult {
    /// Tables resulting from the query, each containing timeseries.
    pub tables: Vec<OxqlTable>,
    /// Summaries of queries run against ClickHouse. Note: we omit this field
    /// from the generated docs, since it is not intended for consumption by
    /// customers.
    #[schemars(skip)]
    pub query_summaries: Option<Vec<OxqlQuerySummary>>,
}

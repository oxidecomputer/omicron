// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP API for ereport producers.

use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ResultsPage;
pub use ereport_types::Ena;
pub use ereport_types::Ereport;
pub use ereport_types::EreporterGenerationUuid;
pub use ereport_types::Reporter;
use openapi_manager_types::{
    SupportedVersion, SupportedVersions, api_versions,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::num::NonZeroU32;
use uuid::Uuid;

api_versions!([
    // WHEN CHANGING THE API (part 1 of 2):
    //
    // +- Pick a new semver and define it in the list below.  The list MUST
    // |  remain sorted, which generally means that your version should go at
    // |  the very top.
    // |
    // |  Duplicate this line, uncomment the *second* copy, update that copy for
    // |  your new API version, and leave the first copy commented out as an
    // |  example for the next person.
    // v
    // (next_int, IDENT),
    (1, INITIAL),
]);

// WHEN CHANGING THE API (part 2 of 2):
//
// The call to `api_versions!` above defines constants of type
// `semver::Version` that you can use in your Dropshot API definition to specify
// the version when a particular endpoint was added or removed.  For example, if
// you used:
//
//     (2, ADD_FOOBAR)
//
// Then you could use `VERSION_ADD_FOOBAR` as the version in which endpoints
// were added or removed.

/// API for ereport producers.
#[dropshot::api_description]
pub trait EreportApi {
    type Context;

    /// Collect a tranche of ereports from this reporter.
    #[endpoint {
        method = POST,
        path = "/ereports/{reporter_id}",
    }]
    async fn ereports_collect(
        rqctx: RequestContext<Self::Context>,
        path: Path<ReporterPath>,
        query: Query<EreportQuery>,
    ) -> Result<HttpResponseOk<Ereports>, HttpError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ReporterPath {
    /// The UUID of the reporter from which to collect ereports.
    pub reporter_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct EreportQuery {
    /// The generation (restart nonce) of the reporter at which all other query
    /// parameters are valid.
    ///
    /// If this value does not match the reporter's current generation, the
    /// reporter's response will include the current generation, and will start
    /// at the earliest known ENA, rather than the provided `last_seen` ENA.`
    pub generation: EreporterGenerationUuid,

    /// If present, the reporter should not include ENAs earlier than this one
    /// in its response, provided that the query's requested generation matches
    /// the current generation.
    pub start_at: Option<Ena>,

    /// The ENA of the last ereport committed to persistent storage from the
    /// requested reporter generation.
    ///
    /// If the generation parameter matches the reporter's current generation,
    /// it is permitted to discard any ereports with ENAs up to and including
    /// this value. If the generation has changed from the provided generation,
    /// the reporter will not discard data.
    pub committed: Option<Ena>,

    /// Maximum number of ereports to return in this tranche.
    pub limit: NonZeroU32,
}

/// A tranche of ereports received from a reporter.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Ereports {
    /// The reporter's current generation ID.
    ///
    /// If this is not equal to the current known generation, then the reporter
    /// has restarted.
    pub generation: EreporterGenerationUuid,
    /// The ereports in this tranche, and the ENA of the next page of ereports
    /// (if one exists).)
    #[serde(flatten)]
    pub reports: ResultsPage<Ereport>,
}

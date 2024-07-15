// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Dropshot API for configuring DNS namespace.
//!
//! ## Shape of the API
//!
//! The DNS configuration API has just two endpoints: PUT and GET of the entire
//! DNS configuration.  This is pretty anti-REST.  But it's important to think
//! about how this server fits into the rest of the system.  When changes are
//! made to DNS data, they're grouped together and assigned a monotonically
//! increasing generation number.  The DNS data is first stored into CockroachDB
//! and then propagated from a distributed fleet of Nexus instances to a
//! distributed fleet of these DNS servers.  If we accepted individual updates to
//! DNS names, then propagating a particular change would be non-atomic, and
//! Nexus would have to do a lot more work to ensure (1) that all changes were
//! propagated (even if it crashes) and (2) that they were propagated in the
//! correct order (even if two Nexus instances concurrently propagate separate
//! changes).
//!
//! This DNS server supports hosting multiple zones.  We could imagine supporting
//! separate endpoints to update the DNS data for a particular zone.  That feels
//! nicer (although it's not clear what it would buy us).  But as with updates to
//! multiple names, Nexus's job is potentially much easier if the entire state
//! for all zones is updated at once.  (Otherwise, imagine how Nexus would
//! implement _renaming_ one zone to another without loss of service.  With
//! a combined endpoint and generation number for all zones, all that's necessary
//! is to configure a new zone with all the same names, and then remove the old
//! zone later in another update.  That can be managed by the same mechanism in
//! Nexus that manages regular name updates.  On the other hand, if there were
//! separate endpoints with separate generation numbers, then Nexus has more to
//! keep track of in order to do the rename safely.)
//!
//! See RFD 367 for more on DNS propagation.
//!
//! ## ETags and Conditional Requests
//!
//! It's idiomatic in HTTP use ETags and conditional requests to provide
//! synchronization.  We could define an ETag to be just the current generation
//! number of the server and honor standard `if-match` headers to fail requests
//! where the generation number doesn't match what the client expects.  This
//! would be fine, but it's rather annoying:
//!
//! 1. When the client wants to propagate generation X, the client would have
//!    make an extra request just to fetch the current ETag, just so it can put
//!    it into the conditional request.
//!
//! 2. If some other client changes the configuration in the meantime, the
//!    conditional request would fail and the client would have to take another
//!    lap (fetching the current config and potentially making another
//!    conditional PUT).
//!
//! 3. This approach would make synchronization opt-in.  If a client (or just
//!    one errant code path) neglected to set the if-match header, we could do
//!    the wrong thing and cause the system to come to rest with the wrong DNS
//!    data.
//!
//! Since the semantics here are so simple (we only ever want to move the
//! generation number forward), we don't bother with ETags or conditional
//! requests.  Instead we have the server implement the behavior we want, which
//! is that when a request comes in to update DNS data to generation X, the
//! server replies with one of:
//!
//! (1) the update has been applied and the server is now running generation X
//!     (client treats this as success)
//!
//! (2) the update was not applied because the server is already at generation X
//!     (client treats this as success)
//!
//! (3) the update was not applied because the server is already at a newer
//!     generation
//!     (client probably starts the whole propagation process over because its
//!     current view of the world is out of date)
//!
//! This way, the DNS data can never move backwards and the client only ever has
//! to make one request.
//!
//! ## Concurrent updates
//!
//! Given that we've got just one API to update the all DNS zones, and given
//! that might therefore take a minute for a large zone, and also that there may
//! be multiple Nexus instances trying to do it at the same time, we need to
//! think a bit about what should happen if two Nexus do try to do it at the same
//! time.  Spoiler: we immediately fail any request to update the DNS data if
//! there's already an update in progress.
//!
//! What else could we do?  We could queue the incoming request behind the
//! in-progress one.  How large do we allow that queue to grow?  At some point
//! we'll need to stop queueing them.  So why bother at all?

use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr},
};

use dropshot::{HttpError, HttpResponseOk, RequestContext};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[dropshot::api_description]
pub trait DnsServerApi {
    type Context;

    #[endpoint(
        method = GET,
        path = "/config",
    )]
    async fn dns_config_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<DnsConfig>, HttpError>;

    #[endpoint(
        method = PUT,
        path = "/config",
    )]
    async fn dns_config_put(
        rqctx: RequestContext<Self::Context>,
        rq: dropshot::TypedBody<DnsConfigParams>,
    ) -> Result<dropshot::HttpResponseUpdatedNoContent, dropshot::HttpError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct DnsConfigParams {
    pub generation: u64,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub zones: Vec<DnsConfigZone>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct DnsConfig {
    pub generation: u64,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub time_applied: chrono::DateTime<chrono::Utc>,
    pub zones: Vec<DnsConfigZone>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct DnsConfigZone {
    pub zone_name: String,
    pub records: HashMap<String, Vec<DnsRecord>>,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "type", content = "data")]
pub enum DnsRecord {
    A(Ipv4Addr),
    AAAA(Ipv6Addr),
    SRV(SRV),
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename = "Srv")]
pub struct SRV {
    pub prio: u16,
    pub weight: u16,
    pub port: u16,
    pub target: String,
}

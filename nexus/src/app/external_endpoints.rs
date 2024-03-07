// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of external HTTPS endpoints
//!
//! The guts of this subsystem are in the separate `nexus-external-endpoints`
//! crate.

use crate::ServerContext;
use nexus_external_endpoints::authority_for_request;
use nexus_external_endpoints::endpoint_for_authority;
use nexus_external_endpoints::ExternalEndpoint;
pub use nexus_external_endpoints::NexusCertResolver;
use omicron_common::api::external::Error;
use std::sync::Arc;

impl super::Nexus {
    /// Attempts to determine which external endpoint the given request is
    /// attempting to reach
    ///
    /// This is intended primarily for unauthenticated requests so that we can
    /// determine which Silo's identity providers we should refer a user to so
    /// that they can log in.  For authenticated users, we know which Silo
    /// they're in.  In the future, we may also want this for authenticated
    /// requests to restrict access to a Silo only via one of its endpoints.
    ///
    /// Normally, this works as follows: a client (whether a browser, CLI, or
    /// otherwise) would be given a Silo-specific DNS name to use to reach one
    /// of our external endpoints.  They'd use our own external DNS servers
    /// (mostly likely indirectly) to resolve this to one of Nexus's external
    /// IPs.  Clients then put that DNS name in either the "host" header (in
    /// HTTP 1.1) or the URL's authority section (in HTTP 2 and later).
    ///
    /// In development, we do not assume that DNS has been set up properly.
    /// That means we might receive requests that appear targeted at an IP
    /// address or maybe are missing these fields altogether.  To support that
    /// case, we'll choose an arbitrary Silo.
    pub fn endpoint_for_request(
        &self,
        rqctx: &dropshot::RequestContext<Arc<ServerContext>>,
    ) -> Result<Arc<ExternalEndpoint>, Error> {
        let log = &rqctx.log;
        let rqinfo = &rqctx.request;
        let requested_authority = authority_for_request(rqinfo)
            .map_err(|e| Error::invalid_request(&format!("{:#}", e)))?;
        endpoint_for_authority(
            log,
            &requested_authority,
            &self.background_tasks.external_endpoints,
        )
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![allow(clippy::disallowed_methods)]

mod diff;

use crate::Error as DnsConfigError;
use anyhow::ensure;
pub use diff::DnsDiff;
use std::collections::HashMap;

progenitor::generate_api!(
    spec = "../../openapi/dns-server.json",
    inner_type = slog::Logger,
    derives = [schemars::JsonSchema, Clone, Eq, PartialEq],
    pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
        slog::debug!(log, "client request";
            "method" => %request.method(),
            "uri" => %request.url(),
            "body" => ?&request.body(),
        );
    }),
    post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
        slog::debug!(log, "client response"; "result" => ?result);
    }),
);

pub const ERROR_CODE_UPDATE_IN_PROGRESS: &'static str = "UpdateInProgress";
pub const ERROR_CODE_BAD_UPDATE_GENERATION: &'static str =
    "BadUpdateGeneration";

/// Returns whether an error from this client should be retried
pub fn is_retryable(error: &DnsConfigError<crate::types::Error>) -> bool {
    let response_value = match error {
        DnsConfigError::CommunicationError(_) => return true,
        DnsConfigError::InvalidRequest(_)
        | DnsConfigError::InvalidResponsePayload(_, _)
        | DnsConfigError::UnexpectedResponse(_)
        | DnsConfigError::InvalidUpgrade(_)
        | DnsConfigError::ResponseBodyError(_)
        | DnsConfigError::PreHookError(_) => return false,
        DnsConfigError::ErrorResponse(response_value) => response_value,
    };

    let status_code = response_value.status();
    if status_code == http::StatusCode::SERVICE_UNAVAILABLE {
        // 503s are practically the definition of retryable errors.
        return true;
    };

    if status_code.is_server_error() {
        // Do not retry any other kind of server error.  The most common by
        // far is 500 ("Internal Server Error"), which we interpret to mean
        // that we hit some server bug.  With anything other than a 503, we
        // have no reason to believe a retry will work, and it's better to
        // report that than try forever.
        return false;
    }

    // We should only be able to get here with a 400-level or 500-level
    // status code.  And we just handled all the 500-level ones.
    assert!(status_code.is_client_error());

    // In general, a client error means we did something wrong.  That
    // usually means we sent an invalid request of some kind.  Retrying
    // it won't help.  There are only two known exceptions to this:
    //
    // 1. 429 ("Too Many Requests") means we're being throttled for
    //    overloading the server.  We don't currently use this on the server
    //    side and we don't expect to see it, but we may as well handle it.
    //
    // 2. There's a particular kind of 409 ("ConflictError") that we can get
    //    from the DNS server that means that somebody else is attempting to
    //    do the same thing that we're trying to do.  In this case, we
    //    should backoff and try again later.
    if status_code == http::StatusCode::TOO_MANY_REQUESTS {
        return true;
    }

    if status_code != http::StatusCode::CONFLICT {
        return false;
    }

    // At this point, we've got a "Conflict" error and we need to look at
    // the specific subcode to figure out if it's the case we want to retry.
    if let Some(code) = &response_value.error_code {
        return code == ERROR_CODE_UPDATE_IN_PROGRESS;
    }

    false
}

type DnsRecords = HashMap<String, Vec<types::DnsRecord>>;

impl types::DnsConfigParams {
    /// Given a high-level DNS configuration, return a reference to its sole
    /// DNS zone.
    ///
    /// # Errors
    ///
    /// Returns an error if there are 0 or more than one zones in this
    /// configuration.
    pub fn sole_zone(&self) -> Result<&types::DnsConfigZone, anyhow::Error> {
        ensure!(
            self.zones.len() == 1,
            "expected exactly one DNS zone, but found {}",
            self.zones.len()
        );
        Ok(&self.zones[0])
    }
}

impl Ord for types::DnsRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use types::DnsRecord;
        match (self, other) {
            // Same kinds: compare the items in them
            (DnsRecord::A(addr1), DnsRecord::A(addr2)) => addr1.cmp(addr2),
            (DnsRecord::Aaaa(addr1), DnsRecord::Aaaa(addr2)) => {
                addr1.cmp(addr2)
            }
            (DnsRecord::Srv(srv1), DnsRecord::Srv(srv2)) => srv1
                .target
                .cmp(&srv2.target)
                .then_with(|| srv1.port.cmp(&srv2.port)),

            // Different kinds: define an arbitrary order among the kinds.
            // We could use std::mem::discriminant() here but it'd be nice if
            // this were stable over time.
            // We define (arbitrarily): A < Aaaa < Srv
            (DnsRecord::A(_), DnsRecord::Aaaa(_) | DnsRecord::Srv(_)) => {
                std::cmp::Ordering::Less
            }
            (DnsRecord::Aaaa(_), DnsRecord::Srv(_)) => std::cmp::Ordering::Less,

            // Anything else will result in "Greater".  But let's be explicit.
            (DnsRecord::Aaaa(_), DnsRecord::A(_))
            | (DnsRecord::Srv(_), DnsRecord::A(_))
            | (DnsRecord::Srv(_), DnsRecord::Aaaa(_)) => {
                std::cmp::Ordering::Greater
            }
        }
    }
}

impl PartialOrd for types::DnsRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

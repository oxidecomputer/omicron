// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

progenitor::generate_api!(
    spec = "../../openapi/dns-server/dns-server-latest.json",
    interface = Positional,
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
    replace = {
        DnsConfig = internal_dns_types::config::DnsConfig,
        DnsConfigParams = internal_dns_types::config::DnsConfigParams,
        DnsConfigZone = internal_dns_types::config::DnsConfigZone,
        DnsRecord = internal_dns_types::config::DnsRecord,
        Srv = internal_dns_types::config::Srv,
    }
);

pub type DnsError = crate::Error<crate::types::Error>;

pub const ERROR_CODE_UPDATE_IN_PROGRESS: &'static str = "UpdateInProgress";
pub const ERROR_CODE_BAD_UPDATE_GENERATION: &'static str =
    "BadUpdateGeneration";
pub const ERROR_CODE_INCOMPATIBLE_RECORD: &'static str = "IncompatibleRecord";

/// Returns whether an error from this client should be retried
pub fn is_retryable(error: &DnsError) -> bool {
    let response_value = match error {
        DnsError::CommunicationError(_) => return true,
        DnsError::InvalidRequest(_)
        | DnsError::InvalidResponsePayload(_, _)
        | DnsError::UnexpectedResponse(_)
        | DnsError::InvalidUpgrade(_)
        | DnsError::ResponseBodyError(_)
        | DnsError::PreHookError(_)
        | DnsError::PostHookError(_) => return false,
        DnsError::ErrorResponse(response_value) => response_value,
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

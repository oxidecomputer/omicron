//! Interface for making API requests to a Sled Agent's Bootstrap API.

use progenitor::generate_api;

generate_api!(
    "../openapi/bootstrap-agent.json",
    slog::Logger,
    |log: &slog::Logger, request: &reqwest::Request| {
        debug!(log, "client request";
            "method" => %request.method(),
            "uri" => %request.url(),
            "body" => ?&request.body(),
        );
    },
    |log: &slog::Logger, result: &Result<_, _>| {
        debug!(log, "client response"; "result" => ?result);
    },
);

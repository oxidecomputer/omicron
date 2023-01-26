// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to wicketd

use std::time::Duration;

use types::RackV1Inventory;

progenitor::generate_api!(
    spec = "../openapi/wicketd.json",
    inner_type = slog::Logger,
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
    derives = [schemars::JsonSchema],
    patch =
        {
        SpIdentifier = { derives = [Copy, PartialEq, Eq, PartialOrd, Ord] },
        SpState = { derives = [ PartialEq, Eq, PartialOrd, Ord] },
        SpComponentInfo= { derives = [ PartialEq, Eq, PartialOrd, Ord] },
        SpIgnition= { derives = [ PartialEq, Eq, PartialOrd, Ord] },
        SpIgnitionSystemType= { derives = [ PartialEq, Eq, PartialOrd, Ord] },
        SpInventory = { derives = [ PartialEq, Eq, PartialOrd, Ord] },
        RackV1Inventory = { derives = [ PartialEq, Eq, PartialOrd, Ord] },
    }
);

/// A domain type for the response from the `get_inventory` method.
///
/// This enum has the same shape as `types::GetInventoryResponse`, but uses `std::time::Duration`
/// rather than `types::Duration`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GetInventoryResponse {
    Response {
        inventory: RackV1Inventory,
        received_ago: Duration,
        last_error: Option<GetInventoryError>,
    },
    Unavailable {
        last_error: Option<GetInventoryError>,
    },
}

impl From<types::GetInventoryResponse> for GetInventoryResponse {
    fn from(response: types::GetInventoryResponse) -> Self {
        match response {
            types::GetInventoryResponse::Response {
                inventory,
                received_ago,
                last_error,
            } => {
                let received_ago =
                    Duration::new(received_ago.secs, received_ago.nanos);
                Self::Response {
                    inventory,
                    received_ago,
                    last_error: last_error.map(GetInventoryError::from),
                }
            }
            types::GetInventoryResponse::Unavailable { last_error } => {
                Self::Unavailable {
                    last_error: last_error.map(GetInventoryError::from),
                }
            }
        }
    }
}

/// A domain type for errors returned from the `get_inventory` method.
///
/// This struct has the same shape as `types::GetInventoryError`, but uses `std::time::Duration`
/// rather than `types::Duration`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GetInventoryError {
    error: String,
    received_ago: Duration,
}

impl From<types::GetInventoryError> for GetInventoryError {
    fn from(error: types::GetInventoryError) -> Self {
        Self {
            error: error.error,
            received_ago: Duration::new(
                error.received_ago.secs,
                error.received_ago.nanos,
            ),
        }
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to wicketd

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
        GetInventoryResponse = { derives = [ PartialEq, Eq, PartialOrd, Ord] },
    }
);

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
        SpComponentCaboose = { derives = [PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize] },
        SpIdentifier = { derives = [Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize] },
        SpState = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize] },
        SpComponentInfo= { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize]},
        SpIgnition= { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize]},
        SpIgnitionSystemType= { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize]},
        SpInventory = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize]},
        RackV1Inventory = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize]},
        RotState = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize]},
        RotImageDetails = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize]},
        RotInventory = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize]},
        RotSlot = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize]},
        ImageVersion = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize]},
        StartUpdateOptions = { derives = [ Serialize, Deserialize, Default ]},
    },
    replace = {
        Duration = std::time::Duration,
        EventReportForWicketdEngineSpec = wicket_common::update_events::EventReport,
        StepEventForWicketdEngineSpec = wicket_common::update_events::StepEvent,
        ProgressEventForWicketdEngineSpec = wicket_common::update_events::ProgressEvent,
        StepEventForGenericSpec = update_engine::events::StepEvent<update_engine::NestedSpec>,
        ProgressEventForGenericSpec = update_engine::events::ProgressEvent<update_engine::NestedSpec>,
        StepEventForInstallinatorSpec = installinator_common::StepEvent,
        ProgressEventForInstallinatorSpec = installinator_common::ProgressEvent,
        M2Slot = installinator_common::M2Slot,
    }
);

/// A type alias for errors returned by this crate.
pub type ClientError = crate::Error<crate::types::Error>;

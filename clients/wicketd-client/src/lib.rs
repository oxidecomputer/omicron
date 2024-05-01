// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to wicketd

progenitor::generate_api!(
    spec = "../../openapi/wicketd.json",
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
        Ipv4Range = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        Ipv6Range = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        IpRange = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        Baseboard = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        BootstrapSledDescription = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        RackInitId = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        RackResetId = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        RackOperationStatus = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        RackNetworkConfigV1 = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        UplinkConfig = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        CurrentRssUserConfigInsensitive = { derives = [ PartialEq, Eq, Serialize, Deserialize ] },
        CurrentRssUserConfigSensitive = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        CurrentRssUserConfig = { derives = [ PartialEq, Eq, Serialize, Deserialize ] },
        GetLocationResponse = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
    },
    replace = {
        BgpConfig = omicron_common::api::internal::shared::BgpConfig,
        BgpPeerConfig = omicron_common::api::internal::shared::BgpPeerConfig,
        ClearUpdateStateResponse = wicket_common::rack_update::ClearUpdateStateResponse,
        Duration = std::time::Duration,
        EventReportForWicketdEngineSpec = wicket_common::update_events::EventReport,
        IpNetwork = ipnetwork::IpNetwork,
        Ipv4Network = ipnetwork::Ipv4Network,
        Ipv6Network = ipnetwork::Ipv6Network,
        M2Slot = installinator_common::M2Slot,
        PortConfigV1 = omicron_common::api::internal::shared::PortConfigV1,
        PortFec = omicron_common::api::internal::shared::PortFec,
        PortSpeed = omicron_common::api::internal::shared::PortSpeed,
        ProgressEventForGenericSpec = update_engine::events::ProgressEvent<update_engine::NestedSpec>,
        ProgressEventForInstallinatorSpec = installinator_common::ProgressEvent,
        ProgressEventForWicketdEngineSpec = wicket_common::update_events::ProgressEvent,
        PutRssUserConfigInsensitive = wicket_common::rack_setup::PutRssUserConfigInsensitive,
        RouteConfig = omicron_common::api::internal::shared::RouteConfig,
        SpIdentifier = wicket_common::rack_update::SpIdentifier,
        SpType = wicket_common::rack_update::SpType,
        StepEventForGenericSpec = update_engine::events::StepEvent<update_engine::NestedSpec>,
        StepEventForInstallinatorSpec = installinator_common::StepEvent,
        StepEventForWicketdEngineSpec = wicket_common::update_events::StepEvent,
        SwitchLocation = omicron_common::api::internal::shared::SwitchLocation,
        UserSpecifiedRackNetworkConfig = wicket_common::rack_setup::UserSpecifiedRackNetworkConfig,
    }
);

/// A type alias for errors returned by this crate.
pub type ClientError = crate::Error<crate::types::Error>;

impl types::Baseboard {
    pub fn identifier(&self) -> &str {
        match &self {
            Self::Gimlet { identifier, .. } => &identifier,
            Self::Pc { identifier, .. } => &identifier,
            Self::Unknown => "unknown",
        }
    }

    pub fn model(&self) -> &str {
        match self {
            Self::Gimlet { model, .. } => &model,
            Self::Pc { model, .. } => &model,
            Self::Unknown => "unknown",
        }
    }

    pub fn revision(&self) -> i64 {
        match self {
            Self::Gimlet { revision, .. } => *revision,
            Self::Pc { .. } => 0,
            Self::Unknown => 0,
        }
    }
}

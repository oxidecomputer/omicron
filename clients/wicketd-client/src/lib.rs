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
        Baseboard = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        RackInitId = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        RackResetId = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        RackOperationStatus = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        RackNetworkConfigV1 = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        UplinkConfig = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        CurrentRssUserConfigInsensitive = { derives = [ PartialEq, Serialize, Deserialize ] },
        CurrentRssUserConfigSensitive = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
        CurrentRssUserConfig = { derives = [ PartialEq, Serialize, Deserialize ] },
        GetLocationResponse = { derives = [ PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize ] },
    },
    replace = {
        AllowedSourceIps = omicron_common::api::internal::shared::AllowedSourceIps,
        Baseboard = sled_hardware_types::Baseboard,
        BgpAuthKey = wicket_common::rack_setup::BgpAuthKey,
        BgpAuthKeyId = wicket_common::rack_setup::BgpAuthKeyId,
        BgpAuthKeyInfo = wicket_common::rack_setup::BgpAuthKeyInfo,
        BgpAuthKeyStatus = wicket_common::rack_setup::BgpAuthKeyStatus,
        BgpConfig = omicron_common::api::internal::shared::BgpConfig,
        BgpPeerAuthKind = wicket_common::rack_setup::BgpPeerAuthKind,
        BgpPeerConfig = omicron_common::api::internal::shared::BgpPeerConfig,
        BootstrapSledDescription = wicket_common::rack_setup::BootstrapSledDescription,
        ClearUpdateStateResponse = wicket_common::rack_update::ClearUpdateStateResponse,
        CurrentRssUserConfigInsensitive = wicket_common::rack_setup::CurrentRssUserConfigInsensitive,
        Duration = std::time::Duration,
        EventReportForWicketdEngineSpec = wicket_common::update_events::EventReport,
        GetBgpAuthKeyInfoResponse = wicket_common::rack_setup::GetBgpAuthKeyInfoResponse,
        IpNetwork = ipnetwork::IpNetwork,
        IpRange = omicron_common::address::IpRange,
        Ipv4Network = ipnetwork::Ipv4Network,
        Ipv6Network = ipnetwork::Ipv6Network,
        Ipv4Range = omicron_common::address::Ipv4Range,
        Ipv6Range = omicron_common::address::Ipv6Range,
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
        UserSpecifiedBgpPeerConfig = wicket_common::rack_setup::UserSpecifiedBgpPeerConfig,
        UserSpecifiedImportExportPolicy = wicket_common::rack_setup::UserSpecifiedImportExportPolicy,
        UserSpecifiedPortConfig = wicket_common::rack_setup::UserSpecifiedPortConfig,
        UserSpecifiedRackNetworkConfig = wicket_common::rack_setup::UserSpecifiedRackNetworkConfig,
        ImportExportPolicy = omicron_common::api::internal::shared::ImportExportPolicy,
    }
);

/// A type alias for errors returned by this crate.
pub type ClientError = crate::Error<crate::types::Error>;

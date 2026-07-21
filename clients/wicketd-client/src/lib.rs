// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to wicketd

progenitor::generate_api!(
    spec = "../../openapi/wicketd.json",
    interface = Positional,
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
    patch = {
        CurrentRssUserConfig = { derives = [PartialEq] },
        CurrentRssUserConfigSensitive = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        GetLocationResponse = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        ImageVersion = { derives = [PartialEq, Eq, PartialOrd, Ord]},
        RackInitId = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        RackNetworkConfigV2 = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        RackOperationStatus = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        RackResetId = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        RotImageDetails = { derives = [PartialEq, Eq, PartialOrd, Ord]},
        UplinkConfig = { derives = [PartialEq, Eq, PartialOrd, Ord] },
    },
    crates = {
        "omicron-uuid-kinds" = "*",
    },
    replace = {
        AbortUpdateOptions = wicket_common::rack_update::AbortUpdateOptions,
        AllowedSourceIps = omicron_common::api::internal::shared::AllowedSourceIps,
        ArtifactId = omicron_common::update::ArtifactId,
        Baseboard = sled_hardware_types::Baseboard,
        BgpAuthKey = wicket_common::rack_setup::BgpAuthKey,
        BgpAuthKeyId = wicketd_commission_types_versions::latest::rack_setup::BgpAuthKeyId,
        BgpAuthKeyInfo = wicket_common::rack_setup::BgpAuthKeyInfo,
        BgpAuthKeyStatus = wicket_common::rack_setup::BgpAuthKeyStatus,
        BgpConfig = omicron_common::api::internal::shared::BgpConfig,
        BgpPeerAuthKind = wicket_common::rack_setup::BgpPeerAuthKind,
        BgpPeerConfig = omicron_common::api::internal::shared::BgpPeerConfig,
        BootstrapSledDescription = wicket_common::rack_setup::BootstrapSledDescription,
        CertificateUploadResponse = wicketd_commission_types_versions::latest::rack_setup::CertificateUploadResponse,
        ClearUpdateStateOptions = wicket_common::rack_update::ClearUpdateStateOptions,
        ClearUpdateStateResponse = wicket_common::rack_update::ClearUpdateStateResponse,
        CurrentRssUserConfigInsensitive = wicket_common::rack_setup::CurrentRssUserConfigInsensitive,
        Duration = std::time::Duration,
        EventReportForUplinkPreflightCheckSpec = wicket_common::preflight_check::EventReport,
        EventReportForWicketdEngineSpec = wicket_common::update_events::EventReport,
        GetBgpAuthKeyInfoResponse = wicket_common::rack_setup::GetBgpAuthKeyInfoResponse,
        ImportExportPolicy = omicron_common::api::internal::shared::ImportExportPolicy,
        IpRange = omicron_common::address::IpRange,
        Ipv4Range = omicron_common::address::Ipv4Range,
        Ipv6Range = omicron_common::address::Ipv6Range,
        LinkFec = sled_agent_types::early_networking::LinkFec,
        LinkSpeed = sled_agent_types::early_networking::LinkSpeed,
        M2Slot = installinator_common::M2Slot,
        PortConfigV2 = omicron_common::api::internal::shared::PortConfigV2,
        ProgressEventForGenericSpec = update_engine::events::ProgressEvent<update_engine::NestedSpec>,
        ProgressEventForInstallinatorSpec = installinator_common::ProgressEvent,
        ProgressEventForUplinkPreflightSpec = wicket_common::preflight_check::ProgressEvent,
        ProgressEventForWicketdEngineSpec = wicket_common::update_events::ProgressEvent,
        PutRssUserConfigInsensitive = wicketd_commission_types_versions::latest::rack_setup::PutRssUserConfigInsensitive,
        RackV1Inventory = wicket_common::inventory::RackV1Inventory,
        RotInventory = wicket_common::inventory::RotInventory,
        RotSlot = wicket_common::inventory::RotSlot,
        RotState = wicket_common::inventory::RotState,
        RssStep = bootstrap_agent_lockstep_types::RssStep,
        SpComponentCaboose = wicket_common::inventory::SpComponentCaboose,
        SpComponentInfo = wicket_common::inventory::SpComponentInfo,
        SpIdentifier = wicket_common::inventory::SpIdentifier,
        SpIgnition = wicket_common::inventory::SpIgnition,
        SpIgnitionSystemType = wicket_common::inventory::SpIgnitionSystemType,
        SpInventory = wicket_common::inventory::SpInventory,
        SpState = wicket_common::inventory::SpState,
        SpType = wicket_common::inventory::SpType,
        StartUpdateOptions = wicket_common::rack_update::StartUpdateOptions,
        StepEventForGenericSpec = update_engine::events::StepEvent<update_engine::NestedSpec>,
        StepEventForUplinkPreflightSpec = wicket_common::preflight_check::StepEvent,
        StepEventForInstallinatorSpec = installinator_common::StepEvent,
        StepEventForWicketdEngineSpec = wicket_common::update_events::StepEvent,
        SwitchSlot = sled_agent_types::early_networking::SwitchSlot,
        UpdateSimulatedResult = wicket_common::rack_update::UpdateSimulatedResult,
        UpdateTargets = wicketd_commission_types_versions::latest::update::UpdateTargets,
        UpdateTestError = wicket_common::rack_update::UpdateTestError,
        UplinkPreflightStepId = wicket_common::preflight_check::UplinkPreflightStepId,
        UserSpecifiedBgpPeerConfig = wicketd_commission_types_versions::latest::rack_setup::UserSpecifiedBgpPeerConfig,
        UserSpecifiedImportExportPolicy = wicketd_commission_types_versions::latest::rack_setup::UserSpecifiedImportExportPolicy,
        UserSpecifiedPortConfig = wicketd_commission_types_versions::latest::rack_setup::UserSpecifiedPortConfig,
        UserSpecifiedRackNetworkConfig = wicketd_commission_types_versions::latest::rack_setup::UserSpecifiedRackNetworkConfig,
    },
    convert = {
        {
            type = "string",
            pattern = r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$",
        } = semver::Version,
    }
);

/// A type alias for errors returned by this crate.
pub type ClientError = crate::Error<crate::types::Error>;

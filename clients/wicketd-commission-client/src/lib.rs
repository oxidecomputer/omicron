// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making requests to the wicketd commissioning API.

progenitor::generate_api!(
    spec = "../../openapi/wicketd-commission/wicketd-commission-latest.json",
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
    crates = {
        "iddqd" = "*",
        "omicron-uuid-kinds" = "*",
        "oxnet" = "0.1.0",
    },
    replace = {
        AllowedSourceIps = wicketd_commission_types_versions::latest::rack_setup::AllowedSourceIps,
        BgpAuthKeyId = wicketd_commission_types_versions::latest::rack_setup::BgpAuthKeyId,
        BgpConfig = wicketd_commission_types_versions::latest::rack_setup::BgpConfig,
        BootstrapSled = wicketd_commission_types_versions::latest::inventory::BootstrapSled,
        Caboose = wicketd_commission_types_versions::latest::inventory::Caboose,
        CertificateUploadResponse = wicketd_commission_types_versions::latest::rack_setup::CertificateUploadResponse,
        ClearUpdateStateParams = wicketd_commission_types_versions::latest::update::ClearUpdateStateParams,
        IgnitionFaults = wicketd_commission_types_versions::latest::inventory::IgnitionFaults,
        IpRange = wicketd_commission_types_versions::latest::rack_setup::IpRange,
        Ipv4Range = wicketd_commission_types_versions::latest::rack_setup::Ipv4Range,
        Ipv6Range = wicketd_commission_types_versions::latest::rack_setup::Ipv6Range,
        LinkFec = wicketd_commission_types_versions::latest::rack_setup::LinkFec,
        LinkSpeed = wicketd_commission_types_versions::latest::rack_setup::LinkSpeed,
        LldpAdminStatus = wicketd_commission_types_versions::latest::rack_setup::LldpAdminStatus,
        LldpPortConfig = wicketd_commission_types_versions::latest::rack_setup::LldpPortConfig,
        LocationInfo = wicketd_commission_types_versions::latest::inventory::LocationInfo,
        ManualPortConfig = wicketd_commission_types_versions::latest::rack_setup::ManualPortConfig,
        MaxPathConfig = wicketd_commission_types_versions::latest::rack_setup::MaxPathConfig,
        PowerState = wicketd_commission_types_versions::latest::inventory::PowerState,
        PutRecoveryUserPasswordHash = wicketd_commission_types_versions::latest::rack_setup::PutRecoveryUserPasswordHash,
        PutRssUserConfigInsensitive = wicketd_commission_types_versions::latest::rack_setup::PutRssUserConfigInsensitive,
        RackOperationStatus = wicketd_commission_types_versions::latest::rack_setup::RackOperationStatus,
        RepositoryDescription = wicketd_commission_types_versions::latest::update::RepositoryDescription,
        RotInfo = wicketd_commission_types_versions::latest::inventory::RotInfo,
        RotSlot = wicketd_commission_types_versions::latest::inventory::RotSlot,
        RouteConfig = wicketd_commission_types_versions::latest::rack_setup::RouteConfig,
        RouterLifetimeConfig = wicketd_commission_types_versions::latest::rack_setup::RouterLifetimeConfig,
        RssStepInfo = wicketd_commission_types_versions::latest::rack_setup::RssStepInfo,
        RunningProgress = wicketd_commission_types_versions::latest::update::RunningProgress,
        SlotCaboose = wicketd_commission_types_versions::latest::inventory::SlotCaboose,
        SpIdentifier = wicketd_commission_types_versions::latest::inventory::SpIdentifier,
        SpIgnitionInfo = wicketd_commission_types_versions::latest::inventory::SpIgnitionInfo,
        SpInfo = wicketd_commission_types_versions::latest::inventory::SpInfo,
        SpInventory = wicketd_commission_types_versions::latest::inventory::SpInventory,
        SpInventoryParams = wicketd_commission_types_versions::latest::inventory::SpInventoryParams,
        SpStateInfo = wicketd_commission_types_versions::latest::inventory::SpStateInfo,
        SpType = wicketd_commission_types_versions::latest::inventory::SpType,
        Stage0Caboose = wicketd_commission_types_versions::latest::inventory::Stage0Caboose,
        SpUpdateProgress = wicketd_commission_types_versions::latest::update::SpUpdateProgress,
        StartUpdateOptions = wicketd_commission_types_versions::latest::update::StartUpdateOptions,
        StartUpdateParams = wicketd_commission_types_versions::latest::update::StartUpdateParams,
        StepOutcome = wicketd_commission_types_versions::latest::update::StepOutcome,
        StepProgress = wicketd_commission_types_versions::latest::update::StepProgress,
        SwitchSlot = wicketd_commission_types_versions::latest::inventory::SwitchSlot,
        TxEqConfig = wicketd_commission_types_versions::latest::rack_setup::TxEqConfig,
        UpdateProgress = wicketd_commission_types_versions::latest::update::UpdateProgress,
        UpdateState = wicketd_commission_types_versions::latest::update::UpdateState,
        UpdateStep = wicketd_commission_types_versions::latest::update::UpdateStep,
        UpdateTargets = wicketd_commission_types_versions::latest::update::UpdateTargets,
        UpdateStepStatus = wicketd_commission_types_versions::latest::update::UpdateStepStatus,
        UserSpecifiedBgpPeerConfig = wicketd_commission_types_versions::latest::rack_setup::UserSpecifiedBgpPeerConfig,
        UserSpecifiedImportExportPolicy = wicketd_commission_types_versions::latest::rack_setup::UserSpecifiedImportExportPolicy,
        UserSpecifiedPortConfig = wicketd_commission_types_versions::latest::rack_setup::UserSpecifiedPortConfig,
        UserSpecifiedRackNetworkConfig = wicketd_commission_types_versions::latest::rack_setup::UserSpecifiedRackNetworkConfig,
        UserSpecifiedRouterPeerAddr = wicketd_commission_types_versions::latest::rack_setup::UserSpecifiedRouterPeerAddr,
        UserSpecifiedUplinkAddressConfig = wicketd_commission_types_versions::latest::rack_setup::UserSpecifiedUplinkAddressConfig,
    },
);

pub type ClientError = crate::Error<crate::types::Error>;

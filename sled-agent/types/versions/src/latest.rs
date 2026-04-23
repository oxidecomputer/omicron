// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all types.

pub mod artifact {
    pub use crate::v1::artifact::ArtifactConfig;
    pub use crate::v1::artifact::ArtifactCopyFromDepotBody;
    pub use crate::v1::artifact::ArtifactCopyFromDepotResponse;
    pub use crate::v1::artifact::ArtifactListResponse;
    pub use crate::v1::artifact::ArtifactPathParam;
    pub use crate::v1::artifact::ArtifactPutResponse;
    pub use crate::v1::artifact::ArtifactQueryParam;
}

pub mod attached_subnet {
    pub use crate::v18::attached_subnet::AttachedSubnet;
    pub use crate::v18::attached_subnet::AttachedSubnetKind;
    pub use crate::v18::attached_subnet::AttachedSubnets;
    pub use crate::v18::attached_subnet::VmmSubnetPathParam;
}

pub mod bootstore {
    pub use crate::v1::bootstore::BootstoreStatus;
    pub use crate::v1::bootstore::EstablishedConnection;
}

pub mod dataset {
    pub use crate::v9::dataset::LocalStoragePathParam;
    pub use crate::v17::dataset::LocalStorageDatasetDeleteRequest;
    pub use crate::v17::dataset::LocalStorageDatasetEnsureRequest;
}

pub mod debug {
    pub use crate::v1::debug::ChickenSwitchDestroyOrphanedDatasets;

    pub use crate::v3::debug::OperatorSwitchZonePolicy;
}

pub mod diagnostics {
    pub use crate::v1::diagnostics::SledDiagnosticsLogsDownloadPathParam;
    pub use crate::v1::diagnostics::SledDiagnosticsLogsDownloadPathParm;
    pub use crate::v1::diagnostics::SledDiagnosticsLogsDownloadQueryParam;

    pub use crate::v36::diagnostics::SledDiagnosticsDebugDropboxDownloadPathParam;
}

pub mod disk {
    pub use crate::v1::disk::DiskEnsureBody;
    pub use crate::v1::disk::DiskPathParam;
    pub use crate::v1::disk::DiskStateRequested;
    pub use crate::v1::disk::DiskType;
    pub use crate::v1::disk::Zpool;
}

// TODO-cleanup These types should move to `system_networking` as a part of
// <https://github.com/oxidecomputer/omicron/issues/10167>.
pub mod early_networking {
    pub use crate::v1::early_networking::BfdMode;
    pub use crate::v1::early_networking::BfdPeerConfig;
    pub use crate::v1::early_networking::ImportExportPolicy;
    pub use crate::v1::early_networking::LldpAdminStatus;
    pub use crate::v1::early_networking::LldpPortConfig;
    pub use crate::v1::early_networking::PortFec;
    pub use crate::v1::early_networking::PortSpeed;
    pub use crate::v1::early_networking::RouteConfig;
    pub use crate::v1::early_networking::SwitchSlot;
    pub use crate::v1::early_networking::TxEqConfig;

    pub use crate::v20::early_networking::BgpConfig;
    pub use crate::v20::early_networking::MaxPathConfig;
    pub use crate::v20::early_networking::MaxPathConfigError;
    pub use crate::v20::early_networking::RouterLifetimeConfig;
    pub use crate::v20::early_networking::RouterLifetimeConfigError;

    pub use crate::v30::early_networking::BgpPeerConfig;
    pub use crate::v30::early_networking::InvalidIpAddrError;
    pub use crate::v30::early_networking::PortConfig;
    pub use crate::v30::early_networking::RackNetworkConfig;
    pub use crate::v30::early_networking::RouterPeerIpAddr;
    pub use crate::v30::early_networking::RouterPeerIpAddrError;
    pub use crate::v30::early_networking::RouterPeerType;
    pub use crate::v30::early_networking::UplinkAddress;
    pub use crate::v30::early_networking::UplinkAddressConfig;
    pub use crate::v30::early_networking::UplinkIpNet;
    pub use crate::v30::early_networking::UplinkIpNetError;
}

pub mod firewall_rules {
    pub use crate::v31::firewall_rules::VpcFirewallRulesEnsureBody;
}

pub mod instance {
    pub use crate::v1::instance::InstanceExternalIpBody;
    pub use crate::v1::instance::InstanceMetadata;
    pub use crate::v1::instance::InstanceMigrationTargetParams;
    pub use crate::v1::instance::VmmIssueDiskSnapshotRequestBody;
    pub use crate::v1::instance::VmmIssueDiskSnapshotRequestPathParam;
    pub use crate::v1::instance::VmmIssueDiskSnapshotRequestResponse;
    pub use crate::v1::instance::VmmPathParam;
    pub use crate::v1::instance::VmmPutStateBody;
    pub use crate::v1::instance::VmmPutStateResponse;
    pub use crate::v1::instance::VmmStateRequested;
    pub use crate::v1::instance::VmmUnregisterResponse;
    pub use crate::v1::instance::VpcPathParam;

    pub use crate::v7::instance::InstanceMulticastBody;
    pub use crate::v7::instance::InstanceMulticastMembership;

    pub use crate::v29::instance::VmmSpec;

    pub use crate::v31::instance::ResolvedVpcFirewallRule;
    pub use crate::v32::instance::ExternalIpConfig;
    pub use crate::v32::instance::ExternalIps;
    pub use crate::v32::instance::ExternalIpv4Config;
    pub use crate::v32::instance::ExternalIpv6Config;
    pub use crate::v32::instance::InstanceEnsureBody;
    pub use crate::v32::instance::InstanceSledLocalConfig;
}

pub mod inventory {
    pub use crate::v1::inventory::Baseboard;
    pub use crate::v1::inventory::BootImageHeader;
    pub use crate::v1::inventory::BootPartitionContents;
    pub use crate::v1::inventory::BootPartitionDetails;
    pub use crate::v1::inventory::ConfigReconcilerInventoryResult;
    pub use crate::v1::inventory::HostPhase2DesiredContents;
    pub use crate::v1::inventory::HostPhase2DesiredSlots;
    pub use crate::v1::inventory::InventoryDataset;
    pub use crate::v1::inventory::InventoryDisk;
    pub use crate::v1::inventory::ManifestBootInventory;
    pub use crate::v1::inventory::ManifestInventory;
    pub use crate::v1::inventory::ManifestNonBootInventory;
    pub use crate::v1::inventory::MupdateOverrideBootInventory;
    pub use crate::v1::inventory::MupdateOverrideInventory;
    pub use crate::v1::inventory::MupdateOverrideNonBootInventory;
    pub use crate::v1::inventory::NetworkInterfaceKind;
    pub use crate::v1::inventory::OmicronZoneDataset;
    pub use crate::v1::inventory::OmicronZoneImageSource;
    pub use crate::v1::inventory::OrphanedDataset;
    pub use crate::v1::inventory::RemoveMupdateOverrideBootSuccessInventory;
    pub use crate::v1::inventory::RemoveMupdateOverrideInventory;
    pub use crate::v1::inventory::SledCpuFamily;
    pub use crate::v1::inventory::SledRole;
    pub use crate::v1::inventory::ZoneArtifactInventory;
    pub use crate::v1::inventory::ZoneKind;

    pub use crate::v10::inventory::NetworkInterface;

    pub use crate::v11::inventory::OmicronZoneConfig;
    pub use crate::v11::inventory::OmicronZoneType;
    pub use crate::v11::inventory::OmicronZonesConfig;
    pub use crate::v11::inventory::SourceNatConfig;
    pub use crate::v11::inventory::SourceNatConfigGeneric;
    pub use crate::v11::inventory::SourceNatConfigV4;
    pub use crate::v11::inventory::SourceNatConfigV6;

    pub use crate::v12::inventory::HealthMonitorInventory;

    pub use crate::v14::inventory::ConfigReconcilerInventoryStatus;
    pub use crate::v14::inventory::OmicronFileSourceResolverInventory;
    pub use crate::v14::inventory::OmicronSingleMeasurement;
    pub use crate::v14::inventory::OmicronSledConfig;
    pub use crate::v14::inventory::ReconciledSingleMeasurement;

    pub use crate::v16::inventory::ConfigReconcilerInventory;
    pub use crate::v16::inventory::SingleMeasurementInventory;

    pub use crate::v24::inventory::InventoryZpool;
    pub use crate::v24::inventory::ZpoolHealth;

    pub use crate::v34::inventory::Inventory;
    pub use crate::v34::inventory::Svc;
    pub use crate::v34::inventory::SvcEnabledNotOnline;
    pub use crate::v34::inventory::SvcEnabledNotOnlineState;
    pub use crate::v34::inventory::SvcState;
    pub use crate::v34::inventory::SvcsEnabledNotOnline;
    pub use crate::v34::inventory::SvcsEnabledNotOnlineResult;
    pub use crate::v34::inventory::SvcsError;

    pub use crate::impls::inventory::ManifestBootInventoryDisplay;
    pub use crate::impls::inventory::ManifestInventoryDisplay;
    pub use crate::impls::inventory::ManifestNonBootInventoryDisplay;
    pub use crate::impls::inventory::MupdateOverrideBootInventoryDisplay;
    pub use crate::impls::inventory::MupdateOverrideInventoryDisplay;
    pub use crate::impls::inventory::MupdateOverrideNonBootInventoryDisplay;
    pub use crate::impls::inventory::OmicronFileSourceResolverInventoryDisplay;
    pub use crate::impls::inventory::SourceNatConfigError;
    pub use crate::impls::inventory::ZoneArtifactInventoryDisplay;
    pub use crate::impls::inventory::ZpoolHealthParseError;
}

pub mod probes {
    pub use crate::v10::probes::ExternalIp;
    pub use crate::v10::probes::IpKind;
    pub use crate::v10::probes::ProbeCreate;
    pub use crate::v10::probes::ProbeSet;
}

pub mod rot {
    pub use crate::v19::attestation::Attestation;
    pub use crate::v19::attestation::CertificateChain;
    pub use crate::v19::attestation::Ed25519Signature;
    pub use crate::v19::attestation::Measurement;
    pub use crate::v19::attestation::MeasurementLog;
    pub use crate::v19::attestation::Nonce;
    pub use crate::v19::attestation::Rot;
    pub use crate::v19::attestation::RotPathParams;
    pub use crate::v19::attestation::Sha3_256Digest;
}

pub mod sled {
    pub use crate::v1::sled::AddSledRequest;
    pub use crate::v1::sled::StartSledAgentRequest;
    pub use crate::v1::sled::StartSledAgentRequestBody;
}

pub mod support_bundle {
    pub use crate::v1::support_bundle::RangeRequestHeaders;
    pub use crate::v1::support_bundle::SupportBundleFilePathParam;
    pub use crate::v1::support_bundle::SupportBundleFinalizeQueryParams;
    pub use crate::v1::support_bundle::SupportBundleListPathParam;
    pub use crate::v1::support_bundle::SupportBundleMetadata;
    pub use crate::v1::support_bundle::SupportBundlePathParam;
    pub use crate::v1::support_bundle::SupportBundleState;
    pub use crate::v1::support_bundle::SupportBundleTransferQueryParams;
}

pub mod system_networking {
    pub use crate::v33::system_networking::ServiceZoneNatEntries;
    pub use crate::v33::system_networking::ServiceZoneNatEntriesError;
    pub use crate::v33::system_networking::ServiceZoneNatEntry;
    pub use crate::v33::system_networking::ServiceZoneNatKind;
    pub use crate::v33::system_networking::SystemNetworkingConfig;
    pub use crate::v33::system_networking::WriteNetworkConfigRequest;
}

pub mod trust_quorum {
    // HTTP request types specific to the sled-agent API: the rest of the types
    // used in the API are inherent to the Trust Quorum protocol and are defined
    // in the crate trust-quorum-types:
    pub use crate::v13::trust_quorum::ProxyCommitRequest;
    pub use crate::v13::trust_quorum::ProxyPrepareAndCommitRequest;

    pub use crate::v15::trust_quorum::TrustQuorumNetworkConfig;
}

pub mod uplink {
    pub use crate::v30::uplink::HostPortConfig;
    pub use crate::v30::uplink::SwitchPorts;
}

pub mod zone_bundle {
    pub use crate::v1::zone_bundle::BundleUtilization;
    pub use crate::v1::zone_bundle::CleanupContext;
    pub use crate::v1::zone_bundle::CleanupContextUpdate;
    pub use crate::v1::zone_bundle::CleanupCount;
    pub use crate::v1::zone_bundle::CleanupPeriod;
    pub use crate::v1::zone_bundle::CleanupPeriodCreateError;
    pub use crate::v1::zone_bundle::PriorityDimension;
    pub use crate::v1::zone_bundle::PriorityOrder;
    pub use crate::v1::zone_bundle::PriorityOrderCreateError;
    pub use crate::v1::zone_bundle::StorageLimit;
    pub use crate::v1::zone_bundle::StorageLimitCreateError;
    pub use crate::v1::zone_bundle::ZoneBundleCause;
    pub use crate::v1::zone_bundle::ZoneBundleFilter;
    pub use crate::v1::zone_bundle::ZoneBundleId;
    pub use crate::v1::zone_bundle::ZoneBundleMetadata;
    pub use crate::v1::zone_bundle::ZonePathParam;
}

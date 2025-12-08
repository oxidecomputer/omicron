// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all types.
//!
//! This module provides floating identifiers that always point to the latest
//! version of each type. Business logic should generally use these re-exports
//! rather than version-specific imports.
//!
//! The sled-agent-api crate should not use this module; it should instead use
//! fixed identifiers from the version-specific modules directly.

pub mod artifact {
    pub use crate::v1::artifact::ArtifactConfig;
}

pub mod bootstore {
    pub use crate::v1::bootstore::BootstoreStatus;
    pub use crate::v1::bootstore::EstablishedConnection;
}

pub mod disk {
    pub use crate::v1::disk::DiskEnsureBody;
    pub use crate::v1::disk::DiskStateRequested;
}

pub mod early_networking {
    pub use crate::v1::early_networking::EarlyNetworkConfig;
    pub use crate::v1::early_networking::EarlyNetworkConfigBody;
}

pub mod instance {
    pub use crate::v1::instance::InstanceExternalIpBody;
    pub use crate::v1::instance::InstanceMetadata;
    pub use crate::v1::instance::InstanceMigrationTargetParams;
    pub use crate::v1::instance::VmmPutStateBody;
    pub use crate::v1::instance::VmmPutStateResponse;
    pub use crate::v1::instance::VmmSpec;
    pub use crate::v1::instance::VmmSpecExt;
    pub use crate::v1::instance::VmmStateRequested;
    pub use crate::v1::instance::VmmUnregisterResponse;

    pub use crate::v7::instance::InstanceMulticastBody;
    pub use crate::v7::instance::InstanceMulticastMembership;

    pub use crate::v10::instance::InstanceEnsureBody;
    pub use crate::v10::instance::InstanceSledLocalConfig;
    pub use crate::v10::instance::VpcFirewallRulesEnsureBody;

    pub use omicron_common::api::internal::shared::ResolvedVpcFirewallRule;
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
    pub use crate::v1::inventory::InventoryZpool;
    pub use crate::v1::inventory::MupdateOverrideBootInventory;
    pub use crate::v1::inventory::MupdateOverrideInventory;
    pub use crate::v1::inventory::MupdateOverrideNonBootInventory;
    pub use crate::v1::inventory::OmicronZoneDataset;
    pub use crate::v1::inventory::OmicronZoneImageSource;
    pub use crate::v1::inventory::OrphanedDataset;
    pub use crate::v1::inventory::RemoveMupdateOverrideBootSuccessInventory;
    pub use crate::v1::inventory::RemoveMupdateOverrideInventory;
    pub use crate::v1::inventory::SledCpuFamily;
    pub use crate::v1::inventory::SledRole;
    pub use crate::v1::inventory::ZoneArtifactInventory;
    pub use crate::v1::inventory::ZoneImageResolverInventory;
    pub use crate::v1::inventory::ZoneKind;
    pub use crate::v1::inventory::ZoneManifestBootInventory;
    pub use crate::v1::inventory::ZoneManifestInventory;
    pub use crate::v1::inventory::ZoneManifestNonBootInventory;

    pub use crate::v10::inventory::ConfigReconcilerInventory;
    pub use crate::v10::inventory::ConfigReconcilerInventoryStatus;
    pub use crate::v10::inventory::Inventory;
    pub use crate::v10::inventory::OmicronSledConfig;
    pub use crate::v10::inventory::OmicronZoneConfig;
    pub use crate::v10::inventory::OmicronZoneType;
    pub use crate::v10::inventory::OmicronZonesConfig;
}

pub mod probes {
    pub use crate::v10::probes::ExternalIp;
    pub use crate::v10::probes::IpKind;
    pub use crate::v10::probes::ProbeCreate;
    pub use crate::v10::probes::ProbeSet;
}

pub mod rack_init {
    pub use crate::bootstrap_v1::rack_init::RecoverySiloConfig;
}

pub mod sled {
    pub use crate::v1::sled::AddSledRequest;
    pub use crate::v1::sled::BaseboardId;
    pub use crate::v1::sled::StartSledAgentRequest;
    pub use crate::v1::sled::StartSledAgentRequestBody;
    pub use crate::v1::sled::UnknownBaseboardError;
}

pub mod support_bundle {
    pub use crate::v1::support_bundle::SupportBundleMetadata;
    pub use crate::v1::support_bundle::SupportBundleState;
}

pub mod zone_bundle {
    pub use crate::v1::zone_bundle::BundleUtilization;
    pub use crate::v1::zone_bundle::CleanupContext;
    pub use crate::v1::zone_bundle::CleanupCount;
    pub use crate::v1::zone_bundle::CleanupPeriod;
    pub use crate::v1::zone_bundle::CleanupPeriodCreateError;
    pub use crate::v1::zone_bundle::PriorityDimension;
    pub use crate::v1::zone_bundle::PriorityOrder;
    pub use crate::v1::zone_bundle::PriorityOrderCreateError;
    pub use crate::v1::zone_bundle::StorageLimit;
    pub use crate::v1::zone_bundle::StorageLimitCreateError;
    pub use crate::v1::zone_bundle::ZoneBundleCause;
    pub use crate::v1::zone_bundle::ZoneBundleId;
    pub use crate::v1::zone_bundle::ZoneBundleMetadata;
}

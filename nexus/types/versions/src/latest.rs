// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all published types.
//!
//! This module provides floating identifiers that always point to the
//! current version of each type. Business logic should use these re-exports
//! rather than versioned identifiers.

pub mod affinity {
    pub use crate::v2025_11_20_00::affinity::AffinityGroup;
    pub use crate::v2025_11_20_00::affinity::AffinityGroupCreate;
    pub use crate::v2025_11_20_00::affinity::AffinityGroupSelector;
    pub use crate::v2025_11_20_00::affinity::AffinityGroupUpdate;
    pub use crate::v2025_11_20_00::affinity::AffinityInstanceGroupMemberPath;
    pub use crate::v2025_11_20_00::affinity::AntiAffinityGroup;
    pub use crate::v2025_11_20_00::affinity::AntiAffinityGroupCreate;
    pub use crate::v2025_11_20_00::affinity::AntiAffinityGroupSelector;
    pub use crate::v2025_11_20_00::affinity::AntiAffinityGroupUpdate;
    pub use crate::v2025_11_20_00::affinity::AntiAffinityInstanceGroupMemberPath;
}

pub mod alert {
    pub use crate::v2025_11_20_00::alert::AlertClass;
    pub use crate::v2025_11_20_00::alert::AlertClassFilter;
    pub use crate::v2025_11_20_00::alert::AlertClassPage;
    pub use crate::v2025_11_20_00::alert::AlertDelivery;
    pub use crate::v2025_11_20_00::alert::AlertDeliveryAttempts;
    pub use crate::v2025_11_20_00::alert::AlertDeliveryId;
    pub use crate::v2025_11_20_00::alert::AlertDeliveryState;
    pub use crate::v2025_11_20_00::alert::AlertDeliveryStateFilter;
    pub use crate::v2025_11_20_00::alert::AlertDeliveryTrigger;
    pub use crate::v2025_11_20_00::alert::AlertProbeResult;
    pub use crate::v2025_11_20_00::alert::AlertReceiver;
    pub use crate::v2025_11_20_00::alert::AlertReceiverKind;
    pub use crate::v2025_11_20_00::alert::AlertReceiverProbe;
    pub use crate::v2025_11_20_00::alert::AlertReceiverSelector;
    pub use crate::v2025_11_20_00::alert::AlertSelector;
    pub use crate::v2025_11_20_00::alert::AlertSubscription;
    pub use crate::v2025_11_20_00::alert::AlertSubscriptionCreate;
    pub use crate::v2025_11_20_00::alert::AlertSubscriptionCreated;
    pub use crate::v2025_11_20_00::alert::AlertSubscriptionSelector;
    pub use crate::v2025_11_20_00::alert::WebhookCreate;
    pub use crate::v2025_11_20_00::alert::WebhookDeliveryAttempt;
    pub use crate::v2025_11_20_00::alert::WebhookDeliveryAttemptResult;
    pub use crate::v2025_11_20_00::alert::WebhookDeliveryResponse;
    pub use crate::v2025_11_20_00::alert::WebhookReceiver;
    pub use crate::v2025_11_20_00::alert::WebhookReceiverConfig;
    pub use crate::v2025_11_20_00::alert::WebhookReceiverUpdate;
    pub use crate::v2025_11_20_00::alert::WebhookSecret;
    pub use crate::v2025_11_20_00::alert::WebhookSecretCreate;
    pub use crate::v2025_11_20_00::alert::WebhookSecretSelector;
    pub use crate::v2025_11_20_00::alert::WebhookSecrets;
}

pub mod audit {
    pub use crate::v2025_11_20_00::audit::AuditLogEntryActor;
    pub use crate::v2025_11_20_00::audit::AuditLogEntryResult;
    pub use crate::v2025_11_20_00::audit::AuditLogParams;

    pub use crate::v2026_01_15_01::audit::AuditLogEntry;
    pub use crate::v2026_01_15_01::audit::AuthMethod;
}

pub mod bfd {
    pub use crate::v2025_11_20_00::bfd::BfdState;
    pub use crate::v2025_11_20_00::bfd::BfdStatus;
}

pub mod device {
    pub use crate::v2025_11_20_00::device::ConsoleSession;
    pub use crate::v2025_11_20_00::device::DeviceAccessToken;
    pub use crate::v2025_11_20_00::device::DeviceAccessTokenGrant;
    pub use crate::v2025_11_20_00::device::DeviceAccessTokenType;
    pub use crate::v2025_11_20_00::device::DeviceAuthResponse;
}

pub mod certificate {
    pub use crate::v2025_11_20_00::certificate::Certificate;
    pub use crate::v2025_11_20_00::certificate::CertificateCreate;
    pub use crate::v2025_11_20_00::certificate::ServiceUsingCertificate;
}

pub mod console {
    pub use crate::v2025_11_20_00::console::LoginPath;
    pub use crate::v2025_11_20_00::console::LoginToProviderPathParam;
    pub use crate::v2025_11_20_00::console::LoginUrlQuery;
    pub use crate::v2025_11_20_00::console::RestPathParam;
}

pub mod device_params {
    pub use crate::v2025_11_20_00::device_params::DeviceAccessTokenRequest;
    pub use crate::v2025_11_20_00::device_params::DeviceAuthRequest;
    pub use crate::v2025_11_20_00::device_params::DeviceAuthVerify;
}

pub mod disk {
    // View types from omicron-common (latest version).
    pub use omicron_common::api::external::{Disk, DiskType};

    // Request types unchanged since INITIAL.
    pub use crate::v2025_11_20_00::disk::{
        BlockSize, DiskMetricName, DiskMetricsPath, DiskSelector,
        ExpectedDigest, FinalizeDisk, ImportBlocksBulkWrite,
    };

    // Request types from READ_ONLY_DISKS_NULLABLE.
    pub use crate::v2026_01_31_00::disk::{
        DiskBackend, DiskCreate, DiskSource,
    };
}

pub mod external_ip {
    pub use crate::v2025_11_20_00::external_ip::ExternalIp;
    pub use crate::v2025_11_20_00::external_ip::IpKind;
    pub use crate::v2025_11_20_00::external_ip::SNatIp;
}

pub mod floating_ip {
    pub use crate::v2025_11_20_00::floating_ip::FloatingIp;
    pub use crate::v2025_11_20_00::floating_ip::FloatingIpAttach;
    pub use crate::v2025_11_20_00::floating_ip::FloatingIpParentKind;
    pub use crate::v2025_11_20_00::floating_ip::FloatingIpSelector;
    pub use crate::v2025_11_20_00::floating_ip::FloatingIpUpdate;

    pub use crate::v2026_01_22_00::floating_ip::AddressAllocator;
    pub use crate::v2026_01_22_00::floating_ip::FloatingIpCreate;
}

pub mod hardware {
    pub use crate::v2025_11_20_00::hardware::Baseboard;
    pub use crate::v2025_11_20_00::hardware::UninitializedSled;
    pub use crate::v2025_11_20_00::hardware::UninitializedSledId;
    pub use crate::v2025_11_20_00::hardware::UpdateableComponentType;
}

pub mod headers {
    pub use crate::v2025_11_20_00::headers::RangeRequest;
}

pub mod image {
    pub use crate::v2025_11_20_00::image::Distribution;
    pub use crate::v2025_11_20_00::image::Image;
    pub use crate::v2025_11_20_00::image::ImageCreate;
    pub use crate::v2025_11_20_00::image::ImageSelector;
    pub use crate::v2025_11_20_00::image::ImageSource;
}

pub mod instance {
    pub use crate::v2025_11_20_00::instance::InstanceDiskAttach;
    pub use crate::v2025_11_20_00::instance::InstanceNetworkInterfaceSelector;
    pub use crate::v2025_11_20_00::instance::InstanceNetworkInterfaceUpdate;
    pub use crate::v2025_11_20_00::instance::InstanceSelector;
    pub use crate::v2025_11_20_00::instance::InstanceSerialConsoleData;
    pub use crate::v2025_11_20_00::instance::InstanceSerialConsoleRequest;
    pub use crate::v2025_11_20_00::instance::InstanceSerialConsoleStreamRequest;
    pub use crate::v2025_11_20_00::instance::MAX_USER_DATA_BYTES;
    pub use crate::v2025_11_20_00::instance::OptionalInstanceSelector;
    pub use crate::v2025_11_20_00::instance::UserData;
    pub use crate::v2025_11_20_00::instance::bool_true;

    pub use crate::v2026_01_03_00::instance::InstanceNetworkInterfaceAttachment;
    pub use crate::v2026_01_03_00::instance::InstanceNetworkInterfaceCreate;
    pub use crate::v2026_01_03_00::instance::IpAssignment;
    pub use crate::v2026_01_03_00::instance::Ipv4Assignment;
    pub use crate::v2026_01_03_00::instance::Ipv6Assignment;
    pub use crate::v2026_01_03_00::instance::PrivateIpStackCreate;
    pub use crate::v2026_01_03_00::instance::PrivateIpv4StackCreate;
    pub use crate::v2026_01_03_00::instance::PrivateIpv6StackCreate;

    pub use crate::v2026_01_05_00::instance::EphemeralIpCreate;
    pub use crate::v2026_01_05_00::instance::ExternalIpCreate;

    pub use crate::v2026_01_08_00::instance::InstanceUpdate;

    pub use crate::v2026_01_23_00::instance::EphemeralIpDetachSelector;
    pub use crate::v2026_01_23_00::instance::ExternalIpDetach;

    pub use crate::v2026_01_31_00::instance::InstanceCreate;
    pub use crate::v2026_01_31_00::instance::InstanceDiskAttachment;
}

pub mod internet_gateway {
    pub use crate::v2025_11_20_00::internet_gateway::DeleteInternetGatewayElementSelector;
    pub use crate::v2025_11_20_00::internet_gateway::InternetGateway;
    pub use crate::v2025_11_20_00::internet_gateway::InternetGatewayCreate;
    pub use crate::v2025_11_20_00::internet_gateway::InternetGatewayDeleteSelector;
    pub use crate::v2025_11_20_00::internet_gateway::InternetGatewayIpAddress;
    pub use crate::v2025_11_20_00::internet_gateway::InternetGatewayIpAddressCreate;
    pub use crate::v2025_11_20_00::internet_gateway::InternetGatewayIpAddressSelector;
    pub use crate::v2025_11_20_00::internet_gateway::InternetGatewayIpPool;
    pub use crate::v2025_11_20_00::internet_gateway::InternetGatewayIpPoolCreate;
    pub use crate::v2025_11_20_00::internet_gateway::InternetGatewayIpPoolSelector;
    pub use crate::v2025_11_20_00::internet_gateway::InternetGatewaySelector;
    pub use crate::v2025_11_20_00::internet_gateway::OptionalInternetGatewaySelector;
}

pub mod ip_pool {
    pub use crate::v2025_11_20_00::ip_pool::IpPool;
    pub use crate::v2025_11_20_00::ip_pool::IpPoolCreate;
    pub use crate::v2025_11_20_00::ip_pool::IpPoolRange;
    pub use crate::v2025_11_20_00::ip_pool::IpPoolSiloLink;
    pub use crate::v2025_11_20_00::ip_pool::IpPoolSiloPath;
    pub use crate::v2025_11_20_00::ip_pool::IpPoolType;
    pub use crate::v2025_11_20_00::ip_pool::IpPoolUpdate;
    pub use crate::v2025_11_20_00::ip_pool::IpPoolUtilization;

    pub use crate::v2026_01_01_00::ip_pool::SiloIpPool;

    pub use crate::v2026_01_05_00::ip_pool::IpPoolLinkSilo;
    pub use crate::v2026_01_05_00::ip_pool::IpPoolSiloUpdate;
    pub use crate::v2026_01_05_00::ip_pool::PoolSelector;
}

pub mod metrics {
    pub use crate::v2025_11_20_00::metrics::ResourceMetrics;
    pub use crate::v2025_11_20_00::metrics::SystemMetricName;
    pub use crate::v2025_11_20_00::metrics::SystemMetricsPathParam;
}

pub mod multicast {
    pub use crate::v2025_11_20_00::multicast::MulticastGroupCreate;
    pub use crate::v2025_11_20_00::multicast::MulticastGroupIpLookupPath;
    pub use crate::v2025_11_20_00::multicast::MulticastGroupMemberRemove;
    pub use crate::v2025_11_20_00::multicast::MulticastGroupUpdate;

    pub use crate::v2026_01_08_00::multicast::InstanceMulticastGroupJoin;
    pub use crate::v2026_01_08_00::multicast::InstanceMulticastGroupPath;
    pub use crate::v2026_01_08_00::multicast::MulticastGroup;
    pub use crate::v2026_01_08_00::multicast::MulticastGroupIdentifier;
    pub use crate::v2026_01_08_00::multicast::MulticastGroupJoinSpec;
    pub use crate::v2026_01_08_00::multicast::MulticastGroupMember;
    pub use crate::v2026_01_08_00::multicast::MulticastGroupMemberAdd;
    pub use crate::v2026_01_08_00::multicast::MulticastGroupMemberPath;
    pub use crate::v2026_01_08_00::multicast::MulticastGroupPath;
    pub use crate::v2026_01_08_00::multicast::MulticastGroupSelector;

    pub use crate::impls::multicast::validate_multicast_ip;
    pub use crate::impls::multicast::validate_source_ip;
}

pub mod networking {
    pub use crate::v2025_11_20_00::networking::Address;
    pub use crate::v2025_11_20_00::networking::AddressConfig;
    pub use crate::v2025_11_20_00::networking::AddressLotBlockCreate;
    pub use crate::v2025_11_20_00::networking::AddressLotCreate;
    pub use crate::v2025_11_20_00::networking::AddressLotSelector;
    pub use crate::v2025_11_20_00::networking::BfdSessionDisable;
    pub use crate::v2025_11_20_00::networking::BfdSessionEnable;
    pub use crate::v2025_11_20_00::networking::BgpAnnounceListSelector;
    pub use crate::v2025_11_20_00::networking::BgpAnnounceSetCreate;
    pub use crate::v2025_11_20_00::networking::BgpAnnounceSetSelector;
    pub use crate::v2025_11_20_00::networking::BgpAnnouncementCreate;
    pub use crate::v2025_11_20_00::networking::BgpConfigSelector;
    pub use crate::v2025_11_20_00::networking::BgpRouteSelector;
    pub use crate::v2025_11_20_00::networking::BgpStatusSelector;
    pub use crate::v2025_11_20_00::networking::LinkConfigCreate;
    pub use crate::v2025_11_20_00::networking::LldpLinkConfigCreate;
    pub use crate::v2025_11_20_00::networking::LldpPortPathSelector;
    pub use crate::v2025_11_20_00::networking::LoopbackAddressCreate;
    pub use crate::v2025_11_20_00::networking::LoopbackAddressPath;
    pub use crate::v2025_11_20_00::networking::Route;
    pub use crate::v2025_11_20_00::networking::RouteConfig;
    pub use crate::v2025_11_20_00::networking::SwitchInterfaceConfigCreate;
    pub use crate::v2025_11_20_00::networking::SwitchInterfaceKind;
    pub use crate::v2025_11_20_00::networking::SwitchPortApplySettings;
    pub use crate::v2025_11_20_00::networking::SwitchPortConfigCreate;
    pub use crate::v2025_11_20_00::networking::SwitchPortGeometry;
    pub use crate::v2025_11_20_00::networking::SwitchPortPageSelector;
    pub use crate::v2025_11_20_00::networking::SwitchPortPathSelector;
    pub use crate::v2025_11_20_00::networking::SwitchPortSelector;
    pub use crate::v2025_11_20_00::networking::SwitchPortSettingsInfoSelector;
    pub use crate::v2025_11_20_00::networking::SwitchPortSettingsSelector;
    pub use crate::v2025_11_20_00::networking::SwitchVlanInterface;
    pub use crate::v2025_11_20_00::networking::SwtichPortSettingsGroupCreate;
    pub use crate::v2025_11_20_00::networking::TxEqConfig;

    pub use crate::v2026_02_13_01::networking::BgpConfigCreate;
    pub use crate::v2026_02_13_01::networking::BgpPeerConfig;
    pub use crate::v2026_02_13_01::networking::SwitchPortSettingsCreate;
}

pub mod oxql {
    pub use crate::v2025_11_20_00::oxql::OxqlQueryResult;
    pub use crate::v2025_11_20_00::oxql::OxqlQuerySummary;
    pub use crate::v2025_11_20_00::oxql::OxqlTable;
}

pub mod policy {
    pub use crate::v2025_11_20_00::policy::FleetRole;
    pub use crate::v2025_11_20_00::policy::IdentityType;
    pub use crate::v2025_11_20_00::policy::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;
    pub use crate::v2025_11_20_00::policy::Policy;
    pub use crate::v2025_11_20_00::policy::ProjectRole;
    pub use crate::v2025_11_20_00::policy::RoleAssignment;
    pub use crate::v2025_11_20_00::policy::SiloRole;
}

pub mod probe {
    pub use crate::v2025_11_20_00::probe::ProbeExternalIp;
    pub use crate::v2025_11_20_00::probe::ProbeExternalIpKind;
    pub use crate::v2025_11_20_00::probe::ProbeListSelector;

    pub use crate::v2026_01_03_00::probe::ProbeInfo;

    pub use crate::v2026_01_05_00::probe::ProbeCreate;
}

pub mod project {
    pub use crate::v2025_11_20_00::project::OptionalProjectSelector;
    pub use crate::v2025_11_20_00::project::Project;
    pub use crate::v2025_11_20_00::project::ProjectCreate;
    pub use crate::v2025_11_20_00::project::ProjectSelector;
    pub use crate::v2025_11_20_00::project::ProjectUpdate;
}

pub mod saml {
    pub use crate::v2025_11_20_00::saml::RelativeUri;
    pub use crate::v2025_11_20_00::saml::RelayState;
}

pub mod scim {
    pub use crate::v2025_11_20_00::scim::ScimClientBearerToken;
    pub use crate::v2025_11_20_00::scim::ScimClientBearerTokenValue;
    pub use crate::v2025_11_20_00::scim::ScimV2GroupPathParam;
    pub use crate::v2025_11_20_00::scim::ScimV2TokenPathParam;
    pub use crate::v2025_11_20_00::scim::ScimV2UserPathParam;
}

pub mod silo {
    pub use crate::v2025_11_20_00::silo::AuthenticationMode;
    pub use crate::v2025_11_20_00::silo::OptionalSiloSelector;
    pub use crate::v2025_11_20_00::silo::Silo;
    pub use crate::v2025_11_20_00::silo::SiloAuthSettings;
    pub use crate::v2025_11_20_00::silo::SiloAuthSettingsUpdate;
    pub use crate::v2025_11_20_00::silo::SiloCreate;
    pub use crate::v2025_11_20_00::silo::SiloIdentityMode;
    pub use crate::v2025_11_20_00::silo::SiloQuotas;
    pub use crate::v2025_11_20_00::silo::SiloQuotasCreate;
    pub use crate::v2025_11_20_00::silo::SiloQuotasUpdate;
    pub use crate::v2025_11_20_00::silo::SiloSelector;
    pub use crate::v2025_11_20_00::silo::SiloUtilization;
    pub use crate::v2025_11_20_00::silo::UserProvisionType;
    pub use crate::v2025_11_20_00::silo::Utilization;
    pub use crate::v2025_11_20_00::silo::VirtualResourceCounts;
}

pub mod snapshot {
    pub use crate::v2025_11_20_00::snapshot::Snapshot;
    pub use crate::v2025_11_20_00::snapshot::SnapshotCreate;
    pub use crate::v2025_11_20_00::snapshot::SnapshotSelector;
    pub use crate::v2025_11_20_00::snapshot::SnapshotState;
}

pub mod support_bundle {
    pub use crate::v2025_11_20_00::support_bundle::SupportBundleCreate;
    pub use crate::v2025_11_20_00::support_bundle::SupportBundleFilePath;
    pub use crate::v2025_11_20_00::support_bundle::SupportBundleInfo;
    pub use crate::v2025_11_20_00::support_bundle::SupportBundlePath;
    pub use crate::v2025_11_20_00::support_bundle::SupportBundleState;
    pub use crate::v2025_11_20_00::support_bundle::SupportBundleUpdate;
}

pub mod switch {
    pub use crate::v2025_11_20_00::switch::Switch;
    pub use crate::v2025_11_20_00::switch::SwitchLinkState;
}

pub mod system {
    pub use crate::v2025_11_20_00::system::AllowList;
    pub use crate::v2025_11_20_00::system::AllowListUpdate;
    pub use crate::v2025_11_20_00::system::Ping;
    pub use crate::v2025_11_20_00::system::PingStatus;
}

pub mod timeseries {
    pub use crate::v2025_11_20_00::timeseries::TimeseriesQuery;
}

pub mod update {
    pub use crate::v2025_11_20_00::update::SetTargetReleaseParams;
    pub use crate::v2025_11_20_00::update::TargetRelease;
    pub use crate::v2025_11_20_00::update::TufRepo;
    pub use crate::v2025_11_20_00::update::TufRepoUpload;
    pub use crate::v2025_11_20_00::update::TufRepoUploadStatus;
    pub use crate::v2025_11_20_00::update::TufSignedRootRole;
    pub use crate::v2025_11_20_00::update::UpdateStatus;
    pub use crate::v2025_11_20_00::update::UpdatesGetRepositoryParams;
    pub use crate::v2025_11_20_00::update::UpdatesPutRepositoryParams;
    pub use crate::v2025_11_20_00::update::UpdatesTrustRoot;
}

pub mod vpc {
    pub use crate::v2025_11_20_00::vpc::OptionalRouterSelector;
    pub use crate::v2025_11_20_00::vpc::OptionalVpcSelector;
    pub use crate::v2025_11_20_00::vpc::RouteSelector;
    pub use crate::v2025_11_20_00::vpc::RouterRouteCreate;
    pub use crate::v2025_11_20_00::vpc::RouterRouteUpdate;
    pub use crate::v2025_11_20_00::vpc::RouterSelector;
    pub use crate::v2025_11_20_00::vpc::SubnetSelector;
    pub use crate::v2025_11_20_00::vpc::Vpc;
    pub use crate::v2025_11_20_00::vpc::VpcCreate;
    pub use crate::v2025_11_20_00::vpc::VpcRouter;
    pub use crate::v2025_11_20_00::vpc::VpcRouterCreate;
    pub use crate::v2025_11_20_00::vpc::VpcRouterKind;
    pub use crate::v2025_11_20_00::vpc::VpcRouterUpdate;
    pub use crate::v2025_11_20_00::vpc::VpcSelector;
    pub use crate::v2025_11_20_00::vpc::VpcSubnet;
    pub use crate::v2025_11_20_00::vpc::VpcSubnetCreate;
    pub use crate::v2025_11_20_00::vpc::VpcSubnetUpdate;
    pub use crate::v2025_11_20_00::vpc::VpcUpdate;
}

pub mod asset {
    pub use crate::v2025_11_20_00::asset::AssetIdentityMetadata;
}

pub mod identity_provider {
    pub use crate::v2025_11_20_00::identity_provider::DerEncodedKeyPair;
    pub use crate::v2025_11_20_00::identity_provider::IdentityProvider;
    pub use crate::v2025_11_20_00::identity_provider::IdentityProviderType;
    pub use crate::v2025_11_20_00::identity_provider::IdpMetadataSource;
    pub use crate::v2025_11_20_00::identity_provider::SamlIdentityProvider;
    pub use crate::v2025_11_20_00::identity_provider::SamlIdentityProviderCreate;
    pub use crate::v2025_11_20_00::identity_provider::SamlIdentityProviderSelector;
}

pub mod physical_disk {
    pub use crate::v2025_11_20_00::physical_disk::PhysicalDisk;
    pub use crate::v2025_11_20_00::physical_disk::PhysicalDiskKind;
    pub use crate::v2025_11_20_00::physical_disk::PhysicalDiskPolicy;
    pub use crate::v2025_11_20_00::physical_disk::PhysicalDiskState;
}

pub mod rack {
    pub use crate::v2025_11_20_00::rack::Rack;

    pub use crate::v2026_01_21_00::rack::RackMembershipAddSledsRequest;
    pub use crate::v2026_01_21_00::rack::RackMembershipChangeState;
    pub use crate::v2026_01_21_00::rack::RackMembershipConfigPathParams;
    pub use crate::v2026_01_21_00::rack::RackMembershipStatus;
    pub use crate::v2026_01_21_00::rack::RackMembershipVersion;
    pub use crate::v2026_01_21_00::rack::RackMembershipVersionParam;
}

pub mod subnet_pool {
    // Types unchanged since EXTERNAL_SUBNET_ATTACHMENT.
    pub use crate::v2026_01_16_01::subnet_pool::SubnetPoolLinkSilo;
    pub use crate::v2026_01_16_01::subnet_pool::SubnetPoolMemberRemove;
    pub use crate::v2026_01_16_01::subnet_pool::SubnetPoolPath;
    pub use crate::v2026_01_16_01::subnet_pool::SubnetPoolSiloLink;
    pub use crate::v2026_01_16_01::subnet_pool::SubnetPoolSiloPath;
    pub use crate::v2026_01_16_01::subnet_pool::SubnetPoolSiloUpdate;
    pub use crate::v2026_01_16_01::subnet_pool::SubnetPoolUpdate;
    pub use crate::v2026_01_16_01::subnet_pool::SubnetPoolUtilization;

    // View types from FLOATING_IP_ALLOCATOR_UPDATE (pool_type removed).
    pub use crate::v2026_01_22_00::subnet_pool::SiloSubnetPool;
    pub use crate::v2026_01_22_00::subnet_pool::SubnetPool;
    pub use crate::v2026_01_22_00::subnet_pool::SubnetPoolMember;
    // Create/update types from FLOATING_IP_ALLOCATOR_UPDATE.
    pub use crate::v2026_01_22_00::subnet_pool::SubnetPoolCreate;
    pub use crate::v2026_01_22_00::subnet_pool::SubnetPoolMemberAdd;
}

pub mod external_subnet {
    // Types unchanged since EXTERNAL_SUBNET_ATTACHMENT.
    pub use crate::v2026_01_16_01::external_subnet::ExternalSubnet;
    pub use crate::v2026_01_16_01::external_subnet::ExternalSubnetAttach;
    pub use crate::v2026_01_16_01::external_subnet::ExternalSubnetPath;
    pub use crate::v2026_01_16_01::external_subnet::ExternalSubnetSelector;
    pub use crate::v2026_01_16_01::external_subnet::ExternalSubnetUpdate;

    // Create types from FLOATING_IP_ALLOCATOR_UPDATE (pool field removed).
    pub use crate::v2026_01_22_00::external_subnet::ExternalSubnetAllocator;
    pub use crate::v2026_01_22_00::external_subnet::ExternalSubnetCreate;
}

pub mod sled {
    pub use crate::v2025_11_20_00::sled::Sled;
    pub use crate::v2025_11_20_00::sled::SledId;
    pub use crate::v2025_11_20_00::sled::SledInstance;
    pub use crate::v2025_11_20_00::sled::SledPolicy;
    pub use crate::v2025_11_20_00::sled::SledProvisionPolicy;
    pub use crate::v2025_11_20_00::sled::SledProvisionPolicyParams;
    pub use crate::v2025_11_20_00::sled::SledProvisionPolicyResponse;
    pub use crate::v2025_11_20_00::sled::SledSelector;
    pub use crate::v2025_11_20_00::sled::SledState;
    pub use crate::v2025_11_20_00::sled::SwitchSelector;
}

pub mod ssh_key {
    pub use crate::v2025_11_20_00::ssh_key::SshKey;
    pub use crate::v2025_11_20_00::ssh_key::SshKeyCreate;
    pub use crate::v2025_11_20_00::ssh_key::SshKeySelector;
}

pub mod user {
    pub use crate::v2025_11_20_00::user::CurrentUser;
    pub use crate::v2025_11_20_00::user::Group;
    pub use crate::v2025_11_20_00::user::OptionalGroupSelector;
    pub use crate::v2025_11_20_00::user::Password;
    pub use crate::v2025_11_20_00::user::User;
    pub use crate::v2025_11_20_00::user::UserBuiltin;
    pub use crate::v2025_11_20_00::user::UserBuiltinCreate;
    pub use crate::v2025_11_20_00::user::UserBuiltinSelector;
    pub use crate::v2025_11_20_00::user::UserCreate;
    pub use crate::v2025_11_20_00::user::UserParam;
    pub use crate::v2025_11_20_00::user::UserPassword;
    pub use crate::v2025_11_20_00::user::UsernamePasswordCredentials;
}

pub mod path_params {
    pub use crate::v2025_11_20_00::path_params::AddressLotPath;
    pub use crate::v2025_11_20_00::path_params::AffinityGroupPath;
    pub use crate::v2025_11_20_00::path_params::AntiAffinityGroupPath;
    pub use crate::v2025_11_20_00::path_params::BlueprintPath;
    pub use crate::v2025_11_20_00::path_params::CertificatePath;
    pub use crate::v2025_11_20_00::path_params::DiskPath;
    pub use crate::v2025_11_20_00::path_params::FloatingIpPath;
    pub use crate::v2025_11_20_00::path_params::GroupPath;
    pub use crate::v2025_11_20_00::path_params::ImagePath;
    pub use crate::v2025_11_20_00::path_params::InstancePath;
    pub use crate::v2025_11_20_00::path_params::InternetGatewayPath;
    pub use crate::v2025_11_20_00::path_params::IpAddressPath;
    pub use crate::v2025_11_20_00::path_params::IpPoolPath;
    pub use crate::v2025_11_20_00::path_params::NetworkInterfacePath;
    pub use crate::v2025_11_20_00::path_params::PhysicalDiskPath;
    pub use crate::v2025_11_20_00::path_params::ProbePath;
    pub use crate::v2025_11_20_00::path_params::ProjectPath;
    pub use crate::v2025_11_20_00::path_params::ProviderPath;
    pub use crate::v2025_11_20_00::path_params::RackPath;
    pub use crate::v2025_11_20_00::path_params::RoutePath;
    pub use crate::v2025_11_20_00::path_params::RouterPath;
    pub use crate::v2025_11_20_00::path_params::SiloPath;
    pub use crate::v2025_11_20_00::path_params::SledPath;
    pub use crate::v2025_11_20_00::path_params::SnapshotPath;
    pub use crate::v2025_11_20_00::path_params::SshKeyPath;
    pub use crate::v2025_11_20_00::path_params::SubnetPath;
    pub use crate::v2025_11_20_00::path_params::SwitchPath;
    pub use crate::v2025_11_20_00::path_params::TokenPath;
    pub use crate::v2025_11_20_00::path_params::TufTrustRootPath;
    pub use crate::v2025_11_20_00::path_params::UserPath;
    pub use crate::v2025_11_20_00::path_params::VpcPath;
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest versions of all published types.
//!
//! This module provides floating identifiers that always point to the
//! current version of each type. Business logic should use these re-exports
//! rather than versioned identifiers.

pub mod affinity {
    pub use crate::v2025112000::affinity::AffinityGroup;
    pub use crate::v2025112000::affinity::AffinityGroupCreate;
    pub use crate::v2025112000::affinity::AffinityGroupSelector;
    pub use crate::v2025112000::affinity::AffinityGroupUpdate;
    pub use crate::v2025112000::affinity::AffinityInstanceGroupMemberPath;
    pub use crate::v2025112000::affinity::AntiAffinityGroup;
    pub use crate::v2025112000::affinity::AntiAffinityGroupCreate;
    pub use crate::v2025112000::affinity::AntiAffinityGroupSelector;
    pub use crate::v2025112000::affinity::AntiAffinityGroupUpdate;
    pub use crate::v2025112000::affinity::AntiAffinityInstanceGroupMemberPath;
}

pub mod alert {
    pub use crate::v2025112000::alert::AlertClass;
    pub use crate::v2025112000::alert::AlertClassFilter;
    pub use crate::v2025112000::alert::AlertClassPage;
    pub use crate::v2025112000::alert::AlertDelivery;
    pub use crate::v2025112000::alert::AlertDeliveryAttempts;
    pub use crate::v2025112000::alert::AlertDeliveryId;
    pub use crate::v2025112000::alert::AlertDeliveryState;
    pub use crate::v2025112000::alert::AlertDeliveryStateFilter;
    pub use crate::v2025112000::alert::AlertDeliveryTrigger;
    pub use crate::v2025112000::alert::AlertProbeResult;
    pub use crate::v2025112000::alert::AlertReceiver;
    pub use crate::v2025112000::alert::AlertReceiverKind;
    pub use crate::v2025112000::alert::AlertReceiverProbe;
    pub use crate::v2025112000::alert::AlertReceiverSelector;
    pub use crate::v2025112000::alert::AlertSelector;
    pub use crate::v2025112000::alert::AlertSubscription;
    pub use crate::v2025112000::alert::AlertSubscriptionCreate;
    pub use crate::v2025112000::alert::AlertSubscriptionCreated;
    pub use crate::v2025112000::alert::AlertSubscriptionSelector;
    pub use crate::v2025112000::alert::WebhookCreate;
    pub use crate::v2025112000::alert::WebhookDeliveryAttempt;
    pub use crate::v2025112000::alert::WebhookDeliveryAttemptResult;
    pub use crate::v2025112000::alert::WebhookDeliveryResponse;
    pub use crate::v2025112000::alert::WebhookReceiver;
    pub use crate::v2025112000::alert::WebhookReceiverConfig;
    pub use crate::v2025112000::alert::WebhookReceiverUpdate;
    pub use crate::v2025112000::alert::WebhookSecret;
    pub use crate::v2025112000::alert::WebhookSecretCreate;
    pub use crate::v2025112000::alert::WebhookSecretSelector;
    pub use crate::v2025112000::alert::WebhookSecrets;
}

pub mod audit {
    pub use crate::v2025112000::audit::AuditLogEntryActor;
    pub use crate::v2025112000::audit::AuditLogEntryResult;
    pub use crate::v2025112000::audit::AuditLogParams;

    pub use crate::v2026011501::audit::AuditLogEntry;
    pub use crate::v2026011501::audit::AuthMethod;
}

pub mod bfd {
    pub use crate::v2025112000::bfd::BfdState;
    pub use crate::v2025112000::bfd::BfdStatus;
}

pub mod device {
    pub use crate::v2025112000::device::ConsoleSession;
    pub use crate::v2025112000::device::DeviceAccessToken;
    pub use crate::v2025112000::device::DeviceAccessTokenGrant;
    pub use crate::v2025112000::device::DeviceAccessTokenType;
    pub use crate::v2025112000::device::DeviceAuthResponse;
}

pub mod certificate {
    pub use crate::v2025112000::certificate::Certificate;
    pub use crate::v2025112000::certificate::CertificateCreate;
    pub use crate::v2025112000::certificate::ServiceUsingCertificate;
}

pub mod console {
    pub use crate::v2025112000::console::LoginPath;
    pub use crate::v2025112000::console::LoginToProviderPathParam;
    pub use crate::v2025112000::console::LoginUrlQuery;
    pub use crate::v2025112000::console::RestPathParam;
}

pub mod device_params {
    pub use crate::v2025112000::device_params::DeviceAccessTokenRequest;
    pub use crate::v2025112000::device_params::DeviceAuthRequest;
    pub use crate::v2025112000::device_params::DeviceAuthVerify;
}

pub mod disk {
    // View types from omicron-common (latest version).
    pub use omicron_common::api::external::{Disk, DiskType};

    // Request types unchanged since INITIAL.
    pub use crate::v2025112000::disk::{
        BlockSize, DiskMetricName, DiskMetricsPath, DiskSelector,
        ExpectedDigest, FinalizeDisk, ImportBlocksBulkWrite,
    };

    // Request types from READ_ONLY_DISKS_NULLABLE.
    pub use crate::v2026013100::disk::{DiskBackend, DiskCreate, DiskSource};
}

pub mod external_ip {
    pub use crate::v2025112000::external_ip::ExternalIp;
    pub use crate::v2025112000::external_ip::IpKind;
    pub use crate::v2025112000::external_ip::SNatIp;
}

pub mod floating_ip {
    pub use crate::v2025112000::floating_ip::FloatingIp;
    pub use crate::v2025112000::floating_ip::FloatingIpAttach;
    pub use crate::v2025112000::floating_ip::FloatingIpParentKind;
    pub use crate::v2025112000::floating_ip::FloatingIpSelector;
    pub use crate::v2025112000::floating_ip::FloatingIpUpdate;

    pub use crate::v2026012200::floating_ip::AddressAllocator;
    pub use crate::v2026012200::floating_ip::FloatingIpCreate;
}

pub mod hardware {
    pub use crate::v2025112000::hardware::Baseboard;
    pub use crate::v2025112000::hardware::UninitializedSled;
    pub use crate::v2025112000::hardware::UninitializedSledId;
    pub use crate::v2025112000::hardware::UpdateableComponentType;
}

pub mod headers {
    pub use crate::v2025112000::headers::RangeRequest;
}

pub mod image {
    pub use crate::v2025112000::image::Distribution;
    pub use crate::v2025112000::image::Image;
    pub use crate::v2025112000::image::ImageCreate;
    pub use crate::v2025112000::image::ImageSelector;
    pub use crate::v2025112000::image::ImageSource;
}

pub mod instance {
    pub use crate::v2025112000::instance::InstanceDiskAttach;
    pub use crate::v2025112000::instance::InstanceNetworkInterfaceSelector;
    pub use crate::v2025112000::instance::InstanceNetworkInterfaceUpdate;
    pub use crate::v2025112000::instance::InstanceSelector;
    pub use crate::v2025112000::instance::InstanceSerialConsoleData;
    pub use crate::v2025112000::instance::InstanceSerialConsoleRequest;
    pub use crate::v2025112000::instance::InstanceSerialConsoleStreamRequest;
    pub use crate::v2025112000::instance::MAX_USER_DATA_BYTES;
    pub use crate::v2025112000::instance::OptionalInstanceSelector;
    pub use crate::v2025112000::instance::UserData;
    pub use crate::v2025112000::instance::bool_true;

    pub use crate::v2026010300::instance::InstanceNetworkInterfaceAttachment;
    pub use crate::v2026010300::instance::InstanceNetworkInterfaceCreate;
    pub use crate::v2026010300::instance::IpAssignment;
    pub use crate::v2026010300::instance::Ipv4Assignment;
    pub use crate::v2026010300::instance::Ipv6Assignment;
    pub use crate::v2026010300::instance::PrivateIpStackCreate;
    pub use crate::v2026010300::instance::PrivateIpv4StackCreate;
    pub use crate::v2026010300::instance::PrivateIpv6StackCreate;

    pub use crate::v2026010500::instance::EphemeralIpCreate;
    pub use crate::v2026010500::instance::ExternalIpCreate;

    pub use crate::v2026010800::instance::InstanceUpdate;

    pub use crate::v2026012300::instance::EphemeralIpDetachSelector;
    pub use crate::v2026012300::instance::ExternalIpDetach;

    pub use crate::v2026013100::instance::InstanceCreate;
    pub use crate::v2026013100::instance::InstanceDiskAttachment;
}

pub mod internet_gateway {
    pub use crate::v2025112000::internet_gateway::DeleteInternetGatewayElementSelector;
    pub use crate::v2025112000::internet_gateway::InternetGateway;
    pub use crate::v2025112000::internet_gateway::InternetGatewayCreate;
    pub use crate::v2025112000::internet_gateway::InternetGatewayDeleteSelector;
    pub use crate::v2025112000::internet_gateway::InternetGatewayIpAddress;
    pub use crate::v2025112000::internet_gateway::InternetGatewayIpAddressCreate;
    pub use crate::v2025112000::internet_gateway::InternetGatewayIpAddressSelector;
    pub use crate::v2025112000::internet_gateway::InternetGatewayIpPool;
    pub use crate::v2025112000::internet_gateway::InternetGatewayIpPoolCreate;
    pub use crate::v2025112000::internet_gateway::InternetGatewayIpPoolSelector;
    pub use crate::v2025112000::internet_gateway::InternetGatewaySelector;
    pub use crate::v2025112000::internet_gateway::OptionalInternetGatewaySelector;
}

pub mod ip_pool {
    pub use crate::v2025112000::ip_pool::IpPool;
    pub use crate::v2025112000::ip_pool::IpPoolCreate;
    pub use crate::v2025112000::ip_pool::IpPoolRange;
    pub use crate::v2025112000::ip_pool::IpPoolSiloLink;
    pub use crate::v2025112000::ip_pool::IpPoolSiloPath;
    pub use crate::v2025112000::ip_pool::IpPoolType;
    pub use crate::v2025112000::ip_pool::IpPoolUpdate;
    pub use crate::v2025112000::ip_pool::IpPoolUtilization;

    pub use crate::v2026010100::ip_pool::SiloIpPool;

    pub use crate::v2026010500::ip_pool::IpPoolLinkSilo;
    pub use crate::v2026010500::ip_pool::IpPoolSiloUpdate;
    pub use crate::v2026010500::ip_pool::PoolSelector;
}

pub mod metrics {
    pub use crate::v2025112000::metrics::ResourceMetrics;
    pub use crate::v2025112000::metrics::SystemMetricName;
    pub use crate::v2025112000::metrics::SystemMetricsPathParam;
}

pub mod multicast {
    pub use crate::v2025112000::multicast::MulticastGroupCreate;
    pub use crate::v2025112000::multicast::MulticastGroupIpLookupPath;
    pub use crate::v2025112000::multicast::MulticastGroupMemberRemove;
    pub use crate::v2025112000::multicast::MulticastGroupUpdate;

    pub use crate::v2026010800::multicast::InstanceMulticastGroupJoin;
    pub use crate::v2026010800::multicast::InstanceMulticastGroupPath;
    pub use crate::v2026010800::multicast::MulticastGroup;
    pub use crate::v2026010800::multicast::MulticastGroupIdentifier;
    pub use crate::v2026010800::multicast::MulticastGroupJoinSpec;
    pub use crate::v2026010800::multicast::MulticastGroupMember;
    pub use crate::v2026010800::multicast::MulticastGroupMemberAdd;
    pub use crate::v2026010800::multicast::MulticastGroupMemberPath;
    pub use crate::v2026010800::multicast::MulticastGroupPath;
    pub use crate::v2026010800::multicast::MulticastGroupSelector;

    pub use crate::impls::multicast::validate_multicast_ip;
    pub use crate::impls::multicast::validate_source_ip;
}

pub mod networking {
    pub use crate::v2025112000::networking::Address;
    pub use crate::v2025112000::networking::AddressConfig;
    pub use crate::v2025112000::networking::AddressLotBlockCreate;
    pub use crate::v2025112000::networking::AddressLotCreate;
    pub use crate::v2025112000::networking::AddressLotSelector;
    pub use crate::v2025112000::networking::BfdSessionDisable;
    pub use crate::v2025112000::networking::BfdSessionEnable;
    pub use crate::v2025112000::networking::BgpAnnounceListSelector;
    pub use crate::v2025112000::networking::BgpAnnounceSetCreate;
    pub use crate::v2025112000::networking::BgpAnnounceSetSelector;
    pub use crate::v2025112000::networking::BgpAnnouncementCreate;
    pub use crate::v2025112000::networking::BgpConfigCreate;
    pub use crate::v2025112000::networking::BgpConfigSelector;
    pub use crate::v2025112000::networking::BgpPeerConfig;
    pub use crate::v2025112000::networking::BgpRouteSelector;
    pub use crate::v2025112000::networking::BgpStatusSelector;
    pub use crate::v2025112000::networking::LinkConfigCreate;
    pub use crate::v2025112000::networking::LldpLinkConfigCreate;
    pub use crate::v2025112000::networking::LldpPortPathSelector;
    pub use crate::v2025112000::networking::LoopbackAddressCreate;
    pub use crate::v2025112000::networking::LoopbackAddressPath;
    pub use crate::v2025112000::networking::Route;
    pub use crate::v2025112000::networking::RouteConfig;
    pub use crate::v2025112000::networking::SwitchInterfaceConfigCreate;
    pub use crate::v2025112000::networking::SwitchInterfaceKind;
    pub use crate::v2025112000::networking::SwitchPortApplySettings;
    pub use crate::v2025112000::networking::SwitchPortConfigCreate;
    pub use crate::v2025112000::networking::SwitchPortGeometry;
    pub use crate::v2025112000::networking::SwitchPortPageSelector;
    pub use crate::v2025112000::networking::SwitchPortPathSelector;
    pub use crate::v2025112000::networking::SwitchPortSelector;
    pub use crate::v2025112000::networking::SwitchPortSettingsCreate;
    pub use crate::v2025112000::networking::SwitchPortSettingsInfoSelector;
    pub use crate::v2025112000::networking::SwitchPortSettingsSelector;
    pub use crate::v2025112000::networking::SwitchVlanInterface;
    pub use crate::v2025112000::networking::SwtichPortSettingsGroupCreate;
    pub use crate::v2025112000::networking::TxEqConfig;
}

pub mod oxql {
    pub use crate::v2025112000::oxql::OxqlQueryResult;
    pub use crate::v2025112000::oxql::OxqlQuerySummary;
    pub use crate::v2025112000::oxql::OxqlTable;
}

pub mod policy {
    pub use crate::v2025112000::policy::FleetRole;
    pub use crate::v2025112000::policy::IdentityType;
    pub use crate::v2025112000::policy::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;
    pub use crate::v2025112000::policy::Policy;
    pub use crate::v2025112000::policy::ProjectRole;
    pub use crate::v2025112000::policy::RoleAssignment;
    pub use crate::v2025112000::policy::SiloRole;
}

pub mod probe {
    pub use crate::v2025112000::probe::ProbeExternalIp;
    pub use crate::v2025112000::probe::ProbeExternalIpKind;
    pub use crate::v2025112000::probe::ProbeListSelector;

    pub use crate::v2026010300::probe::ProbeInfo;

    pub use crate::v2026010500::probe::ProbeCreate;
}

pub mod project {
    pub use crate::v2025112000::project::OptionalProjectSelector;
    pub use crate::v2025112000::project::Project;
    pub use crate::v2025112000::project::ProjectCreate;
    pub use crate::v2025112000::project::ProjectSelector;
    pub use crate::v2025112000::project::ProjectUpdate;
}

pub mod saml {
    pub use crate::v2025112000::saml::RelativeUri;
    pub use crate::v2025112000::saml::RelayState;
}

pub mod scim {
    pub use crate::v2025112000::scim::ScimClientBearerToken;
    pub use crate::v2025112000::scim::ScimClientBearerTokenValue;
    pub use crate::v2025112000::scim::ScimV2GroupPathParam;
    pub use crate::v2025112000::scim::ScimV2TokenPathParam;
    pub use crate::v2025112000::scim::ScimV2UserPathParam;
}

pub mod silo {
    pub use crate::v2025112000::silo::AuthenticationMode;
    pub use crate::v2025112000::silo::OptionalSiloSelector;
    pub use crate::v2025112000::silo::Silo;
    pub use crate::v2025112000::silo::SiloAuthSettings;
    pub use crate::v2025112000::silo::SiloAuthSettingsUpdate;
    pub use crate::v2025112000::silo::SiloCreate;
    pub use crate::v2025112000::silo::SiloIdentityMode;
    pub use crate::v2025112000::silo::SiloQuotas;
    pub use crate::v2025112000::silo::SiloQuotasCreate;
    pub use crate::v2025112000::silo::SiloQuotasUpdate;
    pub use crate::v2025112000::silo::SiloSelector;
    pub use crate::v2025112000::silo::SiloUtilization;
    pub use crate::v2025112000::silo::UserProvisionType;
    pub use crate::v2025112000::silo::Utilization;
    pub use crate::v2025112000::silo::VirtualResourceCounts;
}

pub mod snapshot {
    pub use crate::v2025112000::snapshot::Snapshot;
    pub use crate::v2025112000::snapshot::SnapshotCreate;
    pub use crate::v2025112000::snapshot::SnapshotSelector;
    pub use crate::v2025112000::snapshot::SnapshotState;
}

pub mod support_bundle {
    pub use crate::v2025112000::support_bundle::SupportBundleCreate;
    pub use crate::v2025112000::support_bundle::SupportBundleFilePath;
    pub use crate::v2025112000::support_bundle::SupportBundleInfo;
    pub use crate::v2025112000::support_bundle::SupportBundlePath;
    pub use crate::v2025112000::support_bundle::SupportBundleState;
    pub use crate::v2025112000::support_bundle::SupportBundleUpdate;
}

pub mod switch {
    pub use crate::v2025112000::switch::Switch;
    pub use crate::v2025112000::switch::SwitchLinkState;
}

pub mod system {
    pub use crate::v2025112000::system::AllowList;
    pub use crate::v2025112000::system::AllowListUpdate;
    pub use crate::v2025112000::system::Ping;
    pub use crate::v2025112000::system::PingStatus;
}

pub mod timeseries {
    pub use crate::v2025112000::timeseries::TimeseriesQuery;
}

pub mod update {
    pub use crate::v2025112000::update::SetTargetReleaseParams;
    pub use crate::v2025112000::update::TargetRelease;
    pub use crate::v2025112000::update::TufRepo;
    pub use crate::v2025112000::update::TufRepoUpload;
    pub use crate::v2025112000::update::TufRepoUploadStatus;
    pub use crate::v2025112000::update::TufSignedRootRole;
    pub use crate::v2025112000::update::UpdateStatus;
    pub use crate::v2025112000::update::UpdatesGetRepositoryParams;
    pub use crate::v2025112000::update::UpdatesPutRepositoryParams;
    pub use crate::v2025112000::update::UpdatesTrustRoot;
}

pub mod vpc {
    pub use crate::v2025112000::vpc::OptionalRouterSelector;
    pub use crate::v2025112000::vpc::OptionalVpcSelector;
    pub use crate::v2025112000::vpc::RouteSelector;
    pub use crate::v2025112000::vpc::RouterRouteCreate;
    pub use crate::v2025112000::vpc::RouterRouteUpdate;
    pub use crate::v2025112000::vpc::RouterSelector;
    pub use crate::v2025112000::vpc::SubnetSelector;
    pub use crate::v2025112000::vpc::Vpc;
    pub use crate::v2025112000::vpc::VpcCreate;
    pub use crate::v2025112000::vpc::VpcRouter;
    pub use crate::v2025112000::vpc::VpcRouterCreate;
    pub use crate::v2025112000::vpc::VpcRouterKind;
    pub use crate::v2025112000::vpc::VpcRouterUpdate;
    pub use crate::v2025112000::vpc::VpcSelector;
    pub use crate::v2025112000::vpc::VpcSubnet;
    pub use crate::v2025112000::vpc::VpcSubnetCreate;
    pub use crate::v2025112000::vpc::VpcSubnetUpdate;
    pub use crate::v2025112000::vpc::VpcUpdate;
}

pub mod asset {
    pub use crate::v2025112000::asset::AssetIdentityMetadata;
}

pub mod identity_provider {
    pub use crate::v2025112000::identity_provider::DerEncodedKeyPair;
    pub use crate::v2025112000::identity_provider::IdentityProvider;
    pub use crate::v2025112000::identity_provider::IdentityProviderType;
    pub use crate::v2025112000::identity_provider::IdpMetadataSource;
    pub use crate::v2025112000::identity_provider::SamlIdentityProvider;
    pub use crate::v2025112000::identity_provider::SamlIdentityProviderCreate;
    pub use crate::v2025112000::identity_provider::SamlIdentityProviderSelector;
}

pub mod physical_disk {
    pub use crate::v2025112000::physical_disk::PhysicalDisk;
    pub use crate::v2025112000::physical_disk::PhysicalDiskKind;
    pub use crate::v2025112000::physical_disk::PhysicalDiskPolicy;
    pub use crate::v2025112000::physical_disk::PhysicalDiskState;
}

pub mod rack {
    pub use crate::v2025112000::rack::Rack;

    pub use crate::v2026012100::rack::RackMembershipAddSledsRequest;
    pub use crate::v2026012100::rack::RackMembershipChangeState;
    pub use crate::v2026012100::rack::RackMembershipConfigPathParams;
    pub use crate::v2026012100::rack::RackMembershipStatus;
    pub use crate::v2026012100::rack::RackMembershipVersion;
    pub use crate::v2026012100::rack::RackMembershipVersionParam;
}

pub mod subnet_pool {
    pub use crate::v2026012200::subnet_pool::SiloSubnetPool;
    pub use crate::v2026012200::subnet_pool::SubnetPool;
    pub use crate::v2026012200::subnet_pool::SubnetPoolCreate;
    pub use crate::v2026012200::subnet_pool::SubnetPoolLinkSilo;
    pub use crate::v2026012200::subnet_pool::SubnetPoolMember;
    pub use crate::v2026012200::subnet_pool::SubnetPoolMemberAdd;
    pub use crate::v2026012200::subnet_pool::SubnetPoolMemberRemove;
    pub use crate::v2026012200::subnet_pool::SubnetPoolPath;
    pub use crate::v2026012200::subnet_pool::SubnetPoolSiloLink;
    pub use crate::v2026012200::subnet_pool::SubnetPoolSiloPath;
    pub use crate::v2026012200::subnet_pool::SubnetPoolSiloUpdate;
    pub use crate::v2026012200::subnet_pool::SubnetPoolUpdate;
    pub use crate::v2026012200::subnet_pool::SubnetPoolUtilization;
}

pub mod external_subnet {
    pub use crate::v2026012200::external_subnet::ExternalSubnet;
    pub use crate::v2026012200::external_subnet::ExternalSubnetAllocator;
    pub use crate::v2026012200::external_subnet::ExternalSubnetAttach;
    pub use crate::v2026012200::external_subnet::ExternalSubnetCreate;
    pub use crate::v2026012200::external_subnet::ExternalSubnetPath;
    pub use crate::v2026012200::external_subnet::ExternalSubnetSelector;
    pub use crate::v2026012200::external_subnet::ExternalSubnetUpdate;
}

pub mod sled {
    pub use crate::v2025112000::sled::Sled;
    pub use crate::v2025112000::sled::SledId;
    pub use crate::v2025112000::sled::SledInstance;
    pub use crate::v2025112000::sled::SledPolicy;
    pub use crate::v2025112000::sled::SledProvisionPolicy;
    pub use crate::v2025112000::sled::SledProvisionPolicyParams;
    pub use crate::v2025112000::sled::SledProvisionPolicyResponse;
    pub use crate::v2025112000::sled::SledSelector;
    pub use crate::v2025112000::sled::SledState;
    pub use crate::v2025112000::sled::SwitchSelector;
}

pub mod ssh_key {
    pub use crate::v2025112000::ssh_key::SshKey;
    pub use crate::v2025112000::ssh_key::SshKeyCreate;
    pub use crate::v2025112000::ssh_key::SshKeySelector;
}

pub mod user {
    pub use crate::v2025112000::user::CurrentUser;
    pub use crate::v2025112000::user::Group;
    pub use crate::v2025112000::user::OptionalGroupSelector;
    pub use crate::v2025112000::user::Password;
    pub use crate::v2025112000::user::User;
    pub use crate::v2025112000::user::UserBuiltin;
    pub use crate::v2025112000::user::UserBuiltinCreate;
    pub use crate::v2025112000::user::UserBuiltinSelector;
    pub use crate::v2025112000::user::UserCreate;
    pub use crate::v2025112000::user::UserParam;
    pub use crate::v2025112000::user::UserPassword;
    pub use crate::v2025112000::user::UsernamePasswordCredentials;
}

pub mod path_params {
    pub use crate::v2025112000::path_params::AddressLotPath;
    pub use crate::v2025112000::path_params::AffinityGroupPath;
    pub use crate::v2025112000::path_params::AntiAffinityGroupPath;
    pub use crate::v2025112000::path_params::BlueprintPath;
    pub use crate::v2025112000::path_params::CertificatePath;
    pub use crate::v2025112000::path_params::DiskPath;
    pub use crate::v2025112000::path_params::FloatingIpPath;
    pub use crate::v2025112000::path_params::GroupPath;
    pub use crate::v2025112000::path_params::ImagePath;
    pub use crate::v2025112000::path_params::InstancePath;
    pub use crate::v2025112000::path_params::InternetGatewayPath;
    pub use crate::v2025112000::path_params::IpAddressPath;
    pub use crate::v2025112000::path_params::IpPoolPath;
    pub use crate::v2025112000::path_params::NetworkInterfacePath;
    pub use crate::v2025112000::path_params::PhysicalDiskPath;
    pub use crate::v2025112000::path_params::ProbePath;
    pub use crate::v2025112000::path_params::ProjectPath;
    pub use crate::v2025112000::path_params::ProviderPath;
    pub use crate::v2025112000::path_params::RackPath;
    pub use crate::v2025112000::path_params::RoutePath;
    pub use crate::v2025112000::path_params::RouterPath;
    pub use crate::v2025112000::path_params::SiloPath;
    pub use crate::v2025112000::path_params::SledPath;
    pub use crate::v2025112000::path_params::SnapshotPath;
    pub use crate::v2025112000::path_params::SshKeyPath;
    pub use crate::v2025112000::path_params::SubnetPath;
    pub use crate::v2025112000::path_params::SwitchPath;
    pub use crate::v2025112000::path_params::TokenPath;
    pub use crate::v2025112000::path_params::TufTrustRootPath;
    pub use crate::v2025112000::path_params::UserPath;
    pub use crate::v2025112000::path_params::VpcPath;
}

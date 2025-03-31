// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Enumerations used in the database schema.

use diesel::SqlType;
use diesel::query_builder::QueryId;

/// Macro to define enums with static query IDs.
macro_rules! define_enums {
    ($($enum_name:ident => $postgres_name:literal),* $(,)?) => {
        $(
            #[derive(Clone, Copy, Debug, PartialEq, SqlType, QueryId)]
            #[diesel(postgres_type(name = $postgres_name, schema = "public"))]
            pub struct $enum_name;
        )*
    };
}

/// Macro to define enums without static query IDs.
macro_rules! define_enums_non_static {
    ($($enum_name:ident => $postgres_name:literal),* $(,)?) => {
        $(
            #[derive(Clone, Copy, Debug, PartialEq, SqlType)]
            #[diesel(postgres_type(name = $postgres_name, schema = "public"))]
            pub struct $enum_name;

            impl diesel::query_builder::QueryId for $enum_name {
                type QueryId = ();
                const HAS_STATIC_QUERY_ID: bool = false;
            }
        )*
    };
}

define_enums! {
    // Please keep this list in alphabetical order.
    AddressLotKindEnum => "address_lot_kind",
    AffinityPolicyEnum => "affinity_policy",
    AuthenticationModeEnum => "authentication_mode",
    BfdModeEnum => "bfd_mode",
    BlockSizeEnum => "block_size",
    BpDatasetDispositionEnum => "bp_dataset_disposition",
    BpPhysicalDiskDispositionEnum => "bp_physical_disk_disposition",
    BpZoneDispositionEnum => "bp_zone_disposition",
    CabooseWhichEnum => "caboose_which",
    ClickhouseModeEnum => "clickhouse_mode",
    DatasetKindEnum => "dataset_kind",
    DbSwitchInterfaceKindEnum => "switch_interface_kind",
    DownstairsClientStopRequestReasonEnum => "downstairs_client_stop_request_reason_type",
    DownstairsClientStoppedReasonEnum => "downstairs_client_stopped_reason_type",
    FailureDomainEnum => "failure_domain",
    HwPowerStateEnum => "hw_power_state",
    HwRotSlotEnum => "hw_rot_slot",
    IdentityProviderTypeEnum => "provider_type",
    IdentityTypeEnum => "identity_type",
    IpAttachStateEnum => "ip_attach_state",
    IpKindEnum => "ip_kind",
    IpPoolResourceTypeEnum => "ip_pool_resource_type",
    MigrationStateEnum => "migration_state",
    NetworkInterfaceKindEnum => "network_interface_kind",
    PhysicalDiskKindEnum => "physical_disk_kind",
    PhysicalDiskPolicyEnum => "physical_disk_policy",
    PhysicalDiskStateEnum => "physical_disk_state",
    ProducerKindEnum => "producer_kind",
    ReadOnlyTargetReplacementTypeEnum => "read_only_target_replacement_type",
    RegionReplacementStateEnum => "region_replacement_state",
    RegionReplacementStepTypeEnum => "region_replacement_step_type",
    RegionSnapshotReplacementStateEnum => "region_snapshot_replacement_state",
    RegionSnapshotReplacementStepStateEnum => "region_snapshot_replacement_step_state",
    RotImageErrorEnum => "rot_image_error",
    RotPageWhichEnum => "root_of_trust_page_which",
    RouterRouteKindEnum => "router_route_kind",
    SagaCachedStateEnum => "saga_state",
    ServiceKindEnum => "service_kind",
    SledPolicyEnum => "sled_policy",
    SledRoleEnum => "sled_role",
    SledStateEnum => "sled_state",
    SnapshotStateEnum => "snapshot_state",
    SpTypeEnum => "sp_type",
    SupportBundleStateEnum => "support_bundle_state",
    SwitchLinkFecEnum => "switch_link_fec",
    SwitchLinkSpeedEnum => "switch_link_speed",
    SwitchPortGeometryEnum => "switch_port_geometry",
    TargetReleaseSourceEnum => "target_release_source",
    UpstairsRepairNotificationTypeEnum => "upstairs_repair_notification_type",
    UpstairsRepairTypeEnum => "upstairs_repair_type",
    UserProvisionTypeEnum => "user_provision_type",
    VolumeResourceUsageTypeEnum => "volume_resource_usage_type",
    VpcFirewallRuleActionEnum => "vpc_firewall_rule_action",
    VpcFirewallRuleDirectionEnum => "vpc_firewall_rule_direction",
    VpcFirewallRuleProtocolEnum => "vpc_firewall_rule_protocol",
    VpcFirewallRuleStatusEnum => "vpc_firewall_rule_status",
    VpcRouterKindEnum => "vpc_router_kind",
    ZoneTypeEnum => "zone_type",
}

define_enums_non_static! {
    // Please keep this list in alphabetical order.
    DnsGroupEnum => "dns_group",
    InstanceAutoRestartPolicyEnum => "instance_auto_restart",
    InstanceStateEnum => "instance_state_v2",
    VmmStateEnum => "vmm_state",
}

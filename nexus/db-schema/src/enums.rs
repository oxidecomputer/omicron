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

define_enums! {
    // Please keep this list in alphabetical order.
    AddressLotKindEnum => "address_lot_kind",
    AffinityPolicyEnum => "affinity_policy",
    AlertClassEnum => "alert_class",
    AlertDeliveryTriggerEnum => "alert_delivery_trigger",
    AlertDeliveryStateEnum => "alert_delivery_state",
    AuthenticationModeEnum => "authentication_mode",
    BfdModeEnum => "bfd_mode",
    BlockSizeEnum => "block_size",
    BpDatasetDispositionEnum => "bp_dataset_disposition",
    BpPhysicalDiskDispositionEnum => "bp_physical_disk_disposition",
    BpZoneDispositionEnum => "bp_zone_disposition",
    BpZoneImageSourceEnum => "bp_zone_image_source",
    CabooseWhichEnum => "caboose_which",
    ClearMupdateOverrideBootSuccessEnum => "clear_mupdate_override_boot_success",
    ClickhouseModeEnum => "clickhouse_mode",
    DatasetKindEnum => "dataset_kind",
    DnsGroupEnum => "dns_group",
    DownstairsClientStopRequestReasonEnum => "downstairs_client_stop_request_reason_type",
    DownstairsClientStoppedReasonEnum => "downstairs_client_stopped_reason_type",
    FailureDomainEnum => "failure_domain",
    HwM2SlotEnum => "hw_m2_slot",
    HwPowerStateEnum => "hw_power_state",
    HwRotSlotEnum => "hw_rot_slot",
    IdentityProviderTypeEnum => "provider_type",
    IdentityTypeEnum => "identity_type",
    InstanceAutoRestartPolicyEnum => "instance_auto_restart",
    InstanceMinimumCpuPlatformEnum => "instance_min_cpu_platform",
    InstanceStateEnum => "instance_state_v2",
    InstanceIntendedStateEnum => "instance_intended_state",
    InvConfigReconcilerStatusKindEnum => "inv_config_reconciler_status_kind",
    InvZoneImageSourceEnum => "inv_zone_image_source",
    InvZoneManifestSourceEnum => "inv_zone_manifest_source",
    IpAttachStateEnum => "ip_attach_state",
    IpKindEnum => "ip_kind",
    IpPoolResourceTypeEnum => "ip_pool_resource_type",
    MigrationStateEnum => "migration_state",
    NetworkInterfaceKindEnum => "network_interface_kind",
    OximeterReadModeEnum => "oximeter_read_mode",
    PhysicalDiskKindEnum => "physical_disk_kind",
    PhysicalDiskPolicyEnum => "physical_disk_policy",
    PhysicalDiskStateEnum => "physical_disk_state",
    ProducerKindEnum => "producer_kind",
    ReadOnlyTargetReplacementTypeEnum => "read_only_target_replacement_type",
    RegionReplacementStateEnum => "region_replacement_state",
    RegionReplacementStepTypeEnum => "region_replacement_step_type",
    RegionReservationPercentEnum => "region_reservation_percent",
    RegionSnapshotReplacementStateEnum => "region_snapshot_replacement_state",
    RegionSnapshotReplacementStepStateEnum => "region_snapshot_replacement_step_state",
    RotImageErrorEnum => "rot_image_error",
    RotPageWhichEnum => "root_of_trust_page_which",
    RouterRouteKindEnum => "router_route_kind",
    SagaStateEnum => "saga_state",
    ServiceKindEnum => "service_kind",
    SledCpuFamilyEnum => "sled_cpu_family",
    SledPolicyEnum => "sled_policy",
    SledRoleEnum => "sled_role",
    SledStateEnum => "sled_state",
    SnapshotStateEnum => "snapshot_state",
    SpTypeEnum => "sp_type",
    SupportBundleStateEnum => "support_bundle_state",
    SwitchInterfaceKindEnum => "switch_interface_kind",
    SwitchLinkFecEnum => "switch_link_fec",
    SwitchLinkSpeedEnum => "switch_link_speed",
    SwitchPortGeometryEnum => "switch_port_geometry",
    TargetReleaseSourceEnum => "target_release_source",
    UpstairsRepairNotificationTypeEnum => "upstairs_repair_notification_type",
    UpstairsRepairTypeEnum => "upstairs_repair_type",
    UserDataExportResourceTypeEnum => "user_data_export_resource_type",
    UserDataExportStateEnum => "user_data_export_state",
    UserProvisionTypeEnum => "user_provision_type",
    VmmCpuPlatformEnum => "vmm_cpu_platform",
    VmmStateEnum => "vmm_state",
    VolumeResourceUsageTypeEnum => "volume_resource_usage_type",
    VpcFirewallRuleActionEnum => "vpc_firewall_rule_action",
    VpcFirewallRuleDirectionEnum => "vpc_firewall_rule_direction",
    VpcFirewallRuleStatusEnum => "vpc_firewall_rule_status",
    VpcRouterKindEnum => "vpc_router_kind",
    WebhookDeliveryAttemptResultEnum => "webhook_delivery_attempt_result",
    ZoneTypeEnum => "zone_type",
}

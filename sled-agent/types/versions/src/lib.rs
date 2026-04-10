// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Versioned types for the Sled Agent API.
//!
//! # Adding a new API version
//!
//! When adding a new API version N with added or changed types:
//!
//! 1. Create `<version_name>/mod.rs`, where `<version_name>` is the lowercase
//!    form of the new version's identifier, as defined in the API trait's
//!    `api_versions!` macro.
//!
//! 2. Add to the end of this list:
//!
//!    ```rust,ignore
//!    #[path = "<version_name>/mod.rs"]
//!    pub mod vN;
//!    ```
//!
//! 3. Add your types to the new module, mirroring the module structure from
//!    earlier versions.
//!
//! 4. Update `latest.rs` with new and updated types from the new version.
//!
//! For more information, see the [detailed guide] and [RFD 619].
//!
//! [detailed guide]: https://github.com/oxidecomputer/dropshot-api-manager/blob/main/guides/new-version.md
//! [RFD 619]: https://rfd.shared.oxide.computer/rfd/619

mod impls;
pub mod latest;
#[path = "initial/mod.rs"]
pub mod v1;
#[path = "add_dual_stack_shared_network_interfaces/mod.rs"]
pub mod v10;
#[path = "add_dual_stack_external_ip_config/mod.rs"]
pub mod v11;
#[path = "add_health_monitor/mod.rs"]
pub mod v12;
#[path = "add_trust_quorum/mod.rs"]
pub mod v13;
#[path = "measurements/mod.rs"]
pub mod v14;
#[path = "add_trust_quorum_status/mod.rs"]
pub mod v15;
#[path = "measurement_proper_inventory/mod.rs"]
pub mod v16;
#[path = "two_types_of_delegated_zvol/mod.rs"]
pub mod v17;
#[path = "add_attached_subnets/mod.rs"]
pub mod v18;
#[path = "add_rot_attestation/mod.rs"]
pub mod v19;
#[path = "bgp_v6/mod.rs"]
pub mod v20;
#[path = "remove_health_monitor_keep_checks/mod.rs"]
pub mod v22;
#[path = "add_zpool_health_to_inventory/mod.rs"]
pub mod v24;
#[path = "bootstore_versioning/mod.rs"]
pub mod v25;
#[path = "rack_network_config_not_optional/mod.rs"]
pub mod v26;
#[path = "modify_services_in_inventory/mod.rs"]
pub mod v28;
#[path = "add_vsock_component/mod.rs"]
pub mod v29;
#[path = "add_switch_zone_operator_policy/mod.rs"]
pub mod v3;
#[path = "stronger_bgp_unnumbered_types/mod.rs"]
pub mod v30;
#[path = "add_icmpv6_firewall_support/mod.rs"]
pub mod v31;
#[path = "make_all_external_ip_fields_optional/mod.rs"]
pub mod v32;
#[path = "bootstore_service_nat/mod.rs"]
pub mod v33;
#[path = "modify_svcs_error/mod.rs"]
pub mod v34;
#[path = "add_nexus_lockstep_port_to_inventory/mod.rs"]
pub mod v4;
#[path = "add_probe_put_endpoint/mod.rs"]
pub mod v6;
#[path = "multicast_support/mod.rs"]
pub mod v7;
#[path = "delegate_zvol_to_propolis/mod.rs"]
pub mod v9;

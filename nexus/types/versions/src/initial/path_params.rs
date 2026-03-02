// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Path parameter types for version INITIAL.

use omicron_common::api::external::NameOrId;
use omicron_uuid_kinds::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! path_param {
    ($struct:ident, $param:ident, $name:tt) => {
        #[derive(Serialize, Deserialize, JsonSchema)]
        pub struct $struct {
            #[doc = "Name or ID of the "]
            #[doc = $name]
            pub $param: NameOrId,
        }
    };
}

macro_rules! id_path_param {
    ($struct:ident, $param:ident, $name:tt) => {
        id_path_param!($struct, $param, $name, Uuid);
    };

    ($struct:ident, $param:ident, $name:tt, $uuid_type:ident) => {
        #[derive(Serialize, Deserialize, JsonSchema)]
        pub struct $struct {
            #[doc = "ID of the "]
            #[doc = $name]
            #[schemars(with = "Uuid")]
            pub $param: $uuid_type,
        }
    };
}

path_param!(AffinityGroupPath, affinity_group, "affinity group");
path_param!(AntiAffinityGroupPath, anti_affinity_group, "anti affinity group");
path_param!(MulticastGroupPath, multicast_group, "multicast group");
path_param!(ProjectPath, project, "project");
path_param!(InstancePath, instance, "instance");
path_param!(NetworkInterfacePath, interface, "network interface");
path_param!(VpcPath, vpc, "VPC");
path_param!(SubnetPath, subnet, "subnet");
path_param!(RouterPath, router, "router");
path_param!(RoutePath, route, "route");
path_param!(InternetGatewayPath, gateway, "gateway");
path_param!(FloatingIpPath, floating_ip, "floating IP");
path_param!(DiskPath, disk, "disk");
path_param!(SnapshotPath, snapshot, "snapshot");
path_param!(ImagePath, image, "image");
path_param!(SiloPath, silo, "silo");
path_param!(ProviderPath, provider, "SAML identity provider");
path_param!(IpPoolPath, pool, "IP pool");
path_param!(IpAddressPath, address, "IP address");
path_param!(SshKeyPath, ssh_key, "SSH key");
path_param!(AddressLotPath, address_lot, "address lot");
path_param!(ProbePath, probe, "probe");
path_param!(CertificatePath, certificate, "certificate");

id_path_param!(GroupPath, group_id, "group", SiloGroupUuid);
id_path_param!(UserPath, user_id, "user", SiloUserUuid);
id_path_param!(TokenPath, token_id, "token");
id_path_param!(TufTrustRootPath, trust_root_id, "trust root");

// TODO: The hardware resources should be represented by its UUID or a hardware
// ID that can be used to deterministically generate the UUID.
id_path_param!(RackPath, rack_id, "rack");
id_path_param!(SledPath, sled_id, "sled", SledUuid);
id_path_param!(SwitchPath, switch_id, "switch");
id_path_param!(PhysicalDiskPath, disk_id, "physical disk");

// Internal API parameters
id_path_param!(BlueprintPath, blueprint_id, "blueprint");

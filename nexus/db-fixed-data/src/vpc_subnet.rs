// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_model::VpcSubnet;
use omicron_common::address::{
    DNS_OPTE_IPV4_SUBNET, DNS_OPTE_IPV6_SUBNET, NEXUS_OPTE_IPV4_SUBNET,
    NEXUS_OPTE_IPV6_SUBNET, NTP_OPTE_IPV4_SUBNET, NTP_OPTE_IPV6_SUBNET,
};
use omicron_common::api::external::IdentityMetadataCreateParams;
use once_cell::sync::Lazy;

/// UUID of built-in VPC Subnet for External DNS.
pub static DNS_VPC_SUBNET_ID: Lazy<uuid::Uuid> = Lazy::new(|| {
    "001de000-c470-4000-8000-000000000001"
        .parse()
        .expect("invalid uuid for builtin external dns vpc subnet id")
});

/// UUID of built-in VPC Subnet for Nexus.
pub static NEXUS_VPC_SUBNET_ID: Lazy<uuid::Uuid> = Lazy::new(|| {
    "001de000-c470-4000-8000-000000000002"
        .parse()
        .expect("invalid uuid for builtin nexus vpc subnet id")
});

/// UUID of built-in VPC Subnet for Boundary NTP.
pub static NTP_VPC_SUBNET_ID: Lazy<uuid::Uuid> = Lazy::new(|| {
    "001de000-c470-4000-8000-000000000003"
        .parse()
        .expect("invalid uuid for builtin boundary ntp vpc subnet id")
});

/// Built-in VPC Subnet for External DNS.
pub static DNS_VPC_SUBNET: Lazy<VpcSubnet> = Lazy::new(|| {
    VpcSubnet::new(
        *DNS_VPC_SUBNET_ID,
        *super::vpc::SERVICES_VPC_ID,
        IdentityMetadataCreateParams {
            name: "external-dns".parse().unwrap(),
            description: "Built-in VPC Subnet for Oxide service (external-dns)"
                .to_string(),
        },
        *DNS_OPTE_IPV4_SUBNET,
        *DNS_OPTE_IPV6_SUBNET,
    )
});

/// Built-in VPC Subnet for Nexus.
pub static NEXUS_VPC_SUBNET: Lazy<VpcSubnet> = Lazy::new(|| {
    VpcSubnet::new(
        *NEXUS_VPC_SUBNET_ID,
        *super::vpc::SERVICES_VPC_ID,
        IdentityMetadataCreateParams {
            name: "nexus".parse().unwrap(),
            description: "Built-in VPC Subnet for Oxide service (nexus)"
                .to_string(),
        },
        *NEXUS_OPTE_IPV4_SUBNET,
        *NEXUS_OPTE_IPV6_SUBNET,
    )
});

/// Built-in VPC Subnet for Boundary NTP.
pub static NTP_VPC_SUBNET: Lazy<VpcSubnet> = Lazy::new(|| {
    VpcSubnet::new(
        *NTP_VPC_SUBNET_ID,
        *super::vpc::SERVICES_VPC_ID,
        IdentityMetadataCreateParams {
            name: "boundary-ntp".parse().unwrap(),
            description: "Built-in VPC Subnet for Oxide service (boundary-ntp)"
                .to_string(),
        },
        *NTP_OPTE_IPV4_SUBNET,
        *NTP_OPTE_IPV6_SUBNET,
    )
});

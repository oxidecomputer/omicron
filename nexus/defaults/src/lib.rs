// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Default values for data in the Nexus API, when not provided explicitly in a request.

use omicron_common::api::external;
use omicron_common::api::external::L4Port;
use omicron_common::api::external::Name;
use omicron_common::api::external::VpcFirewallRuleAction;
use omicron_common::api::external::VpcFirewallRuleDirection;
use omicron_common::api::external::VpcFirewallRuleFilter;
use omicron_common::api::external::VpcFirewallRuleHostFilter;
use omicron_common::api::external::VpcFirewallRulePriority;
use omicron_common::api::external::VpcFirewallRuleProtocol;
use omicron_common::api::external::VpcFirewallRuleStatus;
use omicron_common::api::external::VpcFirewallRuleTarget;
use omicron_common::api::external::VpcFirewallRuleUpdate;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use rand::TryRngCore;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::LazyLock;

/// The name provided for a default primary network interface for a guest
/// instance.
pub const DEFAULT_PRIMARY_NIC_NAME: &str = "net0";

/// The default IPv4 subnet range assigned to the default VPC Subnet, when
/// the VPC is created, if one is not provided in the request. See
/// <https://rfd.shared.oxide.computer/rfd/0021> for details.
pub static DEFAULT_VPC_SUBNET_IPV4_BLOCK: LazyLock<Ipv4Net> =
    LazyLock::new(|| Ipv4Net::new(Ipv4Addr::new(172, 30, 0, 0), 22).unwrap());

pub static DEFAULT_FIREWALL_RULES: LazyLock<VpcFirewallRuleUpdateParams> =
    LazyLock::new(|| {
        let default: Name = "default".parse().unwrap();
        let targets = vec![VpcFirewallRuleTarget::Vpc(default.clone())];
        VpcFirewallRuleUpdateParams {
            rules: vec![
                VpcFirewallRuleUpdate {
                    name: "allow-internal-inbound".parse().unwrap(),
                    description:
                        "allow inbound traffic to all instances within the VPC if originated within the VPC"
                            .to_string(),
                    status: VpcFirewallRuleStatus::Enabled,
                    direction: VpcFirewallRuleDirection::Inbound,
                    targets: targets.clone(),
                    filters: VpcFirewallRuleFilter {
                        hosts: Some(vec![VpcFirewallRuleHostFilter::Vpc(default)]),
                        protocols: None,
                        ports: None,
                    },
                    action: VpcFirewallRuleAction::Allow,
                    priority: VpcFirewallRulePriority(65534),
                },
                VpcFirewallRuleUpdate {
                    name: "allow-ssh".parse().unwrap(),
                    description:
                        "allow inbound TCP connections on port 22 from anywhere"
                            .to_string(),
                    status: VpcFirewallRuleStatus::Enabled,
                    direction: VpcFirewallRuleDirection::Inbound,
                    targets: targets.clone(),
                    filters: VpcFirewallRuleFilter {
                        hosts: None,
                        protocols: Some(vec![VpcFirewallRuleProtocol::Tcp]),
                        ports: Some(vec![L4Port::try_from(22u16).unwrap().into()]),
                    },
                    action: VpcFirewallRuleAction::Allow,
                    priority: VpcFirewallRulePriority(65534),
                },
                VpcFirewallRuleUpdate {
                    name: "allow-icmp".parse().unwrap(),
                    description:
                        "allow inbound ICMP traffic from anywhere"
                            .to_string(),
                    status: VpcFirewallRuleStatus::Enabled,
                    direction: VpcFirewallRuleDirection::Inbound,
                    targets,
                    filters: VpcFirewallRuleFilter {
                        hosts: None,
                        protocols: Some(vec![VpcFirewallRuleProtocol::Icmp(None)]),
                        ports: None,
                    },
                    action: VpcFirewallRuleAction::Allow,
                    priority: VpcFirewallRulePriority(65534),
                },
            ]
        }
    });

/// Generate a random VPC IPv6 prefix, in the range `fd00::/48`.
pub fn random_vpc_ipv6_prefix() -> Result<Ipv6Net, external::Error> {
    let mut bytes = [0u8; 16];
    bytes[0] = 0xfd;
    rand::rng().try_fill_bytes(&mut bytes[1..6]).map_err(|_| {
        external::Error::internal_error(
            "Unable to allocate random IPv6 address range",
        )
    })?;
    Ok(Ipv6Net::new(
        Ipv6Addr::from(bytes),
        omicron_common::address::VPC_IPV6_PREFIX_LENGTH,
    )
    .unwrap())
}

#[cfg(test)]
mod tests {
    use omicron_common::api::external::Ipv6NetExt;

    use super::*;

    #[test]
    fn test_random_vpc_ipv6_prefix() {
        let network = random_vpc_ipv6_prefix().unwrap();
        assert!(network.is_vpc_prefix());
        let octets = network.prefix().octets();
        assert!(octets[6..].iter().all(|x| *x == 0));
    }
}

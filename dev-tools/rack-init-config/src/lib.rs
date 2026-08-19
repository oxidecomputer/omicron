// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Construct a [`RackInitializeRequest`] and render the `config-rss.toml`
//! sled-agent parses.
//!
//! This is the sanctioned surface for rack-init automation outside this
//! repository, consumed as a git dependency pinned to an omicron commit.

pub use bootstrap_agent_lockstep_types::BootstrapAddressDiscovery;
pub use bootstrap_agent_lockstep_types::RackInitializeRequest;
pub use bootstrap_agent_lockstep_types::RackOperationStatus;
pub use bootstrap_agent_lockstep_types::RecoverySiloConfig;
pub use bootstrap_agent_lockstep_types::RssStep;
pub use bootstrap_agent_lockstep_types::ServiceIpPoolConfig;
pub use bootstrap_agent_lockstep_types::ServiceIpPoolError;
pub use iddqd::IdOrdMap;
pub use omicron_common::address::IpRange;
pub use omicron_common::address::Ipv4Range;
pub use omicron_common::address::Ipv6Range;
pub use omicron_common::api::external::AllowedSourceIps;
pub use omicron_common::api::external::Name;
pub use omicron_common::api::external::UserId;
pub use omicron_common::api::internal::nexus::Certificate;
pub use omicron_passwords::NewPasswordHash;
pub use oxnet;
pub use sled_agent_types::early_networking::BfdMode;
pub use sled_agent_types::early_networking::BfdPeerConfig;
pub use sled_agent_types::early_networking::BgpConfig;
pub use sled_agent_types::early_networking::BgpPeerConfig;
pub use sled_agent_types::early_networking::EmptyUplinkPortsError;
pub use sled_agent_types::early_networking::ImportExportPolicy;
pub use sled_agent_types::early_networking::LinkFec;
pub use sled_agent_types::early_networking::LinkSpeed;
pub use sled_agent_types::early_networking::LldpAdminStatus;
pub use sled_agent_types::early_networking::LldpPortConfig;
pub use sled_agent_types::early_networking::MaxPathConfig;
pub use sled_agent_types::early_networking::PortConfig;
pub use sled_agent_types::early_networking::RackNetworkConfig;
pub use sled_agent_types::early_networking::RouteConfig;
pub use sled_agent_types::early_networking::RouterLifetimeConfig;
pub use sled_agent_types::early_networking::RouterPeerIpAddr;
pub use sled_agent_types::early_networking::RouterPeerType;
pub use sled_agent_types::early_networking::SwitchSlot;
pub use sled_agent_types::early_networking::TxEqConfig;
pub use sled_agent_types::early_networking::UplinkAddress;
pub use sled_agent_types::early_networking::UplinkAddressConfig;
pub use sled_agent_types::early_networking::UplinkIpNet;
pub use sled_agent_types::early_networking::UplinkPorts;
pub use sled_hardware_types::BaseboardId;

/// Serialize a request in the `config-rss.toml` format sled-agent parses.
pub fn to_config_rss_toml(
    request: &RackInitializeRequest,
) -> Result<String, toml::ser::Error> {
    toml::to_string(request)
}

#[cfg(test)]
mod tests {
    use super::*;

    use sled_agent_rack_setup::rack_initialize_request_from_file;
    use std::collections::BTreeSet;
    use std::net::IpAddr;
    use std::net::Ipv6Addr;

    // The hash for the password "oxide", copied from
    // smf/sled-agent/non-gimlet/config-rss.toml.
    const PASSWORD_HASH: &str = "$argon2id$v=19$m=98304,t=23,p=1$Effh/p6M2ZKdnpJFeGqtGQ$ZtUwcVODAvUAVK6EQ5FJMv+GMlUCo9PQQsy9cagL+EU";

    fn recovery_silo() -> RecoverySiloConfig {
        RecoverySiloConfig {
            silo_name: "recovery".parse().unwrap(),
            user_name: "recovery".parse().unwrap(),
            user_password_hash: PASSWORD_HASH.parse().unwrap(),
        }
    }

    fn service_ip_pools() -> IdOrdMap<ServiceIpPoolConfig> {
        let range = IpRange::V4(
            Ipv4Range::new(
                "198.51.100.20".parse().unwrap(),
                "198.51.100.29".parse().unwrap(),
            )
            .unwrap(),
        );
        let pool = ServiceIpPoolConfig::new(
            "oxide-service-pool-v4".parse().unwrap(),
            "IPv4 IP Pool for Oxide Services".to_string(),
            vec![range],
        )
        .unwrap();
        IdOrdMap::from_iter_unique([pool]).unwrap()
    }

    fn trust_quorum_peers() -> Vec<BaseboardId> {
        (0..3)
            .map(|index| BaseboardId {
                part_number: "913-0000019".to_string(),
                serial_number: format!("2FAKE00{index}"),
            })
            .collect()
    }

    fn bootstrap_addrs() -> BTreeSet<Ipv6Addr> {
        (0..3u16)
            .map(|index| {
                format!("fdb0:a840:2500:{}::1", 2 * index + 1).parse().unwrap()
            })
            .collect()
    }

    fn lldp(port_description: &str) -> LldpPortConfig {
        LldpPortConfig {
            status: LldpAdminStatus::Enabled,
            chassis_id: Some("switch0".to_string()),
            port_id: None,
            port_description: Some(port_description.to_string()),
            system_name: None,
            system_description: None,
            management_addrs: None,
        }
    }

    // A static-routed uplink with single-hop BFD toward the gateway.
    fn static_bfd_request() -> RackInitializeRequest {
        let port = PortConfig {
            routes: vec![RouteConfig {
                destination: "0.0.0.0/0".parse().unwrap(),
                nexthop: "203.0.113.9".parse().unwrap(),
                vlan_id: None,
                rib_priority: None,
            }],
            addresses: vec![UplinkAddressConfig {
                address: UplinkAddress::Static {
                    ip_net: "203.0.113.10/30".parse().unwrap(),
                },
                vlan_id: None,
            }],
            switch: SwitchSlot::Switch0,
            port: "qsfp0".to_string(),
            uplink_port_speed: LinkSpeed::Speed40G,
            uplink_port_fec: Some(LinkFec::None),
            bgp_peers: vec![],
            autoneg: false,
            lldp: Some(lldp("uplink-0")),
            tx_eq: None,
        };
        RackInitializeRequest {
            trust_quorum_peers: Some(trust_quorum_peers()),
            bootstrap_discovery: BootstrapAddressDiscovery::OnlyThese {
                addrs: bootstrap_addrs(),
            },
            ntp_servers: vec!["ntp.eng.oxide.computer".to_string()],
            dns_servers: vec!["198.51.100.1".parse().unwrap()],
            service_ip_pools: service_ip_pools(),
            external_dns_ips: vec!["198.51.100.20".parse().unwrap()],
            external_dns_zone_name: "oxide.example".to_string(),
            external_certificates: vec![],
            recovery_silo: recovery_silo(),
            rack_network_config: RackNetworkConfig {
                rack_subnet: "fd00:1122:3344:0100::/56".parse().unwrap(),
                infra_ip_first: "203.0.113.8".parse().unwrap(),
                infra_ip_last: "203.0.113.15".parse().unwrap(),
                ports: UplinkPorts::new(vec![port]).unwrap(),
                bgp: vec![],
                bfd: vec![BfdPeerConfig {
                    local: Some("203.0.113.10".parse().unwrap()),
                    remote: "203.0.113.9".parse().unwrap(),
                    detection_threshold: 3,
                    required_rx: 1_000_000,
                    mode: BfdMode::SingleHop,
                    switch: SwitchSlot::Switch0,
                }],
            },
            allowed_source_ips: AllowedSourceIps::Any,
            external_jumbo_frames_opt_in_enabled: false,
        }
    }

    // An addrconf uplink with an unnumbered eBGP peer.
    fn bgp_request() -> RackInitializeRequest {
        let port = PortConfig {
            routes: vec![],
            addresses: vec![UplinkAddressConfig {
                address: UplinkAddress::AddrConf,
                vlan_id: None,
            }],
            switch: SwitchSlot::Switch0,
            port: "qsfp0".to_string(),
            uplink_port_speed: LinkSpeed::Speed40G,
            uplink_port_fec: Some(LinkFec::None),
            bgp_peers: vec![BgpPeerConfig {
                asn: 65002,
                port: "qsfp0".to_string(),
                addr: RouterPeerType::Unnumbered {
                    router_lifetime: RouterLifetimeConfig::new(3600).unwrap(),
                },
                hold_time: None,
                idle_hold_time: None,
                delay_open: None,
                connect_retry: None,
                keepalive: None,
                remote_asn: None,
                min_ttl: None,
                md5_auth_key: None,
                multi_exit_discriminator: None,
                communities: vec![],
                local_pref: None,
                enforce_first_as: false,
                allowed_import: ImportExportPolicy::NoFiltering,
                allowed_export: ImportExportPolicy::NoFiltering,
                vlan_id: None,
            }],
            autoneg: false,
            lldp: Some(lldp("uplink-0")),
            tx_eq: None,
        };
        let mut request = static_bfd_request();
        request.rack_network_config = RackNetworkConfig {
            rack_subnet: "fd00:1122:3344:0100::/56".parse().unwrap(),
            infra_ip_first: IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            infra_ip_last: IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            ports: UplinkPorts::new(vec![port]).unwrap(),
            bgp: vec![BgpConfig {
                asn: 65001,
                originate: vec!["198.51.100.0/24".parse().unwrap()],
                shaper: None,
                checker: None,
                max_paths: MaxPathConfig::default(),
            }],
            bfd: vec![],
        };
        request
    }

    fn assert_round_trips(request: &RackInitializeRequest) {
        let toml_text = to_config_rss_toml(request).expect("serializes");
        let dir = camino_tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config-rss.toml");
        std::fs::write(&path, &toml_text).expect("write config-rss.toml");
        let parsed = rack_initialize_request_from_file(&path)
            .expect("sled-agent parses the generated config-rss.toml");
        assert_eq!(request, &parsed);
    }

    #[test]
    fn static_bfd_request_round_trips_through_sled_agent() {
        assert_round_trips(&static_bfd_request());
    }

    #[test]
    fn bgp_request_round_trips_through_sled_agent() {
        assert_round_trips(&bgp_request());
    }

    #[test]
    fn non_gimlet_example_round_trips_through_sled_agent() {
        let example = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../smf/sled-agent/non-gimlet/config-rss.toml"
        );
        let parsed = rack_initialize_request_from_file(example)
            .expect("parse the in-tree example config-rss.toml");
        assert_round_trips(&parsed);
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for multicast types.

use oxnet::{Ipv4Net, Ipv6Net};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Validate that an IP address is suitable for use as a SSM source.
///
/// For specifics, follow-up on RFC 4607:
/// <https://www.rfc-editor.org/rfc/rfc4607>
pub fn validate_source_ip(ip: IpAddr) -> Result<(), String> {
    match ip {
        IpAddr::V4(ipv4) => validate_ipv4_source(ipv4),
        IpAddr::V6(ipv6) => validate_ipv6_source(ipv6),
    }
}

/// Validate that an IPv4 address is suitable for use as a multicast source.
fn validate_ipv4_source(addr: Ipv4Addr) -> Result<(), String> {
    // Must be a unicast address
    if !is_unicast_v4(&addr) {
        return Err(format!("{} is not a unicast address", addr));
    }

    // Exclude problematic addresses (mostly align with Dendrite, but block link-local)
    if addr.is_loopback()
        || addr.is_broadcast()
        || addr.is_unspecified()
        || addr.is_link_local()
    {
        return Err(format!("{} is a special-use address", addr));
    }

    Ok(())
}

/// Validate that an IPv6 address is suitable for use as a multicast source.
fn validate_ipv6_source(addr: Ipv6Addr) -> Result<(), String> {
    // Must be a unicast address
    if !is_unicast_v6(&addr) {
        return Err(format!("{} is not a unicast address", addr));
    }

    // Exclude problematic addresses (align with Dendrite validation, but block link-local)
    if addr.is_loopback()
        || addr.is_unspecified()
        || ((addr.segments()[0] & 0xffc0) == 0xfe80)
    // fe80::/10 link-local
    {
        return Err(format!("{} is a special-use address", addr));
    }

    Ok(())
}

/// Validate that an IP address is a proper multicast address for API validation.
pub fn validate_multicast_ip(ip: IpAddr) -> Result<(), String> {
    match ip {
        IpAddr::V4(ipv4) => validate_ipv4_multicast(ipv4),
        IpAddr::V6(ipv6) => validate_ipv6_multicast(ipv6),
    }
}

// IPv4 link-local multicast range reserved for local network control.
const RESERVED_IPV4_MULTICAST_LINK_LOCAL: Ipv4Addr =
    Ipv4Addr::new(224, 0, 0, 0);
const RESERVED_IPV4_MULTICAST_LINK_LOCAL_PREFIX: u8 = 24;

/// Validates IPv4 multicast addresses.
fn validate_ipv4_multicast(addr: Ipv4Addr) -> Result<(), String> {
    // Verify this is actually a multicast address
    if !addr.is_multicast() {
        return Err(format!("{} is not a multicast address", addr));
    }

    // Block link-local multicast (224.0.0.0/24) as it's reserved for local network control
    let link_local = Ipv4Net::new(
        RESERVED_IPV4_MULTICAST_LINK_LOCAL,
        RESERVED_IPV4_MULTICAST_LINK_LOCAL_PREFIX,
    )
    .unwrap();
    if link_local.contains(addr) {
        return Err(format!(
            "{addr} is in the link-local multicast range (224.0.0.0/24)"
        ));
    }

    Ok(())
}

/// Validates IPv6 multicast addresses.
fn validate_ipv6_multicast(addr: Ipv6Addr) -> Result<(), String> {
    if !addr.is_multicast() {
        return Err(format!("{addr} is not a multicast address"));
    }

    // Define reserved IPv6 multicast subnets using oxnet
    let reserved_subnets = [
        // Interface-local scope (ff01::/16)
        Ipv6Net::new(Ipv6Addr::new(0xff01, 0, 0, 0, 0, 0, 0, 0), 16).unwrap(),
        // Link-local scope (ff02::/16)
        Ipv6Net::new(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 0), 16).unwrap(),
    ];

    // Check reserved subnets
    for subnet in &reserved_subnets {
        if subnet.contains(addr) {
            return Err(format!(
                "{} is in the reserved multicast subnet {}",
                addr, subnet
            ));
        }
    }

    // Note: Admin-local scope (ff04::/16) is allowed for on-premises deployments.
    // Collision avoidance with underlay addresses is handled by the mapping
    // function which sets a collision-avoidance bit in the underlay space.

    Ok(())
}

const fn is_unicast_v4(ip: &Ipv4Addr) -> bool {
    !ip.is_multicast()
}

const fn is_unicast_v6(ip: &Ipv6Addr) -> bool {
    !ip.is_multicast()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::latest::multicast::{
        MulticastGroupCreate, MulticastGroupUpdate,
    };
    use omicron_common::api::external::Nullable;
    use omicron_common::vlan::VlanID;

    #[test]
    fn test_validate_multicast_ip_v4() {
        // Valid IPv4 multicast addresses
        assert!(
            validate_multicast_ip(IpAddr::V4(Ipv4Addr::new(224, 1, 0, 1)))
                .is_ok()
        );
        assert!(
            validate_multicast_ip(IpAddr::V4(Ipv4Addr::new(225, 2, 3, 4)))
                .is_ok()
        );
        assert!(
            validate_multicast_ip(IpAddr::V4(Ipv4Addr::new(231, 5, 6, 7)))
                .is_ok()
        );
        assert!(
            validate_multicast_ip(IpAddr::V4(Ipv4Addr::new(233, 1, 1, 1)))
                .is_ok()
        ); // GLOP addressing - allowed
        assert!(
            validate_multicast_ip(IpAddr::V4(Ipv4Addr::new(239, 1, 1, 1)))
                .is_ok()
        ); // Admin-scoped - allowed

        // Invalid IPv4 multicast addresses - reserved ranges
        assert!(
            validate_multicast_ip(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)))
                .is_err()
        ); // Link-local control
        assert!(
            validate_multicast_ip(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 255)))
                .is_err()
        ); // Link-local control

        // Non-multicast addresses
        assert!(
            validate_multicast_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)))
                .is_err()
        );
        assert!(
            validate_multicast_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)))
                .is_err()
        );
    }

    #[test]
    fn test_validate_multicast_ip_v6() {
        // Valid IPv6 multicast addresses
        assert!(
            validate_multicast_ip(IpAddr::V6(Ipv6Addr::new(
                0xff0e, 0, 0, 0, 0, 0, 0, 1
            )))
            .is_ok()
        ); // Global scope
        assert!(
            validate_multicast_ip(IpAddr::V6(Ipv6Addr::new(
                0xff0d, 0, 0, 0, 0, 0, 0, 1
            )))
            .is_ok()
        ); // Site-local scope
        assert!(
            validate_multicast_ip(IpAddr::V6(Ipv6Addr::new(
                0xff05, 0, 0, 0, 0, 0, 0, 1
            )))
            .is_ok()
        ); // Site-local admin scope - allowed
        assert!(
            validate_multicast_ip(IpAddr::V6(Ipv6Addr::new(
                0xff08, 0, 0, 0, 0, 0, 0, 1
            )))
            .is_ok()
        ); // Org-local admin scope - allowed

        // Invalid IPv6 multicast addresses - reserved ranges
        assert!(
            validate_multicast_ip(IpAddr::V6(Ipv6Addr::new(
                0xff01, 0, 0, 0, 0, 0, 0, 1
            )))
            .is_err()
        ); // Interface-local
        assert!(
            validate_multicast_ip(IpAddr::V6(Ipv6Addr::new(
                0xff02, 0, 0, 0, 0, 0, 0, 1
            )))
            .is_err()
        ); // Link-local

        // Admin-local (ff04::/16) is allowed for on-premises deployments.
        // Collision avoidance is handled by the mapping function which sets
        // a collision-avoidance bit to separate external and underlay spaces.
        assert!(
            validate_multicast_ip(IpAddr::V6(Ipv6Addr::new(
                0xff04, 0, 0, 0, 0, 0, 0, 1
            )))
            .is_ok()
        );

        // Non-multicast addresses
        assert!(
            validate_multicast_ip(IpAddr::V6(Ipv6Addr::new(
                0x2001, 0xdb8, 0, 0, 0, 0, 0, 1
            )))
            .is_err()
        );
    }

    #[test]
    fn test_validate_source_ip_v4() {
        // Valid IPv4 source addresses
        assert!(
            validate_source_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)))
                .is_ok()
        );
        assert!(
            validate_source_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))).is_ok()
        );
        assert!(
            validate_source_ip(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)))
                .is_ok()
        ); // TEST-NET-3

        // Invalid IPv4 source addresses
        assert!(
            validate_source_ip(IpAddr::V4(Ipv4Addr::new(224, 1, 1, 1)))
                .is_err()
        ); // Multicast
        assert!(
            validate_source_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))).is_err()
        ); // Unspecified
        assert!(
            validate_source_ip(IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)))
                .is_err()
        ); // Broadcast
        assert!(
            validate_source_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)))
                .is_err()
        ); // Link-local
    }

    #[test]
    fn test_validate_source_ip_v6() {
        // Valid IPv6 source addresses
        assert!(
            validate_source_ip(IpAddr::V6(Ipv6Addr::new(
                0x2001, 0xdb8, 0, 0, 0, 0, 0, 1
            )))
            .is_ok()
        );
        assert!(
            validate_source_ip(IpAddr::V6(Ipv6Addr::new(
                0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888
            )))
            .is_ok()
        );

        // Invalid IPv6 source addresses
        assert!(
            validate_source_ip(IpAddr::V6(Ipv6Addr::new(
                0xff0e, 0, 0, 0, 0, 0, 0, 1
            )))
            .is_err()
        ); // Multicast
        assert!(
            validate_source_ip(IpAddr::V6(Ipv6Addr::new(
                0, 0, 0, 0, 0, 0, 0, 0
            )))
            .is_err()
        ); // Unspecified
        assert!(
            validate_source_ip(IpAddr::V6(Ipv6Addr::new(
                0, 0, 0, 0, 0, 0, 0, 1
            )))
            .is_err()
        ); // Loopback
    }

    #[test]
    fn test_multicast_group_create_deserialization_with_all_fields() {
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "multicast_ip": "224.1.2.3",
            "source_ips": ["10.0.0.1", "10.0.0.2"],
            "pool": "default",
            "mvlan": 10
        }"#;

        let result: Result<MulticastGroupCreate, _> =
            serde_json::from_str(json);
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(params.identity.name.as_str(), "test-group");
        assert_eq!(
            params.multicast_ip,
            Some(IpAddr::V4(Ipv4Addr::new(224, 1, 2, 3)))
        );
        assert_eq!(
            params.source_ips,
            Some(vec![
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))
            ])
        );
    }

    #[test]
    fn test_multicast_group_create_deserialization_without_optional_fields() {
        // This is the critical test - multicast_ip, source_ips, pool, and mvlan are all optional
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group"
        }"#;

        let result: Result<MulticastGroupCreate, _> =
            serde_json::from_str(json);
        assert!(
            result.is_ok(),
            "Failed to deserialize without optional fields: {:?}",
            result.err()
        );
        let params = result.unwrap();
        assert_eq!(params.identity.name.as_str(), "test-group");
        assert_eq!(params.multicast_ip, None);
        assert_eq!(params.source_ips, None);
        assert_eq!(params.pool, None);
        assert_eq!(params.mvlan, None);
    }

    #[test]
    fn test_multicast_group_create_deserialization_with_empty_source_ips() {
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "multicast_ip": "224.1.2.3",
            "source_ips": []
        }"#;

        let result: Result<MulticastGroupCreate, _> =
            serde_json::from_str(json);
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(params.source_ips, Some(vec![]));
    }

    #[test]
    fn test_multicast_group_create_deserialization_invalid_multicast_ip() {
        // Non-multicast IP should be rejected
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "multicast_ip": "192.168.1.1"
        }"#;

        let result: Result<MulticastGroupCreate, _> =
            serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_multicast_group_create_deserialization_invalid_source_ip() {
        // Multicast address in source_ips should be rejected
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "multicast_ip": "224.1.2.3",
            "source_ips": ["224.0.0.1"]
        }"#;

        let result: Result<MulticastGroupCreate, _> =
            serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_multicast_group_create_deserialization_only_multicast_ip() {
        // Test with only multicast_ip, no source_ips
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "multicast_ip": "224.1.2.3"
        }"#;

        let result: Result<MulticastGroupCreate, _> =
            serde_json::from_str(json);
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(
            params.multicast_ip,
            Some(IpAddr::V4(Ipv4Addr::new(224, 1, 2, 3)))
        );
        assert_eq!(params.source_ips, None);
    }

    #[test]
    fn test_multicast_group_create_deserialization_only_source_ips() {
        // Test with only source_ips, no multicast_ip (will be auto-allocated)
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "source_ips": ["10.0.0.1"]
        }"#;

        let result: Result<MulticastGroupCreate, _> =
            serde_json::from_str(json);
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(params.multicast_ip, None);
        assert_eq!(
            params.source_ips,
            Some(vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))])
        );
    }

    #[test]
    fn test_multicast_group_create_deserialization_explicit_null_fields() {
        // Test with explicit null values for optional fields
        // This is what the CLI sends when fields are not provided
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "multicast_ip": null,
            "source_ips": null,
            "pool": null,
            "mvlan": null
        }"#;

        let result: Result<MulticastGroupCreate, _> =
            serde_json::from_str(json);
        assert!(
            result.is_ok(),
            "Failed to deserialize with explicit null fields: {:?}",
            result.err()
        );
        let params = result.unwrap();
        assert_eq!(params.multicast_ip, None);
        assert_eq!(params.source_ips, None);
        assert_eq!(params.pool, None);
        assert_eq!(params.mvlan, None);
    }

    #[test]
    fn test_multicast_group_create_deserialization_mixed_null_and_values() {
        // Test with some nulls and some values
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "multicast_ip": "224.1.2.3",
            "source_ips": [],
            "pool": null,
            "mvlan": 30
        }"#;

        let result: Result<MulticastGroupCreate, _> =
            serde_json::from_str(json);
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(
            params.multicast_ip,
            Some(IpAddr::V4(Ipv4Addr::new(224, 1, 2, 3)))
        );
        assert_eq!(params.source_ips, Some(vec![]));
        assert_eq!(params.pool, None);
        assert_eq!(params.mvlan, Some(VlanID::new(30).unwrap()));
    }

    #[test]
    fn test_multicast_group_update_deserialization_omit_all_fields() {
        // When fields are omitted, they should be None (no change)
        let json = r#"{
            "name": "test-group"
        }"#;

        let result: Result<MulticastGroupUpdate, _> =
            serde_json::from_str(json);
        assert!(
            result.is_ok(),
            "Failed to deserialize update with omitted fields: {:?}",
            result.err()
        );
        let params = result.unwrap();
        assert_eq!(params.source_ips, None);
        assert_eq!(params.mvlan, None);
    }

    #[test]
    fn test_multicast_group_update_deserialization_explicit_null_mvlan() {
        // When mvlan is explicitly null, it should be Some(Nullable(None)) (clearing the field)
        let json = r#"{
            "name": "test-group",
            "mvlan": null
        }"#;

        let result: Result<MulticastGroupUpdate, _> =
            serde_json::from_str(json);
        assert!(
            result.is_ok(),
            "Failed to deserialize update with null mvlan: {:?}",
            result.err()
        );
        let params = result.unwrap();
        assert_eq!(params.mvlan, Some(Nullable(None)));
    }

    #[test]
    fn test_multicast_group_update_deserialization_set_mvlan() {
        // When mvlan has a value, it should be Some(Nullable(Some(value)))
        let json = r#"{
            "name": "test-group",
            "mvlan": 100
        }"#;

        let result: Result<MulticastGroupUpdate, _> =
            serde_json::from_str(json);
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(
            params.mvlan,
            Some(Nullable(Some(VlanID::new(100).unwrap())))
        );
    }

    #[test]
    fn test_multicast_group_update_deserialization_update_source_ips() {
        // Test updating source_ips
        let json = r#"{
            "name": "test-group",
            "source_ips": ["10.0.0.5", "10.0.0.6"]
        }"#;

        let result: Result<MulticastGroupUpdate, _> =
            serde_json::from_str(json);
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(
            params.source_ips,
            Some(vec![
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 6))
            ])
        );
    }

    #[test]
    fn test_multicast_group_update_deserialization_clear_source_ips() {
        // Empty array should clear source_ips (Any-Source Multicast)
        let json = r#"{
            "name": "test-group",
            "source_ips": []
        }"#;

        let result: Result<MulticastGroupUpdate, _> =
            serde_json::from_str(json);
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(params.source_ips, Some(vec![]));
    }

    #[test]
    fn test_multicast_group_update_deserialization_invalid_mvlan() {
        // VLAN ID 1 should be rejected (reserved)
        let json = r#"{
            "name": "test-group",
            "mvlan": 1
        }"#;

        let result: Result<MulticastGroupUpdate, _> =
            serde_json::from_str(json);
        assert!(result.is_err(), "Should reject reserved VLAN ID 1");
    }
}

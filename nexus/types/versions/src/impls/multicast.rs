// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for multicast types.

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use crate::latest::multicast::MulticastGroupCreate;
    use crate::latest::multicast::MulticastGroupUpdate;

    #[test]
    fn test_multicast_group_create_deserialization_with_all_fields() {
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "multicast_ip": "224.1.2.3",
            "source_ips": ["10.0.0.1", "10.0.0.2"],
            "pool": "default"
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
        // This is the critical test: multicast_ip, source_ips, and pool are all optional
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
        // Test with explicit null values for optional fields.
        // This is what the CLI sends when fields are not provided.
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "multicast_ip": null,
            "source_ips": null,
            "pool": null
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
    }

    #[test]
    fn test_multicast_group_create_deserialization_mixed_null_and_values() {
        // Test with some nulls and some values
        let json = r#"{
            "name": "test-group",
            "description": "Test multicast group",
            "multicast_ip": "224.1.2.3",
            "source_ips": [],
            "pool": null
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
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00;
use omicron_common::api::external::{IdentityMetadataCreateParams, Name};
use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default resources to create in a VPC
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum VpcCreateDefaultsSelection {
    /// Create all default resources
    All {},

    /// Create only the default resources listed in `defaults`. Pass `{}` as
    /// `defaults` to skip them all.
    Explicit { defaults: VpcCreateDefaults },
}

/// Default resources to create in a VPC
///
/// Each field corresponds to one resource. Set a field to an object to create
/// that resource. Omit it or pass `null` to skip it.
///
/// This does not affect the system router, default firewall rules, or default
/// internet gateway, which are always created and do not block deletion of
/// the VPC.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VpcCreateDefaults {
    /// Create the default subnet. Pass `{}` to create it and omit this field
    /// (or pass `null`) to skip it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subnet: Option<SubnetCreateDefaults>,
}

/// Default resources to create in the default subnet
///
/// Including this object in the request creates the default subnet. A subnet
/// has no default resources yet, so the object is always empty.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubnetCreateDefaults {}

/// Create-time parameters for a `Vpc`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The IPv6 prefix for this VPC
    ///
    /// All IPv6 subnets created from this VPC must be taken from this range,
    /// which should be a Unique Local Address in the range `fd00::/48`. The
    /// default subnet, if requested, will take the first `/64` range from this
    /// prefix.
    pub ipv6_prefix: Option<Ipv6Net>,

    pub dns_name: Name,

    /// Default resources to create in the VPC
    ///
    /// Omit this field  or pass `null`  to create all defaults: currently, the
    /// default subnet. Pass an object to specify which resources to create. `{}`
    /// creates none.
    ///
    /// This does not affect the system router, default firewall rules, or default
    /// internet gateway, which are always created and do not block deletion of
    /// the VPC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<VpcCreateDefaults>,
}

impl From<v2025_11_20_00::vpc::VpcCreate> for VpcCreate {
    fn from(old: v2025_11_20_00::vpc::VpcCreate) -> Self {
        Self {
            identity: old.identity,
            ipv6_prefix: old.ipv6_prefix,
            dns_name: old.dns_name,
            defaults: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SubnetCreateDefaults, VpcCreate, VpcCreateDefaults};
    use serde_json::json;

    #[test]
    fn defaults_wire_format() {
        let base = json!({
            "name": "my-vpc",
            "description": "My VPC",
            "ipv6_prefix": null,
            "dns_name": "my-vpc",
        });

        let omitted: VpcCreate = serde_json::from_value(base.clone()).unwrap();
        assert!(omitted.defaults.is_none());
        assert_eq!(serde_json::to_value(omitted).unwrap(), base);

        let empty: VpcCreate = serde_json::from_value(json!({
            "name": "my-vpc",
            "description": "My VPC",
            "ipv6_prefix": null,
            "dns_name": "my-vpc",
            "defaults": {},
        }))
        .unwrap();
        assert_eq!(empty.defaults.unwrap(), VpcCreateDefaults { subnet: None });

        let subnet: VpcCreate = serde_json::from_value(json!({
            "name": "my-vpc",
            "description": "My VPC",
            "ipv6_prefix": null,
            "dns_name": "my-vpc",
            "defaults": { "subnet": {} },
        }))
        .unwrap();
        assert_eq!(
            subnet.defaults.unwrap(),
            VpcCreateDefaults { subnet: Some(SubnetCreateDefaults {}) }
        );

        let unknown_default = json!({
            "name": "my-vpc",
            "description": "My VPC",
            "ipv6_prefix": null,
            "dns_name": "my-vpc",
            "defaults": { "subnett": {} },
        });
        assert!(serde_json::from_value::<VpcCreate>(unknown_default).is_err());
    }
}

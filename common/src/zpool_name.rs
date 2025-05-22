// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zpool labels and kinds shared between Nexus and Sled Agents

use camino::{Utf8Path, Utf8PathBuf};
use daft::Diffable;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
pub const ZPOOL_EXTERNAL_PREFIX: &str = "oxp_";
pub const ZPOOL_INTERNAL_PREFIX: &str = "oxi_";

/// Describes the different classes of Zpools.
#[derive(
    Copy,
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    JsonSchema,
    Diffable,
)]
#[serde(rename_all = "snake_case")]
pub enum ZpoolKind {
    // This zpool is used for external storage (u.2)
    External,
    // This zpool is used for internal storage (m.2)
    Internal,
}

/// A wrapper around a zpool name.
///
/// This expects that the format will be: `ox{i,p}_<UUID>` - we parse the prefix
/// when reading the structure, and validate that the UUID can be utilized.
#[derive(
    Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Diffable,
)]
#[daft(leaf)]
pub struct ZpoolName {
    id: ZpoolUuid,
    kind: ZpoolKind,
}

const ZPOOL_NAME_REGEX: &str = r"^ox[ip]_[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$";

/// Custom JsonSchema implementation to encode the constraints on Name.
impl JsonSchema for ZpoolName {
    fn schema_name() -> String {
        "ZpoolName".to_string()
    }
    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some(
                    "The name of a Zpool".to_string(),
                ),
                description: Some(
                    "Zpool names are of the format ox{i,p}_<UUID>. They are either \
                     Internal or External, and should be unique"
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(schemars::schema::StringValidation {
                pattern: Some(ZPOOL_NAME_REGEX.to_owned()),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

impl ZpoolName {
    pub const fn new_internal(id: ZpoolUuid) -> Self {
        Self { id, kind: ZpoolKind::Internal }
    }

    pub const fn new_external(id: ZpoolUuid) -> Self {
        Self { id, kind: ZpoolKind::External }
    }

    pub const fn id(&self) -> ZpoolUuid {
        self.id
    }

    pub const fn kind(&self) -> ZpoolKind {
        self.kind
    }

    /// Returns a path to a dataset's mountpoint within the zpool.
    ///
    /// For example: oxp_(UUID) -> /pool/ext/(UUID)/(dataset)
    pub fn dataset_mountpoint(
        &self,
        root: &Utf8Path,
        dataset: &str,
    ) -> Utf8PathBuf {
        let mut path = Utf8PathBuf::new();
        path.push(root);
        path.push("pool");
        match self.kind {
            ZpoolKind::External => path.push("ext"),
            ZpoolKind::Internal => path.push("int"),
        };
        path.push(self.id().to_string());
        path.push(dataset);
        path
    }
}

impl<'de> Deserialize<'de> for ZpoolName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ZpoolName::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Serialize for ZpoolName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl FromStr for ZpoolName {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(s) = s.strip_prefix(ZPOOL_EXTERNAL_PREFIX) {
            let id = ZpoolUuid::from_str(s).map_err(|e| e.to_string())?;
            Ok(ZpoolName::new_external(id))
        } else if let Some(s) = s.strip_prefix(ZPOOL_INTERNAL_PREFIX) {
            let id = ZpoolUuid::from_str(s).map_err(|e| e.to_string())?;
            Ok(ZpoolName::new_internal(id))
        } else {
            Err(format!(
                "Bad zpool name {s}; must start with '{ZPOOL_EXTERNAL_PREFIX}' or '{ZPOOL_INTERNAL_PREFIX}'",
            ))
        }
    }
}

impl fmt::Display for ZpoolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix = match self.kind {
            ZpoolKind::External => ZPOOL_EXTERNAL_PREFIX,
            ZpoolKind::Internal => ZPOOL_INTERNAL_PREFIX,
        };
        write!(f, "{prefix}{}", self.id)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_zpool_name_regex() {
        let valid = [
            "oxi_d462a7f7-b628-40fe-80ff-4e4189e2d62b",
            "oxp_d462a7f7-b628-40fe-80ff-4e4189e2d62b",
        ];

        let invalid = [
            "",
            // Whitespace
            " oxp_d462a7f7-b628-40fe-80ff-4e4189e2d62b",
            "oxp_d462a7f7-b628-40fe-80ff-4e4189e2d62b ",
            // Case sensitivity
            "oxp_D462A7F7-b628-40fe-80ff-4e4189e2d62b",
            // Bad prefix
            "ox_d462a7f7-b628-40fe-80ff-4e4189e2d62b",
            "oxa_d462a7f7-b628-40fe-80ff-4e4189e2d62b",
            "oxi-d462a7f7-b628-40fe-80ff-4e4189e2d62b",
            "oxp-d462a7f7-b628-40fe-80ff-4e4189e2d62b",
            // Missing Prefix
            "d462a7f7-b628-40fe-80ff-4e4189e2d62b",
            // Bad UUIDs (Not following UUIDv4 format)
            "oxi_d462a7f7-b628-30fe-80ff-4e4189e2d62b",
            "oxi_d462a7f7-b628-40fe-c0ff-4e4189e2d62b",
        ];

        let r = regress::Regex::new(ZPOOL_NAME_REGEX)
            .expect("validation regex is valid");
        for input in valid {
            let m = r
                .find(input)
                .unwrap_or_else(|| panic!("input {input} did not match regex"));
            assert_eq!(m.start(), 0, "input {input} did not match start");
            assert_eq!(m.end(), input.len(), "input {input} did not match end");
        }

        for input in invalid {
            assert!(
                r.find(input).is_none(),
                "invalid input {input} should not match validation regex"
            );
        }
    }

    #[test]
    fn test_parse_zpool_name_json() {
        #[derive(Serialize, Deserialize, JsonSchema)]
        struct TestDataset {
            pool_name: ZpoolName,
        }

        // Confirm that we can convert from a JSON string to a a ZpoolName
        let json_string =
            r#"{"pool_name":"oxi_d462a7f7-b628-40fe-80ff-4e4189e2d62b"}"#;
        let dataset: TestDataset = serde_json::from_str(json_string)
            .expect("Could not parse ZpoolName from Json Object");
        assert!(matches!(dataset.pool_name.kind, ZpoolKind::Internal));

        // Confirm we can go the other way (ZpoolName to JSON string) too.
        let j = serde_json::to_string(&dataset)
            .expect("Cannot convert back to JSON string");
        assert_eq!(j, json_string);
    }

    fn toml_string(s: &str) -> String {
        format!("zpool_name = \"{}\"", s)
    }

    fn parse_name(s: &str) -> Result<ZpoolName, toml::de::Error> {
        toml_string(s)
            .parse::<toml::Value>()
            .expect("Cannot parse as TOML value")
            .get("zpool_name")
            .expect("Missing key")
            .clone()
            .try_into::<ZpoolName>()
    }

    #[test]
    fn test_parse_external_zpool_name() {
        let uuid: ZpoolUuid =
            "d462a7f7-b628-40fe-80ff-4e4189e2d62b".parse().unwrap();
        let good_name = format!("{}{}", ZPOOL_EXTERNAL_PREFIX, uuid);

        let name = parse_name(&good_name).expect("Cannot parse as ZpoolName");
        assert_eq!(uuid, name.id());
        assert_eq!(ZpoolKind::External, name.kind());
    }

    #[test]
    fn test_parse_internal_zpool_name() {
        let uuid: ZpoolUuid =
            "d462a7f7-b628-40fe-80ff-4e4189e2d62b".parse().unwrap();
        let good_name = format!("{}{}", ZPOOL_INTERNAL_PREFIX, uuid);

        let name = parse_name(&good_name).expect("Cannot parse as ZpoolName");
        assert_eq!(uuid, name.id());
        assert_eq!(ZpoolKind::Internal, name.kind());
    }

    #[test]
    fn test_parse_bad_zpool_names() {
        let bad_names = vec![
            // Nonsense string
            "this string is GARBAGE",
            // Missing prefix
            "d462a7f7-b628-40fe-80ff-4e4189e2d62b",
            // Underscores
            "oxp_d462a7f7_b628_40fe_80ff_4e4189e2d62b",
        ];

        for bad_name in &bad_names {
            assert!(
                parse_name(&bad_name).is_err(),
                "Parsing {} should fail",
                bad_name
            );
        }
    }
}

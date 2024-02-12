// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for managing Zpools.

use crate::{execute, ExecutionError, PFEXEC};
use camino::{Utf8Path, Utf8PathBuf};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

const ZPOOL_EXTERNAL_PREFIX: &str = "oxp_";
const ZPOOL_INTERNAL_PREFIX: &str = "oxi_";
const ZPOOL: &str = "/usr/sbin/zpool";

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[error("Failed to parse output: {0}")]
pub struct ParseError(String);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Zpool execution error: {0}")]
    Execution(#[from] crate::ExecutionError),

    #[error(transparent)]
    Parse(#[from] ParseError),

    #[error("No Zpools found")]
    NoZpools,
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to create zpool: {err}")]
pub struct CreateError {
    #[from]
    err: Error,
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to destroy zpool: {err}")]
pub struct DestroyError {
    #[from]
    err: Error,
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to list zpools: {err}")]
pub struct ListError {
    #[from]
    err: Error,
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to get info for zpool '{name}': {err}")]
pub struct GetInfoError {
    name: String,
    #[source]
    err: Error,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZpoolHealth {
    /// The device is online and functioning.
    Online,
    /// One or more components are degraded or faulted, but sufficient
    /// replicas exist to continue functioning.
    Degraded,
    /// One or more components are degraded or faulted, and insufficient
    /// replicas exist to continue functioning.
    Faulted,
    /// The device was explicitly taken offline by "zpool offline".
    Offline,
    /// The device was physically removed.
    Removed,
    /// The device could not be opened.
    Unavailable,
}

impl FromStr for ZpoolHealth {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ONLINE" => Ok(ZpoolHealth::Online),
            "DEGRADED" => Ok(ZpoolHealth::Degraded),
            "FAULTED" => Ok(ZpoolHealth::Faulted),
            "OFFLINE" => Ok(ZpoolHealth::Offline),
            "REMOVED" => Ok(ZpoolHealth::Removed),
            "UNAVAIL" => Ok(ZpoolHealth::Unavailable),
            _ => Err(ParseError(format!("Unrecognized zpool 'health': {}", s))),
        }
    }
}

/// Describes a Zpool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZpoolInfo {
    name: String,
    size: u64,
    allocated: u64,
    free: u64,
    health: ZpoolHealth,
}

impl ZpoolInfo {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    #[allow(dead_code)]
    pub fn allocated(&self) -> u64 {
        self.allocated
    }

    #[allow(dead_code)]
    pub fn free(&self) -> u64 {
        self.free
    }

    #[allow(dead_code)]
    pub fn health(&self) -> ZpoolHealth {
        self.health
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn new_hardcoded(name: String) -> ZpoolInfo {
        ZpoolInfo {
            name,
            size: 1024 * 1024 * 64,
            allocated: 1024,
            free: 1024 * 1023 * 64,
            health: ZpoolHealth::Online,
        }
    }
}

impl FromStr for ZpoolInfo {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Lambda helpers for error handling.
        let expected_field = |name| {
            ParseError(format!("Missing '{}' value in zpool list output", name))
        };
        let failed_to_parse = |name, err| {
            ParseError(format!("Failed to parse field '{}': {}", name, err))
        };

        let mut values = s.trim().split_whitespace();
        let name =
            values.next().ok_or_else(|| expected_field("name"))?.to_string();
        let size = values
            .next()
            .ok_or_else(|| expected_field("size"))?
            .parse::<u64>()
            .map_err(|e| failed_to_parse("size", e))?;
        let allocated = values
            .next()
            .ok_or_else(|| expected_field("allocated"))?
            .parse::<u64>()
            .map_err(|e| failed_to_parse("allocated", e))?;
        let free = values
            .next()
            .ok_or_else(|| expected_field("free"))?
            .parse::<u64>()
            .map_err(|e| failed_to_parse("free", e))?;
        let health = values
            .next()
            .ok_or_else(|| expected_field("health"))?
            .parse::<ZpoolHealth>()?;

        Ok(ZpoolInfo { name, size, allocated, free, health })
    }
}

/// Wraps commands for interacting with ZFS pools.
pub struct Zpool {}

#[cfg_attr(any(test, feature = "testing"), mockall::automock, allow(dead_code))]
impl Zpool {
    pub fn create(
        name: &ZpoolName,
        vdev: &Utf8Path,
    ) -> Result<(), CreateError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).arg("create");
        cmd.arg(&name.to_string());
        cmd.arg(vdev);
        execute(&mut cmd).map_err(Error::from)?;

        // Ensure that this zpool has the encryption feature enabled
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL)
            .arg("set")
            .arg("feature@encryption=enabled")
            .arg(&name.to_string());
        execute(&mut cmd).map_err(Error::from)?;

        Ok(())
    }

    pub fn destroy(name: &ZpoolName) -> Result<(), DestroyError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).arg("destroy");
        cmd.arg(&name.to_string());
        execute(&mut cmd).map_err(Error::from)?;
        Ok(())
    }

    pub fn import(name: &ZpoolName) -> Result<(), Error> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).arg("import").arg("-f");
        cmd.arg(&name.to_string());
        match execute(&mut cmd) {
            Ok(_) => Ok(()),
            Err(ExecutionError::CommandFailure(err_info)) => {
                // I'd really prefer to match on a specific error code, but the
                // command always returns "1" on failure.
                if err_info
                    .stderr
                    .contains("a pool with that name is already created")
                {
                    Ok(())
                } else {
                    Err(ExecutionError::CommandFailure(err_info).into())
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn export(name: &ZpoolName) -> Result<(), Error> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).arg("export").arg(&name.to_string());
        execute(&mut cmd)?;

        Ok(())
    }

    /// `zpool set failmode=continue <name>`
    pub fn set_failmode_continue(name: &ZpoolName) -> Result<(), Error> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL)
            .arg("set")
            .arg("failmode=continue")
            .arg(&name.to_string());
        execute(&mut cmd)?;
        Ok(())
    }

    pub fn list() -> Result<Vec<ZpoolName>, ListError> {
        let mut command = std::process::Command::new(ZPOOL);
        let cmd = command.args(&["list", "-Hpo", "name"]);

        let output = execute(cmd).map_err(Error::from)?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let zpool = stdout
            .lines()
            .filter_map(|line| line.parse::<ZpoolName>().ok())
            .collect();
        Ok(zpool)
    }

    #[cfg_attr(test, allow(dead_code))]
    pub fn get_info(name: &str) -> Result<ZpoolInfo, GetInfoError> {
        let mut command = std::process::Command::new(ZPOOL);
        let cmd = command.args(&[
            "list",
            "-Hpo",
            "name,size,allocated,free,health",
            name,
        ]);

        let output = execute(cmd).map_err(|err| GetInfoError {
            name: name.to_string(),
            err: err.into(),
        })?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let zpool = stdout.parse::<ZpoolInfo>().map_err(|err| {
            GetInfoError { name: name.to_string(), err: err.into() }
        })?;
        Ok(zpool)
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, JsonSchema)]
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
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ZpoolName {
    id: Uuid,
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
    pub const fn new_internal(id: Uuid) -> Self {
        Self { id, kind: ZpoolKind::Internal }
    }

    pub const fn new_external(id: Uuid) -> Self {
        Self { id, kind: ZpoolKind::External }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn kind(&self) -> ZpoolKind {
        self.kind
    }

    /// Returns a path to a dataset's mountpoint within the zpool.
    ///
    /// For example: oxp_(UUID) -> /pool/ext/(UUID)/(dataset)
    pub fn dataset_mountpoint(&self, dataset: &str) -> Utf8PathBuf {
        let mut path = Utf8PathBuf::new();
        path.push("/pool");
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
            let id = Uuid::from_str(s).map_err(|e| e.to_string())?;
            Ok(ZpoolName::new_external(id))
        } else if let Some(s) = s.strip_prefix(ZPOOL_INTERNAL_PREFIX) {
            let id = Uuid::from_str(s).map_err(|e| e.to_string())?;
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
        let uuid: Uuid =
            "d462a7f7-b628-40fe-80ff-4e4189e2d62b".parse().unwrap();
        let good_name = format!("{}{}", ZPOOL_EXTERNAL_PREFIX, uuid);

        let name = parse_name(&good_name).expect("Cannot parse as ZpoolName");
        assert_eq!(uuid, name.id());
        assert_eq!(ZpoolKind::External, name.kind());
    }

    #[test]
    fn test_parse_internal_zpool_name() {
        let uuid: Uuid =
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

    #[test]
    fn test_parse_zpool() {
        let name = "rpool";
        let size = 10000;
        let allocated = 6000;
        let free = 4000;
        let health = "ONLINE";

        // We should be able to tolerate any whitespace between columns.
        let input = format!(
            "{} {}    {} \t\t\t {} {}",
            name, size, allocated, free, health
        );
        let output: ZpoolInfo = input.parse().unwrap();
        assert_eq!(output.name(), name);
        assert_eq!(output.size(), size);
        assert_eq!(output.allocated(), allocated);
        assert_eq!(output.free(), free);
        assert_eq!(output.health(), ZpoolHealth::Online);
    }

    #[test]
    fn test_parse_zpool_missing_column() {
        let name = "rpool";
        let size = 10000;
        let allocated = 6000;
        let free = 4000;
        let _health = "ONLINE";

        // Similar to the prior test case, just omit "health".
        let input = format!("{} {} {} {}", name, size, allocated, free);
        let result: Result<ZpoolInfo, ParseError> = input.parse();

        let expected_err = ParseError(
            "Missing 'health' value in zpool list output".to_owned(),
        );
        assert_eq!(result.unwrap_err(), expected_err,);
    }
}

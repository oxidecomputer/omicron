// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for managing Zpools.

use crate::illumos::execute;
use omicron_common::api::external::Error as ExternalError;
use serde::{Deserialize, Deserializer};
use std::str::FromStr;
use uuid::Uuid;

const ZPOOL: &str = "/usr/sbin/zpool";

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ParseError {
    #[error("Failed to parse output as UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("Failed to parse output: {0}")]
    Parse(String),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Zpool execution error: {0}")]
    Execution(#[from] crate::illumos::ExecutionError),

    #[error(transparent)]
    Parse(#[from] ParseError),

    #[error("Failed to execute subcommand: {0}")]
    Command(ExternalError),
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
            _ => Err(ParseError::Parse(format!(
                "Unrecognized zpool 'health': {}",
                s
            ))),
        }
    }
}

/// Describes a Zpool.
#[derive(Clone, Debug)]
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
}

impl FromStr for ZpoolInfo {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Lambda helpers for error handling.
        let expected_field = |name| {
            ParseError::Parse(format!(
                "Missing '{}' value in zpool list output",
                name
            ))
        };
        let failed_to_parse = |name, err| {
            ParseError::Parse(format!(
                "Failed to parse field '{}': {}",
                name, err
            ))
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

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Zpool {
    pub fn get_info(name: &str) -> Result<ZpoolInfo, Error> {
        let mut command = std::process::Command::new(ZPOOL);
        let cmd = command.args(&[
            "list",
            "-Hpo",
            "name,size,allocated,free,health",
            name,
        ]);

        let output = execute(cmd)?;
        let stdout = String::from_utf8(output.stdout)
            .map_err(|e| ParseError::Utf8(e))?;

        let zpool = stdout.parse::<ZpoolInfo>()?;
        Ok(zpool)
    }
}

/// A wrapper around a zpool name.
///
/// This expects that the format will be: oxp_<UUID> - we parse
/// the prefix when reading the structure, and validate that the UUID
/// can be utilized.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ZpoolName(Uuid);

impl ZpoolName {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn id(&self) -> Uuid {
        self.0
    }
}

impl<'de> Deserialize<'de> for ZpoolName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let s = s.strip_prefix("oxp_").ok_or_else(|| {
            serde::de::Error::custom(
                "Bad zpool prefix - must start with 'oxp_'",
            )
        })?;
        let id = Uuid::from_str(&s).map_err(serde::de::Error::custom)?;
        Ok(ZpoolName(id))
    }
}

impl ToString for ZpoolName {
    fn to_string(&self) -> String {
        format!("oxp_{}", self.0.to_string())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn toml_string(s: &str) -> String {
        format!("zpool_name = \"{}\"", s)
    }

    fn parse_name(s: &str) -> Result<ZpoolName, toml::de::Error> {
        toml_string(&s)
            .parse::<toml::Value>()
            .expect("Cannot parse as TOML value")
            .get("zpool_name")
            .expect("Missing key")
            .clone()
            .try_into::<ZpoolName>()
    }

    #[test]
    fn test_parse_zpool_name() {
        let uuid: Uuid =
            "d462a7f7-b628-40fe-80ff-4e4189e2d62b".parse().unwrap();
        let good_name = format!("oxp_{}", uuid);

        let name = parse_name(&good_name).expect("Cannot parse as ZpoolName");
        assert_eq!(uuid, name.id());
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

        let expected_err = ParseError::Parse(
            "Missing 'health' value in zpool list output".to_owned(),
        );
        assert_eq!(result.unwrap_err(), expected_err,);
    }
}

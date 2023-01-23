// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for managing Zpools.

use crate::illumos::{execute, PFEXEC};
use serde::{Deserialize, Deserializer};
use std::str::FromStr;
use uuid::Uuid;

const ZPOOL_PREFIX: &str = "oxp_";
const ZPOOL: &str = "/usr/sbin/zpool";

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[error("Failed to parse output: {0}")]
pub struct ParseError(String);

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Zpool execution error: {0}")]
    Execution(#[from] crate::illumos::ExecutionError),

    #[error(transparent)]
    Parse(#[from] ParseError),
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to create zpool: {err}")]
pub struct CreateError {
    #[from]
    #[source]
    err: Error,
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to list zpools: {err}")]
pub struct ListError {
    #[from]
    #[source]
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

#[cfg_attr(test, mockall::automock)]
impl Zpool {
    pub fn create(
        name: ZpoolName,
        vdev: &std::path::PathBuf,
    ) -> Result<(), CreateError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZPOOL,
            "create",
            &name.to_string(),
            &vdev.to_string_lossy(),
        ]);
        execute(cmd).map_err(Error::from)?;
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
        let s = s.strip_prefix(ZPOOL_PREFIX).ok_or_else(|| {
            serde::de::Error::custom(
                "Bad zpool prefix - must start with 'oxp_'",
            )
        })?;
        let id = Uuid::from_str(&s).map_err(serde::de::Error::custom)?;
        Ok(ZpoolName(id))
    }
}

impl FromStr for ZpoolName {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.strip_prefix(ZPOOL_PREFIX).ok_or_else(|| {
            format!("Bad zpool name {}; must start with {}", s, ZPOOL_PREFIX)
        })?;
        let id = Uuid::from_str(&s).map_err(|e| e.to_string())?;
        Ok(ZpoolName(id))
    }
}

impl ToString for ZpoolName {
    fn to_string(&self) -> String {
        format!("{}{}", ZPOOL_PREFIX, self.0)
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
        let good_name = format!("{}{}", ZPOOL_PREFIX, uuid);

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

        let expected_err = ParseError(
            "Missing 'health' value in zpool list output".to_owned(),
        );
        assert_eq!(result.unwrap_err(), expected_err,);
    }
}

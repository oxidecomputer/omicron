// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for managing Zpools.

use crate::{ExecutionError, PFEXEC, execute_async};
use camino::{Utf8Path, Utf8PathBuf};
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::str::FromStr;
use tokio::process::Command;

pub use omicron_common::zpool_name::ZpoolName;

const ZPOOL: &str = "/usr/sbin/zpool";

pub const ZPOOL_MOUNTPOINT_ROOT: &str = "/";

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

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ZpoolOrRamdisk {
    Zpool(ZpoolName),
    Ramdisk,
}

/// A path which exists within a pool.
///
/// By storing these types together, it's possible to answer
/// whether or not a path exists on a particular device.
// Technically we could re-derive the pool name from the path,
// but that involves some string parsing, and honestly I'd just
// Rather Not.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PathInPool {
    pub pool: ZpoolOrRamdisk,
    pub path: Utf8PathBuf,
}

// TODO-K: Make sure all of this makes sense
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
struct UnhealthyZpool {
    pool: String,
    status: ZpoolHealth,
    action: String,
    scan: String,
    config: UnhealthyZpoolConfig,
    errors: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
struct UnhealthyZpoolConfig {
    name: String,
    state: ZpoolHealth,
    read: u64,
    write: u64,
    cksum: u64,
}

impl FromStr for UnhealthyZpool {
    type Err = ParseError;

    // TODO_K: Actually parse the result
    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Ok(UnhealthyZpool {
            pool: "hi".to_string(),
            status: ZpoolHealth::Degraded,
            action: "hi".to_string(),
            scan: "hi".to_string(),
            config: UnhealthyZpoolConfig {
                name: "hi".to_string(),
                state: ZpoolHealth::Degraded,
                read: 0,
                write: 0,
                cksum: 0,
            },
            errors: "hi".to_string(),
        })
    }
}
// TODO-K: Make sure this makes sense up to here

/// Wraps commands for interacting with ZFS pools.
pub struct Zpool(());

/// Describes the API for interfacing with zpools
///
/// This is a trait so that it can be faked out for tests.
#[async_trait::async_trait]
pub trait Api: Send + Sync {
    async fn create(
        &self,
        name: &ZpoolName,
        vdev: &Utf8Path,
    ) -> Result<(), CreateError> {
        let mut cmd = Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).args(["create", "-o", "ashift=12"]);
        cmd.arg(&name.to_string());
        cmd.arg(vdev);
        execute_async(&mut cmd).await.map_err(Error::from)?;

        // Ensure that this zpool has the encryption feature enabled
        let mut cmd = Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL)
            .arg("set")
            .arg("feature@encryption=enabled")
            .arg(&name.to_string());
        execute_async(&mut cmd).await.map_err(Error::from)?;

        Ok(())
    }
}

impl Api for Zpool {}

impl Zpool {
    pub fn real_api() -> Self {
        Self(())
    }

    pub async fn destroy(name: &ZpoolName) -> Result<(), DestroyError> {
        let mut cmd = Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).arg("destroy");
        cmd.arg(&name.to_string());
        execute_async(&mut cmd).await.map_err(Error::from)?;
        Ok(())
    }

    pub async fn import(name: &ZpoolName) -> Result<(), Error> {
        let mut cmd = Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).arg("import").arg("-f");
        cmd.arg(&name.to_string());
        match execute_async(&mut cmd).await {
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

    pub async fn export(name: &ZpoolName) -> Result<(), Error> {
        let mut cmd = Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).arg("export").arg(&name.to_string());
        execute_async(&mut cmd).await?;

        Ok(())
    }

    /// `zpool set failmode=continue <name>`
    pub async fn set_failmode_continue(name: &ZpoolName) -> Result<(), Error> {
        let mut cmd = Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL)
            .arg("set")
            .arg("failmode=continue")
            .arg(&name.to_string());
        execute_async(&mut cmd).await?;
        Ok(())
    }

    pub async fn list() -> Result<Vec<ZpoolName>, ListError> {
        let mut command = Command::new(ZPOOL);
        let cmd = command.args(&["list", "-Hpo", "name"]);

        let output = execute_async(cmd).await.map_err(Error::from)?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let zpool = stdout
            .lines()
            .filter_map(|line| line.parse::<ZpoolName>().ok())
            .collect();
        Ok(zpool)
    }

    #[cfg_attr(test, allow(dead_code))]
    pub async fn get_info(name: &str) -> Result<ZpoolInfo, GetInfoError> {
        let mut command = Command::new(ZPOOL);
        let cmd = command.args(&[
            "list",
            "-Hpo",
            "name,size,allocated,free,health",
            name,
        ]);

        let output = execute_async(cmd).await.map_err(|err| GetInfoError {
            name: name.to_string(),
            err: err.into(),
        })?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let zpool = stdout.parse::<ZpoolInfo>().map_err(|err| {
            GetInfoError { name: name.to_string(), err: err.into() }
        })?;
        Ok(zpool)
    }

    pub async fn status_unhealthy() -> Result<Vec<UnhealthyZpool>, ListError> {
        let mut command = Command::new(ZPOOL);
        let cmd = command.args(&["status", "-x"]);

        let output = execute_async(cmd).await.map_err(Error::from)?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let zpool = stdout
            .lines()
            .filter_map(|line| line.parse::<UnhealthyZpool>().ok())
            .collect();
        Ok(zpool)
    }
}

#[cfg(test)]
mod test {
    use super::*;

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

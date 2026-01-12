// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for managing Zpools.

use crate::{ExecutionError, PFEXEC, execute_async};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use slog::Logger;
use slog::error;
use slog::info;
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

/// Lists unhealthy zpools, parsing errors if any, and the time the health check
/// for zpools ran.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UnhealthyZpoolsResult {
    pub zpools: Vec<String>,
    pub errors: Vec<String>,
    pub time_of_status: Option<DateTime<Utc>>,
}

impl UnhealthyZpoolsResult {
    pub fn new() -> Self {
        Self { zpools: vec![], errors: vec![], time_of_status: None }
    }

    pub fn is_empty(&self) -> bool {
        self.zpools.is_empty()
            && self.errors.is_empty()
            && self.time_of_status == None
    }

    #[cfg_attr(not(target_os = "illumos"), allow(dead_code))]
    fn parse(log: &Logger, data: &[u8]) -> Self {
        let mut zpools = vec![];
        let mut errors = vec![];
        if data.is_empty() {
            return Self { zpools, errors, time_of_status: Some(Utc::now()) };
        }

        // Example of the response from running `zpool list -Hpo health,name`
        //
        // FAULTED fakepool1
        // FAULTED fakepool2
        // ONLINE  rpool
        let s = String::from_utf8_lossy(data);
        let lines = s.trim().lines();

        for line in lines {
            let line = line.trim();
            let mut pool = line.split_whitespace();

            if let Some(state_str) = pool.next() {
                // Only attempt to parse a zpool that is in a non-functional
                // state.
                match ZpoolHealth::from_str(state_str) {
                    Ok(ZpoolHealth::Faulted)
                    | Ok(ZpoolHealth::Offline)
                    | Ok(ZpoolHealth::Removed)
                    | Ok(ZpoolHealth::Unavailable) => {
                        if let Some(name) = pool.next() {
                            zpools.push(name.to_string());
                        } else {
                            errors.push(format!(
                                "Unexpected output line: {line}"
                            ));
                            error!(
                                log,
                                "unable to parse; output line missing zpool name";
                                "line" => line,
                            );
                            continue;
                        }
                    }
                    // Pool is in a healthy or degraded state, skip it.
                    Ok(_) => {}
                    Err(e) => {
                        errors.push(format!("{e}"));
                        info!(
                            log,
                            "output from 'zpool list' contains a zpool with \
                            an unknown state: {state_str}",
                        );
                    }
                }
            }
        }

        Self { zpools, errors, time_of_status: Some(Utc::now()) }
    }
}

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

    /// Lists zpools that are in a unhealthy non-functional state. Specifically
    /// if they are in the following states:
    ///
    ///  - Faulted
    ///  - Offline
    ///  - Removed
    ///  - Unavailable
    #[cfg(target_os = "illumos")]
    pub async fn status_unhealthy(
        log: &Logger,
    ) -> Result<UnhealthyZpoolsResult, ExecutionError> {
        let mut command = Command::new(ZPOOL);
        let cmd = command.args(&["list", "-Hpo", "health,name"]);
        info!(log, "Retrieving information from zpools");
        let output = execute_async(cmd).await?;
        let zpool_result = UnhealthyZpoolsResult::parse(&log, &output.stdout);
        info!(log, "Successfully retrieved unhealthy zpools");
        Ok(zpool_result)
    }

    #[cfg(not(target_os = "illumos"))]
    pub async fn status_unhealthy(
        log: &Logger,
    ) -> Result<UnhealthyZpoolsResult, ExecutionError> {
        info!(log, "OS not illumos, will not retrieve zpool information");
        let zpool_result = UnhealthyZpoolsResult::new();
        Ok(zpool_result)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use slog::Drain;
    use slog::o;
    use slog_term::FullFormat;
    use slog_term::PlainDecorator;
    use slog_term::TestStdoutWriter;

    fn log() -> slog::Logger {
        let decorator = PlainDecorator::new(TestStdoutWriter);
        let drain = FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    }

    #[test]
    fn test_unhealthy_zpool_parse_success() {
        let output = r#"FAULTED fakepool1
UNAVAIL fakepool2
ONLINE  rpool
"#;

        let log = log();
        let result = UnhealthyZpoolsResult::parse(&log, output.as_bytes());

        // We want to make sure we only have two unhealthy pools
        assert_eq!(
            result.zpools,
            vec!["fakepool1".to_string(), "fakepool2".to_string()]
        );
        assert_eq!(result.errors.len(), 0);
        assert!(result.time_of_status.is_some());
    }

    #[test]
    fn test_unhealthy_zpool_parse_none_success() {
        let output = r#"DEGRADED fakepool1
ONLINE   fakepool2
ONLINE   rpool
"#;

        let log = log();
        let result = UnhealthyZpoolsResult::parse(&log, output.as_bytes());

        // We want to make sure we only have zero unhealthy pools
        assert_eq!(result.zpools.len(), 0);
        assert_eq!(result.errors.len(), 0);
        assert!(result.time_of_status.is_some());
    }

    #[test]
    fn test_unhealthy_zpool_empty_success() {
        let output = r#""#;

        let log = log();
        let result = UnhealthyZpoolsResult::parse(&log, output.as_bytes());

        // We want to make sure we only have zero unhealthy pools
        assert_eq!(result.zpools.len(), 0);
        assert_eq!(result.errors.len(), 0);
        assert!(result.time_of_status.is_some());
    }

    #[test]
    fn test_unhealthy_zpool_parse_unknown_status_fail() {
        let output = r#"BARNACLES! fakepool1
FAULTED fakepool2
ONLINE  rpool
"#;

        let log = log();
        let result = UnhealthyZpoolsResult::parse(&log, output.as_bytes());

        assert_eq!(result.zpools, vec!["fakepool2".to_string()]);
        assert_eq!(
            result.errors,
            vec![
                "Failed to parse output: Unrecognized zpool 'health': BARNACLES!"
                .to_string(),
            ]
        );
        assert!(result.time_of_status.is_some());
    }

    #[test]
    fn test_unhealthy_zpool_parse_zpool_fail() {
        let output = r#"FAULTED
ONLINE  rpool
"#;

        let log = log();
        let result = UnhealthyZpoolsResult::parse(&log, output.as_bytes());

        assert_eq!(result.zpools.len(), 0);
        assert_eq!(
            result.errors,
            vec!["Unexpected output line: FAULTED".to_string(),],
        );
        assert!(result.time_of_status.is_some());
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

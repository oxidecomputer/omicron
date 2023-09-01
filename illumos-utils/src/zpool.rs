// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for managing Zpools.

use camino::Utf8Path;
use helios_fusion::zpool::ParseError;
use helios_fusion::{BoxedExecutor, ExecutionError, PFEXEC};

pub use helios_fusion::zpool::ZpoolHealth;
pub use helios_fusion::zpool::ZpoolInfo;
pub use helios_fusion::zpool::ZpoolKind;
pub use helios_fusion::zpool::ZpoolName;
pub use helios_fusion::ZPOOL;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Zpool execution error: {0}")]
    Execution(#[from] ExecutionError),

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

/// Wraps commands for interacting with ZFS pools.
pub struct Zpool {}

impl Zpool {
    pub fn create(
        executor: &BoxedExecutor,
        name: ZpoolName,
        vdev: &Utf8Path,
    ) -> Result<(), CreateError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).arg("create");
        cmd.arg(&name.to_string());
        cmd.arg(vdev);
        executor.execute(&mut cmd).map_err(Error::from)?;

        // Ensure that this zpool has the encryption feature enabled
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL)
            .arg("set")
            .arg("feature@encryption=enabled")
            .arg(&name.to_string());
        executor.execute(&mut cmd).map_err(Error::from)?;

        Ok(())
    }

    pub fn import(
        executor: &BoxedExecutor,
        name: ZpoolName,
    ) -> Result<(), Error> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).arg("import").arg("-f");
        cmd.arg(&name.to_string());
        match executor.execute(&mut cmd) {
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

    pub fn export(
        executor: &BoxedExecutor,
        name: &ZpoolName,
    ) -> Result<(), Error> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL).arg("export").arg(&name.to_string());
        executor.execute(&mut cmd)?;

        Ok(())
    }

    /// `zpool set failmode=continue <name>`
    pub fn set_failmode_continue(
        executor: &BoxedExecutor,
        name: &ZpoolName,
    ) -> Result<(), Error> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");
        cmd.arg(ZPOOL)
            .arg("set")
            .arg("failmode=continue")
            .arg(&name.to_string());
        executor.execute(&mut cmd)?;
        Ok(())
    }

    pub fn list(executor: &BoxedExecutor) -> Result<Vec<ZpoolName>, ListError> {
        let mut command = std::process::Command::new(ZPOOL);
        let cmd = command.args(&["list", "-Hpo", "name"]);

        let output = executor.execute(cmd).map_err(Error::from)?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let zpool = stdout
            .lines()
            .filter_map(|line| line.parse::<ZpoolName>().ok())
            .collect();
        Ok(zpool)
    }

    pub fn get_info(
        executor: &BoxedExecutor,
        name: &str,
    ) -> Result<ZpoolInfo, GetInfoError> {
        let mut command = std::process::Command::new(ZPOOL);
        let cmd = command.args(&[
            "list",
            "-Hpo",
            "name,size,allocated,free,health",
            name,
        ]);

        let output = executor.execute(cmd).map_err(|err| GetInfoError {
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

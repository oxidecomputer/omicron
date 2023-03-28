// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Wrappers around reservoir controls

use crate::{execute, PFEXEC};
use omicron_common::api::external::ByteCount;

const RSRVRCTRL: &str = "/usr/lib/rsrvrctl";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Reservoir size must be a multiple of MiB, not: {0}")]
    InvalidSize(ByteCount),

    #[error("Failed to run {RSRVRCTRL}: {0}")]
    ExecutionError(#[from] crate::ExecutionError),
}

pub struct ReservoirControl {}

impl ReservoirControl {
    pub fn set(size: ByteCount) -> Result<(), Error> {
        if size.to_bytes() % (1024 * 1024) != 0 {
            return Err(Error::InvalidSize(size));
        }

        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");

        cmd.arg(RSRVRCTRL);
        cmd.arg("-s");
        cmd.arg(size.to_whole_mebibytes().to_string());

        execute(&mut cmd)?;
        Ok(())
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parsing of CLI-based arguments to a Helios system

// TODO: REMOVE
#![allow(dead_code)]

use crate::host::ZoneName;

use helios_fusion::Input;
use helios_fusion::{
    DLADM, IPADM, PFEXEC, ROUTE, SVCADM, SVCCFG, ZFS, ZLOGIN, ZONEADM, ZONECFG,
    ZPOOL,
};

// Command-line utilities
pub(crate) mod dladm;
pub(crate) mod ipadm;
pub(crate) mod route;
pub(crate) mod svcadm;
pub(crate) mod svccfg;
pub(crate) mod zfs;
pub(crate) mod zoneadm;
pub(crate) mod zonecfg;
pub(crate) mod zpool;

// Utilities for parsing
mod parse;

use crate::cli::parse::InputExt;

pub(crate) enum KnownCommand {
    Coreadm, // TODO
    Dladm(dladm::Command),
    Dumpadm, // TODO
    Ipadm(ipadm::Command),
    Fstyp,    // TODO
    RouteAdm, // TODO
    Route(route::Command),
    Savecore, // TODO
    Svccfg(svccfg::Command),
    Svcadm(svcadm::Command),
    Zfs(zfs::Command),
    Zoneadm(zoneadm::Command),
    Zonecfg(zonecfg::Command),
    Zpool(zpool::Command),
}

pub(crate) struct Command {
    with_pfexec: bool,
    in_zone: Option<ZoneName>,
    cmd: KnownCommand,
}

impl Command {
    pub fn with_pfexec(&self) -> bool {
        self.with_pfexec
    }
    pub fn in_zone(&self) -> &Option<ZoneName> {
        &self.in_zone
    }
    pub fn cmd(&self) -> &KnownCommand {
        &self.cmd
    }
    pub fn as_cmd(self) -> KnownCommand {
        self.cmd
    }
}

impl TryFrom<Input> for Command {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        let mut with_pfexec = false;
        let mut in_zone = None;

        while input.program == PFEXEC {
            with_pfexec = true;
            input.shift_program()?;
        }
        if input.program == ZLOGIN {
            input.shift_program()?;
            in_zone = Some(ZoneName(input.shift_program()?));
        }

        use KnownCommand::*;
        let cmd = match input.program.as_str() {
            DLADM => Dladm(dladm::Command::try_from(input)?),
            IPADM => Ipadm(ipadm::Command::try_from(input)?),
            ROUTE => Route(route::Command::try_from(input)?),
            SVCCFG => Svccfg(svccfg::Command::try_from(input)?),
            SVCADM => Svcadm(svcadm::Command::try_from(input)?),
            ZFS => Zfs(zfs::Command::try_from(input)?),
            ZONEADM => Zoneadm(zoneadm::Command::try_from(input)?),
            ZONECFG => Zonecfg(zonecfg::Command::try_from(input)?),
            ZPOOL => Zpool(zpool::Command::try_from(input)?),
            _ => return Err(format!("Unknown command: {}", input.program)),
        };

        Ok(Command { with_pfexec, in_zone, cmd })
    }
}

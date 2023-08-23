// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates an illumos system

// TODO: REMOVE
#![allow(dead_code)]

use crate::host::ZoneName;

use helios_fusion::Input;
use helios_fusion::{
    DLADM, IPADM, PFEXEC, ROUTE, SVCADM, SVCCFG, ZFS, ZLOGIN, ZONEADM, ZONECFG,
    ZPOOL,
};

// Command-line utilities
mod dladm;
mod ipadm;
mod route;
mod svcadm;
mod svccfg;
mod zfs;
mod zoneadm;
mod zonecfg;
mod zpool;

// Utilities for parsing
mod parse;

use crate::cli::parse::InputExt;

enum KnownCommand {
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

struct Command {
    with_pfexec: bool,
    in_zone: Option<ZoneName>,
    cmd: KnownCommand,
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

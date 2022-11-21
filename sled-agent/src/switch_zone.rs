// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Special-case software for handling for the switch zone.
//!
//! Most zones are launched via the ServiceManager, which is controlled by the
//! sled agent. This switch zone, however, must be launched before the sled
//! agent is running, and before the underlay network is operational.

use crate::illumos::dladm::Dladm;
use crate::illumos::link::Link;
use crate::params::DendriteAsic;
use crate::params::ServiceType;
use crate::params::ServiceZoneRequest;
use crate::smf_helper::{Error as SmfError, SmfHelper};
use anyhow::anyhow;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::MGS_PORT;
use slog::Logger;
use std::path::Path;

// TODO TODO TODO COLLAPSE THESE METHODS INTO SERVICES.RS

/// A wrapper around the Switch Zone, with methods to enable or disable it.
#[derive(Clone)]
pub struct SwitchZoneManager {}

impl SwitchZoneManager {
    // Check the services intended to run in the zone to determine whether any
    // physical devices need to be mapped into the zone when it is created.
    pub fn devices_needed(
        req: &ServiceZoneRequest,
    ) -> Result<Vec<String>, anyhow::Error> {
        let mut devices = vec![];
        for svc in &req.services {
            match svc {
                ServiceType::Dendrite { asic: DendriteAsic::TofinoAsic } => {
                    // When running on a real sidecar, we need the /dev/tofino
                    // device to talk to the tofino ASIC.
                    devices.push("/dev/tofino".to_string());
                }
                _ => (),
            }
        }

        for dev in &devices {
            if !Path::new(dev).exists() {
                return Err(anyhow!("Missing device: {dev}"));
            }
        }
        Ok(devices)
    }

    // Check the services intended to run in the zone to determine whether any
    // physical links or vnics need to be mapped into the zone when it is created.
    //
    // NOTE: This function is implemented to return the first link found, under
    // the assumption that "at most one" would be necessary.
    pub fn link_needed(
        req: &ServiceZoneRequest,
    ) -> Result<Option<Link>, anyhow::Error> {
        for svc in &req.services {
            match svc {
                ServiceType::Tfport { pkt_source } => {
                    // The tfport service requires a MAC device to/from which sidecar
                    // packets may be multiplexed.  If the link isn't present, don't
                    // bother trying to start the zone.
                    match Dladm::verify_link(pkt_source) {
                        Ok(link) => {
                            return Ok(Some(link));
                        }
                        Err(_) => {
                            return Err(anyhow!("Missing link: {pkt_source}"));
                        }
                    }
                }
                _ => (),
            }
        }
        Ok(None)
    }

    // Check the services intended to run in the zone to determine whether any
    // additional privileges need to be enabled for the zone.
    pub fn privs_needed(req: &ServiceZoneRequest) -> Vec<String> {
        let mut needed = Vec::new();
        for svc in &req.services {
            if let ServiceType::Tfport { .. } = svc {
                needed.push("default".to_string());
                needed.push("sys_dl_config".to_string());
            }
        }
        needed
    }

    pub fn launch_service(
        log: &Logger,
        smfh: &SmfHelper,
        service: &ServiceType,
        request: &ServiceZoneRequest,
    ) -> Result<(), SmfError> {
        match service {
            ServiceType::ManagementGatewayService => {
                info!(log, "Setting up MGS service");
                smfh.setprop("config/id", request.id)?;
                if let Some(address) = request.addresses.get(0) {
                    smfh.setprop(
                        "config/address",
                        &format!("[{}]:{}", address, MGS_PORT),
                    )?;
                }
                smfh.refresh()?;
            }
            ServiceType::Dendrite { asic } => {
                info!(log, "Setting up dendrite service");

                if let Some(address) = request.addresses.get(0) {
                    smfh.setprop(
                        "config/address",
                        &format!("[{}]:{}", address, DENDRITE_PORT),
                    )?;
                }
                match *asic {
                    DendriteAsic::TofinoAsic => smfh.setprop(
                        "config/port_config",
                        "/opt/oxide/dendrite/misc/sidecar_config.toml",
                    )?,
                    DendriteAsic::TofinoStub => smfh.setprop(
                        "config/port_config",
                        "/opt/oxide/dendrite/misc/model_config.toml",
                    )?,
                    DendriteAsic::Softnpu => {}
                };
                smfh.refresh()?;
            }
            ServiceType::Tfport { pkt_source } => {
                info!(log, "Setting up tfport service");

                smfh.setprop("config/pkt_source", pkt_source)?;
                if let Some(address) = request.addresses.get(0) {
                    smfh.setprop("config/host", &format!("[{}]", address))?;
                }
                smfh.setprop("config/port", &format!("{}", DENDRITE_PORT))?;
                smfh.refresh()?;
            }
            _ => panic!("Not a switch service"),
        }
        Ok(())
    }
}

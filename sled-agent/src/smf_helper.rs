// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::params::ServiceType;
use illumos_utils::running_zone::RunningZone;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to do '{intent}' by running command in zone: {err}")]
    ZoneCommand {
        intent: String,
        #[source]
        err: illumos_utils::running_zone::RunCommandError,
    },
}

pub struct SmfHelper<'t> {
    running_zone: &'t RunningZone,
    service_name: String,
    smf_name: String,
    default_smf_name: String,
}

impl<'t> SmfHelper<'t> {
    pub fn new(running_zone: &'t RunningZone, service: &ServiceType) -> Self {
        let service_name = service.to_string();
        let smf_name = format!("svc:/system/illumos/{}", service);
        let default_smf_name = format!("{}:default", smf_name);

        SmfHelper { running_zone, service_name, smf_name, default_smf_name }
    }

    pub fn import_manifest(&self) -> Result<(), Error> {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCCFG,
                "import",
                &format!(
                    "/var/svc/manifest/site/{}/manifest.xml",
                    self.service_name
                ),
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: "importing manifest".to_string(),
                err,
            })?;
        Ok(())
    }

    pub fn setprop<P, V>(&self, prop: P, val: V) -> Result<(), Error>
    where
        P: ToString,
        V: ToString,
    {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCCFG,
                "-s",
                &self.smf_name,
                "setprop",
                &format!("{}={}", prop.to_string(), val.to_string()),
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: format!("set {} smf property", prop.to_string()),
                err,
            })?;
        Ok(())
    }

    pub fn addpropvalue<P, V>(&self, prop: P, val: V) -> Result<(), Error>
    where
        P: ToString,
        V: ToString,
    {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCCFG,
                "-s",
                &self.smf_name,
                "addpropvalue",
                &prop.to_string(),
                &val.to_string(),
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: format!("add {} smf property value", prop.to_string()),
                err,
            })?;
        Ok(())
    }

    pub fn delpropvalue<P, V>(&self, prop: P, val: V) -> Result<(), Error>
    where
        P: ToString,
        V: ToString,
    {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCCFG,
                "-s",
                &self.smf_name,
                "delpropvalue",
                &prop.to_string(),
                &val.to_string(),
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: format!("del {} smf property value", prop.to_string()),
                err,
            })?;
        Ok(())
    }

    pub fn refresh(&self) -> Result<(), Error> {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCCFG,
                "-s",
                &self.default_smf_name,
                "refresh",
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: format!(
                    "Refresh SMF manifest {}",
                    self.default_smf_name
                ),
                err,
            })?;
        Ok(())
    }

    pub fn restart(&self) -> Result<(), Error> {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCADM,
                "restart",
                &self.default_smf_name,
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: format!("Restart {} service", self.default_smf_name),
                err,
            })?;
        Ok(())
    }

    pub fn enable(&self) -> Result<(), Error> {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCADM,
                "enable",
                "-t",
                &self.default_smf_name,
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: format!("Enable {} service", self.default_smf_name),
                err,
            })?;
        Ok(())
    }
}

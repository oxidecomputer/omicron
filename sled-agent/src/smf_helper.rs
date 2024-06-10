// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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

pub trait Service {
    fn service_name(&self) -> String;
    fn smf_name(&self) -> String;
}

pub struct SmfHelper<'t> {
    running_zone: &'t RunningZone,
    _service_name: String,
    smf_name: String,
    default_smf_name: String,
}

impl<'t> SmfHelper<'t> {
    pub fn new(running_zone: &'t RunningZone, service: &impl Service) -> Self {
        let _service_name = service.service_name();
        let smf_name = service.smf_name();
        let default_smf_name = format!("{}:default", smf_name);

        SmfHelper {
            running_zone,
            _service_name,
            smf_name,
            default_smf_name,
        }
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

    pub fn addpropvalue_type<P, V, T>(
        &self,
        prop: P,
        val: V,
        valtype: T,
    ) -> Result<(), Error>
    where
        P: ToString,
        V: ToString,
        T: ToString,
    {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCCFG,
                "-s",
                &self.smf_name,
                "addpropvalue",
                &prop.to_string(),
                &format!("{}:", valtype.to_string()),
                &val.to_string(),
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: format!("add {} smf property value", prop.to_string()),
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

    pub fn addpropvalue_default_instance<P, V>(
        &self,
        prop: P,
        val: V,
    ) -> Result<(), Error>
    where
        P: ToString,
        V: ToString,
    {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCCFG,
                "-s",
                &self.default_smf_name,
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

    pub fn addpropgroup<P, T>(
        &self,
        propgroup: P,
        grouptype: T,
    ) -> Result<(), Error>
    where
        P: ToString,
        T: ToString,
    {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCCFG,
                "-s",
                &self.smf_name,
                "addpg",
                &propgroup.to_string(),
                &grouptype.to_string(),
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: format!(
                    "add {} ({}) smf property group",
                    propgroup.to_string(),
                    grouptype.to_string()
                ),
                err,
            })?;
        Ok(())
    }

    pub fn delpropgroup<P>(&self, propgroup: P) -> Result<(), Error>
    where
        P: ToString,
    {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCCFG,
                "-s",
                &self.smf_name,
                "delpg",
                &propgroup.to_string(),
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: format!(
                    "del {} smf property group",
                    propgroup.to_string()
                ),
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

    pub fn delpropvalue_default_instance<P, V>(
        &self,
        prop: P,
        val: V,
    ) -> Result<(), Error>
    where
        P: ToString,
        V: ToString,
    {
        self.running_zone
            .run_cmd(&[
                illumos_utils::zone::SVCCFG,
                "-s",
                &self.default_smf_name,
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
}

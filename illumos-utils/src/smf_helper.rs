// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::running_zone::RunningZone;
use crate::zone::SVCCFG;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to do '{intent}' by running command in zone: {err}")]
    ZoneCommand {
        intent: String,
        #[source]
        err: crate::running_zone::RunCommandError,
    },
}

pub trait Service {
    fn service_name(&self) -> String;
    fn smf_name(&self) -> String;
}

pub struct SmfHelper<'t> {
    running_zone: &'t RunningZone,
    smf_name: String,
    default_smf_name: String,
}

impl<'t> SmfHelper<'t> {
    pub fn new(running_zone: &'t RunningZone, service: &impl Service) -> Self {
        let smf_name = service.smf_name();
        let default_smf_name = format!("{}:default", smf_name);

        SmfHelper { running_zone, smf_name, default_smf_name }
    }

    pub fn setprop_default_instance<P, V>(
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
                SVCCFG,
                "-s",
                &self.default_smf_name,
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
                SVCCFG,
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

    pub fn addpropvalue_type_default_instance<P, V, T>(
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
                SVCCFG,
                "-s",
                &self.default_smf_name,
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
                SVCCFG,
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
                SVCCFG,
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

    pub fn delpropvalue_default_instance<P, V>(
        &self,
        prop: P,
        val: V,
    ) -> Result<(), Error>
    where
        P: ToString,
        V: ToString,
    {
        match self
            .running_zone
            .run_cmd(&[
                SVCCFG,
                "-s",
                &self.default_smf_name,
                "delpropvalue",
                &prop.to_string(),
                &val.to_string(),
            ])
            .map_err(|err| Error::ZoneCommand {
                intent: format!("del {} smf property value", prop.to_string()),
                err,
            }) {
            Ok(_) => (),
            Err(e) => {
                // If a property already doesn't exist we don't need to
                // return an error
                if !e.to_string().contains("No such property") {
                    return Err(e);
                }
            }
        };

        Ok(())
    }

    pub fn refresh(&self) -> Result<(), Error> {
        self.running_zone
            .run_cmd(&[
                SVCCFG,
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

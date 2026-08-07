// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::running_zone::RunningZone;
use crate::zone::SVCADM;
use crate::zone::SVCCFG;
use crate::zone::SVCS;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to do '{intent}' by running command in zone")]
    ZoneCommand {
        intent: String,
        #[source]
        err: crate::running_zone::RunCommandError,
    },
}

pub trait Service {
    fn service_name(&self) -> &str;
    fn smf_name(&self) -> String;
}

pub struct SmfHelper<'t> {
    running_zone: &'t RunningZone,
    smf_name: String,
    default_smf_name: String,
}

fn matches_no_such_property(e: &dyn std::error::Error) -> bool {
    e.to_string().contains("No such property")
        || match e.source() {
            Some(source) => matches_no_such_property(source),
            None => false,
        }
}

fn matches_already_exists(e: &dyn std::error::Error) -> bool {
    e.to_string().contains("already exists")
        || match e.source() {
            Some(source) => matches_already_exists(source),
            None => false,
        }
}

fn matches_not_in_maintenance(e: &dyn std::error::Error) -> bool {
    e.to_string().contains("not in a maintenance")
        || match e.source() {
            Some(source) => matches_not_in_maintenance(source),
            None => false,
        }
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
                &format!("\"{}\"", val.to_string()),
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

    /// Add a property group to the `:default` instance, tolerating an
    /// existing one so that retries don't bubble up side-effects.
    ///
    /// Instance-scoped variant of [`Self::addpropgroup`]. When a service
    /// ships its `config` group at the service level, `svccfg setprop`
    /// against the instance does not auto-create the group, so the first
    /// instance-scoped write fails with "No such property group" unless the
    /// group is created here first.
    pub fn addpropgroup_default_instance<P, T>(
        &self,
        propgroup: P,
        grouptype: T,
    ) -> Result<(), Error>
    where
        P: ToString,
        T: ToString,
    {
        match self.running_zone.run_cmd(&[
            SVCCFG,
            "-s",
            &self.default_smf_name,
            "addpg",
            &propgroup.to_string(),
            &grouptype.to_string(),
        ]) {
            Ok(_) => Ok(()),
            // An already-existing property group is fine, as it means a
            // prior attempt created it and re-adding is a no-op.
            Err(err) if matches_already_exists(&err) => Ok(()),
            Err(err) => Err(Error::ZoneCommand {
                intent: format!(
                    "add {} ({}) smf property group",
                    propgroup.to_string(),
                    grouptype.to_string()
                ),
                err,
            }),
        }
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

    pub fn delpropvalue<P, V>(&self, prop: P, val: V) -> Result<(), Error>
    where
        P: ToString,
        V: ToString,
    {
        match self
            .running_zone
            .run_cmd(&[
                SVCCFG,
                "-s",
                &self.smf_name,
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
                if !matches_no_such_property(&e) {
                    return Err(e);
                }
            }
        };

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
                if !matches_no_such_property(&e) {
                    return Err(e);
                }
            }
        };

        Ok(())
    }

    pub fn refresh(&self) -> Result<(), Error> {
        self.running_zone
            .run_cmd(&[SVCCFG, "-s", &self.default_smf_name, "refresh"])
            .map_err(|err| Error::ZoneCommand {
                intent: format!(
                    "Refresh SMF manifest {}",
                    self.default_smf_name
                ),
                err,
            })?;
        Ok(())
    }

    pub fn enable(&self) -> Result<(), Error> {
        self.running_zone
            .run_cmd(&[SVCADM, "enable", &self.smf_name])
            .map_err(|err| Error::ZoneCommand {
                intent: format!("Enable SMF service {}", self.default_smf_name),
                err,
            })?;
        Ok(())
    }

    pub fn disable(&self) -> Result<(), Error> {
        self.running_zone
            .run_cmd(&[SVCADM, "disable", &self.smf_name])
            .map_err(|err| Error::ZoneCommand {
                intent: format!(
                    "Disable SMF service {}",
                    self.default_smf_name
                ),
                err,
            })?;
        Ok(())
    }

    /// Clear a service instance out of the maintenance state so a subsequent
    /// `enable` or `restart` can take effect. `svcadm clear` fails on an
    /// instance that is not in maintenance (or degraded), and that case is
    /// tolerated here so callers can invoke this unconditionally.
    pub fn clear(&self) -> Result<(), Error> {
        match self.running_zone.run_cmd(&[SVCADM, "clear", &self.smf_name]) {
            Ok(_) => Ok(()),
            Err(err) if matches_not_in_maintenance(&err) => Ok(()),
            Err(err) => Err(Error::ZoneCommand {
                intent: format!(
                    "Clear maintenance state of SMF service {}",
                    self.default_smf_name
                ),
                err,
            }),
        }
    }

    /// Restart the service instance via `svcadm restart`.
    ///
    /// The restart is an asynchronous request to `svc.startd`. Callers that
    /// need the service back online should confirm via [`Self::state`].
    pub fn restart(&self) -> Result<(), Error> {
        self.running_zone
            .run_cmd(&[SVCADM, "restart", &self.smf_name])
            .map_err(|err| Error::ZoneCommand {
                intent: format!(
                    "Restart SMF service {}",
                    self.default_smf_name
                ),
                err,
            })?;
        Ok(())
    }

    /// Report the current state of the service's `:default` instance as
    /// printed by `svcs -H -o state`, for example "online" or "maintenance".
    ///
    /// `svcadm enable` and `restart` are asynchronous requests to
    /// `svc.startd`, so callers that need the service running must observe
    /// the state rather than trust those commands' exit status.
    pub fn state(&self) -> Result<String, Error> {
        self.running_zone
            .run_cmd(&[SVCS, "-H", "-o", "state", &self.default_smf_name])
            .map(|out| out.trim().to_string())
            .map_err(|err| Error::ZoneCommand {
                intent: format!(
                    "Query state of SMF service {}",
                    self.default_smf_name
                ),
                err,
            })
    }
}

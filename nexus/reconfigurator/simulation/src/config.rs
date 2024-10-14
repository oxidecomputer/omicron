// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::ops::Deref;

use indexmap::IndexSet;
use omicron_common::api::external::Name;

use crate::errors::{DuplicateError, MissingError};

/// Versioned simulator configuration.
///
/// This is part of the state that is versioned and stored in the store.
#[derive(Clone, Debug)]
pub struct SimConfig {
    /// Set of silo names configured
    ///
    /// These are used to determine the contents of external DNS.
    silo_names: IndexSet<Name>,

    /// External DNS zone name configured
    external_dns_zone_name: String,

    /// The number of Nexus zones to create.
    ///
    /// TODO: This doesn't quite fit in here because it's more of a policy
    /// setting than a config option. But we can't set it in the
    /// `SystemDescription` because need to persist policy across system wipes.
    /// So users have to remember to set num_nexus twice: once in the config
    /// and once in the policy.
    ///
    /// We can likely make this better after addressing
    /// https://github.com/oxidecomputer/omicron/issues/6803.
    num_nexus: Option<u16>,
}

impl SimConfig {
    pub(crate) fn new() -> Self {
        Self {
            // We use "example-silo" here rather than "default-silo" to make it
            // clear that we're in a test environment.
            silo_names: std::iter::once("example-silo".parse().unwrap())
                .collect(),
            external_dns_zone_name: String::from("oxide.example"),
            num_nexus: None,
        }
    }

    #[inline]
    pub fn silo_names(&self) -> impl ExactSizeIterator<Item = &Name> {
        self.silo_names.iter()
    }

    #[inline]
    pub fn external_dns_zone_name(&self) -> &str {
        &self.external_dns_zone_name
    }

    #[inline]
    pub fn num_nexus(&self) -> Option<u16> {
        self.num_nexus
    }

    pub(crate) fn to_mut(&self) -> MutableSimConfig {
        MutableSimConfig { config: self.clone(), log: Vec::new() }
    }
}

#[derive(Clone, Debug)]
pub struct MutableSimConfig {
    config: SimConfig,
    log: Vec<SimConfigLogEntry>,
}

impl MutableSimConfig {
    pub fn set_silo_names(&mut self, names: impl IntoIterator<Item = Name>) {
        self.config.silo_names = names.into_iter().collect();
        self.log.push(SimConfigLogEntry::SetSiloNames(
            self.config.silo_names.clone(),
        ));
    }

    pub fn add_silo(&mut self, name: Name) -> Result<(), DuplicateError> {
        if self.config.silo_names.contains(&name) {
            return Err(DuplicateError::silo_name(name));
        }
        self.config.silo_names.insert(name.clone());
        self.log.push(SimConfigLogEntry::AddSilo(name));
        Ok(())
    }

    pub fn remove_silo(&mut self, name: Name) -> Result<(), MissingError> {
        if !self.config.silo_names.shift_remove(&name) {
            return Err(MissingError::silo_name(name));
        }
        self.log.push(SimConfigLogEntry::RemoveSilo(name));
        Ok(())
    }

    pub fn set_external_dns_zone_name(&mut self, name: String) {
        self.config.external_dns_zone_name = name.clone();
        self.log.push(SimConfigLogEntry::SetExternalDnsZoneName(name));
    }

    pub fn set_num_nexus(&mut self, num_nexus: u16) {
        self.config.num_nexus = Some(num_nexus);
    }

    pub fn wipe(&mut self) {
        self.config = SimConfig::new();
        self.log.push(SimConfigLogEntry::Wipe);
    }

    pub(crate) fn into_parts(self) -> (SimConfig, Vec<SimConfigLogEntry>) {
        (self.config, self.log)
    }
}

impl Deref for MutableSimConfig {
    type Target = SimConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

#[derive(Clone, Debug)]
pub enum SimConfigLogEntry {
    AddSilo(Name),
    RemoveSilo(Name),
    SetSiloNames(IndexSet<Name>),
    SetExternalDnsZoneName(String),
    Wipe,
}

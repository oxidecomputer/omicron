// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{collections::BTreeSet, fmt};

use indexmap::IndexSet;
use nexus_types::deployment::{Blueprint, BlueprintTarget};
use omicron_common::api::external::{Generation, Name};
use omicron_uuid_kinds::OmicronZoneUuid;

use crate::{
    LoadSerializedResultBuilder,
    errors::{DuplicateError, KeyError},
    utils::join_comma_or_none,
};

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
    /// So callers have to remember to set num_nexus twice: once in the config
    /// and once in the policy.
    ///
    /// We can likely make this better after addressing
    /// <https://github.com/oxidecomputer/omicron/issues/6803>.
    num_nexus: Option<u16>,

    /// The Nexus generation to treat as the active set for the purposes of
    /// simulating handoff between updates.
    active_nexus_zone_generation: Generation,

    /// Explicit override for active nexus zones.
    /// When set, overrides generation-based inference.
    explicit_active_nexus_zones: Option<BTreeSet<OmicronZoneUuid>>,

    /// Explicit override for not-yet nexus zones.
    /// When set, overrides generation-based inference.
    explicit_not_yet_nexus_zones: Option<BTreeSet<OmicronZoneUuid>>,
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
            active_nexus_zone_generation: Generation::new(),
            explicit_active_nexus_zones: None,
            explicit_not_yet_nexus_zones: None,
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

    #[inline]
    pub fn active_nexus_zone_generation(&self) -> Generation {
        self.active_nexus_zone_generation
    }

    #[inline]
    pub fn explicit_active_nexus_zones(
        &self,
    ) -> Option<&BTreeSet<OmicronZoneUuid>> {
        self.explicit_active_nexus_zones.as_ref()
    }

    #[inline]
    pub fn explicit_not_yet_nexus_zones(
        &self,
    ) -> Option<&BTreeSet<OmicronZoneUuid>> {
        self.explicit_not_yet_nexus_zones.as_ref()
    }

    pub(crate) fn to_mut(&self) -> SimConfigBuilder {
        SimConfigBuilder {
            inner: SimConfigBuilderInner { config: self.clone() },
            log: Vec::new(),
        }
    }
}

/// A [`SimConfig`] that can be changed to create new states.
///
/// Returned by
/// [`SimStateBuilder::config_mut`](crate::SimStateBuilder::config_mut).
#[derive(Clone, Debug)]
pub struct SimConfigBuilder {
    inner: SimConfigBuilderInner,
    log: Vec<SimConfigLogEntry>,
}

impl SimConfigBuilder {
    // These methods are duplicated from `SimConfig`. The forwarding is all
    // valid because we don't cache pending changes in this struct, instead
    // making them directly to the underlying config. If we did cache changes,
    // we'd need to be more careful about how we forward these methods.

    #[inline]
    pub fn silo_names(&self) -> impl ExactSizeIterator<Item = &Name> {
        self.inner.config.silo_names()
    }

    #[inline]
    pub fn external_dns_zone_name(&self) -> &str {
        self.inner.config.external_dns_zone_name()
    }

    #[inline]
    pub fn num_nexus(&self) -> Option<u16> {
        self.inner.config.num_nexus()
    }

    #[inline]
    pub fn active_nexus_zone_generation(&self) -> Generation {
        self.inner.config.active_nexus_zone_generation()
    }

    #[inline]
    pub fn explicit_active_nexus_zones(
        &self,
    ) -> Option<&BTreeSet<OmicronZoneUuid>> {
        self.inner.config.explicit_active_nexus_zones()
    }

    #[inline]
    pub fn explicit_not_yet_nexus_zones(
        &self,
    ) -> Option<&BTreeSet<OmicronZoneUuid>> {
        self.inner.config.explicit_not_yet_nexus_zones()
    }

    /// Load a serialized configuration state.
    pub(crate) fn load_serialized(
        &mut self,
        external_dns_zone_names: Vec<String>,
        silo_names: Vec<Name>,
        active_nexus_zones: &BTreeSet<OmicronZoneUuid>,
        target_blueprint: Option<&BlueprintTarget>,
        all_blueprints: &[Blueprint],
        res: &mut LoadSerializedResultBuilder,
    ) -> LoadSerializedConfigResult {
        self.inner.load_serialized_inner(
            external_dns_zone_names,
            silo_names,
            active_nexus_zones,
            target_blueprint,
            all_blueprints,
            res,
        )
    }

    pub fn add_silo(&mut self, name: Name) -> Result<(), DuplicateError> {
        self.inner.add_silo_inner(name.clone())?;
        self.log.push(SimConfigLogEntry::AddSilo(name));
        Ok(())
    }

    pub fn remove_silo(&mut self, name: Name) -> Result<(), KeyError> {
        self.inner.remove_silo_inner(name.clone())?;
        self.log.push(SimConfigLogEntry::RemoveSilo(name));
        Ok(())
    }

    pub fn set_external_dns_zone_name(&mut self, name: String) {
        self.inner.set_external_dns_zone_name_inner(name.clone());
        self.log.push(SimConfigLogEntry::SetExternalDnsZoneName(name));
    }

    pub fn set_num_nexus(&mut self, num_nexus: u16) {
        self.inner.set_num_nexus_inner(num_nexus);
        self.log.push(SimConfigLogEntry::SetNumNexus(num_nexus));
    }

    pub fn set_active_nexus_zone_generation(&mut self, generation: Generation) {
        self.inner.set_active_nexus_zone_generation(generation);
        self.log
            .push(SimConfigLogEntry::SetActiveNexusZoneGeneration(generation));
    }

    pub fn set_explicit_active_nexus_zones(
        &mut self,
        zones: Option<BTreeSet<OmicronZoneUuid>>,
    ) {
        self.inner.set_explicit_active_nexus_zones(zones.clone());
        self.log.push(SimConfigLogEntry::SetExplicitActiveNexusZones(zones));
    }

    pub fn set_explicit_not_yet_nexus_zones(
        &mut self,
        zones: Option<BTreeSet<OmicronZoneUuid>>,
    ) {
        self.inner.set_explicit_not_yet_nexus_zones(zones.clone());
        self.log.push(SimConfigLogEntry::SetExplicitNotYetNexusZones(zones));
    }

    pub fn wipe(&mut self) {
        self.inner.wipe_inner();
        self.log.push(SimConfigLogEntry::Wipe);
    }

    pub(crate) fn into_parts(self) -> (SimConfig, Vec<SimConfigLogEntry>) {
        (self.inner.config, self.log)
    }
}

#[derive(Clone, Debug)]
pub enum SimConfigLogEntry {
    LoadSerialized(LoadSerializedConfigResult),
    AddSilo(Name),
    RemoveSilo(Name),
    SetSiloNames(IndexSet<Name>),
    SetExternalDnsZoneName(String),
    SetNumNexus(u16),
    SetActiveNexusZoneGeneration(Generation),
    SetExplicitActiveNexusZones(Option<BTreeSet<OmicronZoneUuid>>),
    SetExplicitNotYetNexusZones(Option<BTreeSet<OmicronZoneUuid>>),
    Wipe,
}

/// The output of loading a serializable state into a [`SimConfigBuilder`].
#[derive(Clone, Debug)]
#[must_use]
pub struct LoadSerializedConfigResult {
    /// The external DNS zone name loaded.
    pub external_dns_zone_name: Option<String>,

    /// The silo names loaded.
    pub silo_names: Vec<Name>,

    /// The generation of active Nexus zones loaded.
    pub active_nexus_generation: Generation,
}

impl fmt::Display for LoadSerializedConfigResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.external_dns_zone_name {
            writeln!(f, "configured external DNS zone name: {name}")?;
        } else {
            writeln!(f, "existing external DNS zone name retained")?;
        }

        writeln!(
            f,
            "configured silo names: {}",
            join_comma_or_none(&self.silo_names)
        )?;

        writeln!(
            f,
            "active Nexus generation: {}",
            self.active_nexus_generation,
        )?;

        Ok(())
    }
}

/// Inner structure for configuration building.
///
/// This is mostly to ensure a clean separation between the public API which
/// adds log entries, and internal methods which don't.
#[derive(Clone, Debug)]
struct SimConfigBuilderInner {
    config: SimConfig,
}

impl SimConfigBuilderInner {
    fn load_serialized_inner(
        &mut self,
        external_dns_zone_names: Vec<String>,
        silo_names: Vec<Name>,
        active_nexus_zones: &BTreeSet<OmicronZoneUuid>,
        target_blueprint: Option<&BlueprintTarget>,
        all_blueprints: &[Blueprint],
        res: &mut LoadSerializedResultBuilder,
    ) -> LoadSerializedConfigResult {
        let nnames = external_dns_zone_names.len();
        let external_dns_zone_name = match nnames {
            0 => None,
            1 => Some(external_dns_zone_names[0].clone()),
            2.. => {
                res.warnings.push(format!(
                    "found {} external DNS names; using only the first one",
                    nnames
                ));
                Some(external_dns_zone_names[0].clone())
            }
        };

        if let Some(name) = &external_dns_zone_name {
            self.set_external_dns_zone_name_inner(name.clone());
        }

        self.set_silo_names_inner(silo_names.clone());

        // Determine the active Nexus generation by comparing the current set of
        // active Nexus zones to the current target blueprint.
        let active_nexus_generation = match determine_active_nexus_generation(
            active_nexus_zones,
            target_blueprint,
            all_blueprints,
        ) {
            Ok(generation) => generation,
            Err(message) => {
                res.warnings.push(format!(
                    "could not determine active Nexus \
                     generation from serialized state: \
                     {message} (using default of 1)"
                ));
                Generation::new()
            }
        };
        self.set_active_nexus_zone_generation(active_nexus_generation);

        LoadSerializedConfigResult {
            external_dns_zone_name,
            silo_names,
            active_nexus_generation,
        }
    }

    // Not public: the only caller of this is load_serialized.
    fn set_silo_names_inner(&mut self, names: impl IntoIterator<Item = Name>) {
        self.config.silo_names = names.into_iter().collect();
    }

    fn add_silo_inner(&mut self, name: Name) -> Result<(), DuplicateError> {
        if self.config.silo_names.contains(&name) {
            return Err(DuplicateError::silo_name(name));
        }
        self.config.silo_names.insert(name.clone());
        Ok(())
    }

    fn remove_silo_inner(&mut self, name: Name) -> Result<(), KeyError> {
        if !self.config.silo_names.shift_remove(&name) {
            return Err(KeyError::silo_name(name));
        }
        Ok(())
    }

    fn set_external_dns_zone_name_inner(&mut self, name: String) {
        self.config.external_dns_zone_name = name;
    }

    fn set_num_nexus_inner(&mut self, num_nexus: u16) {
        self.config.num_nexus = Some(num_nexus);
    }

    fn set_active_nexus_zone_generation(&mut self, generation: Generation) {
        self.config.active_nexus_zone_generation = generation;
    }

    fn set_explicit_active_nexus_zones(
        &mut self,
        zones: Option<BTreeSet<OmicronZoneUuid>>,
    ) {
        self.config.explicit_active_nexus_zones = zones;
    }

    fn set_explicit_not_yet_nexus_zones(
        &mut self,
        zones: Option<BTreeSet<OmicronZoneUuid>>,
    ) {
        self.config.explicit_not_yet_nexus_zones = zones;
    }

    fn wipe_inner(&mut self) {
        self.config = SimConfig::new();
    }
}

fn determine_active_nexus_generation(
    active_nexus_zones: &BTreeSet<OmicronZoneUuid>,
    target_blueprint: Option<&BlueprintTarget>,
    all_blueprints: &[Blueprint],
) -> Result<Generation, String> {
    let Some(target_blueprint) = target_blueprint else {
        return Err("no target blueprint set".to_string());
    };

    let Some(blueprint) =
        all_blueprints.iter().find(|bp| bp.id == target_blueprint.target_id)
    else {
        return Err(format!(
            "target blueprint {} not found",
            target_blueprint.target_id
        ));
    };

    let maybe_gen = blueprint
        .find_generation_for_nexus(active_nexus_zones)
        .map_err(|err| format!("{err:#}"))?;

    maybe_gen.ok_or_else(|| {
        format!(
            "could not find Nexus zones in current target blueprint: {:?}",
            active_nexus_zones
                .iter()
                .map(|z| z.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

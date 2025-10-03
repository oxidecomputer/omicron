// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use nexus_inventory::CollectionBuilder;
use nexus_types::deployment::UnstableReconfiguratorState;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::{CollectionUuid, ReconfiguratorSimUuid};
use sync_ptr::SyncConstPtr;

use crate::{
    LoadSerializedConfigResult, LoadSerializedSystemResult, SimConfigBuilder,
    SimConfigLogEntry, SimRng, SimRngBuilder, SimRngLogEntry, SimSystem,
    SimSystemBuilder, SimSystemLogEntry, Simulator, config::SimConfig,
    errors::NonEmptySystemError,
};

/// A top-level, versioned snapshot of reconfigurator state.
///
/// This snapshot consists of a system, along with a policy and a stateful RNG.
#[derive(Clone, Debug)]
pub struct SimState {
    // A pointer to the root state (self if the current state *is* the root
    // state). This is used to check in `SimStateBuilder` that the simulator is
    // the same as the one that created this state.
    //
    // We store the root state, not the simulator itself, because the root state
    // is stored behind an `Arc`. This means that the address stays stable even
    // if the `Simulator` struct is cloned or moved in memory.
    root_state: SyncConstPtr<SimState>,
    id: ReconfiguratorSimUuid,
    // The parent state that this state was derived from.
    parent: Option<ReconfiguratorSimUuid>,
    // The state's generation, starting from 0.
    //
    // TODO: Should this be its own type to avoid confusion with other
    // Generation instances? Generation numbers start from 1, but in our case 0
    // provides a better user experience.
    generation: Generation,
    description: String,
    system: SimSystem,
    config: SimConfig,
    rng: SimRng,
    // A log of changes in this state compared to the parent state.
    log: SimStateLog,
}

impl SimState {
    pub(crate) fn new_root(seed: String) -> Arc<Self> {
        Arc::new_cyclic(|state| {
            Self {
                // Store a pointer to the root state's allocation to ensure that
                // unrelated histories aren't mixed up.
                //
                // SAFETY: We only care about pointer equality, and never
                // dereference this pointer.
                root_state: unsafe { SyncConstPtr::new(state.as_ptr()) },
                id: Simulator::ROOT_ID,
                parent: None,
                // We don't normally use generation 0 in the production system, but
                // having it here means that we can present a better user
                // experience (first change is generation 1).
                generation: Generation::from_u32(0),
                description: "root state".to_string(),
                system: SimSystem::new(),
                config: SimConfig::new(),
                rng: SimRng::from_seed(seed),
                log: SimStateLog {
                    system: Vec::new(),
                    config: Vec::new(),
                    rng: Vec::new(),
                },
            }
        })
    }

    #[inline]
    #[must_use]
    pub fn id(&self) -> ReconfiguratorSimUuid {
        self.id
    }

    #[inline]
    #[must_use]
    pub fn parent(&self) -> Option<ReconfiguratorSimUuid> {
        self.parent
    }

    #[inline]
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[inline]
    #[must_use]
    pub fn system(&self) -> &SimSystem {
        &self.system
    }

    #[inline]
    #[must_use]
    pub fn config(&self) -> &SimConfig {
        &self.config
    }

    #[inline]
    #[must_use]
    pub fn rng(&self) -> &SimRng {
        &self.rng
    }

    #[inline]
    #[must_use]
    pub fn log(&self) -> &SimStateLog {
        &self.log
    }

    /// Convert the state to a serializable form.
    ///
    /// Return a [`UnstableReconfiguratorState`] with information about the
    /// current state.
    pub fn to_serializable(
        &self,
    ) -> anyhow::Result<UnstableReconfiguratorState> {
        let planning_input = self
            .system()
            .description()
            .to_planning_input_builder()
            .context("creating planning input builder")?
            .build();

        Ok(UnstableReconfiguratorState {
            planning_input,
            collections: self.system.all_collections().cloned().collect(),
            target_blueprint: self.system().target_blueprint(),
            blueprints: self.system.all_blueprints().cloned().collect(),
            internal_dns: self
                .system
                .all_internal_dns()
                .map(|params| (params.generation, params.clone()))
                .collect(),
            external_dns: self
                .system
                .all_external_dns()
                .map(|params| (params.generation, params.clone()))
                .collect(),
            silo_names: self.config.silo_names().cloned().collect(),
            external_dns_zone_names: vec![
                self.config.external_dns_zone_name().to_owned(),
            ],
        })
    }

    pub fn to_mut(&self) -> SimStateBuilder {
        SimStateBuilder {
            root_state: self.root_state,
            parent: self.id,
            parent_gen: self.generation,
            system: self.system.to_mut(),
            config: self.config.to_mut(),
            rng: self.rng.to_mut(),
        }
    }
}

/// A [`SimState`] that can be changed to create new states.
///
/// Created by [`SimState::to_mut`].
///
/// `SimStateBuilder` is ephemeral, so it can be freely mutated without
/// affecting anything else about the system. To store it into a system, call
/// [`Self::commit`].
#[derive(Clone, Debug)]
pub struct SimStateBuilder {
    // Used to check that the simulator is the same as the one that created
    // this state.
    root_state: SyncConstPtr<SimState>,
    parent: ReconfiguratorSimUuid,
    parent_gen: Generation,
    system: SimSystemBuilder,
    config: SimConfigBuilder,
    rng: SimRngBuilder,
}

impl SimStateBuilder {
    #[inline]
    #[must_use]
    pub fn parent(&self) -> ReconfiguratorSimUuid {
        self.parent
    }

    #[inline]
    #[must_use]
    pub fn system_mut(&mut self) -> &mut SimSystemBuilder {
        &mut self.system
    }

    #[inline]
    #[must_use]
    pub fn config_mut(&mut self) -> &mut SimConfigBuilder {
        &mut self.config
    }

    #[inline]
    #[must_use]
    pub fn rng_mut(&mut self) -> &mut SimRngBuilder {
        &mut self.rng
    }

    /// Load a serialized state into an empty system.
    ///
    /// If the primary collection ID is not provided, the serialized state must
    /// only contain one collection.
    pub fn load_serialized(
        &mut self,
        state: UnstableReconfiguratorState,
        primary_collection_id: Option<CollectionUuid>,
    ) -> anyhow::Result<LoadSerializedResult> {
        if !self.system.is_empty() {
            return Err(anyhow!(NonEmptySystemError::new()));
        }

        let primary_collection_id =
            get_primary_collection_id(&state, primary_collection_id)?;

        // NOTE: If more error cases are added, ensure that they're checked
        // before this point. This ensures that the system is not modified if
        // there are errors.

        let mut res = LoadSerializedResultBuilder::default();

        let config = self.config.load_serialized(
            state.external_dns_zone_names.clone(),
            state.silo_names.clone(),
            state.planning_input.active_nexus_zones(),
            state.target_blueprint.as_ref(),
            &state.blueprints,
            &mut res,
        );
        let system =
            self.system.load_serialized(state, primary_collection_id, &mut res);

        Ok(LoadSerializedResult {
            primary_collection_id,
            system,
            config,
            warnings: res.warnings,
        })
    }

    /// Commit the current state to the simulator, returning the new state's
    /// UUID.
    ///
    /// # Panics
    ///
    /// Panics if `sim` is not the same simulator that created this state.
    /// This should ordinarily never happen and always indicates a
    /// programming error.
    #[must_use = "callers should update their pointers with the returned UUID"]
    pub fn commit(
        self,
        description: String,
        sim: &mut Simulator,
    ) -> ReconfiguratorSimUuid {
        // Check for unrelated histories.
        if !std::ptr::eq(sim.root_state(), self.root_state.inner()) {
            panic!(
                "this state was created by a different simulator than the one \
                 it is being committed to"
            );
        }

        let id = sim.next_sim_uuid();
        let (system, system_log) = self.system.into_parts();
        let (config, config_log) = self.config.into_parts();
        let (rng, rng_log) = self.rng.into_parts();
        let log = SimStateLog {
            system: system_log,
            config: config_log,
            rng: rng_log,
        };
        let state = SimState {
            root_state: self.root_state,
            id,
            description,
            parent: Some(self.parent),
            generation: self.parent_gen.next(),
            system,
            config,
            rng,
            log,
        };
        sim.add_state(Arc::new(state));
        id
    }

    // TODO: should probably enforce that RNG is set, maybe by hiding the
    // SystemDescription struct?
    pub fn to_collection_builder(
        &mut self,
    ) -> anyhow::Result<CollectionBuilder> {
        let mut builder = self
            .system
            .description()
            .to_collection_builder()
            .context("generating inventory")?;

        let rng = self.rng.next_collection_rng();
        builder.set_rng(rng);

        Ok(builder)
    }
}

/// A log of changes made to a state compared to the parent.
#[derive(Clone, Debug)]
pub struct SimStateLog {
    pub system: Vec<SimSystemLogEntry>,
    pub config: Vec<SimConfigLogEntry>,
    pub rng: Vec<SimRngLogEntry>,
}

/// The output of loading a serializable state into a system.
#[derive(Clone, Debug)]
#[must_use]
pub struct LoadSerializedResult {
    /// The primary collection ID.
    pub primary_collection_id: CollectionUuid,

    /// Information about what was loaded into the system.
    pub system: LoadSerializedSystemResult,

    /// Information about loaded configuration parameters.
    pub config: LoadSerializedConfigResult,

    // TODO: Storing warnings as strings is a carryover from reconfigurator-cli.
    // We may wish to store warnings in a more structured form, maybe as part
    // of LoadSerializedSystemResult.
    /// Non-fatal warnings that occurred.
    pub warnings: Vec<String>,
}

/// Check and get the primary collection ID for a serialized state.
fn get_primary_collection_id(
    state: &UnstableReconfiguratorState,
    provided: Option<CollectionUuid>,
) -> anyhow::Result<CollectionUuid> {
    match provided {
        Some(id) => {
            // Check that the collection ID is valid.
            if state.collections.iter().any(|c| c.id == id) {
                Ok(id)
            } else {
                bail!("collection {} not found in data", id)
            }
        }
        None => match state.collections.len() {
            1 => Ok(state.collections[0].id),
            0 => bail!(
                "no collection_id specified and file contains 0 collections"
            ),
            count => bail!(
                "no collection_id specified and file contains {} \
                    collections: {}",
                count,
                state
                    .collections
                    .iter()
                    .map(|c| c.id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        },
    }
}

#[derive(Debug, Default)]
pub(crate) struct LoadSerializedResultBuilder {
    pub(crate) warnings: Vec<String>,
}

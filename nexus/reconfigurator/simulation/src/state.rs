// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{anyhow, bail, Context};
use nexus_inventory::CollectionBuilder;
use nexus_reconfigurator_planning::system::SledHwInventory;
use nexus_types::deployment::{SledFilter, UnstableReconfiguratorState};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::{CollectionUuid, ReconfiguratorSimUuid};

use crate::{
    config::SimConfig, errors::NonEmptySystemError, MutableSimSystem,
    SimConfigBuilder, SimConfigLogEntry, SimRng, SimSystem, SimSystemLogEntry,
    Simulator,
};

/// A point-in-time snapshot of reconfigurator state.
///
/// This snapshot consists of a system, along with a policy and a stateful RNG.
#[derive(Clone, Debug)]
pub struct SimState {
    id: ReconfiguratorSimUuid,
    // The parent state that this state was derived from.
    parent: Option<ReconfiguratorSimUuid>,
    // The state's generation, starting from 0.
    //
    // XXX should this be its own type to avoid confusion with other Generation
    // instances?
    generation: Generation,
    description: String,
    system: SimSystem,
    config: SimConfig,
    rng: SimRng,
    // A log of changes in this state compared to the parent state.
    log: SimStateLog,
}

impl SimState {
    pub(crate) fn new_root(seed: String) -> Self {
        Self {
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
            log: SimStateLog { system: Vec::new(), config: Vec::new() },
        }
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
            external_dns_zone_names: vec![self
                .config
                .external_dns_zone_name()
                .to_owned()],
        })
    }

    pub fn to_mut(&self) -> SimStateBuilder {
        SimStateBuilder {
            parent: self.id,
            parent_gen: self.generation,
            system: self.system.to_mut(),
            config: self.config.to_mut(),
            rng: self.rng.clone(),
        }
    }
}

/// Reconfigurator state that can be changed.
///
/// `SimStateBuilder` is ephemeral, so it can be freely mutated without
/// affecting anything else about the system. To store it into a system, commit
/// it to a `SimStore`.
#[derive(Clone, Debug)]
pub struct SimStateBuilder {
    parent: ReconfiguratorSimUuid,
    parent_gen: Generation,
    system: MutableSimSystem,
    config: SimConfigBuilder,
    rng: SimRng,
}

impl SimStateBuilder {
    #[inline]
    #[must_use]
    pub fn parent(&self) -> ReconfiguratorSimUuid {
        self.parent
    }

    #[inline]
    #[must_use]
    pub fn system_mut(&mut self) -> &mut MutableSimSystem {
        &mut self.system
    }

    #[inline]
    #[must_use]
    pub fn config_mut(&mut self) -> &mut SimConfigBuilder {
        &mut self.config
    }

    #[inline]
    #[must_use]
    pub fn rng_mut(&mut self) -> &mut SimRng {
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
    ) -> anyhow::Result<LoadResult> {
        if !self.system.is_empty() {
            return Err(anyhow!(NonEmptySystemError::new()));
        }

        let collection_id =
            get_primary_collection_id(&state, primary_collection_id)?;

        // NOTE: If more error cases are added, ensure that they're checked
        // before load_serialized_inner is called. This ensures that the system
        // is not modified if there are errors.
        let mut res = LoadResultBuilder::default();
        self.load_serialized_inner(state, collection_id, &mut res);

        Ok(LoadResult {
            primary_collection_id: collection_id,
            notices: res.notices,
            warnings: res.warnings,
        })
    }

    // This method MUST be infallible. It should only be called after checking
    // the invariant: the primary collection ID is valid.
    fn load_serialized_inner(
        &mut self,
        state: UnstableReconfiguratorState,
        primary_collection_id: CollectionUuid,
        res: &mut LoadResultBuilder,
    ) {
        res.notices.push(format!(
            "using collection {} as source of sled inventory data",
            primary_collection_id,
        ));
        let primary_collection = state
            .collections
            .iter()
            .find(|c| c.id == primary_collection_id)
            .expect("invariant: primary collection ID is valid");

        for (sled_id, sled_details) in
            state.planning_input.all_sleds(SledFilter::Commissioned)
        {
            let Some(inventory_sled_agent) =
                primary_collection.sled_agents.get(&sled_id)
            else {
                res.warnings.push(format!(
                    "sled {}: skipped (no inventory found for sled agent in \
                     collection {}",
                    sled_id, primary_collection_id
                ));
                continue;
            };

            let inventory_sp = inventory_sled_agent
                .baseboard_id
                .as_ref()
                .and_then(|baseboard_id| {
                    let inv_sp = primary_collection.sps.get(baseboard_id);
                    let inv_rot = primary_collection.rots.get(baseboard_id);
                    if let (Some(inv_sp), Some(inv_rot)) = (inv_sp, inv_rot) {
                        Some(SledHwInventory {
                            baseboard_id: &baseboard_id,
                            sp: inv_sp,
                            rot: inv_rot,
                        })
                    } else {
                        None
                    }
                });

            // XXX: Should this error ever happen? The only case where it
            // errors is if the sled ID is already present, but we know that
            // the system is empty, and the state's planning input is keyed by
            // sled ID, so there should be no duplicates.
            let result = self.system.description_mut().sled_full(
                sled_id,
                sled_details.policy,
                sled_details.state,
                sled_details.resources.clone(),
                inventory_sp,
                inventory_sled_agent,
            );

            match result {
                Ok(_) => {
                    res.notices.push(format!("sled {}: loaded", sled_id));
                }
                Err(error) => {
                    // Failing to load a sled shouldn't really happen, but if
                    // it does, it is a non-fatal error.
                    res.warnings.push(format!("sled {}: {:#}", sled_id, error));
                }
            };
        }

        for collection in state.collections {
            let collection_id = collection.id;
            match self.system.add_collection(collection) {
                Ok(_) => {
                    res.notices
                        .push(format!("collection {}: loaded", collection_id));
                }
                Err(_) => {
                    res.warnings.push(format!(
                        "collection {}: skipped (duplicate found)",
                        collection_id,
                    ));
                }
            }
        }

        for blueprint in state.blueprints {
            let blueprint_id = blueprint.id;
            match self.system.add_blueprint(blueprint) {
                Ok(_) => {
                    res.notices
                        .push(format!("blueprint {}: loaded", blueprint_id));
                }
                Err(_) => {
                    res.notices.push(format!(
                        "blueprint {}: skipped (duplicate found)",
                        blueprint_id,
                    ));
                }
            }
        }

        self.system.description_mut().service_ip_pool_ranges(
            state.planning_input.service_ip_pool_ranges().to_vec(),
        );
        res.notices.push(format!(
            // TODO: better output format?
            "loaded service IP pool ranges: {:?}",
            state.planning_input.service_ip_pool_ranges()
        ));

        self.system.set_internal_dns(state.internal_dns);
        self.system.set_external_dns(state.external_dns);

        let nnames = state.external_dns_zone_names.len();
        if nnames > 0 {
            if nnames > 1 {
                res.warnings.push(format!(
                    "found {} external DNS names; using only the first one",
                    nnames
                ));
            }
            self.config.set_external_dns_zone_name(
                state.external_dns_zone_names[0].clone(),
            );
        }

        // TODO: Currently this doesn't return notices for DNS and silo names.
        // The only caller of this function prints them separately after
        // committing this state. We may want to record this information in the
        // MergeResult instead.

        // TODO: log what happened here. This is a cross-cutting change so we
        // may want to log it as a single big entry (like
        // MutableSimSystem::load_example) rather than lots of little ones.
    }

    /// Commit the current state to the store, returning the new state's UUID.
    #[must_use = "you should update your state with the new UUID"]
    pub fn commit(
        self,
        description: String,
        sim: &mut Simulator,
    ) -> ReconfiguratorSimUuid {
        let id = sim.next_sim_uuid();
        let (system, system_log) = self.system.into_parts();
        let (config, config_log) = self.config.into_parts();
        let log = SimStateLog { system: system_log, config: config_log };
        let state = SimState {
            id,
            description,
            parent: Some(self.parent),
            generation: self.parent_gen.next(),
            system,
            config,
            rng: self.rng,
            log,
        };
        sim.add_state(state);
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
}

/// The output of merging a serializable state into a mutable state.
#[derive(Clone, Debug)]
#[must_use]
pub struct LoadResult {
    // TODO: Storing notices and warnings as strings is a carryover from
    // reconfigurator-cli. We may wish to store data in a more structured form.
    // For example, store a map of sled IDs to their statuses, etc.
    /// The primary collection ID.
    pub primary_collection_id: CollectionUuid,

    /// Notices for the caller to display.
    pub notices: Vec<String>,

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
struct LoadResultBuilder {
    notices: Vec<String>,
    warnings: Vec<String>,
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A simulated reconfigurator system.

use std::{collections::BTreeMap, fmt, sync::Arc};

use chrono::Utc;
use indexmap::IndexMap;
use nexus_reconfigurator_planning::{
    example::ExampleSystem,
    system::{SledHwInventory, SystemDescription},
};
use nexus_types::{
    deployment::{
        Blueprint, BlueprintTarget, SledFilter, UnstableReconfiguratorState,
    },
    internal_api::params::{DnsConfigParams, DnsConfigZone},
    inventory::{CabooseWhich, Collection},
};
use omicron_common::{
    address::IpRange, api::external::Generation, disk::M2Slot,
};
use omicron_uuid_kinds::{BlueprintUuid, CollectionUuid, SledUuid};
use strum::IntoEnumIterator as _;

use crate::{
    LoadSerializedResultBuilder,
    errors::{DuplicateError, KeyError, NonEmptySystemError},
    utils::{insert_sorted_by, join_comma_or_none},
};

/// A versioned, simulated reconfigurator system.
#[derive(Clone, Debug)]
pub struct SimSystem {
    // Implementation note: an alternative way to store data would be for
    // `Simulator` to carry a global store with it, and then each system only
    // stores the presence of blueprints/collections/etc rather than the
    // objects themselves. In other words, a simulator-wide object store. A few
    // things become easier that way, such as being able to iterate over all
    // known objects of a given type.
    //
    // However, there are a few issues with this approach in practice:
    //
    // 1. The blueprints and collections are not guaranteed to be unique per
    //    UUID. Unlike (say) source control commit hashes, UUIDs are not
    //    content-hashed, so the same UUID can be associated with different
    //    blueprints/collections.
    // 2. DNS configs are absolutely not unique per generation! Again, not
    //    content-hashed.
    // 3. The mutable system may wish to not add objects to the store until
    //    it's committed. This means that the mutable system would probably
    //    have to maintain a list of pending objects to add to the store. That
    //    complicates some of the internals, if not the API.
    // 4. We'll have to figure out how to manage access to the store, so
    //    SimSystemBuilder can access existing blueprints/collections while
    //    it's in flight. Options include:
    //
    //    - Storing a reference to the store as a `&mut` reference (not great
    //      because multiple parts can't build up new states at the same time).
    //    - Storing a reference as `&Mutex<Store>` or `Arc<Mutex<Store>>`.
    //    - Storing a reference as `&RefCell<Store>` or `Rc<RefCell<Store>>`.
    //
    // None of these are insurmountable, but they do make the global store a
    // bit less appealing than it might seem at first glance.
    //
    /// Describes the sleds in the system.
    ///
    /// This resembles what we get from the `sled` table in a real system.  It
    /// also contains enough information to generate inventory collections that
    /// describe the system.
    description: SystemDescription,

    /// Inventory collections created by the user. Invariant: the `IndexMap`
    /// is ordered by time started.
    ///
    /// Stored with `Arc` to allow cheap cloning.
    collections: IndexMap<CollectionUuid, Arc<Collection>>,

    /// Blueprints created by the user. Invariant: the `IndexMap` is ordered by
    /// time created.
    ///
    /// Stored with `Arc` to allow cheap cloning.
    blueprints: IndexMap<BlueprintUuid, Arc<Blueprint>>,

    /// Current target blueprint.
    target_blueprint: Option<BlueprintTarget>,

    /// Internal DNS configurations.
    ///
    /// Stored with `Arc` to allow cheap cloning.
    internal_dns: BTreeMap<Generation, Arc<DnsConfigParams>>,

    /// External DNS configurations.
    ///
    /// Stored with `Arc` to allow cheap cloning.
    external_dns: BTreeMap<Generation, Arc<DnsConfigParams>>,
}

impl SimSystem {
    pub fn new() -> Self {
        Self {
            description: SystemDescription::new(),
            collections: IndexMap::new(),
            blueprints: IndexMap::new(),
            target_blueprint: None,
            internal_dns: BTreeMap::new(),
            external_dns: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.description.has_sleds()
            && self.collections.is_empty()
            && self.blueprints.is_empty()
            && self.target_blueprint.is_none()
            && self.internal_dns.is_empty()
            && self.external_dns.is_empty()
    }

    #[inline]
    pub fn description(&self) -> &SystemDescription {
        &self.description
    }

    pub fn resolve_collection_id(
        &self,
        original: CollectionId,
    ) -> Result<ResolvedCollectionId, KeyError> {
        let resolved = match original {
            CollectionId::Latest => {
                // The invariant of self.collections is that the last element is
                // the latest.
                let (id, _) = self
                    .collections
                    .last()
                    .ok_or(KeyError::collection(original))?;
                *id
            }
            CollectionId::Id(id) => id,
        };
        Ok(ResolvedCollectionId { original, resolved })
    }

    pub fn get_collection(
        &self,
        id: &ResolvedCollectionId,
    ) -> Result<&Collection, KeyError> {
        match self.collections.get(&id.resolved()) {
            Some(c) => Ok(&**c),
            None => Err(KeyError::resolved_collection(id.clone())),
        }
    }

    pub fn all_collections(
        &self,
    ) -> impl ExactSizeIterator<Item = &Collection> {
        self.collections.values().map(|c| &**c)
    }

    pub fn resolve_blueprint_id(
        &self,
        original: BlueprintId,
    ) -> Result<ResolvedBlueprintId, KeyError> {
        let resolved = match original {
            BlueprintId::Target => {
                self.target_blueprint
                    .ok_or_else(|| KeyError::blueprint(original))?
                    .target_id
            }
            BlueprintId::Latest => {
                // The invariant of self.blueprints is that the last element is
                // the latest.
                let (id, _) = self
                    .blueprints
                    .last()
                    .ok_or_else(|| KeyError::blueprint(original))?;
                *id
            }
            BlueprintId::Id(id) => id,
        };
        Ok(ResolvedBlueprintId { original, resolved })
    }

    pub fn get_blueprint(
        &self,
        id: &ResolvedBlueprintId,
    ) -> Result<&Blueprint, KeyError> {
        match self.blueprints.get(&id.resolved()) {
            Some(b) => Ok(&**b),
            None => Err(KeyError::resolved_blueprint(id.clone())),
        }
    }

    /// Combines `self.resolve_blueprint_id` and `self.get_blueprint`.
    ///
    /// This can be convenient when the intermediate [`ResolvedBlueprintId`] is
    /// not needed.
    pub fn resolve_and_get_blueprint(
        &self,
        original: BlueprintId,
    ) -> Result<&Blueprint, KeyError> {
        let id = self.resolve_blueprint_id(original)?;
        self.get_blueprint(&id)
    }

    pub fn target_blueprint(&self) -> Option<BlueprintTarget> {
        self.target_blueprint
    }

    pub fn all_blueprints(&self) -> impl ExactSizeIterator<Item = &Blueprint> {
        self.blueprints.values().map(|b| &**b)
    }

    pub fn get_internal_dns(
        &self,
        generation: Generation,
    ) -> Result<&DnsConfigParams, KeyError> {
        match self.internal_dns.get(&generation) {
            Some(d) => Ok(&**d),
            None => Err(KeyError::internal_dns(generation)),
        }
    }

    pub fn all_internal_dns(
        &self,
    ) -> impl ExactSizeIterator<Item = &DnsConfigParams> {
        self.internal_dns.values().map(|d| &**d)
    }

    pub fn get_external_dns(
        &self,
        generation: Generation,
    ) -> Result<&DnsConfigParams, KeyError> {
        match self.external_dns.get(&generation) {
            Some(d) => Ok(&**d),
            None => Err(KeyError::external_dns(generation)),
        }
    }

    pub fn all_external_dns(
        &self,
    ) -> impl ExactSizeIterator<Item = &DnsConfigParams> {
        self.external_dns.values().map(|d| &**d)
    }

    pub(crate) fn to_mut(&self) -> SimSystemBuilder {
        SimSystemBuilder {
            inner: SimSystemBuilderInner { system: self.clone() },
            log: Vec::new(),
        }
    }
}

/// A [`SimSystem`] that can be changed to create new states.
///
/// Returned by
/// [`SimStateBuilder::system_mut`](crate::SimStateBuilder::system_mut).
#[derive(Clone, Debug)]
pub struct SimSystemBuilder {
    // An inner structure.
    //
    // Methods on `SimSystemBuilder` add log entries, but those on `SimSystemBuilderInner` do not.
    inner: SimSystemBuilderInner,
    // Operation log on the system.
    log: Vec<SimSystemLogEntry>,
}

impl SimSystemBuilder {
    // These methods are duplicated from `SimSystem`. The forwarding is all
    // valid because we don't cache pending changes in this struct, instead
    // making them directly to the underlying system. If we did cache changes,
    // we'd need to be more careful about how we forward these methods.

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.system.is_empty()
    }

    #[inline]
    pub fn description(&self) -> &SystemDescription {
        &self.inner.system.description()
    }

    #[inline]
    pub fn resolve_collection_id(
        &self,
        original: CollectionId,
    ) -> Result<ResolvedCollectionId, KeyError> {
        self.inner.system.resolve_collection_id(original)
    }

    #[inline]
    pub fn get_collection(
        &self,
        id: &ResolvedCollectionId,
    ) -> Result<&Collection, KeyError> {
        self.inner.system.get_collection(id)
    }

    #[inline]
    pub fn all_collections(
        &self,
    ) -> impl ExactSizeIterator<Item = &Collection> {
        self.inner.system.all_collections()
    }

    #[inline]
    pub fn resolve_blueprint_id(
        &self,
        original: BlueprintId,
    ) -> Result<ResolvedBlueprintId, KeyError> {
        self.inner.system.resolve_blueprint_id(original)
    }

    #[inline]
    pub fn get_blueprint(
        &self,
        id: &ResolvedBlueprintId,
    ) -> Result<&Blueprint, KeyError> {
        self.inner.system.get_blueprint(id)
    }

    #[inline]
    pub fn all_blueprints(&self) -> impl ExactSizeIterator<Item = &Blueprint> {
        self.inner.system.all_blueprints()
    }

    #[inline]
    pub fn get_internal_dns(
        &self,
        generation: Generation,
    ) -> Result<&DnsConfigParams, KeyError> {
        self.inner.system.get_internal_dns(generation)
    }

    #[inline]
    pub fn all_internal_dns(
        &self,
    ) -> impl ExactSizeIterator<Item = &DnsConfigParams> {
        self.inner.system.all_internal_dns()
    }

    #[inline]
    pub fn get_external_dns(
        &self,
        generation: Generation,
    ) -> Result<&DnsConfigParams, KeyError> {
        self.inner.system.get_external_dns(generation)
    }

    #[inline]
    pub fn all_external_dns(
        &self,
    ) -> impl ExactSizeIterator<Item = &DnsConfigParams> {
        self.inner.system.all_external_dns()
    }

    // TODO: track changes to the SystemDescription -- we'll probably want to
    // have a separation between a type that represents a read-only system
    // description and a type that can mutate it.
    pub fn description_mut(&mut self) -> &mut SystemDescription {
        &mut self.inner.system.description
    }

    pub fn load_example(
        &mut self,
        example: ExampleSystem,
        blueprint: Blueprint,
        internal_dns: DnsConfigZone,
        external_dns: DnsConfigZone,
    ) -> Result<(), NonEmptySystemError> {
        if !self.is_empty() {
            return Err(NonEmptySystemError::new());
        }

        // NOTE: If more error cases are added, ensure that they're checked
        // before load_example_inner is called. This ensures that the system is
        // not modified if there are errors.

        self.log.push(SimSystemLogEntry::LoadExample {
            collection_id: example.collection.id,
            blueprint_id: blueprint.id,
            internal_dns_version: blueprint.internal_dns_version,
            external_dns_version: blueprint.external_dns_version,
        });
        self.inner.load_example_inner(
            example,
            blueprint,
            internal_dns,
            external_dns,
        );

        Ok(())
    }

    /// Load a serialized system state.
    ///
    /// This requires that the system be empty, and panics if it is not.
    pub(crate) fn load_serialized(
        &mut self,
        state: UnstableReconfiguratorState,
        primary_collection_id: CollectionUuid,
        res: &mut LoadSerializedResultBuilder,
    ) -> LoadSerializedSystemResult {
        assert!(self.is_empty(), "caller must check is_empty first");

        let system_res =
            self.inner.load_serialized_inner(state, primary_collection_id, res);
        self.log.push(SimSystemLogEntry::LoadSerialized(system_res.clone()));
        system_res
    }

    pub fn add_collection(
        &mut self,
        collection: impl Into<Arc<Collection>>,
    ) -> Result<(), DuplicateError> {
        let collection = collection.into();
        let collection_id = collection.id;
        self.inner.add_collection_inner(collection)?;
        // Only add the log entry if the collection was successfully added.
        self.log.push(SimSystemLogEntry::AddCollection(collection_id));
        Ok(())
    }

    pub fn add_blueprint(
        &mut self,
        blueprint: impl Into<Arc<Blueprint>>,
    ) -> Result<(), DuplicateError> {
        let blueprint = blueprint.into();
        let blueprint_id = blueprint.id;
        self.inner.add_blueprint_inner(blueprint)?;
        // Only add the log entry if the blueprint was successfully added.
        self.log.push(SimSystemLogEntry::AddBlueprint(blueprint_id));
        Ok(())
    }

    pub fn add_internal_dns(
        &mut self,
        params: impl Into<Arc<DnsConfigParams>>,
    ) -> Result<(), DuplicateError> {
        let params = params.into();
        let generation = params.generation;
        self.inner.add_internal_dns_inner(params)?;
        // Only add the log entry if the new generation was successfully added.
        self.log.push(SimSystemLogEntry::AddInternalDns(generation));
        Ok(())
    }

    pub fn add_external_dns(
        &mut self,
        params: impl Into<Arc<DnsConfigParams>>,
    ) -> Result<(), DuplicateError> {
        let params = params.into();
        let generation = params.generation;
        self.inner.add_external_dns_inner(params)?;
        // Only add the log entry if the new generation was successfully added.
        self.log.push(SimSystemLogEntry::AddExternalDns(generation));
        Ok(())
    }

    pub fn wipe(&mut self) {
        self.inner.wipe_inner();
        self.log.push(SimSystemLogEntry::Wipe);
    }

    pub(crate) fn into_parts(self) -> (SimSystem, Vec<SimSystemLogEntry>) {
        (self.inner.system, self.log)
    }
}

/// An identifier for a blueprint.
#[derive(Clone, Copy, Debug)]
pub enum BlueprintId {
    /// The target blueprint.
    Target,

    /// The latest blueprint added to the system.
    Latest,

    /// The specified blueprint.
    Id(BlueprintUuid),
}

/// An identifier for a blueprint after it has been resolved.
#[derive(Clone, Debug)]
pub struct ResolvedBlueprintId {
    /// The original blueprint ID.
    original: BlueprintId,

    /// The resolved blueprint ID.
    resolved: BlueprintUuid,
}

impl ResolvedBlueprintId {
    /// Return the original blueprint ID.
    pub fn original(&self) -> BlueprintId {
        self.original
    }

    /// Return the resolved blueprint ID.
    pub fn resolved(&self) -> BlueprintUuid {
        self.resolved
    }
}

impl fmt::Display for ResolvedBlueprintId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.original {
            BlueprintId::Target => {
                write!(f, "target blueprint ({})", self.resolved)
            }
            BlueprintId::Latest => {
                write!(f, "latest blueprint ({})", self.resolved)
            }
            BlueprintId::Id(id) => write!(f, "blueprint {id}"),
        }
    }
}

/// An identifier for an inventory collection.
#[derive(Clone, Copy, Debug)]
pub enum CollectionId {
    /// The latest inventory collection added to the system.
    Latest,

    /// The specified inventory collection.
    Id(CollectionUuid),
}

/// An identifier for a blueprint after it has been resolved.
#[derive(Clone, Debug)]
pub struct ResolvedCollectionId {
    /// The original collection ID.
    original: CollectionId,

    /// The resolved collection ID.
    resolved: CollectionUuid,
}

impl ResolvedCollectionId {
    /// Return the original collection ID.
    pub fn original(&self) -> CollectionId {
        self.original
    }

    /// Return the resolved collection ID.
    pub fn resolved(&self) -> CollectionUuid {
        self.resolved
    }
}

impl fmt::Display for ResolvedCollectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.original {
            CollectionId::Latest => {
                write!(f, "latest collection ({})", self.resolved)
            }
            CollectionId::Id(id) => write!(f, "collection {id}"),
        }
    }
}

/// A log entry corresponding to an individual operation on a
/// [`SimSystemBuilder`].
#[derive(Clone, Debug)]
pub enum SimSystemLogEntry {
    LoadExample {
        collection_id: CollectionUuid,
        blueprint_id: BlueprintUuid,
        internal_dns_version: Generation,
        external_dns_version: Generation,
    },
    LoadSerialized(LoadSerializedSystemResult),
    AddCollection(CollectionUuid),
    AddBlueprint(BlueprintUuid),
    AddInternalDns(Generation),
    AddExternalDns(Generation),
    Wipe,
}

/// The result of loading a serialized system state.
///
/// Returned by [`LoadSerializedResult`](crate::LoadSerializedResult), as well
/// as stored as part of [`SimSystemLogEntry::LoadSerialized`].
#[derive(Clone, Debug)]
#[must_use]
pub struct LoadSerializedSystemResult {
    /// The primary collection ID.
    pub primary_collection_id: CollectionUuid,

    /// The sled IDs successfully loaded.
    pub sled_ids: Vec<SledUuid>,

    /// The collection IDs successfully loaded.
    pub collection_ids: Vec<CollectionUuid>,

    /// The blueprint IDs successfully loaded.
    pub blueprint_ids: Vec<BlueprintUuid>,

    /// The service IP pool ranges.
    pub service_ip_pool_ranges: Vec<IpRange>,

    /// Internal DNS generations.
    pub internal_dns_generations: Vec<Generation>,

    /// External DNS generations.
    pub external_dns_generations: Vec<Generation>,
}

impl LoadSerializedSystemResult {
    /// Create a new `LoadSerializedSystemResult`.
    pub(crate) fn new(primary_collection_id: CollectionUuid) -> Self {
        Self {
            primary_collection_id,
            sled_ids: Vec::new(),
            collection_ids: Vec::new(),
            blueprint_ids: Vec::new(),
            service_ip_pool_ranges: Vec::new(),
            internal_dns_generations: Vec::new(),
            external_dns_generations: Vec::new(),
        }
    }
}

impl fmt::Display for LoadSerializedSystemResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "using collection {} as source of sled inventory data",
            self.primary_collection_id,
        )?;
        writeln!(f, "loaded sleds: {}", join_comma_or_none(&self.sled_ids))?;
        writeln!(
            f,
            "loaded collections: {}",
            join_comma_or_none(&self.collection_ids)
        )?;
        writeln!(
            f,
            "loaded blueprints: {}",
            join_comma_or_none(&self.blueprint_ids)
        )?;
        writeln!(
            // TODO: output format for IP ranges that's not just Debug?
            f,
            "loaded service IP pool ranges: {:?}",
            self.service_ip_pool_ranges,
        )?;
        writeln!(
            f,
            "loaded internal DNS generations: {}",
            join_comma_or_none(&self.internal_dns_generations)
        )?;
        writeln!(
            f,
            "loaded external DNS generations: {}",
            join_comma_or_none(&self.external_dns_generations)
        )?;

        Ok(())
    }
}

/// Inner structure for system building.
///
/// This is mostly to ensure a clean separation between the public API which
/// adds log entries, and internal methods which don't.
#[derive(Clone, Debug)]
pub(super) struct SimSystemBuilderInner {
    system: SimSystem,
}

impl SimSystemBuilderInner {
    // This method MUST be infallible. It should only be called after checking
    // the invariant: the system must be empty.
    fn load_example_inner(
        &mut self,
        example: ExampleSystem,
        blueprint: Blueprint,
        internal_dns: DnsConfigZone,
        external_dns: DnsConfigZone,
    ) {
        self.system.description = example.system;

        self.add_collection_inner(Arc::new(example.collection))
            .expect("already checked that system is empty");

        let generation_to_serial = |generation: Generation| -> u32 {
            generation.as_u64().try_into().expect("generation fits into a u32")
        };

        self.system.internal_dns.insert(
            blueprint.internal_dns_version,
            Arc::new(DnsConfigParams {
                generation: blueprint.internal_dns_version,
                serial: generation_to_serial(blueprint.internal_dns_version),
                // TODO: probably want to make time controllable by the caller.
                time_created: Utc::now(),
                zones: vec![internal_dns],
            }),
        );
        self.system.external_dns.insert(
            blueprint.external_dns_version,
            Arc::new(DnsConfigParams {
                generation: blueprint.external_dns_version,
                serial: generation_to_serial(blueprint.external_dns_version),
                // TODO: probably want to make time controllable by the caller.
                time_created: Utc::now(),
                zones: vec![external_dns],
            }),
        );

        let initial_blueprint_id = example.initial_blueprint.id;
        let target_blueprint_id = blueprint.id;

        self.add_blueprint_inner(Arc::new(example.initial_blueprint))
            .expect("already checked that system is empty");

        self.system.target_blueprint = Some(BlueprintTarget {
            target_id: target_blueprint_id,
            enabled: true,
            time_made_target: blueprint.time_created,
        });

        // XXX: it's not normal, but hypothetically possible, that the initial
        // and target blueprints have the same ID. This will panic if so. Maybe
        // we should make it not panic.
        self.add_blueprint_inner(Arc::new(blueprint)).unwrap_or_else(|_| {
            panic!(
                "possible conflict between initial blueprint \
                 (ID {initial_blueprint_id}) and target blueprint \
                 (ID {target_blueprint_id}"
            )
        });
    }

    // This method MUST be infallible. It should only be called after checking
    // the invariant: the primary collection ID is valid.
    fn load_serialized_inner(
        &mut self,
        state: UnstableReconfiguratorState,
        primary_collection_id: CollectionUuid,
        res: &mut LoadSerializedResultBuilder,
    ) -> LoadSerializedSystemResult {
        let mut system_res =
            LoadSerializedSystemResult::new(primary_collection_id);
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
                     collection {})",
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
                    let stage0 = primary_collection
                        .caboose_for(CabooseWhich::Stage0, baseboard_id)
                        .map(|c| c.caboose.clone());
                    let stage0_next = primary_collection
                        .caboose_for(CabooseWhich::Stage0Next, baseboard_id)
                        .map(|c| c.caboose.clone());
                    let sp_host_phase_1_active_slot = primary_collection
                        .host_phase_1_active_slot_for(baseboard_id)
                        .map(|s| s.slot);
                    let sp_host_phase_1_hash_flash = M2Slot::iter()
                        .filter_map(|slot| {
                            let found = primary_collection
                                .host_phase_1_flash_hash_for(
                                    slot,
                                    baseboard_id,
                                )?;
                            Some((slot, found.hash))
                        })
                        .collect();
                    let sp_active = primary_collection
                        .caboose_for(CabooseWhich::SpSlot0, baseboard_id)
                        .map(|c| c.caboose.clone());
                    let sp_inactive = primary_collection
                        .caboose_for(CabooseWhich::SpSlot1, baseboard_id)
                        .map(|c| c.caboose.clone());
                    if let (Some(inv_sp), Some(inv_rot)) = (inv_sp, inv_rot) {
                        Some(SledHwInventory {
                            baseboard_id: &baseboard_id,
                            sp: inv_sp,
                            rot: inv_rot,
                            stage0,
                            stage0_next,
                            sp_host_phase_1_active_slot,
                            sp_host_phase_1_hash_flash,
                            sp_active,
                            sp_inactive,
                        })
                    } else {
                        None
                    }
                });

            let result = self.system.description.sled_full(
                sled_id,
                sled_details.policy,
                sled_details.state,
                sled_details.resources.clone(),
                inventory_sp,
                inventory_sled_agent,
            );

            match result {
                Ok(_) => {
                    system_res.sled_ids.push(sled_id);
                }
                Err(error) => {
                    // XXX: Should this error ever happen? The only case where
                    // it errors is if the sled ID is already present, but:
                    //
                    // * we know that the system is empty
                    // * and the state's planning input is keyed by sled ID
                    //
                    // so there should be no duplicates.
                    //
                    // In any case, if it does happen, it's a non-fatal error.
                    res.warnings.push(format!("sled {}: {:#}", sled_id, error));
                }
            };
        }

        for collection in state.collections {
            let collection_id = collection.id;
            match self.add_collection_inner(Arc::new(collection)) {
                Ok(_) => {
                    system_res.collection_ids.push(collection_id);
                }
                Err(_) => {
                    res.warnings.push(format!(
                        "collection {}: skipped (duplicate found)",
                        collection_id,
                    ));
                }
            }
        }

        self.system.target_blueprint = state.target_blueprint;

        for blueprint in state.blueprints {
            let blueprint_id = blueprint.id;
            match self.add_blueprint_inner(Arc::new(blueprint)) {
                Ok(_) => {
                    system_res.blueprint_ids.push(blueprint_id);
                }
                Err(_) => {
                    res.warnings.push(format!(
                        "blueprint {}: skipped (duplicate found)",
                        blueprint_id,
                    ));
                }
            }
        }

        self.system.description.service_ip_pool_ranges(
            state.planning_input.service_ip_pool_ranges().to_vec(),
        );
        system_res.service_ip_pool_ranges =
            state.planning_input.service_ip_pool_ranges().to_vec();

        self.set_internal_dns(state.internal_dns);
        self.set_external_dns(state.external_dns);

        self.system
            .description
            .set_tuf_repo(state.planning_input.tuf_repo().clone());

        system_res
    }

    fn add_collection_inner(
        &mut self,
        collection: Arc<Collection>,
    ) -> Result<(), DuplicateError> {
        let collection_id = collection.id;
        let time_started = collection.time_started;

        match insert_sorted_by(
            &mut self.system.collections,
            collection_id,
            collection,
            |_, other| other.time_started <= time_started,
        ) {
            Ok(()) => Ok(()),
            Err(_) => {
                Err(DuplicateError::collection(CollectionId::Id(collection_id)))
            }
        }
    }

    fn add_blueprint_inner(
        &mut self,
        blueprint: Arc<Blueprint>,
    ) -> Result<(), DuplicateError> {
        let blueprint_id = blueprint.id;
        let time_created = blueprint.time_created;

        match insert_sorted_by(
            &mut self.system.blueprints,
            blueprint_id,
            blueprint,
            |_, other| other.time_created <= time_created,
        ) {
            Ok(()) => Ok(()),
            Err(_) => {
                Err(DuplicateError::blueprint(BlueprintId::Id(blueprint_id)))
            }
        }
    }

    fn add_internal_dns_inner(
        &mut self,
        params: Arc<DnsConfigParams>,
    ) -> Result<(), DuplicateError> {
        let generation = params.generation;
        match self.system.internal_dns.entry(generation) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(params);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                Err(DuplicateError::internal_dns(generation))
            }
        }
    }

    fn add_external_dns_inner(
        &mut self,
        params: Arc<DnsConfigParams>,
    ) -> Result<(), DuplicateError> {
        let generation = params.generation;
        match self.system.external_dns.entry(generation) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(params);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                Err(DuplicateError::external_dns(generation))
            }
        }
    }

    // Not public: the only caller of this is load_serialized.
    fn set_internal_dns(
        &mut self,
        dns: impl IntoIterator<Item = (Generation, DnsConfigParams)>,
    ) {
        let internal_dns = dns
            .into_iter()
            .map(|(generation, params)| (generation, Arc::new(params)))
            .collect();
        self.system.internal_dns = internal_dns;
    }

    // Not public: the only caller of this is load_serialized.
    fn set_external_dns(
        &mut self,
        dns: impl IntoIterator<Item = (Generation, DnsConfigParams)>,
    ) {
        let external_dns = dns
            .into_iter()
            .map(|(generation, params)| (generation, Arc::new(params)))
            .collect();
        self.system.external_dns = external_dns;
    }

    fn wipe_inner(&mut self) {
        self.system = SimSystem::new();
    }
}

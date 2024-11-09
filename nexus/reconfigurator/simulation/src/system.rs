// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A simulated reconfigurator system.

use std::{collections::BTreeMap, sync::Arc};

use chrono::Utc;
use indexmap::IndexMap;
use nexus_reconfigurator_planning::{
    example::ExampleSystem, system::SystemDescription,
};
use nexus_types::{
    deployment::Blueprint,
    internal_api::params::{DnsConfigParams, DnsConfigZone},
    inventory::Collection,
};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::CollectionUuid;
use uuid::Uuid;

use crate::errors::{DuplicateError, KeyError, NonEmptySystemError};

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
    // 4. We'll have to figure out how to manage the store (so SimSystemBuilder
    //    can access existing blueprints/collections while it's in flight).
    //    Storing a &mut reference is not an option, and we probably want it to
    //    be thread-safe, so the options seem to be either `&Mutex<Store>` or
    //    `Arc<Mutex<Store>>`. Our current approach is more simplistic, but
    //    also lock-free.
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

    /// Inventory collections created by the user.
    ///
    /// Stored with `Arc` to allow cheap cloning.
    collections: IndexMap<CollectionUuid, Arc<Collection>>,

    /// Blueprints created by the user.
    ///
    /// Stored with `Arc` to allow cheap cloning.
    blueprints: IndexMap<Uuid, Arc<Blueprint>>,

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
            internal_dns: BTreeMap::new(),
            external_dns: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.description.has_sleds()
            && self.collections.is_empty()
            && self.blueprints.is_empty()
            && self.internal_dns.is_empty()
            && self.external_dns.is_empty()
    }

    #[inline]
    pub fn description(&self) -> &SystemDescription {
        &self.description
    }

    pub fn get_collection(
        &self,
        id: CollectionUuid,
    ) -> Result<&Collection, KeyError> {
        match self.collections.get(&id) {
            Some(c) => Ok(&**c),
            None => Err(KeyError::collection(id)),
        }
    }

    pub fn all_collections(
        &self,
    ) -> impl ExactSizeIterator<Item = &Collection> {
        self.collections.values().map(|c| &**c)
    }

    pub fn get_blueprint(&self, id: Uuid) -> Result<&Blueprint, KeyError> {
        match self.blueprints.get(&id) {
            Some(b) => Ok(&**b),
            None => Err(KeyError::blueprint(id)),
        }
    }

    pub fn all_blueprints(&self) -> impl ExactSizeIterator<Item = &Blueprint> {
        self.blueprints.values().map(|b| &**b)
    }

    pub fn get_internal_dns(
        &self,
        generation: Generation,
    ) -> Result<&DnsConfigParams, KeyError> {
        self.internal_dns
            .get(&generation)
            .map(|d| &**d)
            .ok_or_else(|| KeyError::internal_dns(generation))
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
        self.external_dns
            .get(&generation)
            .map(|d| &**d)
            .ok_or_else(|| KeyError::external_dns(generation))
    }

    pub fn all_external_dns(
        &self,
    ) -> impl ExactSizeIterator<Item = &DnsConfigParams> {
        self.external_dns.values().map(|d| &**d)
    }

    pub(crate) fn to_mut(&self) -> SimSystemBuilder {
        SimSystemBuilder { system: self.clone(), log: Vec::new() }
    }
}

/// A [`SimSystem`] that can be changed to create new states.
///
/// Returned by
/// [`SimStateBuilder::system_mut`](crate::SimStateBuilder::system_mut).
#[derive(Clone, Debug)]
pub struct SimSystemBuilder {
    // The underlying `SimSystem`.
    system: SimSystem,
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
        self.system.is_empty()
    }

    #[inline]
    pub fn description(&self) -> &SystemDescription {
        &self.system.description()
    }

    #[inline]
    pub fn get_collection(
        &self,
        id: CollectionUuid,
    ) -> Result<&Collection, KeyError> {
        self.system.get_collection(id)
    }

    #[inline]
    pub fn all_collections(
        &self,
    ) -> impl ExactSizeIterator<Item = &Collection> {
        self.system.all_collections()
    }

    #[inline]
    pub fn get_blueprint(&self, id: Uuid) -> Result<&Blueprint, KeyError> {
        self.system.get_blueprint(id)
    }

    #[inline]
    pub fn all_blueprints(&self) -> impl ExactSizeIterator<Item = &Blueprint> {
        self.system.all_blueprints()
    }

    #[inline]
    pub fn get_internal_dns(
        &self,
        generation: Generation,
    ) -> Result<&DnsConfigParams, KeyError> {
        self.system.get_internal_dns(generation)
    }

    #[inline]
    pub fn all_internal_dns(
        &self,
    ) -> impl ExactSizeIterator<Item = &DnsConfigParams> {
        self.system.all_internal_dns()
    }

    #[inline]
    pub fn get_external_dns(
        &self,
        generation: Generation,
    ) -> Result<&DnsConfigParams, KeyError> {
        self.system.get_external_dns(generation)
    }

    #[inline]
    pub fn all_external_dns(
        &self,
    ) -> impl ExactSizeIterator<Item = &DnsConfigParams> {
        self.system.all_external_dns()
    }

    // TODO: track changes to the SystemDescription -- we'll probably want to
    // have a separation between a type that represents a read-only system
    // description and a type that can mutate it.
    pub fn description_mut(&mut self) -> &mut SystemDescription {
        &mut self.system.description
    }

    pub fn load_example(
        &mut self,
        example: ExampleSystem,
        blueprint: Blueprint,
        internal_dns: DnsConfigZone,
        external_dns: DnsConfigZone,
    ) -> Result<(), NonEmptySystemError> {
        if !self.system.is_empty() {
            return Err(NonEmptySystemError::new());
        }

        // NOTE: If more error cases are added, ensure that they're checked
        // before load_example_inner is called. This ensures that the system is
        // not modified if there are errors.
        self.load_example_inner(example, blueprint, internal_dns, external_dns);
        Ok(())
    }

    // This method MUST be infallible. It should only be called after checking
    // the invariant: the system must be empty.
    fn load_example_inner(
        &mut self,
        example: ExampleSystem,
        blueprint: Blueprint,
        internal_dns: DnsConfigZone,
        external_dns: DnsConfigZone,
    ) {
        self.log.push(SimSystemLogEntry::LoadExample {
            collection_id: example.collection.id,
            blueprint_id: blueprint.id,
            internal_dns_version: blueprint.internal_dns_version,
            external_dns_version: blueprint.external_dns_version,
        });

        self.system.description = example.system;
        self.system
            .collections
            .insert(example.collection.id, Arc::new(example.collection));
        self.system.internal_dns.insert(
            blueprint.internal_dns_version,
            Arc::new(DnsConfigParams {
                generation: blueprint.internal_dns_version,
                // TODO: probably want to make time controllable by the caller.
                time_created: Utc::now(),
                zones: vec![internal_dns],
            }),
        );
        self.system.external_dns.insert(
            blueprint.external_dns_version,
            Arc::new(DnsConfigParams {
                generation: blueprint.external_dns_version,
                // TODO: probably want to make time controllable by the caller.
                time_created: Utc::now(),
                zones: vec![external_dns],
            }),
        );
        self.system.blueprints.insert(
            example.initial_blueprint.id,
            Arc::new(example.initial_blueprint),
        );
        self.system.blueprints.insert(blueprint.id, Arc::new(blueprint));
    }

    pub fn add_collection(
        &mut self,
        collection: impl Into<Arc<Collection>>,
    ) -> Result<(), DuplicateError> {
        let collection = collection.into();
        self.add_collection_inner(collection)
    }

    fn add_collection_inner(
        &mut self,
        collection: Arc<Collection>,
    ) -> Result<(), DuplicateError> {
        let collection_id = collection.id;
        match self.system.collections.entry(collection_id) {
            indexmap::map::Entry::Vacant(entry) => {
                entry.insert(collection);
                self.log.push(SimSystemLogEntry::AddCollection(collection_id));
                Ok(())
            }
            indexmap::map::Entry::Occupied(_) => {
                Err(DuplicateError::collection(collection_id))
            }
        }
    }

    pub fn add_blueprint(
        &mut self,
        blueprint: impl Into<Arc<Blueprint>>,
    ) -> Result<(), DuplicateError> {
        let blueprint = blueprint.into();
        self.add_blueprint_inner(blueprint)
    }

    fn add_blueprint_inner(
        &mut self,
        blueprint: Arc<Blueprint>,
    ) -> Result<(), DuplicateError> {
        let blueprint_id = blueprint.id;
        match self.system.blueprints.entry(blueprint_id) {
            indexmap::map::Entry::Vacant(entry) => {
                entry.insert(blueprint);
                self.log.push(SimSystemLogEntry::AddBlueprint(blueprint_id));
                Ok(())
            }
            indexmap::map::Entry::Occupied(_) => {
                Err(DuplicateError::blueprint(blueprint_id))
            }
        }
    }

    pub fn add_internal_dns(
        &mut self,
        params: impl Into<Arc<DnsConfigParams>>,
    ) -> Result<(), DuplicateError> {
        let params = params.into();
        self.add_internal_dns_inner(params)
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

    pub fn add_external_dns(
        &mut self,
        params: impl Into<Arc<DnsConfigParams>>,
    ) -> Result<(), DuplicateError> {
        let params = params.into();
        self.add_external_dns_inner(params)
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

    pub fn wipe(&mut self) {
        self.system = SimSystem::new();
        self.log.push(SimSystemLogEntry::Wipe);
    }

    // Not public: the only users that want to replace DNS wholesale are
    // internal to this crate.
    pub(crate) fn set_internal_dns(
        &mut self,
        dns: impl IntoIterator<Item = (Generation, DnsConfigParams)>,
    ) {
        let internal_dns = dns
            .into_iter()
            .map(|(generation, params)| (generation, Arc::new(params)))
            .collect();
        self.system.internal_dns = internal_dns;
    }

    // Not public: the only users that want to replace DNS wholesale are
    // internal to this crate.
    pub(crate) fn set_external_dns(
        &mut self,
        dns: impl IntoIterator<Item = (Generation, DnsConfigParams)>,
    ) {
        let external_dns = dns
            .into_iter()
            .map(|(generation, params)| (generation, Arc::new(params)))
            .collect();
        self.system.external_dns = external_dns;
    }

    pub(crate) fn into_parts(self) -> (SimSystem, Vec<SimSystemLogEntry>) {
        (self.system, self.log)
    }
}

/// A log entry corresponding to an individual operation on a
/// [`SimSystemBuilder`].
#[derive(Clone, Debug)]
pub enum SimSystemLogEntry {
    LoadExample {
        collection_id: CollectionUuid,
        blueprint_id: Uuid,
        internal_dns_version: Generation,
        external_dns_version: Generation,
    },
    AddCollection(CollectionUuid),
    AddBlueprint(Uuid),
    Wipe,
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates zpools and datasets on an illumos system

use crate::types::{DatasetName, DatasetProperty, DatasetType};

use camino::Utf8PathBuf;
use helios_fusion::zpool::{ZpoolHealth, ZpoolName};
use petgraph::stable_graph::{StableGraph, WalkNeighbors};
use std::collections::HashMap;
use std::str::FromStr;

pub(crate) struct FakeZpool {
    zix: NodeIndex,
    name: ZpoolName,
    vdev: Utf8PathBuf,

    pub imported: bool,
    pub health: ZpoolHealth,
    pub properties: HashMap<String, String>,
}

impl FakeZpool {
    pub(crate) fn new(
        zix: NodeIndex,
        name: ZpoolName,
        vdev: Utf8PathBuf,
    ) -> Self {
        Self {
            zix,
            name,
            vdev,
            imported: false,
            health: ZpoolHealth::Online,
            properties: HashMap::new(),
        }
    }
}

struct DatasetProperties(HashMap<DatasetProperty, String>);

impl DatasetProperties {
    fn get(&self, property: DatasetProperty) -> Result<String, String> {
        self.0
            .get(&property)
            .map(|k| k.to_string())
            .ok_or_else(|| format!("Missing '{property}' property"))
    }

    fn insert<V: Into<String>>(&mut self, k: DatasetProperty, v: V) {
        self.0.insert(k, v.into());
    }
}

pub(crate) struct FakeDataset {
    zix: NodeIndex,
    properties: DatasetProperties,
    ty: DatasetType,
}

impl FakeDataset {
    fn new(
        zix: NodeIndex,
        properties: HashMap<DatasetProperty, String>,
        ty: DatasetType,
    ) -> Self {
        Self { zix, properties: DatasetProperties(properties), ty }
    }

    pub fn ty(&self) -> DatasetType {
        self.ty
    }

    fn mountpoint(&self) -> Option<Utf8PathBuf> {
        self.properties
            .get(DatasetProperty::Mountpoint)
            .map(|s| Utf8PathBuf::from(s))
            .ok()
    }

    fn mount(&mut self, new: Utf8PathBuf) -> Result<(), String> {
        match self.ty {
            DatasetType::Filesystem => {
                if self.properties.get(DatasetProperty::Mountpoint)? != "none" {
                    return Err("Already mounted".to_string());
                }
            }
            _ => return Err("Not a filesystem".to_string()),
        };
        self.properties.insert(DatasetProperty::Mountpoint, new);
        self.properties.insert(DatasetProperty::Mounted, "yes");
        Ok(())
    }

    // TODO: Confirm that the filesystem isn't used by zones?
    fn unmount(&mut self) -> Result<(), String> {
        let mountpoint = match &mut self.ty {
            DatasetType::Filesystem => {
                self.properties.get(DatasetProperty::Mountpoint)?
            }
            _ => return Err("Not a filesystem".to_string()),
        };
        if mountpoint == "none" {
            return Err("Filesystem is not mounted".to_string());
        }
        self.properties.insert(DatasetProperty::Mountpoint, "none");
        self.properties.insert(DatasetProperty::Mounted, "no");
        Ok(())
    }
}

pub(crate) enum Znode {
    Root,
    Zpool(ZpoolName),
    Dataset(DatasetName),
}

impl Znode {
    pub fn to_string(&self) -> String {
        match self {
            Znode::Root => "/".to_string(),
            Znode::Zpool(name) => name.to_string(),
            Znode::Dataset(name) => name.as_str().to_string(),
        }
    }
}

/// The type of an index used to lookup nodes in the znode DAG.
pub(crate) type NodeIndex =
    petgraph::graph::NodeIndex<petgraph::graph::DefaultIx>;

/// Describes access to zpools and datasets that exist within the system.
///
/// On Helios, datasets exist as children of zpools, within a DAG structure.
/// Understanding the relationship between these znodes (dubbed "znodes", to
/// include both zpools and datasets) is important to accurately emulate
/// many ZFS operations, such as deletion, which can conditionally succeed or
/// fail depeending on the prescence of children.
pub(crate) struct Znodes {
    // Describes the connectivity between nodes
    all_znodes: StableGraph<Znode, ()>,
    zix_root: NodeIndex,

    // Individual nodes themselves
    zpools: HashMap<ZpoolName, FakeZpool>,
    datasets: HashMap<DatasetName, FakeDataset>,
}

impl Znodes {
    pub(crate) fn new() -> Self {
        let mut all_znodes = StableGraph::new();
        let zix_root = all_znodes.add_node(Znode::Root);
        Self {
            all_znodes,
            zix_root,
            zpools: HashMap::new(),
            datasets: HashMap::new(),
        }
    }

    // Zpool access methods

    pub fn get_zpool(&self, name: &ZpoolName) -> Option<&FakeZpool> {
        self.zpools.get(name)
    }

    pub fn get_zpool_mut(
        &mut self,
        name: &ZpoolName,
    ) -> Option<&mut FakeZpool> {
        self.zpools.get_mut(name)
    }

    pub fn all_zpools(&self) -> impl Iterator<Item = (&ZpoolName, &FakeZpool)> {
        self.zpools.iter()
    }

    // Dataset access methods

    pub fn get_dataset(&self, name: &DatasetName) -> Option<&FakeDataset> {
        self.datasets.get(name)
    }

    // Index-based access methods

    /// Returns the index of the "root" of the Znode DAG.
    ///
    /// This node does not actually exist, but the children of this node should
    /// be zpools.
    pub fn root_index(&self) -> NodeIndex {
        self.zix_root
    }

    /// Returns the node index of a znode
    pub fn index_of(&self, name: &str) -> Result<NodeIndex, String> {
        if !name.contains('/') {
            if let Some(pool) = self.zpools.get(&ZpoolName::from_str(name)?) {
                return Ok(pool.zix);
            }
        }
        if let Some(dataset) = self.datasets.get(&DatasetName::new(name)?) {
            return Ok(dataset.zix);
        }
        Err(format!("{} not found", name))
    }

    /// Looks up a Znode by an index
    pub fn lookup_by_index(&self, index: NodeIndex) -> Option<&Znode> {
        self.all_znodes.node_weight(index)
    }

    /// Describe the type of a znode by string
    pub fn type_str(&self, node: &Znode) -> Result<&'static str, String> {
        match node {
            Znode::Root => {
                Err("Invalid (root) node for type string".to_string())
            }
            Znode::Zpool(_) => Ok("fileystem"),
            Znode::Dataset(name) => {
                let dataset = self
                    .datasets
                    .get(&name)
                    .ok_or_else(|| "Missing dataset".to_string())?;
                match dataset.ty {
                    DatasetType::Filesystem => Ok("filesystem"),
                    DatasetType::Snapshot => Ok("snapshot"),
                    DatasetType::Volume => Ok("volume"),
                }
            }
        }
    }

    pub fn children(
        &self,
        zix: NodeIndex,
    ) -> impl Iterator<Item = NodeIndex> + '_ {
        self.all_znodes.neighbors_directed(zix, petgraph::Direction::Outgoing)
    }

    pub fn children_mut(
        &self,
        zix: NodeIndex,
    ) -> WalkNeighbors<petgraph::graph::DefaultIx> {
        self.all_znodes
            .neighbors_directed(zix, petgraph::Direction::Outgoing)
            .detach()
    }

    pub fn add_zpool(
        &mut self,
        name: ZpoolName,
        vdev: Utf8PathBuf,
        import: bool,
    ) -> Result<(), String> {
        if self.zpools.contains_key(&name) {
            return Err(format!(
                "Cannot create pool name '{name}': already exists"
            ));
        }

        let zix = self.all_znodes.add_node(Znode::Zpool(name.clone()));
        self.all_znodes.add_edge(self.zix_root, zix, ());

        let mut pool = FakeZpool::new(zix, name.clone(), vdev);
        pool.imported = import;
        self.zpools.insert(name, pool);

        Ok(())
    }

    pub fn add_dataset(
        &mut self,
        name: DatasetName,
        mut properties: HashMap<DatasetProperty, String>,
        ty: DatasetType,
    ) -> Result<(), String> {
        for property in properties.keys() {
            if !property.target().contains(ty.into()) {
                return Err(format!(
                    "Cannot create {ty} with property {property}"
                ));
            }
        }

        if self.datasets.contains_key(&name) {
            return Err(format!(
                "Cannot create '{}': already exists",
                name.as_str()
            ));
        }

        properties.insert(DatasetProperty::Type, ty.to_string());
        properties.insert(DatasetProperty::Name, name.to_string());

        match &ty {
            DatasetType::Filesystem => {
                properties.insert(DatasetProperty::Mounted, "no".to_string());
                properties
                    .entry(DatasetProperty::Mountpoint)
                    .or_insert("none".to_string());
            }
            DatasetType::Volume => (),
            DatasetType::Snapshot => (),
        }

        let parent = if let Some((parent, _)) = name.as_str().rsplit_once('/') {
            parent
        } else {
            return Err(format!("Cannot create '{}': No parent dataset. Try creating one under an existing filesystem or zpool?", name.as_str()));
        };

        let parent_zix = self.index_of(parent)?;
        if !self.all_znodes.contains_node(parent_zix) {
            return Err(format!(
                "Cannot create fs '{}': Missing parent node: {}",
                name.as_str(),
                parent
            ));
        }

        let zix = self.all_znodes.add_node(Znode::Dataset(name.clone()));
        self.all_znodes.add_edge(parent_zix, zix, ());
        self.datasets.insert(name, FakeDataset::new(zix, properties, ty));

        Ok(())
    }

    pub fn mount(
        &mut self,
        load_keys: bool,
        name: &DatasetName,
    ) -> Result<(), String> {
        let dataset = self
            .datasets
            .get_mut(&name)
            .ok_or_else(|| format!("Cannot mount '{name}': Does not exist"))?;
        let properties = &mut dataset.properties;
        let mountpoint_property =
            properties.get(DatasetProperty::Mountpoint)?;
        if !mountpoint_property.starts_with('/') {
            return Err(format!(
                "Cannot mount '{name}' with mountpoint: {mountpoint_property}"
            ));
        }

        let mounted_property = properties.get(DatasetProperty::Mounted)?;
        assert_eq!(mounted_property, "no");
        let encryption_property =
            properties.get(DatasetProperty::Encryption)?;
        let encrypted = match encryption_property.as_str() {
            "off" => false,
            "aes-256-gcm" => true,
            _ => {
                return Err(format!(
                    "Unsupported encryption: {encryption_property}"
                ))
            }
        };

        let keylocation_property =
            properties.get(DatasetProperty::Keylocation)?;
        if encrypted {
            if !load_keys {
                return Err(format!(
                    "Cannot mount '{name}': Use 'zfs mount -l {name}'"
                ));
            }
            let Some(keylocation) = keylocation_property.strip_prefix("file://").map(|k| Utf8PathBuf::from(k)) else {
                return Err(format!("Cannot read from key location: {keylocation_property}"));
            };
            if !keylocation.exists() {
                return Err(format!("Key at {keylocation} does not exist"));
            }

            let keyformat = properties.get(DatasetProperty::Keyformat)?;
            if keyformat != "raw" {
                return Err(format!(
                    "Cannot mount '{name}': Unknown keyformat: {keyformat}"
                ));
            }
        }

        // Perform modifications to "mount" filesystem

        dataset
            .mount(Utf8PathBuf::from(mountpoint_property))
            .map_err(|err| format!("Cannot mount '{name}': {err}"))?;

        Ok(())
    }

    pub fn destroy_dataset(
        &mut self,
        name: &DatasetName,
        // TODO: Not emulating this option
        _recusive_dependents: bool,
        recursive_children: bool,
        force_unmount: bool,
    ) -> Result<(), String> {
        let dataset = self.datasets.get_mut(&name).ok_or_else(|| {
            format!("Cannot destroy '{name}': Does not exist")
        })?;

        if dataset.mountpoint().is_some() {
            if !force_unmount {
                return Err(format!("Cannot destroy '{name}': Mounted"));
            }
            dataset.unmount()?;
        }

        let zix = dataset.zix;

        let mut children = self.children_mut(zix);
        while let Some(child_idx) = children.next_node(&self.all_znodes) {
            let child_name = self
                .lookup_by_index(child_idx)
                .map(|znode| znode.to_string())
                .ok_or_else(|| format!("Child node missing name"))?;
            let child_name = DatasetName::new(child_name)?;

            if !recursive_children {
                return Err(format!("Cannot delete dataset {name}: has children (e.g.: {child_name})"));
            }
            self.destroy_dataset(
                &child_name,
                _recusive_dependents,
                recursive_children,
                force_unmount,
            )?;
        }
        self.all_znodes.remove_node(zix);
        self.datasets.remove(&name);

        Ok(())
    }
}

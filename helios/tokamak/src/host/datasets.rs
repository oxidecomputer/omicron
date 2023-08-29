// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates datasets

use crate::types::dataset;

use camino::Utf8PathBuf;
use petgraph::stable_graph::{StableGraph, WalkNeighbors};
use std::collections::HashMap;

struct DatasetProperties(HashMap<dataset::Property, String>);

impl DatasetProperties {
    fn get(&self, property: dataset::Property) -> Result<String, String> {
        self.0
            .get(&property)
            .map(|k| k.to_string())
            .ok_or_else(|| format!("Missing '{property}' property"))
    }

    fn insert<V: Into<String>>(&mut self, k: dataset::Property, v: V) {
        self.0.insert(k, v.into());
    }
}

pub(crate) struct FakeDataset {
    idx: NodeIndex,
    properties: DatasetProperties,
    ty: dataset::Type,
}

impl FakeDataset {
    fn new(
        idx: NodeIndex,
        properties: HashMap<dataset::Property, String>,
        ty: dataset::Type,
    ) -> Self {
        Self { idx, properties: DatasetProperties(properties), ty }
    }

    pub fn ty(&self) -> dataset::Type {
        self.ty
    }

    fn mountpoint(&self) -> Option<Utf8PathBuf> {
        self.properties
            .get(dataset::Property::Mountpoint)
            .map(|s| Utf8PathBuf::from(s))
            .ok()
    }

    fn mount(&mut self, new: Utf8PathBuf) -> Result<(), String> {
        match self.ty {
            dataset::Type::Filesystem => {
                if self.properties.get(dataset::Property::Mountpoint)? != "none"
                {
                    return Err("Already mounted".to_string());
                }
            }
            _ => return Err("Not a filesystem".to_string()),
        };
        self.properties.insert(dataset::Property::Mountpoint, new);
        self.properties.insert(dataset::Property::Mounted, "yes");
        Ok(())
    }

    // TODO: Confirm that the filesystem isn't used by zones?
    fn unmount(&mut self) -> Result<(), String> {
        let mountpoint = match &mut self.ty {
            dataset::Type::Filesystem => {
                self.properties.get(dataset::Property::Mountpoint)?
            }
            _ => return Err("Not a filesystem".to_string()),
        };
        if mountpoint == "none" {
            return Err("Filesystem is not mounted".to_string());
        }
        self.properties.insert(dataset::Property::Mountpoint, "none");
        self.properties.insert(dataset::Property::Mounted, "no");
        Ok(())
    }
}

pub(crate) enum Znode {
    Root,
    Dataset(dataset::Name),
}

impl Znode {
    pub fn to_string(&self) -> String {
        match self {
            Znode::Root => "/".to_string(),
            Znode::Dataset(name) => name.as_str().to_string(),
        }
    }
}

/// The type of an index used to lookup nodes in the DAG.
pub(crate) type NodeIndex =
    petgraph::graph::NodeIndex<petgraph::graph::DefaultIx>;

/// Describes access to zpools and datasets that exist within the system.
///
/// On Helios, datasets exist as children of zpools, within a DAG structure.
/// Understanding the relationship between these datasets is important to
/// accurately emulate many ZFS operations, such as deletion, which can
/// conditionally succeed or fail depending on the prescence of children.
pub(crate) struct Datasets {
    // Describes the connectivity between nodes
    dataset_graph: StableGraph<Znode, ()>,
    dataset_graph_root: NodeIndex,

    // Individual nodes themselves
    datasets: HashMap<dataset::Name, FakeDataset>,
}

impl Datasets {
    pub(crate) fn new() -> Self {
        let mut dataset_graph = StableGraph::new();
        let dataset_graph_root = dataset_graph.add_node(Znode::Root);
        Self { dataset_graph, dataset_graph_root, datasets: HashMap::new() }
    }

    pub fn get_dataset(&self, name: &dataset::Name) -> Option<&FakeDataset> {
        self.datasets.get(name)
    }

    /// Returns the index of the "root" of the Znode DAG.
    ///
    /// This node does not actually exist, but the children of this node should
    /// be zpools.
    pub fn root_index(&self) -> NodeIndex {
        self.dataset_graph_root
    }

    /// Returns the node index of a dataset
    pub fn index_of(&self, name: &str) -> Result<NodeIndex, String> {
        if let Some(dataset) = self.datasets.get(&dataset::Name::new(name)?) {
            return Ok(dataset.idx);
        }
        Err(format!("{} not found", name))
    }

    /// Looks up a Znode by an index
    pub fn lookup_by_index(&self, index: NodeIndex) -> Option<&Znode> {
        self.dataset_graph.node_weight(index)
    }

    /// Describe the type of a dataset by string
    pub fn type_str(&self, node: &Znode) -> Result<&'static str, String> {
        match node {
            Znode::Root => {
                Err("Invalid (root) node for type string".to_string())
            }
            Znode::Dataset(name) => {
                let dataset = self
                    .datasets
                    .get(&name)
                    .ok_or_else(|| "Missing dataset".to_string())?;
                match dataset.ty {
                    dataset::Type::Filesystem => Ok("filesystem"),
                    dataset::Type::Snapshot => Ok("snapshot"),
                    dataset::Type::Volume => Ok("volume"),
                }
            }
        }
    }

    pub fn children(
        &self,
        idx: NodeIndex,
    ) -> impl Iterator<Item = NodeIndex> + '_ {
        self.dataset_graph
            .neighbors_directed(idx, petgraph::Direction::Outgoing)
    }

    pub fn children_mut(
        &self,
        idx: NodeIndex,
    ) -> WalkNeighbors<petgraph::graph::DefaultIx> {
        self.dataset_graph
            .neighbors_directed(idx, petgraph::Direction::Outgoing)
            .detach()
    }

    pub fn add_dataset(
        &mut self,
        name: dataset::Name,
        mut properties: HashMap<dataset::Property, String>,
        ty: dataset::Type,
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

        properties.insert(dataset::Property::Type, ty.to_string());
        properties.insert(dataset::Property::Name, name.to_string());

        match &ty {
            dataset::Type::Filesystem => {
                properties.insert(dataset::Property::Mounted, "no".to_string());
                properties
                    .entry(dataset::Property::Mountpoint)
                    .or_insert("none".to_string());
            }
            dataset::Type::Volume => (),
            dataset::Type::Snapshot => (),
        }

        let parent = if let Some((parent, _)) = name.as_str().rsplit_once('/') {
            parent
        } else {
            return Err(format!("Cannot create '{}': No parent dataset. Try creating one under an existing filesystem or zpool?", name.as_str()));
        };

        let parent_idx = self.index_of(parent)?;
        if !self.dataset_graph.contains_node(parent_idx) {
            return Err(format!(
                "Cannot create fs '{}': Missing parent node: {}",
                name.as_str(),
                parent
            ));
        }

        let idx = self.dataset_graph.add_node(Znode::Dataset(name.clone()));
        self.dataset_graph.add_edge(parent_idx, idx, ());
        self.datasets.insert(name, FakeDataset::new(idx, properties, ty));

        Ok(())
    }

    pub fn mount(
        &mut self,
        load_keys: bool,
        name: &dataset::Name,
    ) -> Result<(), String> {
        let dataset = self
            .datasets
            .get_mut(&name)
            .ok_or_else(|| format!("Cannot mount '{name}': Does not exist"))?;
        let properties = &mut dataset.properties;
        let mountpoint_property =
            properties.get(dataset::Property::Mountpoint)?;
        if !mountpoint_property.starts_with('/') {
            return Err(format!(
                "Cannot mount '{name}' with mountpoint: {mountpoint_property}"
            ));
        }

        let mounted_property = properties.get(dataset::Property::Mounted)?;
        assert_eq!(mounted_property, "no");
        let encryption_property =
            properties.get(dataset::Property::Encryption)?;
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
            properties.get(dataset::Property::Keylocation)?;
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

            let keyformat = properties.get(dataset::Property::Keyformat)?;
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
        name: &dataset::Name,
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

        let idx = dataset.idx;

        let mut children = self.children_mut(idx);
        while let Some(child_idx) = children.next_node(&self.dataset_graph) {
            let child_name = self
                .lookup_by_index(child_idx)
                .map(|n| n.to_string())
                .ok_or_else(|| format!("Child node missing name"))?;
            let child_name = dataset::Name::new(child_name)?;

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
        self.dataset_graph.remove_node(idx);
        self.datasets.remove(&name);

        Ok(())
    }
}

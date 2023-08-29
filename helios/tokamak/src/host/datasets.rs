// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates datasets

use crate::types::dataset;

use camino::Utf8PathBuf;
use helios_fusion::zpool::ZpoolName;
use petgraph::stable_graph::{StableGraph, WalkNeighbors};
use std::collections::HashMap;

pub(crate) struct DatasetProperties(HashMap<dataset::Property, String>);

impl DatasetProperties {
    pub(crate) fn get(
        &self,
        property: dataset::Property,
    ) -> Result<String, String> {
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

    pub fn properties(&self) -> &DatasetProperties {
        &self.properties
    }

    pub fn ty(&self) -> dataset::Type {
        self.ty
    }

    fn mounted(&self) -> bool {
        self.properties
            .get(dataset::Property::Mounted)
            .map(|s| s == "yes")
            .unwrap_or(false)
    }

    fn mountpoint(&self) -> Option<Utf8PathBuf> {
        self.properties
            .get(dataset::Property::Mountpoint)
            .map(|s| Utf8PathBuf::from(s))
            .ok()
    }

    fn mount(&mut self, load_keys: bool) -> Result<(), String> {
        let properties = &mut self.properties;

        match self.ty {
            dataset::Type::Filesystem => {
                if properties.get(dataset::Property::Mounted)? != "no" {
                    return Err("Already mounted".to_string());
                }
            }
            _ => return Err("Not a filesystem".to_string()),
        };

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

        if encrypted {
            if !load_keys {
                return Err(format!("Use 'zfs mount -l'"));
            }
            let keylocation_property =
                properties.get(dataset::Property::Keylocation)?;
            let Some(keylocation) = keylocation_property.strip_prefix("file://").map(|k| Utf8PathBuf::from(k)) else {
                return Err(format!("Cannot read from key location: {keylocation_property}"));
            };
            if !keylocation.exists() {
                return Err(format!("Key at {keylocation} does not exist"));
            }

            let keyformat = properties.get(dataset::Property::Keyformat)?;
            if keyformat != "raw" {
                return Err(format!("Unknown keyformat: {keyformat}"));
            }
        }

        let mountpoint = properties.get(dataset::Property::Mountpoint)?;
        if !mountpoint.starts_with('/') {
            return Err(format!("Cannot mount with mountpoint: {mountpoint}"));
        }

        properties.insert(dataset::Property::Mounted, "yes");
        Ok(())
    }

    // TODO: Confirm that the filesystem isn't used by zones?
    fn unmount(&mut self) -> Result<(), String> {
        let mounted = match &mut self.ty {
            dataset::Type::Filesystem => {
                self.properties.get(dataset::Property::Mounted)?
            }
            _ => return Err("Not a filesystem".to_string()),
        };
        if mounted == "no" {
            return Err("Filesystem is not mounted".to_string());
        }
        self.properties.insert(dataset::Property::Mounted, "no");
        Ok(())
    }
}

pub(crate) enum DatasetNode {
    Root,
    Dataset(dataset::Name),
}

impl DatasetNode {
    pub fn to_string(&self) -> String {
        match self {
            DatasetNode::Root => "/".to_string(),
            DatasetNode::Dataset(name) => name.as_str().to_string(),
        }
    }

    pub fn dataset_name(&self) -> Option<&dataset::Name> {
        match self {
            DatasetNode::Root => None,
            DatasetNode::Dataset(name) => Some(name),
        }
    }
}

pub(crate) enum DatasetInsert {
    // Used to create datasets that correspond with zpools.
    //
    // These datasets do not required parents, and may be directly
    // attached to the root of the dataset DAG.
    WithoutParent(ZpoolName),

    // Used to create datasets "the normal way", where the name should imply a
    // parent dataset which already exists in the dataset DAG.
    WithParent(dataset::Name),
}

impl DatasetInsert {
    fn name(&self) -> dataset::Name {
        use DatasetInsert::*;
        match self {
            WithoutParent(zpool) => dataset::Name::new(zpool.to_string())
                .expect("Zpool names should be valid datasets"),
            WithParent(name) => name.clone(),
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
    dataset_graph: StableGraph<DatasetNode, ()>,
    dataset_graph_root: NodeIndex,

    // Individual nodes themselves
    datasets: HashMap<dataset::Name, FakeDataset>,
}

impl Datasets {
    pub(crate) fn new() -> Self {
        let mut dataset_graph = StableGraph::new();
        let dataset_graph_root = dataset_graph.add_node(DatasetNode::Root);
        Self { dataset_graph, dataset_graph_root, datasets: HashMap::new() }
    }

    pub fn get_dataset(&self, name: &dataset::Name) -> Option<&FakeDataset> {
        self.datasets.get(name)
    }

    pub fn get_dataset_mut(
        &mut self,
        name: &dataset::Name,
    ) -> Option<&mut FakeDataset> {
        self.datasets.get_mut(name)
    }

    /// Returns the index of the "root" of the DatasetNode DAG.
    ///
    /// This node does not actually exist, but the children of this node should
    /// be datasets from zpools.
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

    /// Looks up a DatasetNode by an index
    pub fn lookup_by_index(&self, index: NodeIndex) -> Option<&DatasetNode> {
        self.dataset_graph.node_weight(index)
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
        insert: DatasetInsert,
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

        let name = insert.name();
        if self.datasets.contains_key(&name) {
            return Err(format!(
                "Cannot create '{}': already exists",
                name.as_str()
            ));
        }

        properties.insert(dataset::Property::Type, ty.to_string());
        properties.insert(dataset::Property::Name, name.to_string());
        properties
            .entry(dataset::Property::Encryption)
            .or_insert("off".to_string());
        properties.entry(dataset::Property::Zoned).or_insert("off".to_string());

        match &ty {
            dataset::Type::Filesystem => {
                properties
                    .entry(dataset::Property::Atime)
                    .or_insert("on".to_string());
                properties.insert(dataset::Property::Mounted, "no".to_string());
                properties
                    .entry(dataset::Property::Mountpoint)
                    .or_insert("none".to_string());
            }
            dataset::Type::Volume => (),
            dataset::Type::Snapshot => (),
        }

        let (parent, parent_idx) = match insert {
            DatasetInsert::WithoutParent(_) => {
                if ty != dataset::Type::Filesystem {
                    return Err(format!("Cannot create '{name}' as anything other than a filesystem"));
                }
                ("/", self.root_index())
            }
            DatasetInsert::WithParent(_) => {
                let parent = if let Some((parent, _)) =
                    name.as_str().rsplit_once('/')
                {
                    parent
                } else {
                    return Err(format!("Cannot create '{}': No parent dataset. Try creating one under an existing filesystem or zpool?", name.as_str()));
                };

                let parent_idx = self.index_of(parent)?;

                (parent, parent_idx)
            }
        };
        if !self.dataset_graph.contains_node(parent_idx) {
            return Err(format!(
                "Cannot create fs '{}': Missing parent node: {}",
                name.as_str(),
                parent
            ));
        }
        let idx =
            self.dataset_graph.add_node(DatasetNode::Dataset(name.clone()));
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

        dataset
            .mount(load_keys)
            .map_err(|err| format!("Cannot mount '{name}': {err}"))?;

        Ok(())
    }

    pub fn unmount(&mut self, name: &dataset::Name) -> Result<(), String> {
        let dataset = self.datasets.get_mut(&name).ok_or_else(|| {
            format!("Cannot unmount '{name}': Does not exist")
        })?;
        dataset
            .unmount()
            .map_err(|err| format!("Cannot unmount '{name}': {err}"))?;
        Ok(())
    }

    pub fn destroy(
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

        if dataset.mounted() {
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
            self.destroy(
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

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;
    use uuid::Uuid;

    fn expect_err<T, S: Into<String>>(
        result: Result<T, String>,
        expected: S,
    ) -> Result<(), String> {
        let expected: String = expected.into();
        let errmsg = result.err().unwrap();
        if !errmsg.contains(&expected) {
            return Err(format!(
                "Bad error: Expected: '{expected}', but saw '{errmsg}'"
            ));
        }
        Ok(())
    }

    #[test]
    fn create_dataset_tree() {
        let mut datasets = Datasets::new();
        assert_eq!(None, datasets.children(datasets.root_index()).next());

        let id = Uuid::new_v4();
        let zpool = format!("oxp_{id}");

        // Create a filesystem for a fake zpool

        let zpool_dataset = dataset::Name::new(&zpool).unwrap();
        let zpool = ZpoolName::from_str(&zpool).unwrap();
        datasets
            .add_dataset(
                DatasetInsert::WithoutParent(zpool.clone()),
                HashMap::new(),
                dataset::Type::Filesystem,
            )
            .expect("Failed to add dataset");

        // Create a dataset as a child of that fake zpool filesystem

        let dataset_a =
            dataset::Name::new(format!("{zpool}/dataset_a")).unwrap();
        datasets
            .add_dataset(
                DatasetInsert::WithParent(dataset_a.clone()),
                HashMap::new(),
                dataset::Type::Filesystem,
            )
            .expect("Failed to add datasets");

        // Create a child of the previous child

        let dataset_b =
            dataset::Name::new(format!("{dataset_a}/dataset_b")).unwrap();
        datasets
            .add_dataset(
                DatasetInsert::WithParent(dataset_b.clone()),
                HashMap::new(),
                dataset::Type::Filesystem,
            )
            .expect("Failed to add datasets");

        let dataset_c =
            dataset::Name::new(format!("{zpool}/dataset_c")).unwrap();
        datasets
            .add_dataset(
                DatasetInsert::WithParent(dataset_c.clone()),
                HashMap::new(),
                dataset::Type::Filesystem,
            )
            .expect("Failed to add datasets");

        // The layout should look like the following:
        //
        // oxp_<UUID>
        // oxp_<UUID>/dataset_a
        // oxp_<UUID>/dataset_a/dataset_b
        // oxp_<UUID>/dataset_c

        let z = datasets.get_dataset(&zpool_dataset).unwrap();
        let a = datasets.get_dataset(&dataset_a).unwrap();
        let b = datasets.get_dataset(&dataset_b).unwrap();
        let c = datasets.get_dataset(&dataset_c).unwrap();
        assert_eq!(z.ty, dataset::Type::Filesystem);
        assert_eq!(a.ty, dataset::Type::Filesystem);
        assert_eq!(b.ty, dataset::Type::Filesystem);
        assert_eq!(c.ty, dataset::Type::Filesystem);

        // Root of datasets
        let mut children = datasets.children(datasets.root_index());
        assert_eq!(Some(z.idx), children.next());
        assert_eq!(None, children.next());

        // Zpool -> Datasets
        let mut children = datasets.children(z.idx);
        assert_eq!(Some(c.idx), children.next());
        assert_eq!(Some(a.idx), children.next());
        assert_eq!(None, children.next());

        // Dataset with children
        let mut children = datasets.children(a.idx);
        assert_eq!(Some(b.idx), children.next());
        assert_eq!(None, children.next());

        // Leaf nodes
        assert_eq!(None, datasets.children(b.idx).next());
        assert_eq!(None, datasets.children(c.idx).next());
    }

    #[test]
    fn filesystem_properties() {
        let mut datasets = Datasets::new();

        let id = Uuid::new_v4();
        let zpool_str_name = format!("oxp_{id}");

        // Create a filesystem for a fake zpool

        let zpool_dataset = dataset::Name::new(&zpool_str_name).unwrap();
        let zpool = ZpoolName::from_str(&zpool_str_name).unwrap();
        datasets
            .add_dataset(
                DatasetInsert::WithoutParent(zpool.clone()),
                HashMap::new(),
                dataset::Type::Filesystem,
            )
            .expect("Failed to add dataset");

        let d = datasets.get_dataset(&zpool_dataset).unwrap();
        use dataset::Property::*;
        assert_eq!("on", d.properties.get(Atime).unwrap());
        assert_eq!("off", d.properties.get(Encryption).unwrap());
        assert_eq!("no", d.properties.get(Mounted).unwrap());
        assert_eq!("none", d.properties.get(Mountpoint).unwrap());
        assert_eq!(zpool_str_name, d.properties.get(Name).unwrap());
        assert_eq!("filesystem", d.properties.get(Type).unwrap());
        assert_eq!("off", d.properties.get(Zoned).unwrap());
    }

    #[test]
    fn filesystem_mount() {
        let mut datasets = Datasets::new();

        let id = Uuid::new_v4();
        let zpool_str_name = format!("oxp_{id}");

        // Create a filesystem for a fake zpool

        let zpool_dataset = dataset::Name::new(&zpool_str_name).unwrap();
        let zpool = ZpoolName::from_str(&zpool_str_name).unwrap();
        datasets
            .add_dataset(
                DatasetInsert::WithoutParent(zpool.clone()),
                HashMap::new(),
                dataset::Type::Filesystem,
            )
            .expect("Failed to add dataset");

        let d = datasets.get_dataset(&zpool_dataset).unwrap();
        use dataset::Property::*;
        assert_eq!("no", d.properties.get(Mounted).unwrap());
        assert_eq!("none", d.properties.get(Mountpoint).unwrap());
        drop(d);

        // Try to mount using the "none" mountpoint
        let load_keys = false;
        expect_err(
            datasets.mount(load_keys, &zpool_dataset),
            "Cannot mount with mountpoint: none",
        )
        .unwrap();

        // Update the mountpoint, try again
        let d = datasets.get_dataset_mut(&zpool_dataset).unwrap();
        d.properties.insert(Mountpoint, "/foobar");
        drop(d);

        // We can mount it successfully
        datasets.mount(load_keys, &zpool_dataset).unwrap();

        // Re-mounting returns an error
        expect_err(
            datasets.mount(load_keys, &zpool_dataset),
            "Already mounted",
        )
        .unwrap();

        // We can unmount successfully
        datasets.unmount(&zpool_dataset).unwrap();

        // Re-unmounting returns an error
        expect_err(
            datasets.unmount(&zpool_dataset),
            "Filesystem is not mounted",
        )
        .unwrap();
    }

    #[test]
    fn invalid_dataset_insertion() {
        let mut datasets = Datasets::new();

        let id = Uuid::new_v4();
        let zpool_str_name = format!("oxp_{id}");
        let zpool = ZpoolName::from_str(&zpool_str_name).unwrap();

        // Invalid property (meant for volume)
        expect_err(
            datasets.add_dataset(
                DatasetInsert::WithoutParent(zpool.clone()),
                HashMap::from([(
                    dataset::Property::Volsize,
                    "10G".to_string(),
                )]),
                dataset::Type::Filesystem,
            ),
            "Cannot create filesystem with property volsize",
        )
        .unwrap();

        // Cannot create volume for "without parent"
        expect_err(
            datasets.add_dataset(
                DatasetInsert::WithoutParent(zpool.clone()),
                HashMap::new(),
                dataset::Type::Volume,
            ),
            format!("Cannot create '{zpool_str_name}' as anything other than a filesystem"),
        ).unwrap();

        // Cannot create filesystem "WithParent" that does not exist
        expect_err(
            datasets.add_dataset(
                DatasetInsert::WithParent(
                    dataset::Name::new("mydataset").unwrap(),
                ),
                HashMap::new(),
                dataset::Type::Filesystem,
            ),
            format!("No parent dataset"),
        )
        .unwrap();

        expect_err(
            datasets.add_dataset(
                DatasetInsert::WithParent(
                    dataset::Name::new("mydataset/nested").unwrap(),
                ),
                HashMap::new(),
                dataset::Type::Filesystem,
            ),
            format!("mydataset not found"),
        )
        .unwrap();
    }

    #[test]
    fn destroy_datasets() {
        let mut datasets = Datasets::new();
        assert_eq!(None, datasets.children(datasets.root_index()).next());

        let id = Uuid::new_v4();
        let zpool = format!("oxp_{id}");

        // Create a filesystem for a fake zpool

        let zpool_dataset = dataset::Name::new(&zpool).unwrap();
        let zpool = ZpoolName::from_str(&zpool).unwrap();
        datasets
            .add_dataset(
                DatasetInsert::WithoutParent(zpool.clone()),
                HashMap::new(),
                dataset::Type::Filesystem,
            )
            .expect("Failed to add dataset");

        let dataset_a =
            dataset::Name::new(format!("{zpool}/dataset_a")).unwrap();
        datasets
            .add_dataset(
                DatasetInsert::WithParent(dataset_a.clone()),
                HashMap::new(),
                dataset::Type::Filesystem,
            )
            .expect("Failed to add datasets");

        let dataset_b =
            dataset::Name::new(format!("{dataset_a}/dataset_b")).unwrap();
        datasets
            .add_dataset(
                DatasetInsert::WithParent(dataset_b.clone()),
                HashMap::new(),
                dataset::Type::Filesystem,
            )
            .expect("Failed to add datasets");

        // Cannot destroy dataset with children

        let recusive_dependents = true;
        let recursive_children = false;
        let force_unmount = false;
        expect_err(
            datasets.destroy(
                &dataset_a,
                recusive_dependents,
                recursive_children,
                force_unmount,
            ),
            &format!("has children"),
        )
        .unwrap();

        // The datasets still exist
        datasets.get_dataset(&zpool_dataset).unwrap();
        datasets.get_dataset(&dataset_a).unwrap();
        datasets.get_dataset(&dataset_b).unwrap();

        // Try with the recursive children option

        let recursive_children = true;
        datasets
            .destroy(
                &dataset_a,
                recusive_dependents,
                recursive_children,
                force_unmount,
            )
            .unwrap();

        // The destroyed datasets are gone

        datasets.get_dataset(&zpool_dataset).unwrap();
        assert!(datasets.get_dataset(&dataset_a).is_none());
        assert!(datasets.get_dataset(&dataset_b).is_none());
    }
}

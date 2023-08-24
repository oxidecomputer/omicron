// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates an illumos system

// TODO: REMOVE
#![allow(dead_code)]

use crate::{FakeChild, FakeExecutor, FakeExecutorBuilder};

use camino::Utf8PathBuf;
use helios_fusion::interfaces::libc;
use helios_fusion::interfaces::swapctl;
use helios_fusion::zpool::ZpoolName;
use helios_fusion::{Child, Input, Output, OutputExt};
use ipnetwork::IpNetwork;
use petgraph::stable_graph::StableGraph;
use slog::Logger;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Read;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub enum LinkType {
    Etherstub,
    Vnic,
}

#[derive(Debug, PartialEq, Eq)]
pub struct LinkName(pub String);
struct Link {
    pub ty: LinkType,
    pub parent: Option<LinkName>,
    pub properties: HashMap<String, String>,
}

pub struct IpInterfaceName(pub String);
pub struct IpInterface {}

pub enum RouteDestination {
    Default,
    Literal(IpNetwork),
}

pub struct Route {
    pub destination: RouteDestination,
    pub gateway: IpNetwork,
}

#[derive(Debug)]
pub struct ServiceName(pub String);

pub struct Service {
    pub state: smf::SmfState,
    pub properties: HashMap<smf::PropertyName, smf::PropertyValue>,
}

struct ZoneEnvironment {
    id: u64,
    links: HashMap<LinkName, Link>,
    ip_interfaces: HashMap<IpInterfaceName, IpInterface>,
    routes: Vec<Route>,
    services: HashMap<ServiceName, Service>,
}

impl ZoneEnvironment {
    fn new(id: u64) -> Self {
        Self {
            id,
            links: HashMap::new(),
            ip_interfaces: HashMap::new(),
            routes: vec![],
            services: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ZoneName(pub String);

pub struct ZoneConfig {
    pub state: zone::State,
    pub brand: String,
    // zonepath
    pub path: Utf8PathBuf,
    pub datasets: Vec<zone::Dataset>,
    pub devices: Vec<zone::Device>,
    pub nets: Vec<zone::Net>,
    pub fs: Vec<zone::Fs>,
    // E.g. zone image, overlays, etc.
    pub layers: Vec<Utf8PathBuf>,
}

struct Zone {
    config: ZoneConfig,
    environment: ZoneEnvironment,
}

// A "process", which is either currently executing or completed.
//
// It's up to the caller to check-in on an "executing" process
// by calling "wait" on it.
enum ProcessState {
    Executing(std::thread::JoinHandle<Output>),
    Completed(Output),
}

impl ProcessState {
    fn wait(self) -> Output {
        match self {
            ProcessState::Executing(handle) => {
                handle.join().expect("Failed to wait for spawned process")
            }
            ProcessState::Completed(output) => output,
        }
    }
}

fn to_stderr<S: AsRef<str>>(s: S) -> Output {
    Output::failure().set_stderr(s)
}

struct FakeZpool {
    zix: ZnodeIdx,
    name: ZpoolName,
    vdev: Utf8PathBuf,
}

impl FakeZpool {
    fn new(zix: ZnodeIdx, name: ZpoolName, vdev: Utf8PathBuf) -> Self {
        Self { zix, name, vdev }
    }
}

enum DatasetFlavor {
    Filesystem,
    Volume {
        sparse: bool,
        // Defaults to 8KiB
        blocksize: u64,
        size: u64,
    },
}

struct FakeDataset {
    zix: ZnodeIdx,
    properties: Vec<(String, String)>,
    flavor: DatasetFlavor,
}

impl FakeDataset {
    fn new(
        zix: ZnodeIdx,
        properties: Vec<(String, String)>,
        flavor: DatasetFlavor,
    ) -> Self {
        Self { zix, properties, flavor }
    }
}

enum Znode {
    Root,
    Zpool(ZpoolName),
    Dataset(FilesystemName),
}

impl Znode {
    fn to_string(&self) -> String {
        match self {
            Znode::Root => "/".to_string(),
            Znode::Zpool(name) => name.to_string(),
            Znode::Dataset(name) => name.as_str().to_string(),
        }
    }
}

type ZnodeIdx = petgraph::graph::NodeIndex<petgraph::graph::DefaultIx>;

struct Znodes {
    // Describes the connectivity between nodes
    all_znodes: StableGraph<Znode, ()>,
    zix_root: ZnodeIdx,

    // Individual nodes themselves
    zpools: HashMap<ZpoolName, FakeZpool>,
    datasets: HashMap<FilesystemName, FakeDataset>,
}

impl Znodes {
    fn new() -> Self {
        let mut all_znodes = StableGraph::new();
        let zix_root = all_znodes.add_node(Znode::Root);
        Self {
            all_znodes,
            zix_root,
            zpools: HashMap::new(),
            datasets: HashMap::new(),
        }
    }

    fn name_to_zix(&self, name: &DatasetName) -> Result<ZnodeIdx, String> {
        if !name.0.contains('/') {
            if let Some(pool) = self.zpools.get(&ZpoolName::from_str(&name.0)?)
            {
                return Ok(pool.zix);
            }
        }
        if let Some(dataset) = self.datasets.get(&FilesystemName::new(&name.0)?)
        {
            return Ok(dataset.zix);
        }
        Err(format!("{} not found", name.0))
    }

    fn type_str(&self, zix: ZnodeIdx) -> Result<&'static str, String> {
        match self
            .all_znodes
            .node_weight(zix)
            .ok_or_else(|| "Unknown node".to_string())?
        {
            Znode::Root => {
                Err("Invalid (root) node for type string".to_string())
            }
            Znode::Zpool(_) => Ok("fileystem"),
            Znode::Dataset(name) => {
                let dataset = self
                    .datasets
                    .get(&name)
                    .ok_or_else(|| "Missing dataset".to_string())?;
                match dataset.flavor {
                    DatasetFlavor::Filesystem => Ok("filesystem"),
                    DatasetFlavor::Volume { .. } => Ok("volume"),
                }
            }
        }
    }

    fn children(&self, zix: ZnodeIdx) -> impl Iterator<Item = ZnodeIdx> + '_ {
        self.all_znodes.neighbors_directed(zix, petgraph::Direction::Outgoing)
    }

    fn add_zpool(
        &mut self,
        name: ZpoolName,
        vdev: Utf8PathBuf,
    ) -> Result<(), String> {
        if self.zpools.contains_key(&name) {
            return Err(format!(
                "Cannot create pool name '{name}': already exists"
            ));
        }

        let zix = self.all_znodes.add_node(Znode::Zpool(name.clone()));
        self.all_znodes.add_edge(self.zix_root, zix, ());
        self.zpools.insert(name.clone(), FakeZpool { zix, name, vdev });

        Ok(())
    }

    fn add_dataset(
        &mut self,
        name: FilesystemName,
        properties: Vec<(String, String)>,
        flavor: DatasetFlavor,
    ) -> Result<(), String> {
        if self.datasets.contains_key(&name) {
            return Err(format!("Cannot create '{}': already exists", name.0));
        }

        let parent = if let Some((parent, _)) = name.0.rsplit_once('/') {
            parent
        } else {
            return Err(format!("Cannot create '{}': No parent dataset. Try creating one under an existing filesystem or zpool?", name.as_str()));
        };

        let parent_zix = self.name_to_zix(&DatasetName(parent.to_string()))?;
        if !self.all_znodes.contains_node(parent_zix) {
            return Err(format!(
                "Cannot create fs '{}': Missing parent node: {}",
                name.as_str(),
                parent
            ));
        }

        let zix = self.all_znodes.add_node(Znode::Dataset(name.clone()));
        self.all_znodes.add_edge(parent_zix, zix, ());
        self.datasets.insert(name, FakeDataset::new(zix, properties, flavor));

        Ok(())
    }

    fn destroy_dataset(&mut self, name: &DatasetName) -> Result<(), String> {
        let name = FilesystemName::new(&name.0)?;
        let dataset = self.datasets.get(&name).ok_or_else(|| {
            format!(
                "Cannot destroy datasets '{}': Does not exist",
                name.as_str()
            )
        })?;

        let zix = dataset.zix;

        // TODO: Add additional validation that this dataset is removable.
        //
        // - TODO: Confirm, no children? (see: recursive flag)
        // - TODO: Not being used by any zones right now?
        // - TODO: Not mounted?

        self.all_znodes.remove_node(zix);
        self.datasets.remove(&name);

        Ok(())
    }
}

struct FakeHostInner {
    log: Logger,
    global: ZoneEnvironment,
    zones: HashMap<ZoneName, Zone>,

    vdevs: HashSet<Utf8PathBuf>,
    znodes: Znodes,
    swap_devices: Vec<swapctl::SwapDevice>,

    processes: HashMap<u32, ProcessState>,
}

impl FakeHostInner {
    fn new(log: Logger) -> Self {
        Self {
            log,
            global: ZoneEnvironment::new(0),
            zones: HashMap::new(),
            vdevs: HashSet::new(),
            znodes: Znodes::new(),
            swap_devices: vec![],
            processes: HashMap::new(),
        }
    }

    fn run(
        &mut self,
        inner: &Arc<Mutex<FakeHostInner>>,
        child: &mut FakeChild,
    ) -> Result<ProcessState, Output> {
        let input = Input::from(child.command());

        let cmd = crate::cli::Command::try_from(input).map_err(to_stderr)?;
        // TODO: Pick the right zone, act on it.
        //
        // TODO: If we can, complete immediately.
        // Otherwise, spawn a ProcessState::Executing thread, and grab
        // whatever stuff we need from the FakeChild.

        let _with_pfexec = cmd.with_pfexec();
        let zone = (*cmd.in_zone()).clone();

        use crate::cli::KnownCommand::*;
        match cmd.as_cmd() {
            Zfs(zfs_cmd) => {
                use crate::cli::zfs::Command::*;
                if zone.is_some() {
                    return Err(to_stderr(
                        "Not Supported: 'zfs' commands within zone",
                    ));
                }
                match zfs_cmd {
                    CreateFilesystem { properties, name } => {
                        let flavor = DatasetFlavor::Filesystem;
                        self.znodes
                            .add_dataset(name.clone(), properties, flavor)
                            .map_err(to_stderr)?;

                        Ok(ProcessState::Completed(
                            Output::success().set_stdout(format!(
                                "Created {} successfully",
                                name.as_str()
                            )),
                        ))
                    }
                    CreateVolume {
                        properties,
                        sparse,
                        blocksize,
                        size,
                        name,
                    } => {
                        let flavor = DatasetFlavor::Volume {
                            sparse,
                            blocksize: blocksize.unwrap_or(8192),
                            size,
                        };

                        let mut keylocation = None;
                        let mut keysize = 0;

                        for (k, v) in &properties {
                            match k.as_str() {
                                "keylocation" => keylocation = Some(v.as_str()),
                                "encryption" => match v.as_str() {
                                    "aes-256-gcm" => keysize = 32,
                                    _ => {
                                        return Err(Output::failure()
                                            .set_stderr(
                                                "Unsupported encryption",
                                            ))
                                    }
                                },
                                _ => (),
                            }
                        }

                        if keylocation == Some("file:///dev/stdin")
                            && keysize > 0
                        {
                            let inner = inner.clone();
                            let mut stdin = child.stdin().clone();
                            return Ok(ProcessState::Executing(
                                std::thread::spawn(move || {
                                    let mut secret =
                                        Vec::with_capacity(keysize);
                                    if let Err(err) =
                                        stdin.read_exact(&mut secret)
                                    {
                                        return Output::failure().set_stderr(
                                            format!(
                                                "Cannot read from stdin: {err}"
                                            ),
                                        );
                                    }

                                    let mut inner = inner.lock().unwrap();
                                    match inner
                                        .znodes
                                        .add_dataset(name, properties, flavor)
                                    {
                                        Ok(()) => Output::success(),
                                        Err(err) => {
                                            Output::failure().set_stderr(err)
                                        }
                                    }
                                }),
                            ));
                        }

                        self.znodes
                            .add_dataset(name.clone(), properties, flavor)
                            .map_err(to_stderr)?;

                        Ok(ProcessState::Completed(Output::success()))
                    }
                    Destroy {
                        recursive_dependents,
                        recursive_children,
                        force_unmount,
                        name,
                    } => {
                        self.znodes
                            .destroy_dataset(&name)
                            .map_err(to_stderr)?;

                        Ok(ProcessState::Completed(
                            Output::success()
                                .set_stdout(format!("{} destroyed", name.0)),
                        ))
                    }
                    List { recursive, depth, properties, datasets } => {
                        let mut targets = if let Some(datasets) = datasets {
                            let mut targets = VecDeque::new();

                            // If we explicitly request datasets, only return
                            // information for the exact matches, unless a
                            // recursive walk was requested.
                            let depth = if recursive { depth } else { Some(0) };

                            for dataset in datasets {
                                let zix = self
                                    .znodes
                                    .name_to_zix(&dataset)
                                    .map_err(to_stderr)?;
                                targets.push_back((zix, depth));
                            }

                            targets
                        } else {
                            // Bump whatever the depth was up by one, since we
                            // don't display anything for the root node.
                            VecDeque::from([(
                                self.znodes.zix_root,
                                depth.map(|d| d + 1),
                            )])
                        };

                        let mut output = String::new();

                        while let Some((target, depth)) = targets.pop_front() {
                            let node = self.znodes.all_znodes.node_weight(target)
                                .expect("We should have looked up the znode earlier...");

                            let (add_children, child_depth) =
                                if let Some(depth) = depth {
                                    if depth > 0 {
                                        (true, Some(depth - 1))
                                    } else {
                                        (false, None)
                                    }
                                } else {
                                    (true, None)
                                };

                            if add_children {
                                for child in self.znodes.children(target) {
                                    targets.push_front((child, child_depth));
                                }
                            }

                            if target == self.znodes.zix_root {
                                // Skip the root node, as there is nothing to
                                // display for it.
                                continue;
                            }

                            for property in &properties {
                                match property.as_str() {
                                    "name" => {
                                        output.push_str(&node.to_string())
                                    }
                                    "type" => output.push_str(
                                        &self
                                            .znodes
                                            .type_str(target)
                                            .map_err(to_stderr)?,
                                    ),
                                    _ => {
                                        return Err(to_stderr(format!(
                                            "Unknown property: {property}"
                                        )))
                                    }
                                }
                                output.push_str("\t");
                            }
                            output.push_str("\n");
                        }

                        Ok(ProcessState::Completed(
                            Output::success().set_stdout(output),
                        ))
                    }
                    // TODO: Finish these
                    _ => return Err(to_stderr("Not implemented (yet")),
                }
            }
            Zpool(zpool_cmd) => {
                use crate::cli::zpool::Command::*;
                if zone.is_some() {
                    return Err(to_stderr(
                        "Not Supported: 'zpool' commands within zone",
                    ));
                }
                match zpool_cmd {
                    Create { pool, vdev } => {
                        if !self.vdevs.contains(&vdev) {
                            return Err(to_stderr(format!(
                                "Cannot create zpool: {vdev} does not exist"
                            )));
                        }

                        self.znodes
                            .add_zpool(pool.clone(), vdev.clone())
                            .map_err(to_stderr)?;
                    }
                    // TODO: Finish these
                    _ => return Err(to_stderr("Not implemented (yet")),
                }
                todo!();
            }
            _ => todo!(),
        }
    }

    // Handle requests from an executor to spawn a new child.
    //
    // We aren't acting on "self" here to allow a background thread to clone
    // access to ourselves.
    fn handle_spawn(inner: &Arc<Mutex<FakeHostInner>>, child: &mut FakeChild) {
        let mut me = inner.lock().unwrap();

        assert!(
            me.processes.get(&child.id()).is_none(),
            "Process is already spawned: {}",
            Input::from(child.command()),
        );

        let process = match me.run(inner, child) {
            Ok(process) => process,
            Err(err) => ProcessState::Completed(err),
        };
        me.processes.insert(child.id(), process);
    }

    // Handle requests from an executor to wait for a child to complete.
    //
    // NOTE: This function panics if the child was not previously spawned.
    fn handle_wait(&mut self, child: &mut FakeChild) -> Output {
        self.processes
            .remove(&child.id())
            .unwrap_or_else(|| {
                panic!(
                    "Waiting for a child that has not been spawned: {}",
                    Input::from(child.command())
                );
            })
            .wait()
    }
}

struct FakeHost {
    executor: Arc<FakeExecutor>,
    inner: Arc<Mutex<FakeHostInner>>,
}

impl FakeHost {
    pub fn new(log: Logger) -> Arc<Self> {
        let inner = Arc::new(Mutex::new(FakeHostInner::new(log.clone())));

        // Plumbing to ensure that commands through the executor act on
        // "FakeHostInner", by going to an appropriate callback method.
        let inner_for_spawn = inner.clone();
        let inner_for_wait = inner.clone();
        let builder = FakeExecutorBuilder::new(log)
            .spawn_handler(Box::new(move |child| {
                FakeHostInner::handle_spawn(&inner_for_spawn, child);
            }))
            .wait_handler(Box::new(move |child| {
                let mut inner = inner_for_wait.lock().unwrap();
                inner.handle_wait(child)
            }));

        Arc::new(Self { executor: builder.build(), inner })
    }

    fn page_size(&self) -> i64 {
        4096
    }
}

impl libc::Libc for FakeHost {
    fn sysconf(&self, arg: i32) -> std::io::Result<i64> {
        use ::libc::_SC_PAGESIZE;

        match arg {
            _SC_PAGESIZE => Ok(self.page_size()),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "unknown sysconf",
            )),
        }
    }
}

impl swapctl::Swapctl for FakeHost {
    fn list_swap_devices(
        &self,
    ) -> Result<Vec<swapctl::SwapDevice>, swapctl::Error> {
        Ok(self.inner.lock().unwrap().swap_devices.clone())
    }

    fn add_swap_device(
        &self,
        path: String,
        start: u64,
        length: u64,
    ) -> Result<(), swapctl::Error> {
        let inner = &mut self.inner.lock().unwrap();

        const PATH_PREFIX: &str = "/dev/zvol/dsk/";
        let volume = if let Some(volume) = path.strip_prefix(PATH_PREFIX) {
            match FilesystemName::new(volume.to_string()) {
                Ok(name) => name,
                Err(err) => {
                    let msg = err.to_string();
                    return Err(swapctl::Error::AddDevice {
                        msg,
                        path,
                        start,
                        length,
                    });
                }
            }
        } else {
            let msg = format!("path does not start with: {PATH_PREFIX}");
            return Err(swapctl::Error::AddDevice { msg, path, start, length });
        };

        if let Some(dataset) = inner.znodes.datasets.get(&volume) {
            match dataset.flavor {
                DatasetFlavor::Volume { .. } => (),
                _ => {
                    let msg = format!(
                        "Dataset '{}' exists, but is not a volume",
                        volume.0
                    );
                    return Err(swapctl::Error::AddDevice {
                        msg,
                        path,
                        start,
                        length,
                    });
                }
            }
        } else {
            let msg = format!("Volume '{}' does not exist", volume.0);
            return Err(swapctl::Error::AddDevice { msg, path, start, length });
        }

        if start != 0 || length != 0 {
            let msg = "Try setting start = 0 and length = 0".to_string();
            return Err(swapctl::Error::AddDevice { msg, path, start, length });
        };

        let swap_devices = &mut inner.swap_devices;
        for device in &*swap_devices {
            if device.path == path {
                let msg = "device already used for swap".to_string();
                return Err(swapctl::Error::AddDevice {
                    msg,
                    path,
                    start,
                    length,
                });
            }
        }

        swap_devices.push(swapctl::SwapDevice {
            path,
            start,
            length,
            // NOTE: Using dummy values until we have a reasonable way to
            // populate this info.
            total_pages: 0xffff,
            free_pages: 0xffff,
            flags: 0xffff,
        });
        Ok(())
    }
}

impl helios_fusion::Host for FakeHost {
    fn executor(&self) -> &dyn helios_fusion::Executor {
        &*self.executor
    }

    fn swapctl(&self) -> &dyn swapctl::Swapctl {
        self
    }

    fn libc(&self) -> &dyn libc::Libc {
        self
    }
}

#[derive(Debug, PartialEq)]
pub enum AddrType {
    Dhcp,
    Static(IpNetwork),
    Addrconf,
}

/// The name of a ZFS filesystem, volume, or snapshot
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct DatasetName(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FilesystemName(String);

impl FilesystemName {
    pub fn new<S: Into<String>>(s: S) -> Result<Self, String> {
        let s: String = s.into();
        if s.is_empty() {
            return Err("Invalid name: Empty string".to_string());
        }
        if s.ends_with('/') {
            return Err(format!("Invalid name {s}: trailing slash in name"));
        }

        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<FilesystemName> for DatasetName {
    fn from(name: FilesystemName) -> Self {
        Self(name.0)
    }
}

pub type VolumeName = FilesystemName;

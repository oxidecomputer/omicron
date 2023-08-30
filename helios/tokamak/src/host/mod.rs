// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates an illumos system

// TODO: REMOVE
#![allow(dead_code)]

use crate::types::dataset;
use crate::{FakeChild, FakeExecutor, FakeExecutorBuilder};

use camino::Utf8PathBuf;
use helios_fusion::interfaces::libc;
use helios_fusion::interfaces::swapctl;
use helios_fusion::zpool::ZpoolName;
use helios_fusion::{Child, Input, Output, OutputExt};
use ipnetwork::IpNetwork;
use slog::Logger;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Read;
use std::sync::{Arc, Mutex};

mod datasets;
mod zpools;

use datasets::{DatasetInsert, Datasets};
use zpools::{FakeZpool, Zpools};

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

// A context parameter which is passed between subcommands.
//
// Mostly used to simplify argument passing.
struct ProcessContext<'a> {
    host: &'a Arc<Mutex<FakeHostInner>>,
    child: &'a mut FakeChild,
}

impl<'a> ProcessContext<'a> {
    fn new(
        host: &'a Arc<Mutex<FakeHostInner>>,
        child: &'a mut FakeChild,
    ) -> Self {
        Self { host, child }
    }

    // Spawns a thread which waits for stdin to be fully written, then executes
    // a user-supplied function.
    fn read_all_stdin_and_then<
        F: FnOnce(Vec<u8>) -> Output + Send + 'static,
    >(
        &self,
        f: F,
    ) -> ProcessState {
        let mut stdin = self.child.stdin().take_reader();
        ProcessState::Executing(std::thread::spawn(move || {
            let mut buf = Vec::new();
            if let Err(err) = stdin.read_to_end(&mut buf) {
                return Output::failure()
                    .set_stderr(format!("Cannot read from stdin: {err}"));
            }

            f(buf)
        }))
    }
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

struct FakeHostInner {
    log: Logger,
    global: ZoneEnvironment,
    zones: HashMap<ZoneName, Zone>,

    vdevs: HashSet<Utf8PathBuf>,
    datasets: Datasets,
    zpools: Zpools,
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
            datasets: Datasets::new(),
            zpools: Zpools::new(),
            swap_devices: vec![],
            processes: HashMap::new(),
        }
    }

    fn run_process(
        &mut self,
        context: ProcessContext<'_>,
    ) -> Result<ProcessState, Output> {
        let input = Input::from(context.child.command());

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
            Zfs(cmd) => self.run_zfs(context, cmd, zone),
            Zpool(cmd) => self.run_zpool(context, cmd, zone),
            _ => todo!(),
        }
    }

    fn run_zfs(
        &mut self,
        context: ProcessContext<'_>,
        cmd: crate::cli::zfs::Command,
        zone: Option<ZoneName>,
    ) -> Result<ProcessState, Output> {
        use crate::cli::zfs::Command::*;
        if zone.is_some() {
            return Err(to_stderr("Not Supported: 'zfs' commands within zone"));
        }
        match cmd {
            CreateFilesystem { properties, name } => {
                for property in properties.keys() {
                    if property.access() == dataset::PropertyAccess::ReadOnly {
                        return Err(to_stderr(
                            "Not supported: {property} is a read-only property",
                        ));
                    }
                }

                self.datasets
                    .add_dataset(
                        DatasetInsert::WithParent(name.clone()),
                        properties,
                        dataset::Type::Filesystem,
                    )
                    .map_err(to_stderr)?;

                Ok(ProcessState::Completed(Output::success().set_stdout(
                    format!("Created {} successfully\n", name.as_str()),
                )))
            }
            CreateVolume { mut properties, sparse, blocksize, size, name } => {
                for property in properties.keys() {
                    if property.access() == dataset::PropertyAccess::ReadOnly {
                        return Err(to_stderr(
                            "Not supported: {property} is a read-only property",
                        ));
                    }
                }

                let blocksize = blocksize.unwrap_or(8192);
                if sparse {
                    properties.insert(
                        dataset::Property::Reservation,
                        "0".to_string(),
                    );
                } else {
                    // NOTE: This isn't how much metadata is used, but it's
                    // a number we can use that represents "this is larger than
                    // the usable size of the volume".
                    //
                    // See:
                    //
                    // $ zfs get -Hp used,volsize,refreservation <ZPOOL>
                    //
                    // For any non-sparse zpool.
                    let reserved_size = size + (8 << 20);
                    properties.insert(
                        dataset::Property::Reservation,
                        reserved_size.to_string(),
                    );
                }
                properties.insert(
                    dataset::Property::Volblocksize,
                    blocksize.to_string(),
                );
                properties.insert(dataset::Property::Volsize, size.to_string());

                let mut keylocation = None;
                let mut keysize = 0;

                for (k, v) in &properties {
                    match k {
                        dataset::Property::Keylocation => {
                            keylocation = Some(v.to_string())
                        }
                        dataset::Property::Encryption => match v.as_str() {
                            "aes-256-gcm" => keysize = 32,
                            _ => {
                                return Err(Output::failure()
                                    .set_stderr("Unsupported encryption"))
                            }
                        },
                        _ => (),
                    }
                }

                let inner = context.host.clone();
                let add_dataset = move || {
                    let mut inner = inner.lock().unwrap();
                    match inner.datasets.add_dataset(
                        DatasetInsert::WithParent(name),
                        properties,
                        dataset::Type::Volume,
                    ) {
                        Ok(()) => Output::success(),
                        Err(err) => Output::failure().set_stderr(err),
                    }
                };

                if keylocation.as_deref() == Some("file:///dev/stdin") {
                    return Ok(context.read_all_stdin_and_then(move |input| {
                        if input.len() != keysize {
                            return Output::failure().set_stderr(format!(
                                "Bad key length: {}",
                                input.len()
                            ));
                        }
                        add_dataset()
                    }));
                }
                Ok(ProcessState::Completed(add_dataset()))
            }
            Destroy {
                recursive_dependents,
                recursive_children,
                force_unmount,
                name,
            } => {
                self.datasets
                    .destroy(
                        &name,
                        recursive_dependents,
                        recursive_children,
                        force_unmount,
                    )
                    .map_err(to_stderr)?;

                Ok(ProcessState::Completed(
                    Output::success().set_stdout(format!("{} destroyed", name)),
                ))
            }
            Get { recursive, depth, fields, properties, datasets } => {
                let mut targets = if let Some(datasets) = datasets {
                    let mut targets = VecDeque::new();

                    let depth = if recursive { depth } else { Some(0) };
                    for dataset in datasets {
                        let zix = self
                            .datasets
                            .index_of(dataset.as_str())
                            .map_err(to_stderr)?;
                        targets.push_back((zix, depth));
                    }
                    targets
                } else {
                    VecDeque::from([(
                        self.datasets.root_index(),
                        depth.map(|d| d + 1),
                    )])
                };

                let mut output = String::new();

                while let Some((target, depth)) = targets.pop_front() {
                    let node = self.datasets.lookup_by_index(target).expect(
                        "We should have looked up the dataset earlier...",
                    );

                    let (add_children, child_depth) = if let Some(depth) = depth
                    {
                        if depth > 0 {
                            (true, Some(depth - 1))
                        } else {
                            (false, None)
                        }
                    } else {
                        (true, None)
                    };

                    if add_children {
                        for child in self.datasets.children(target) {
                            targets.push_front((child, child_depth));
                        }
                    }

                    if target == self.datasets.root_index() {
                        // Skip the root node, as there is nothing to
                        // display for it.
                        continue;
                    }

                    for property in &properties {
                        for field in &fields {
                            match field.as_str() {
                                "name" => output.push_str(&node.to_string()),
                                "property" => {
                                    output.push_str(&property.to_string())
                                }
                                "value" => {
                                    // TODO: Look up, across whatever
                                    // the node type is.
                                    todo!();
                                }
                                f => {
                                    return Err(to_stderr(format!(
                                        "Unknown field: {f}"
                                    )))
                                }
                            }
                        }
                    }
                }
                todo!();
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
                            .datasets
                            .index_of(dataset.as_str())
                            .map_err(to_stderr)?;
                        targets.push_back((zix, depth));
                    }

                    targets
                } else {
                    // Bump whatever the depth was up by one, since we
                    // don't display anything for the root node.
                    VecDeque::from([(
                        self.datasets.root_index(),
                        depth.map(|d| d + 1),
                    )])
                };

                let mut output = String::new();

                while let Some((target, depth)) = targets.pop_front() {
                    let (add_children, child_depth) = if let Some(depth) = depth
                    {
                        if depth > 0 {
                            (true, Some(depth - 1))
                        } else {
                            (false, None)
                        }
                    } else {
                        (true, None)
                    };

                    if add_children {
                        for child in self.datasets.children(target) {
                            targets.push_front((child, child_depth));
                        }
                    }

                    if target == self.datasets.root_index() {
                        // Skip the root node, as there is nothing to
                        // display for it.
                        continue;
                    }
                    let dataset_name = self
                        .datasets
                        .lookup_by_index(target)
                        .expect("We should have looked up this node earlier...")
                        .dataset_name()
                        .expect("Cannot access name");

                    let dataset = self
                        .datasets
                        .get_dataset(&dataset_name)
                        .expect("Cannot access dataset");

                    for property in &properties {
                        let value = dataset
                            .properties()
                            .get(*property)
                            .map_err(|err| to_stderr(err))?;

                        output.push_str(&value);
                        output.push_str("\t");
                    }
                    output.push_str("\n");
                }

                Ok(ProcessState::Completed(
                    Output::success().set_stdout(output),
                ))
            }
            Mount { load_keys, filesystem } => {
                self.datasets
                    .mount(load_keys, &filesystem)
                    .map_err(to_stderr)?;
                Ok(ProcessState::Completed(
                    Output::success()
                        .set_stdout(format!("{} mounted", filesystem)),
                ))
            }
            Set { properties, name } => {
                // TODO
                todo!("Calling zfs set with properties: {properties:?} on '{name}', not implemented");
            }
        }
    }

    fn run_zpool(
        &mut self,
        _context: ProcessContext<'_>,
        cmd: crate::cli::zpool::Command,
        zone: Option<ZoneName>,
    ) -> Result<ProcessState, Output> {
        use crate::cli::zpool::Command::*;
        if zone.is_some() {
            return Err(to_stderr(
                "Not Supported: 'zpool' commands within zone",
            ));
        }
        match cmd {
            Create { pool, vdev } => {
                if !self.vdevs.contains(&vdev) {
                    return Err(to_stderr(format!(
                        "Cannot create zpool: device '{vdev}' does not exist"
                    )));
                }

                let import = true;
                self.zpools
                    .insert(pool.clone(), vdev.clone(), import)
                    .map_err(to_stderr)?;

                let mut dataset_properties = HashMap::new();
                dataset_properties
                    .insert(dataset::Property::Mountpoint, format!("/{pool}"));
                self.datasets
                    .add_dataset(
                        DatasetInsert::WithoutParent(pool),
                        dataset_properties,
                        dataset::Type::Filesystem,
                    )
                    .expect("Failed to add dataset after creating zpool");
                Ok(ProcessState::Completed(Output::success()))
            }
            Export { pool: name } => {
                let Some(mut pool) = self.zpools.get_mut(&name) else {
                    return Err(to_stderr(format!("pool does not exist")));
                };

                if !pool.imported {
                    return Err(to_stderr(format!(
                        "cannot export pool which is already exported"
                    )));
                }
                pool.imported = false;
                Ok(ProcessState::Completed(Output::success()))
            }
            Import { force: _, pool: name } => {
                let Some(mut pool) = self.zpools.get_mut(&name) else {
                    return Err(to_stderr(format!("pool does not exist")));
                };

                if pool.imported {
                    return Err(to_stderr(format!(
                        "a pool with that name is already created"
                    )));
                }
                pool.imported = true;
                Ok(ProcessState::Completed(Output::success()))
            }
            List { properties, pools } => {
                let mut output = String::new();
                let mut display = |name: &ZpoolName,
                                   pool: &FakeZpool,
                                   properties: &Vec<String>|
                 -> Result<(), _> {
                    for property in properties {
                        match property.as_str() {
                            "name" => output.push_str(&format!("{}", name)),
                            "health" => {
                                output.push_str(&pool.health.to_string())
                            }
                            _ => {
                                return Err(to_stderr(format!(
                                    "Unknown property: {property}"
                                )))
                            }
                        }
                        output.push_str("\t");
                    }
                    output.push_str("\n");
                    Ok(())
                };

                if let Some(pools) = pools {
                    for name in &pools {
                        let pool = self.zpools.get(name).ok_or_else(|| {
                            to_stderr(format!("{} does not exist", name))
                        })?;

                        if !pool.imported {
                            return Err(to_stderr(format!(
                                "{} not imported",
                                name
                            )));
                        }

                        display(&name, &pool, &properties)?;
                    }
                } else {
                    for (name, pool) in self.zpools.all() {
                        if pool.imported {
                            display(&name, &pool, &properties)?;
                        }
                    }
                }

                Ok(ProcessState::Completed(
                    Output::success().set_stdout(output),
                ))
            }
            Set { property, value, pool: name } => {
                let Some(pool) = self.zpools.get_mut(&name) else {
                    return Err(to_stderr(format!("{} does not exist", name)));
                };
                pool.properties.insert(property, value);
                Ok(ProcessState::Completed(Output::success()))
            }
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

        let process = match me.run_process(ProcessContext::new(inner, child)) {
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

pub struct FakeHost {
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

    pub fn add_devices(&self, vdevs: &Vec<Utf8PathBuf>) {
        let mut inner = self.inner.lock().unwrap();

        for vdev in vdevs {
            inner.vdevs.insert(vdev.clone());
        }
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
            match dataset::Name::new(volume.to_string()) {
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

        if let Some(dataset) = inner.datasets.get_dataset(&volume) {
            match dataset.ty() {
                dataset::Type::Volume => (),
                _ => {
                    let msg = format!(
                        "Dataset '{}' exists, but is not a volume",
                        volume.as_str()
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
            let msg = format!("Volume '{}' does not exist", volume.as_str());
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

#[cfg(test)]
mod test {
    use super::*;
    use helios_fusion::Host;
    use omicron_test_utils::dev::test_setup_log;
    use std::process::Command;
    use uuid::Uuid;

    #[test]
    fn create_zpool_creates_dataset_too() {
        let logctx = test_setup_log("create_zpool_creates_dataset_too");
        let log = &logctx.log;

        let id = Uuid::new_v4();
        let zpool_name = format!("oxp_{id}");
        let vdev = "/mydevice";

        let host = FakeHost::new(log.clone());
        host.add_devices(&vec![Utf8PathBuf::from(vdev)]);

        // Create the zpool
        let output = host
            .executor()
            .execute(Command::new(helios_fusion::ZPOOL).args([
                "create",
                &zpool_name,
                vdev,
            ]))
            .expect("Failed to run zpool create command");
        assert!(output.status.success());

        // Observe the ZFS filesystem exists
        let output = host
            .executor()
            .execute(Command::new(helios_fusion::ZFS).args([
                "list",
                "-Hp",
                &zpool_name,
            ]))
            .expect("Failed to run zfs list command");
        assert!(output.status.success());

        logctx.cleanup_successful();
    }
}

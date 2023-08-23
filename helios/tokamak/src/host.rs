// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates an illumos system

// TODO: REMOVE
#![allow(dead_code)]

use crate::{FakeExecutor, FakeExecutorBuilder};

use camino::Utf8PathBuf;
use helios_fusion::interfaces::libc;
use helios_fusion::interfaces::swapctl;
use helios_fusion::zpool::ZpoolName;
use ipnetwork::IpNetwork;
use slog::Logger;
use std::collections::{HashMap, HashSet};
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

#[derive(Debug)]
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

struct FakeHost {
    executor: Arc<FakeExecutor>,
    global: ZoneEnvironment,
    zones: HashMap<ZoneName, Zone>,

    // TODO: Is this the right abstraction layer?
    // How do you want to represent zpools & filesystems?
    //
    // TODO: Should filesystems be part of the "ZoneEnvironment" abstraction?
    zpools: HashSet<ZpoolName>,

    swap_devices: Mutex<Vec<swapctl::SwapDevice>>,
}

impl FakeHost {
    pub fn new(log: Logger) -> Arc<Self> {
        Arc::new(Self {
            executor: FakeExecutorBuilder::new(log).build(),
            global: ZoneEnvironment::new(0),
            zones: HashMap::new(),
            zpools: HashSet::new(),
            swap_devices: Mutex::new(vec![]),
        })
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
        Ok(self.swap_devices.lock().unwrap().clone())
    }

    fn add_swap_device(
        &self,
        path: String,
        start: u64,
        length: u64,
    ) -> Result<(), swapctl::Error> {
        // TODO: Parse path, ensure the zvol exists?

        let mut swap_devices = self.swap_devices.lock().unwrap();
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
#[derive(Debug, Eq, PartialEq)]
pub struct DatasetName(pub String);

#[derive(Debug, Eq, PartialEq)]
pub struct FilesystemName(pub String);
impl From<FilesystemName> for DatasetName {
    fn from(name: FilesystemName) -> Self {
        Self(name.0)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct VolumeName(pub String);
impl From<VolumeName> for DatasetName {
    fn from(name: VolumeName) -> Self {
        Self(name.0)
    }
}

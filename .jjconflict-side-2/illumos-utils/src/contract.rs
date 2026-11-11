// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use libc::ctid_t;
use nix::fcntl;
use nix::sys::stat;
use slog::{Logger, debug, error};
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_uint;
use std::ffi::c_void;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;
use std::path::Path;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

#[allow(non_camel_case_types)]
type ct_evthdl_t = *mut c_void;
#[allow(non_camel_case_types)]
type ct_evid_t = u64;

#[link(name = "contract")]
unsafe extern "C" {
    fn ct_tmpl_set_critical(fd: c_int, events: c_uint) -> c_int;
    fn ct_tmpl_set_informative(fd: c_int, events: c_uint) -> c_int;
    fn ct_pr_tmpl_set_fatal(fd: c_int, events: c_uint) -> c_int;
    fn ct_pr_tmpl_set_param(fd: c_int, params: c_uint) -> c_int;
    fn ct_dev_tmpl_set_minor(fd: c_int, minor: *const c_char) -> c_int;
    fn ct_tmpl_activate(fd: c_int) -> c_int;
    fn ct_tmpl_create(fd: c_int, ctid: *mut c_int) -> c_int;
    fn ct_tmpl_clear(fd: c_int) -> c_int;
    fn ct_ctl_abandon(fd: c_int) -> c_int;
    fn ct_ctl_ack(fd: c_int, evid: ct_evid_t) -> c_int;
    fn ct_event_read_critical(fd: c_int, ev: *mut ct_evthdl_t) -> c_int;
    fn ct_event_get_type(ev: ct_evthdl_t) -> u32;
    fn ct_event_get_ctid(ev: ct_evthdl_t) -> ctid_t;
    fn ct_event_get_evid(ev: ct_evthdl_t) -> ct_evid_t;
    fn ct_event_free(ev: ct_evthdl_t);
}

// Convert an error message into an ExecutionError::ContractFailure
use crate::ExecutionError as Error;
type Result<T> = std::result::Result<T, crate::ExecutionError>;
fn err<T>(msg: impl ToString) -> Result<T> {
    let msg = msg.to_string();
    let err = std::io::Error::last_os_error();
    Err(Error::ContractFailure { msg, err })
}

#[derive(Clone, Copy, Debug)]
pub enum ContractType {
    Process,
    Device,
}

impl ContractType {
    pub fn path_prefix(self) -> &'static Path {
        &Path::new(match self {
            Self::Process => "/system/contract/process",
            Self::Device => "/system/contract/device",
        })
    }
}

// Constants related to process contracts
// Only kill process group on fatal errors.
pub const CT_PR_PGRPONLY: c_uint = 0x04;
// Automatically detach inherited contracts.
pub const CT_PR_REGENT: c_uint = 0x08;

// Event types reported on a contract

// Common events:
pub const CT_EV_NEGEND: c_uint = 0x0; // Contract negotiation ended

// Process events:
pub const CT_PR_EV_EMPTY: c_uint = 0x1; // Contract has become empty.
pub const CT_PR_EV_HWERR: c_uint = 0x20; // Process had an uncorrectable error.

// Device events
pub const CT_DEV_EV_OFFLINE: c_uint = 0x4; // Device has gone offline

#[derive(Debug)]
pub struct ContractEvent {
    pub event: ct_evthdl_t,
    pub event_id: ct_evid_t,
    pub typ: c_uint,
    pub ctid: ctid_t,
}
unsafe impl Sync for ContractEvent {}
unsafe impl Send for ContractEvent {}

impl Drop for ContractEvent {
    fn drop(&mut self) {
        unsafe { ct_event_free(self.event) };
    }
}

/// A Watcher is used to wait for events related to contracts
pub struct Watcher(OwnedFd);

impl AsRawFd for Watcher {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl Watcher {
    /// Return a Watcher for a specific type of contract event.  The watcher
    /// will return all events of the requested type, and it is the caller's
    /// responsibility to filter for the events relevent to them.
    pub fn new(typ: ContractType) -> Self {
        let path = typ.path_prefix().join("pbundle");
        let fd =
            fcntl::open(&path, fcntl::OFlag::O_RDONLY, stat::Mode::empty())
                .expect("we have a contract, so this file must exist");
        Watcher(fd)
    }

    /// Block until a contract event occurs.
    pub fn watch(&self, log: &slog::Logger) -> ContractEvent {
        loop {
            let mut event: ct_evthdl_t = std::ptr::null_mut();
            let evp: *mut ct_evthdl_t = &mut event;
            // The event endpoint was not opened as non-blocking, so
            // ct_event_read_critical(3CONTRACT) will block until a new
            // critical event is available on the channel.
            slog::debug!(log, "entering contract ioctl");
            match unsafe { ct_event_read_critical(self.as_raw_fd(), evp) } {
                0 => {
                    slog::debug!(log, "back from contract ioctl");
                    let typ = unsafe { ct_event_get_type(event) };
                    let event_id = unsafe { ct_event_get_evid(event) };
                    let ctid = unsafe { ct_event_get_ctid(event) };
                    return ContractEvent { event, event_id, typ, ctid };
                }
                err => {
                    // ct_event_read_critical(3CONTRACT) does not state any
                    // error values for this function if the file descriptor
                    // was not opened non-blocking, but inspection of the
                    // library code shows that various errnos could be returned
                    // in situations such as failure to allocate memory. In
                    // those cases, log a message and pause to avoid entering a
                    // tight loop if the problem persists.
                    error!(
                        log,
                        "Unexpected response from contract event channel: {}",
                        std::io::Error::from_raw_os_error(err)
                    );
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }
}

/// A Control is used to communicate a response to the contract system in
/// response to an event.  In practice this is limited to acknowledging an event
/// and cancelling a contract.
pub struct Control {
    ctid: ctid_t,
    fd: OwnedFd,
}
impl AsRawFd for Control {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl Control {
    /// Construct a new Control for the specified contract
    pub fn new(typ: ContractType, ctid: ctid_t) -> Result<Self> {
        let path = typ.path_prefix().join(ctid.to_string()).join("ctl");
        fcntl::open(&path, fcntl::OFlag::O_WRONLY, stat::Mode::empty())
            .map(|fd| Control { ctid, fd })
            .map_err(|e| crate::ExecutionError::ContractFailure {
                msg: format!("failed to open control path {}", path.display()),
                err: e.into(),
            })
    }

    /// Acknowledge an event on the contract
    pub fn ack(&self, event_id: ct_evid_t) -> Result<()> {
        match unsafe { ct_ctl_ack(self.as_raw_fd(), event_id) } {
            0 => Ok(()),
            _ => err(format!("failed to acknowledge event {}", event_id)),
        }
    }

    /// Abandon the contract
    pub fn abandon(self) -> Result<()> {
        match unsafe { ct_ctl_abandon(self.as_raw_fd()) } {
            0 => Ok(()),
            _ => err(format!("failed to abandon contract {}", self.ctid)),
        }
    }
}

// This thread watches for critical events coming from all process
// contracts held by sled-agent, and reaps (abandons) contracts which
// become empty. Process contracts are used in conjunction with
// zone_enter() in order to run commands within non-global zones, and
// the contracts used for this come from templates that define becoming
// empty as a critical event.
pub fn process_contract_reaper(log: Logger) {
    let watcher = Watcher::new(ContractType::Process);
    loop {
        let event = watcher.watch(&log);
        if event.typ != CT_PR_EV_EMPTY {
            continue;
        }

        let ctl = match Control::new(ContractType::Process, event.ctid) {
            Ok(c) => c,
            Err(e) => {
                error!(&log, "Failed to open contract control: {e:?}");
                continue;
            }
        };

        if let Err(e) = ctl.abandon() {
            error!(log, "{e:?}");
        } else {
            debug!(&log, "Abandoned contract {}", event.ctid)
        }
    }
}

// A Rust wrapper around the process contract template.
#[derive(Debug)]
pub struct Template(OwnedFd);
impl AsRawFd for Template {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl Drop for Template {
    fn drop(&mut self) {
        self.clear();
    }
}

fn get_tfpkt_device_path() -> Option<PathBuf> {
    let link = nix::fcntl::readlink("/dev/tfpkt0").ok()?;
    let path = PathBuf::from(link);
    if let Ok(path) = path.strip_prefix("../devices") {
        // Because the path is relative, the PathBuf strip_prefix()
        // operation will also return a relative path - stripping off the
        // leading "/".  Put it back.
        Some(Path::new("/").join(path))
    } else {
        Some(path)
    }
}

impl Template {
    pub fn new(typ: ContractType) -> Result<Self> {
        let path = typ.path_prefix().join("template");
        let fd = fcntl::open(&path, fcntl::OFlag::O_RDWR, stat::Mode::empty())
            .map_err(|e| crate::ExecutionError::ContractFailure {
                msg: format!(
                    "failed to open contract template {}",
                    path.display()
                ),
                err: e.into(),
            })?;
        let rawfd = fd.as_raw_fd();

        // The two different contract types are initialized with different
        // settings.  These settings are hardcoded and specific to the manner in
        // which the contracts are used within omicron.
        match typ {
            ContractType::Process => {
                //
                // Nothing is inherited, we do not allow the contract to be
                // orphaned, and the only event which is delivered is EV_EMPTY,
                // indicating that the contract has become empty. These events are
                // consumed by contract_reaper() above.
                //
                // See illumos sources in `usr/src/cmd/zlogin/zlogin.c` in the
                // implementation of `init_template()` for details.
                if unsafe { ct_tmpl_set_critical(rawfd, CT_PR_EV_EMPTY) } != 0 {
                    err("set_critical in process template")
                } else if unsafe { ct_tmpl_set_informative(rawfd, 0) } != 0 {
                    err("set_informative in process template")
                } else if unsafe { ct_pr_tmpl_set_fatal(rawfd, CT_PR_EV_HWERR) }
                    != 0
                {
                    err("set_fatal in process template")
                } else if unsafe {
                    ct_pr_tmpl_set_param(rawfd, CT_PR_PGRPONLY | CT_PR_REGENT)
                } != 0
                {
                    err("set_param in process template")
                } else if unsafe { ct_tmpl_activate(rawfd) } != 0 {
                    err("activating in process template")
                } else {
                    Ok(Self(fd))
                }
            }
            ContractType::Device => {
                // The only device contract we currently support is for the
                // tfpkt device.  If we ever want to support something else then
                // we will need to include a device type, path, and/or instance
                // number as an additional argument to this template creation
                // function.
                //
                // Note: what we are actually interested in is the removal of
                // the "tofino" device.  However, illumos won't report the
                // removal of that device while it is still assigned to a zone.
                // Since our goal is to shut down the zone to allow the removal
                // of the device, we have a chicken/egg problem. Instead, we
                // watch here for the removal of "tfpkt", which is a child of
                // the "tofino", and whose removal is a reliable indicator that
                // the tofino is gone.
                let Some(path) = get_tfpkt_device_path() else {
                    return err("unable to find tfpkt in device tree");
                };

                // We want to be notified when the device goes offline
                if unsafe { ct_tmpl_set_critical(rawfd, CT_DEV_EV_OFFLINE) }
                    != 0
                {
                    err("set_critical in device template")
                } else if unsafe {
                    use std::os::unix::ffi::OsStrExt;
                    let os_path = std::ffi::CString::new(
                        path.clone().into_os_string().as_bytes(),
                    ).expect("a path from the OS should be convertable into a CString");
                    ct_dev_tmpl_set_minor(rawfd, os_path.as_ptr())
                } != 0
                {
                    err(format!(
                        "set_minor to {} in device template",
                        path.display()
                    ))
                } else if unsafe { ct_tmpl_activate(rawfd) } != 0 {
                    err("activating device template")
                } else {
                    Ok(Self(fd))
                }
            }
        }
    }

    pub fn create(&self) -> Result<ctid_t> {
        let mut ctid = 0;
        match unsafe { ct_tmpl_create(self.as_raw_fd(), &mut ctid) } {
            0 => Ok(ctid),
            _ => err("constructing contract"),
        }
    }

    pub fn clear(&self) {
        unsafe { ct_tmpl_clear(self.as_raw_fd()) };
    }
}

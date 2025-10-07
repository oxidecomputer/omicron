#[cfg(target_os = "illumos")]
use libc::ctid_t;
use slog::{Logger, debug, error};
use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_uint;
use std::ffi::c_void;
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
fn err(msg: impl ToString) -> crate::ExecutionError {
    return crate::ExecutionError::ContractFailure {
        msg: msg.to_string(),
        err: std::io::Error::last_os_error(),
    };
}

// Construct a path to a file in the contract filesystem
fn path(typ: ContractType, id: Option<c_int>, file: &str) -> CString {
    let prefix = match typ {
        ContractType::Process => "/system/contract/process",
        ContractType::Device => "/system/contract/device",
    };
    let id = match id {
        Some(i) => format!("/{i}"),
        None => String::new(),
    };
    CString::new(format!("{prefix}{id}/{file}")).unwrap()
}

#[derive(Clone, Copy, Debug)]
pub enum ContractType {
    Process,
    Device,
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
pub struct Watcher {
    fd: c_int,
}

impl Drop for Watcher {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

impl Watcher {
    /// Return a Watcher for a specific type of contract event.  The watcher
    /// will return all events of the requested type, and it is the callers
    /// responsibility to filter for the events relevent to them.
    pub fn new(typ: ContractType) -> Self {
        let path = path(typ, None, "pbundle");
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY) };
        if fd < 0 {
            panic!(
                "Could not open {:?}: {}",
                path,
                std::io::Error::last_os_error()
            );
        }
        Watcher { fd }
    }

    /// Block until a contract event occurs.
    pub fn watch(&self, log: &slog::Logger) -> ContractEvent {
        loop {
            let mut event: ct_evthdl_t = std::ptr::null_mut();
            let evp: *mut ct_evthdl_t = &mut event;
            // The event endpoint was not opened as non-blocking, so
            // ct_event_read_critical(3CONTRACT) will block until a new
            // critical event is available on the channel.
            match unsafe { ct_event_read_critical(self.fd, evp) } {
                0 => {
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
    fd: c_int,
}

impl Drop for Control {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

impl Control {
    /// Construct a new Control for the specified contract
    pub fn new(
        typ: ContractType,
        ctid: ctid_t,
    ) -> Result<Self, crate::ExecutionError> {
        let path = path(typ, Some(ctid), "ctl");
        match unsafe { libc::open(path.as_ptr(), libc::O_WRONLY) } {
            fd if fd >= 0 => Ok(Control { ctid, fd }),
            _ => Err(err(format!(
                "opening control path {}",
                path.into_string().unwrap()
            ))),
        }
    }

    /// Acknowledge an event on the contract
    pub fn ack(
        &self,
        event_id: ct_evid_t,
    ) -> Result<(), crate::ExecutionError> {
        match unsafe { ct_ctl_ack(self.fd, event_id) } {
            0 => Ok(()),
            _ => Err(err(format!("failed to acknowledge event {}", event_id))),
        }
    }

    /// Abandon the contract
    pub fn abandon(self) -> Result<(), crate::ExecutionError> {
        match unsafe { ct_ctl_abandon(self.fd) } {
            0 => Ok(()),
            _ => Err(err(format!("failed to abandon contract {}", self.ctid))),
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
pub struct Template {
    fd: c_int,
}

impl Drop for Template {
    fn drop(&mut self) {
        self.clear();
        // Ignore any error, since printing may interfere with `slog`'s
        // structured output.
        unsafe { libc::close(self.fd) };
    }
}

fn get_tfpkt_device_path() -> Option<Vec<i8>> {
    let dev_path = CString::new("/dev/tfpkt0").unwrap();
    let mut path_buf = [0i8; 1024];
    let sz = unsafe {
        libc::readlink(dev_path.as_ptr(), path_buf.as_mut_ptr(), 1024)
    };
    if sz < 0 {
        None
    } else {
        let left = 10;
        let right = path_buf.iter().position(|a| *a == 0).unwrap();
        Some(path_buf[left..right + 1].to_vec())
    }
}

impl Template {
    pub fn new(typ: ContractType) -> Result<Self, crate::ExecutionError> {
        let path = path(typ, None, "template");
        let fd = match unsafe { libc::open(path.as_ptr(), libc::O_RDWR) } {
            fd if fd >= 0 => Ok(fd),
            _ => Err(err(format!(
                "opening template {}",
                path.into_string().unwrap()
            ))),
        }?;

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
                if unsafe { ct_tmpl_set_critical(fd, CT_PR_EV_EMPTY) } != 0 {
                    Err(err("set_critical in process template"))
                } else if unsafe { ct_tmpl_set_informative(fd, 0) } != 0 {
                    Err(err("set_informative in process template"))
                } else if unsafe { ct_pr_tmpl_set_fatal(fd, CT_PR_EV_HWERR) }
                    != 0
                {
                    Err(err("set_fatal in process template"))
                } else if unsafe {
                    ct_pr_tmpl_set_param(fd, CT_PR_PGRPONLY | CT_PR_REGENT)
                } != 0
                {
                    Err(err("set_param in process template"))
                } else if unsafe { ct_tmpl_activate(fd) } != 0 {
                    Err(err("activating in process template"))
                } else {
                    Ok(Self { fd })
                }
            }
            ContractType::Device => {
                // The only device contract we currently support is for the
                // tfpkt device.  If we ever want to support something else
                // then we will need to include a device
                // type, path, and/or instance number as an additional argument
                // to this template creation function.
                let cpath = match get_tfpkt_device_path() {
                    Some(c) => Ok(c),
                    None => Err(err("unable to find tfpkt in device tree")),
                }?;

                // We want to be notified when the device goes offline
                if unsafe { ct_tmpl_set_critical(fd, CT_DEV_EV_OFFLINE) } != 0 {
                    Err(err("set_critical in device template"))
                } else if unsafe { ct_dev_tmpl_set_minor(fd, cpath.as_ptr()) }
                    != 0
                {
                    let cpath: Vec<u8> =
                        cpath.iter().map(|c| *c as u8).collect();
                    Err(err(format!(
                        "set_minor to {} in device template",
                        str::from_utf8(&cpath[..]).unwrap()
                    )))
                } else if unsafe { ct_tmpl_activate(fd) } != 0 {
                    Err(err("activation device template"))
                } else {
                    Ok(Self { fd })
                }
            }
        }
        .inspect_err(|_e| {
            unsafe { libc::close(fd) };
        })
    }

    pub fn create(&self) -> Result<ctid_t, crate::ExecutionError> {
        let mut ctid = 0;
        match unsafe { ct_tmpl_create(self.fd, &mut ctid) } {
            0 => Ok(ctid),
            _ => Err(err("constructing contract")),
        }
    }

    pub fn clear(&self) {
        unsafe { ct_tmpl_clear(self.fd) };
    }
}

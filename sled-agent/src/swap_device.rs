// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Operations for creating a system swap device.

use std::io::Read;
use zeroize::Zeroize;

#[derive(Debug, thiserror::Error)]
pub enum SwapDeviceError {
    #[error("Error running ZFS command: {0}")]
    Zfs(illumos_utils::ExecutionError),

    #[error("Error listing swap devices: {0}")]
    ListDevices(String),

    #[error(
        "Error adding swap device: {msg} (path=\"{path}\", start={start}, length={length})"
    )]
    AddDevice { msg: String, path: String, start: u64, length: u64 },

    #[error("{msg}: {error}")]
    Misc { msg: String, error: String },
}

/// Ensure the system has a swap device, creating the underlying block
/// device if necessary.
///
/// The swap device is an encrypted zvol that lives on the M.2 disk that the
/// system booted from.  Because it booted from the disk, we know for certain
/// the system can access it. We encrypt the zvol because arbitrary system
/// memory could exist in swap, including sensitive data. The zvol is encrypted
/// with an ephemeral key; we throw it away immediately after creation and
/// create a new zvol if we find one on startup (that isn't backing a current
/// swap device). An ephemeral key is prudent because the kernel has the key
/// once the device is created, and there is no need for anything else to ever
/// decrypt swap.
///
/// To achieve idempotency in the case of crash and restart, we do the following:
///   1. On startup, check if there is a swap device. If one exists, we are done.
///      Swap devices do not persist across reboot by default, so if a device
///      already exists, this isn't our first time starting after boot. The
///      device may be in use. Changes to how the swap device is setup, should we
///      decide to do that, will be across reboots (as this is how sled-agent is
///      upgraded), so we will get a shot to make changes across upgrade.
///   2. If there is no swap device, check for a zvol at the known path on the
///      M.2 that we booted from. If we find such a zvol, delete it.
///   3. Create an encrypted zvol with a randomly generated key that is
///      immediately discarded.
///   4. Add the zvol as a swap device with swapctl(2).
///
/// Note that this introduces a sled-agent upgrade consideration if we ever
/// choose to change how we set up the device. A configured swap device does not
/// persist across reboot by default, but a zvol does. Thus, if the well known
/// path for the zvol ever changes, we will need to at least support a window
/// where we check for both the previously well-known path and the new
/// configuration.
pub(crate) fn ensure_swap_device(
    log: &slog::Logger,
    boot_zpool_name: &illumos_utils::zpool::ZpoolName,
    size_gb: u32,
) -> Result<(), SwapDeviceError> {
    assert!(size_gb > 0);

    let devs = swapctl::list_swap_devices()?;
    if !devs.is_empty() {
        if devs.len() > 1 {
            // This should really never happen unless we've made a mistake, but it's
            // probably fine to have more than one swap device. Thus, don't panic
            // over it, but do log a warning so there is evidence that we found
            // extra devices.
            //
            // Eventually, we should probably send an ereport here.
            warn!(
                log,
                "Found multiple existing swap devices on startup: {:?}", devs
            );
        } else {
            info!(log, "Swap device already exists: {:?}", devs);
        }

        return Ok(());
    }

    let swap_zvol = format!("{}/{}", boot_zpool_name, "swap");
    if zvol_exists(&swap_zvol)? {
        info!(log, "swap zvol \"{}\" aleady exists; destroying", swap_zvol);
        zvol_destroy(&swap_zvol)?;
        info!(log, "swap zvol \"{}\" destroyed", swap_zvol);
    }

    create_encrypted_swap_zvol(log, &swap_zvol, size_gb)?;

    // The process of paging out uses block I/O, so use the "dsk" version of
    // the zvol path (as opposed to "rdsk", which is for character/raw access.)
    let blk_zvol_path = format!("/dev/zvol/dsk/{}", swap_zvol);
    info!(log, "adding swap device: swapname=\"{}\"", blk_zvol_path);

    // Specifying 0 length tells the kernel to use the size of the device.
    swapctl::add_swap_device(blk_zvol_path, 0, 0)?;

    Ok(())
}

/// Check whether the given zvol exists.
fn zvol_exists(name: &str) -> Result<bool, SwapDeviceError> {
    let mut command = std::process::Command::new(illumos_utils::zfs::ZFS);
    let cmd = command.args(&["list", "-Hpo", "name,type"]);

    let output =
        illumos_utils::execute(cmd).map_err(|e| SwapDeviceError::Zfs(e))?;
    let stdout = String::from_utf8_lossy(&output.stdout);

    for line in stdout.lines() {
        let v: Vec<_> = line.split('\t').collect();

        if v[0] != name {
            continue;
        }
        if v[1] != "volume" {
            return Err(SwapDeviceError::Misc {
                msg: "could verify zvol".to_string(),
                error: format!(
                    "found dataset \"{}\" for swap device, but it is not a volume",
                    name
                ),
            });
        } else {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Destroys a zvol at the given path.
fn zvol_destroy(name: &str) -> Result<(), SwapDeviceError> {
    let mut command = std::process::Command::new(illumos_utils::zfs::ZFS);
    let cmd = command.args(&["destroy", name]);
    illumos_utils::execute(cmd).map_err(|e| SwapDeviceError::Zfs(e))?;

    Ok(())
}

/// Creates an encrypted zvol at the input path with the given size.
///
/// We create the key from random bytes, pipe it to stdin to minimize copying,
/// and zeroize the buffer after the zvol is created.
fn create_encrypted_swap_zvol(
    log: &slog::Logger,
    name: &str,
    size_gb: u32,
) -> Result<(), SwapDeviceError> {
    info!(
        log,
        "attempting to create encrypted zvol: name=\"{}\", size_gb={}",
        name,
        size_gb
    );

    // Create the zvol, piping the random bytes to stdin.
    let size_arg = format!("{}G", size_gb);
    let page_size =
        illumos_utils::libc::sysconf(libc::_SC_PAGESIZE).map_err(|e| {
            SwapDeviceError::Misc {
                msg: "could not access PAGESIZE from sysconf".to_string(),
                error: e.to_string(),
            }
        })?;
    let mut command = std::process::Command::new(illumos_utils::zfs::ZFS);
    #[rustfmt::skip]
    let cmd = command
        // create sparse volume of a given size with no reservation, exported as
        // a block device at: /dev/zvol/{dsk,rdsk}/<name>
        .arg("create")
        .args(&["-V", &size_arg])
        .arg("-s")

        // equivalent to volblocksize=blocksize
        // should use PAGESIZE from sysconf
        .args(&["-b", &page_size.to_string()])

        // don't use log device
        .args(&["-o", "logbias=throughput"])

        // only cache metadata in ARC
        // don't use secondary cache
        .args(&["-o", "primarycache=metadata"])
        .args(&["-o", "secondarycache=none"])

        // encryption format, with a raw key (32 bytes), with the key coming
        // from stdin
        .args(&["-o", "encryption=aes-256-gcm"])
        .args(&["-o", "keyformat=raw"])
        .args(&["-o", "keylocation=file:///dev/stdin"])

        // name of device: something like, oxi_<M.2 zpool uuid>/swap
        .arg(name)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    // Generate random bytes for the key.
    let mut urandom = std::fs::OpenOptions::new()
        .create(false)
        .read(true)
        .open("/dev/urandom")
        .map_err(|e| SwapDeviceError::Misc {
            msg: "could not open /dev/urandom".to_string(),
            error: e.to_string(),
        })?;
    let mut secret = vec![0u8; 32];
    urandom.read_exact(&mut secret).map_err(|e| SwapDeviceError::Misc {
        msg: "could not read from /dev/urandom".to_string(),
        error: e.to_string(),
    })?;

    // Spawn the process, writing the key in through stdin.
    let mut child = cmd.spawn().map_err(|e| SwapDeviceError::Misc {
        msg: format!("failed to spawn `zfs create` for zvol \"{}\"", name),
        error: e.to_string(),
    })?;
    let mut stdin = child.stdin.take().unwrap();
    let child_log = log.clone();
    let hdl = std::thread::spawn(move || {
        use std::io::Write;
        let res = stdin.write_all(&secret);
        if res.is_err() {
            error!(
                child_log,
                "could not write key to stdin of `zfs create` for swap zvol: {:?}",
                res
            );
        }
        secret.zeroize();
    });

    // Wait for the process, and the thread writing the bytes to it, to complete.
    let output =
        child.wait_with_output().map_err(|e| SwapDeviceError::Misc {
            msg: "failed to read stdout".to_string(),
            error: e.to_string(),
        })?;
    hdl.join().unwrap();

    if !output.status.success() {
        return Err(SwapDeviceError::Zfs(illumos_utils::output_to_exec_error(
            &command, &output,
        )));
    }

    info!(
        log,
        "successfully created encrypted zvol: name=\"{}\", size_gb={}",
        name,
        size_gb
    );
    Ok(())
}

/// Wrapper functions around swapctl(2) operations
mod swapctl {
    use crate::swap_device::SwapDeviceError;

    /// A representation of a swap device, as returned from swapctl(2) SC_LIST
    #[derive(Debug)]
    #[allow(dead_code)]
    pub(crate) struct SwapDevice {
        /// path to the resource
        path: String,

        /// starting block on device used for swap
        start: u64,

        /// length of swap area
        length: u64,

        /// total number of pages used for swapping
        total_pages: u64,

        /// free npages for swapping
        free_pages: u64,

        flags: i64,
    }

    // swapctl(2)
    #[cfg(target_os = "illumos")]
    unsafe extern "C" {
        fn swapctl(cmd: i32, arg: *mut libc::c_void) -> i32;
    }

    // TODO: in the limit, we probably want to stub out all illumos-specific
    // calls, and perhaps define an alternate version of this module for
    // non-illumos targets. But currently, this code is only used by the real
    // sled agent, and there is a fair amount of work there to make the real
    // sled agent work on non-illumos targets. So for now, just stub out this
    // piece.
    #[cfg(not(target_os = "illumos"))]
    unsafe fn swapctl(_cmd: i32, _arg: *mut libc::c_void) -> i32 {
        panic!("swapctl(2) only on illumos");
    }

    // swapctl(2) commands
    const SC_ADD: i32 = 0x1;
    const SC_LIST: i32 = 0x2;
    #[allow(dead_code)]
    const SC_REMOVE: i32 = 0x3;
    const SC_GETNSWP: i32 = 0x4;

    // argument for SC_ADD and SC_REMOVE
    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    struct swapres {
        sr_name: *const libc::c_char,
        sr_start: libc::off_t,
        sr_length: libc::off_t,
    }

    // argument for SC_LIST: swaptbl with an embedded array of swt_n swapents
    #[repr(C)]
    #[derive(Debug, Clone)]
    struct swaptbl {
        swt_n: i32,
        swt_ent: [swapent; N_SWAPENTS],
    }
    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    struct swapent {
        ste_path: *const libc::c_char,
        ste_start: libc::off_t,
        ste_length: libc::off_t,
        ste_pages: libc::c_long,
        ste_free: libc::c_long,
        ste_flags: libc::c_long,
    }
    impl Default for swapent {
        fn default() -> Self {
            Self {
                ste_path: std::ptr::null_mut(),
                ste_start: 0,
                ste_length: 0,
                ste_pages: 0,
                ste_free: 0,
                ste_flags: 0,
            }
        }
    }

    /// The argument for SC_LIST (struct swaptbl) requires an embedded array in
    /// the struct, with swt_n entries, each of which requires a pointer to store
    /// the path to the device.
    ///
    /// Ideally, we would want to query the number of swap devices on the system
    /// via SC_GETNSWP, allocate enough memory for each device entry, then pass
    /// in pointers to memory to the list command. Unfortunately, creating a
    /// generically large array embedded in a struct that can be passed to C is a
    /// bit of a challenge in safe Rust. So instead, we just pick a reasonable
    /// max number of devices to list.
    ///
    /// We pick a max of 3 devices, somewhat arbitrarily. We only ever expect to
    /// see 0 or 1 swap device(s); if there are more, that is a bug. In the case
    /// that we see more than 1 swap device, we log a warning, and eventually, we
    /// should send an ereport.
    const N_SWAPENTS: usize = 3;

    /// Wrapper around swapctl(2) call. All commands except SC_GETNSWP require an
    /// argument, hence `data` being an optional parameter.
    unsafe fn swapctl_cmd<T>(
        cmd: i32,
        data: Option<std::ptr::NonNull<T>>,
    ) -> std::io::Result<u32> {
        assert!(
            cmd >= SC_ADD && cmd <= SC_GETNSWP,
            "invalid swapctl cmd: {cmd}"
        );

        let ptr = match data {
            Some(v) => v.as_ptr() as *mut libc::c_void,
            None => std::ptr::null_mut(),
        };

        let res = unsafe { swapctl(cmd, ptr) };
        if res == -1 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(res as u32)
    }

    /// List swap devices on the system.
    pub(crate) fn list_swap_devices() -> Result<Vec<SwapDevice>, SwapDeviceError>
    {
        // Each swapent requires a char * pointer in our control for the
        // `ste_path` field,, which the kernel will fill in with a path if there
        // is a swap device for that entry. Because these pointers are mutated
        // by the kernel, we mark them as mutable. (Note that the compiler will
        // happily accept these definitions as non-mutable, since it can't know
        // what happens to the pointers on the C side, but not marking them as
        // mutable when they may be in fact be mutated is undefined behavior).
        //
        // Per limits.h(3HEAD), PATH_MAX is the max number of bytes in a path
        // name, including the null terminating character, so these buffers
        // have sufficient space for paths on the system.
        const MAXPATHLEN: usize = libc::PATH_MAX as usize;
        let mut p1 = [0i8; MAXPATHLEN];
        let mut p2 = [0i8; MAXPATHLEN];
        let mut p3 = [0i8; MAXPATHLEN];
        let entries: [swapent; N_SWAPENTS] = [
            swapent {
                ste_path: &mut p1 as *mut libc::c_char,
                ..Default::default()
            },
            swapent {
                ste_path: &mut p2 as *mut libc::c_char,
                ..Default::default()
            },
            swapent {
                ste_path: &mut p3 as *mut libc::c_char,
                ..Default::default()
            },
        ];

        let mut list_req =
            swaptbl { swt_n: N_SWAPENTS as i32, swt_ent: entries };
        // Unwrap safety: We know this isn't null because we just created it
        let ptr = std::ptr::NonNull::new(&mut list_req).unwrap();
        let n_devices = unsafe {
            swapctl_cmd(SC_LIST, Some(ptr))
                .map_err(|e| SwapDeviceError::ListDevices(e.to_string()))?
        };

        let mut devices = Vec::with_capacity(n_devices as usize);
        for i in 0..n_devices as usize {
            let e = list_req.swt_ent[i];

            // Safety: CStr::from_ptr is documented as safe if:
            //   1. The pointer contains a valid null terminator at the end of
            //      the string
            //   2. The pointer is valid for reads of bytes up to and including
            //      the null terminator
            //   3. The memory referenced by the return CStr is not mutated for
            //      the duration of lifetime 'a
            //
            // (1) is true because we initialize the buffers for ste_path as all
            // 0s, and their length is long enough to include the null
            // terminator for all paths on the system.
            // (2) should be guaranteed by the syscall itself, and we can know
            // how many entries are valid via its return value.
            // (3) We aren't currently mutating the memory referenced by the
            // CStr, though there's nothing here enforcing that.
            let p = unsafe { std::ffi::CStr::from_ptr(e.ste_path) };
            let path = String::from_utf8_lossy(p.to_bytes()).to_string();

            devices.push(SwapDevice {
                path,
                start: e.ste_start as u64,
                length: e.ste_length as u64,
                total_pages: e.ste_pages as u64,
                free_pages: e.ste_free as u64,
                flags: e.ste_flags,
            });
        }

        Ok(devices)
    }

    /// Add a swap device at the given path.
    pub fn add_swap_device(
        path: String,
        start: u64,
        length: u64,
    ) -> Result<(), SwapDeviceError> {
        let path_cp = path.clone();
        let name = std::ffi::CString::new(path).map_err(|e| {
            SwapDeviceError::AddDevice {
                msg: format!("could not convert path to CString: {}", e,),
                path: path_cp.clone(),
                start,
                length,
            }
        })?;

        let mut add_req = swapres {
            sr_name: name.as_ptr(),
            sr_start: start as i64,
            sr_length: length as i64,
        };
        // Unwrap safety: We know this isn't null because we just created it
        let ptr = std::ptr::NonNull::new(&mut add_req).unwrap();
        let res = unsafe {
            swapctl_cmd(SC_ADD, Some(ptr)).map_err(|e| {
                SwapDeviceError::AddDevice {
                    msg: e.to_string(),
                    path: path_cp,
                    start,
                    length,
                }
            })?
        };
        assert_eq!(res, 0);

        Ok(())
    }
}

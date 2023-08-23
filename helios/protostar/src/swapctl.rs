// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Operations for creating a system swap device.

use helios_fusion::interfaces::swapctl::{Error, SwapDevice, Swapctl};

// swapctl(2)
#[cfg(target_os = "illumos")]
extern "C" {
    fn swapctl(cmd: i32, arg: *mut libc::c_void) -> i32;
}

// TODO: in the limit, we probably want to stub out all illumos-specific
// calls, and perhaps define an alternate version of this module for
// non-illumos targets. But currently, this code is only used by the real
// sled agent, and there is a fair amount of work there to make the real
// sled agent work on non-illumos targets. So for now, just stub out this
// piece.
#[cfg(not(target_os = "illumos"))]
fn swapctl(_cmd: i32, _arg: *mut libc::c_void) -> i32 {
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
    assert!(cmd >= SC_ADD && cmd <= SC_GETNSWP, "invalid swapctl cmd: {cmd}");

    let ptr = match data {
        Some(v) => v.as_ptr() as *mut libc::c_void,
        None => std::ptr::null_mut(),
    };

    let res = swapctl(cmd, ptr);
    if res == -1 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(res as u32)
}

#[derive(Default)]
pub(crate) struct RealSwapctl {}

impl Swapctl for RealSwapctl {
    /// List swap devices on the system.
    fn list_swap_devices(&self) -> Result<Vec<SwapDevice>, Error> {
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
                .map_err(|e| Error::ListDevices(e.to_string()))?
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
                path: path,
                start: e.ste_start as u64,
                length: e.ste_length as u64,
                total_pages: e.ste_pages as u64,
                free_pages: e.ste_free as u64,
                flags: e.ste_flags,
            });
        }

        Ok(devices)
    }

    fn add_swap_device(
        &self,
        path: String,
        start: u64,
        length: u64,
    ) -> Result<(), Error> {
        let path_cp = path.clone();
        let name =
            std::ffi::CString::new(path).map_err(|e| Error::AddDevice {
                msg: format!("could not convert path to CString: {}", e,),
                path: path_cp.clone(),
                start: start,
                length: length,
            })?;

        let mut add_req = swapres {
            sr_name: name.as_ptr(),
            sr_start: start as i64,
            sr_length: length as i64,
        };
        // Unwrap safety: We know this isn't null because we just created it
        let ptr = std::ptr::NonNull::new(&mut add_req).unwrap();
        let res = unsafe {
            swapctl_cmd(SC_ADD, Some(ptr)).map_err(|e| Error::AddDevice {
                msg: e.to_string(),
                path: path_cp,
                start: start,
                length: length,
            })?
        };
        assert_eq!(res, 0);

        Ok(())
    }
}

use fs_err as fs;
use libc::{c_char, c_int, c_void, pid_t};
use thiserror::Error;

use std::{
    collections::BTreeSet,
    ffi::{CStr, CString},
    os::fd::AsRawFd,
    path::Path,
};

const CT_ALL: &str = "/system/contract/all";
const OXIDE_FMRI: &str = "svc:/oxide/";
const CTD_ALL: i32 = 2;

#[allow(non_camel_case_types)]
type ct_stathdl_t = *mut c_void;

#[link(name = "contract")]
extern "C" {
    fn ct_status_read(
        fd: c_int,
        detail: c_int,
        stathdlp: *mut ct_stathdl_t,
    ) -> c_int;
    fn ct_status_free(stathdlp: ct_stathdl_t);
    fn ct_pr_status_get_members(
        stathdlp: ct_stathdl_t,
        pidpp: *mut *mut pid_t,
        n: *mut u32,
    ) -> c_int;
    fn ct_pr_status_get_svc_fmri(
        stathdlp: ct_stathdl_t,
        fmri: *mut *mut c_char,
    ) -> c_int;
}

#[derive(Error, Debug)]
pub enum ContractError {
    #[error("fix me")]
    Io(#[from] std::io::Error),
}

pub struct ContractStatus {
    handle: ct_stathdl_t,
}

impl Drop for ContractStatus {
    fn drop(&mut self) {
        unsafe { ct_status_free(self.handle) };
    }
}

macro_rules! libcall_io {
        ($fn: ident ( $($arg: expr), * $(,)*) ) => {{
            let res = unsafe { $fn($($arg, )*) };
            if res == 0 {
                Ok(res)
            } else {
                Err(std::io::Error::last_os_error())
            }
        }};
    }

impl ContractStatus {
    fn new(contract_status: &Path) -> Result<Self, ContractError> {
        let file = fs::File::open(contract_status)?;
        let mut handle: ct_stathdl_t = std::ptr::null_mut();
        libcall_io!(ct_status_read(file.as_raw_fd(), CTD_ALL, &mut handle,))?;
        // XXX For now
        assert!(!handle.is_null());

        Ok(Self { handle })
    }

    fn get_members(&self) -> Result<&[i32], ContractError> {
        let mut numpids = 0;
        let mut pids: *mut pid_t = std::ptr::null_mut();

        let pids = {
            libcall_io!(ct_pr_status_get_members(
                self.handle,
                &mut pids,
                &mut numpids,
            ))?;

            unsafe {
                if pids.is_null() {
                    &[]
                } else {
                    std::slice::from_raw_parts(pids, numpids as usize)
                }
            }
        };

        Ok(pids)
    }

    fn get_fmri(&self) -> Result<Option<CString>, ContractError> {
        // The lifetime of this string is tied to the lifetime of the status
        // handle itself and will be cleaned up when the handle is freed.
        let mut ptr: *mut c_char = std::ptr::null_mut();
        libcall_io!(ct_pr_status_get_svc_fmri(self.handle, &mut ptr))?;

        if ptr.is_null() {
            return Ok(None);
        }

        let cstr = unsafe { CStr::from_ptr(ptr) };
        Ok(Some(cstr.to_owned()))
    }
}

pub fn find_oxide_pids() -> Result<BTreeSet<i32>, ContractError> {
    let mut pids = BTreeSet::new();
    let ents = fs::read_dir(CT_ALL)?;
    for ct in ents {
        let ctid = ct?;
        let mut path = ctid.path();
        path.push("status");

        let status = match ContractStatus::new(path.as_path()) {
            Ok(status) => status,
            // XXX skip over this
            Err(_) => {
                // XXX log?
                // the contract is gone?
                continue;
            }
        };

        let fmri = status.get_fmri()?.unwrap_or_default();
        if fmri.to_string_lossy().starts_with(OXIDE_FMRI) {
            pids.extend(status.get_members()?);
        }
    }

    Ok(pids)
}

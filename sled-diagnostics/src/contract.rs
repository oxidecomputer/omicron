// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// ! Bindings to libcontract(3lib).

use fs_err as fs;
use libc::{c_char, c_int, c_void, pid_t};
use slog::{Logger, warn};
use thiserror::Error;

use std::{
    collections::BTreeSet,
    ffi::{CStr, CString},
    os::fd::AsRawFd,
    path::Path,
};

const CT_ALL: &str = "/system/contract/all";
// Most Oxide services
const OXIDE_FMRI: &str = "svc:/oxide/";
// NB: Used for propolis zones
const ILLUMOS_FMRI: &str = "svc:/system/illumos/";
const CTD_ALL: i32 = 2;

#[allow(non_camel_case_types)]
type ct_stathdl_t = *mut c_void;

#[link(name = "contract")]
unsafe extern "C" {
    fn ct_status_read(
        fd: c_int,
        detail: c_int,
        stathdlp: *mut ct_stathdl_t,
    ) -> c_int;
    fn ct_status_free(stathdlp: ct_stathdl_t);
    fn ct_status_get_id(stathdlp: ct_stathdl_t) -> i32;
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
    #[error(transparent)]
    FileIo(#[from] std::io::Error),
    #[error(
        "Failed to call ct_pr_status_get_svc_fmri for contract {ctid}: {error}"
    )]
    Fmri { ctid: i32, error: std::io::Error },
    #[error(
        "Failed to call ct_pr_status_get_members for contract {ctid}: {error}"
    )]
    Members { ctid: i32, error: std::io::Error },
    #[error(
        "ct_status_read returned successfully but handed back a null ptr for {0}"
    )]
    Null(std::path::PathBuf),
    #[error("Failed to call ct_status_read on {path}: {error}")]
    StatusRead { path: std::path::PathBuf, error: std::io::Error },
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
        libcall_io!(ct_status_read(file.as_raw_fd(), CTD_ALL, &mut handle,))
            .map_err(|error| ContractError::StatusRead {
                path: contract_status.to_path_buf(),
                error,
            })?;

        // We don't ever expect the system to hand back a null ptr when
        // returning success but let's be extra cautious anyways.
        if handle.is_null() {
            return Err(ContractError::Null(contract_status.to_path_buf()));
        }

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
            ))
            .map_err(|error| {
                let ctid = unsafe { ct_status_get_id(self.handle) };
                ContractError::Members { ctid, error }
            })?;

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
        libcall_io!(ct_pr_status_get_svc_fmri(self.handle, &mut ptr)).map_err(
            |error| {
                let ctid = unsafe { ct_status_get_id(self.handle) };
                ContractError::Fmri { ctid, error }
            },
        )?;

        if ptr.is_null() {
            return Ok(None);
        }

        let cstr = unsafe { CStr::from_ptr(ptr) };
        Ok(Some(cstr.to_owned()))
    }
}

pub fn find_oxide_pids(log: &Logger) -> Result<BTreeSet<i32>, ContractError> {
    let mut pids = BTreeSet::new();
    let ents = fs::read_dir(CT_ALL)?;
    for ct in ents {
        let ctid = ct?;
        let mut path = ctid.path();
        path.push("status");

        let status = match ContractStatus::new(path.as_path()) {
            Ok(status) => status,
            Err(e) => {
                // There's a race between the time we find the contracts to the
                // time we attempt to read the contract's status. We can safely
                // skip all of the errors for diagnostics purposes but we should
                // leave a log in our wake.
                warn!(log, "Failed to read contract ({:?}): {}", path, e);
                continue;
            }
        };

        let fmri_owned = status.get_fmri()?.unwrap_or_default();
        let fmri = fmri_owned.to_string_lossy();
        if fmri.starts_with(OXIDE_FMRI) || fmri.starts_with(ILLUMOS_FMRI) {
            pids.extend(status.get_members()?);
        }
    }

    Ok(pids)
}

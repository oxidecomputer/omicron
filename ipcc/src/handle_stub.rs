use std::{
    ffi::{c_int, CStr, CString},
    ptr,
};

use crate::ffi::*;
use crate::ipcc_common::*;

pub struct IpccHandle;

impl IpccHandle {
    pub fn new() -> Result<Self, IpccError> {
        panic!("ipcc unavailable on this platform")
    }

    pub(crate) fn key_lookup(
        &self,
        key: u8,
        buf: &mut [u8],
    ) -> Result<usize, IpccError> {
        panic!("ipcc unavailable on this platform")
    }
}

extern crate illumos_utils;
extern crate libc;

use illumos_utils::dkio::dk_minfo_ext;

include!(concat!(env!("OUT_DIR"), "/dkio.rs"));

#![allow(clippy::redundant_closure_call, clippy::needless_lifetimes)]

include!(concat!(env!("OUT_DIR"), "/ddm-admin-client.rs"));

impl Copy for types::Ipv6Prefix {}

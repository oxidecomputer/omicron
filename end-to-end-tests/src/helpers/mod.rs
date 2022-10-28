pub mod ctx;

use self::ctx::nexus_addr;
use anyhow::{bail, Result};
use oxide_client::types::Name;
use rand::{thread_rng, Rng};
use std::net::{IpAddr, Ipv4Addr};

pub fn generate_name(prefix: &str) -> Result<Name> {
    format!("{}-{:x}", prefix, thread_rng().gen_range(0..0xfff_ffff_ffffu64))
        .try_into()
        .map_err(anyhow::Error::msg)
}

/// HACK: we're picking a range that doesn't conflict with either iliana's or
/// the lab environment's IP ranges. This is not terribly robust. (in both
/// iliana's environment and the lab, the last octet is 20; in my environment
/// the DHCP range is 100-249, and in the buildomat lab environment the network
/// is currently private.)
pub fn get_system_ip_pool() -> Result<(Ipv4Addr, Ipv4Addr)> {
    let nexus_addr = match nexus_addr().ip() {
        IpAddr::V4(addr) => addr.octets(),
        IpAddr::V6(_) => bail!("not sure what to do about IPv6 here"),
    };

    let first = [nexus_addr[0], nexus_addr[1], nexus_addr[2], 50].into();
    let last = [nexus_addr[0], nexus_addr[1], nexus_addr[2], 90].into();

    Ok((first, last))
}

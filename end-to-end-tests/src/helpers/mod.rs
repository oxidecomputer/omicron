pub mod cli;
pub mod ctx;
pub mod icmp;

use self::ctx::nexus_addr;
use anyhow::{Result, bail};
use oxide_client::types::Name;
use rand::Rng;
use std::env;
use std::net::{IpAddr, Ipv4Addr};

pub fn generate_name(prefix: &str) -> Result<Name> {
    format!("{}-{:x}", prefix, rand::rng().random_range(0..0xfff_ffff_ffffu64))
        .try_into()
        .map_err(anyhow::Error::msg)
}

pub async fn get_system_ip_pool() -> Result<(Ipv4Addr, Ipv4Addr)> {
    if let (Ok(s), Ok(e)) = (env::var("IPPOOL_START"), env::var("IPPOOL_END")) {
        return Ok((
            s.parse::<Ipv4Addr>().expect("IPPOOL_START is not an IP address"),
            e.parse::<Ipv4Addr>().expect("IPPOOL_END is not an IP address"),
        ));
    }

    let nexus_addr = match nexus_addr().await? {
        IpAddr::V4(addr) => addr.octets(),
        IpAddr::V6(_) => bail!("not sure what to do about IPv6 here"),
    };

    // HACK: we're picking a range that doesn't conflict with either iliana's
    // or the lab environment's IP ranges. This is not terribly robust. (in
    // both iliana's environment and the lab, the last octet is 20; in my
    // environment the DHCP range is 100-249, and in the buildomat lab
    // environment the network is currently private.)
    let first = [nexus_addr[0], nexus_addr[1], nexus_addr[2], 50].into();
    let last = [nexus_addr[0], nexus_addr[1], nexus_addr[2], 90].into();

    Ok((first, last))
}

pub mod cli;
pub mod ctx;
pub mod icmp;

use self::ctx::nexus_addr;
use anyhow::{Result, bail};
use oxide_client::types::{IpRange, Ipv4Range, Ipv6Range, Name};
use rand::Rng;
use std::env;
use std::net::{IpAddr, Ipv4Addr};

pub fn generate_name(prefix: &str) -> Result<Name> {
    format!("{}-{:x}", prefix, rand::rng().random_range(0..0xfff_ffff_ffffu64))
        .try_into()
        .map_err(anyhow::Error::msg)
}

pub async fn get_system_ip_pool() -> Result<(IpAddr, IpAddr)> {
    if let (Ok(s), Ok(e)) = (env::var("IPPOOL_START"), env::var("IPPOOL_END")) {
        return Ok((
            s.parse::<Ipv4Addr>()
                .expect("IPPOOL_START is not an IP address")
                .into(),
            e.parse::<Ipv4Addr>()
                .expect("IPPOOL_END is not an IP address")
                .into(),
        ));
    }

    let nexus_addr = match nexus_addr().await? {
        IpAddr::V4(addr) => addr.octets(),
        IpAddr::V6(_) => bail!("IPv6 IP Pools are not yet supported"),
    };

    // HACK: we're picking a range that doesn't conflict with either iliana's
    // or the lab environment's IP ranges. This is not terribly robust. (in
    // both iliana's environment and the lab, the last octet is 20; in my
    // environment the DHCP range is 100-249, and in the buildomat lab
    // environment the network is currently private.)
    let first = [nexus_addr[0], nexus_addr[1], nexus_addr[2], 50].into();
    let last = [nexus_addr[0], nexus_addr[1], nexus_addr[2], 90].into();

    Ok((IpAddr::V4(first), IpAddr::V4(last)))
}

/// Try to construct an IP range from a pair of addresses.
///
/// An error is returned if the addresses aren't the same version, or first >
/// last.
pub fn try_create_ip_range(first: IpAddr, last: IpAddr) -> Result<IpRange> {
    match (first, last) {
        (IpAddr::V4(first), IpAddr::V4(last)) => {
            anyhow::ensure!(
                first <= last,
                "IP range first address must be <= last"
            );
            Ok(IpRange::V4(Ipv4Range { first, last }))
        }
        (IpAddr::V6(first), IpAddr::V6(last)) => {
            anyhow::ensure!(
                first <= last,
                "IP range first address must be <= last"
            );
            Ok(IpRange::V6(Ipv6Range { first, last }))
        }
        (_, _) => anyhow::bail!(
            "Invalid IP addresses for IP range: {first} and {last}"
        ),
    }
}

use anyhow::{bail, Result};
use end_to_end_tests::helpers::ctx::{build_client, nexus_addr, Context};
use end_to_end_tests::helpers::generate_name;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oxide_client::types::{
    ByteCount, DiskCreate, DiskSource, IpPoolCreate, IpRange, Ipv4Range,
};
use oxide_client::{ClientDisksExt, ClientIpPoolsExt, ClientOrganizationsExt};
use std::net::IpAddr;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let client = build_client()?;

    // ===== ENSURE NEXUS IS UP ===== //
    eprintln!("waiting for nexus to come up...");
    wait_for_condition(
        || async {
            client
                .organization_list()
                .send()
                .await
                .map_err(|_| CondCheckError::<oxide_client::Error>::NotYet)
        },
        &Duration::from_secs(1),
        &Duration::from_secs(300),
    )
    .await?;

    // ===== CREATE IP POOL ===== //
    eprintln!("creating IP pool...");
    let nexus_addr = match nexus_addr().ip() {
        IpAddr::V4(addr) => addr.octets(),
        IpAddr::V6(_) => bail!("not sure what to do about IPv6 here"),
    };
    // TODO: not really sure about a good heuristic for selecting an IP address
    // range here. in both my (iliana's) environment and the lab, the last octet
    // is 20; in my environment the DHCP range is 100-249, and in the buildomat
    // lab environment the network is currently private.
    let first = [nexus_addr[0], nexus_addr[1], nexus_addr[2], 50].into();
    let last = [nexus_addr[0], nexus_addr[1], nexus_addr[2], 90].into();

    let pool_name = client
        .ip_pool_create()
        .body(IpPoolCreate {
            name: generate_name("ip-pool")?,
            description: String::new(),
            organization: None,
            project: None,
        })
        .send()
        .await?
        .name
        .clone();
    client
        .ip_pool_range_add()
        .pool_name(pool_name)
        .body(IpRange::V4(Ipv4Range { first, last }))
        .send()
        .await?;

    // ===== ENSURE DATASETS ARE READY ===== //
    eprintln!("ensuring datasets are ready...");
    let ctx = Context::from_client(client).await?;
    let disk_name = generate_name("disk")?;
    wait_for_condition(
        || async {
            ctx.client
                .disk_create()
                .organization_name(ctx.org_name.clone())
                .project_name(ctx.project_name.clone())
                .body(DiskCreate {
                    name: disk_name.clone(),
                    description: String::new(),
                    disk_source: DiskSource::Blank {
                        block_size: 512.try_into().unwrap(),
                    },
                    size: ByteCount(1024 * 1024 * 1024),
                })
                .send()
                .await
                .map_err(|_| CondCheckError::<oxide_client::Error>::NotYet)
        },
        &Duration::from_secs(1),
        &Duration::from_secs(120),
    )
    .await?;
    ctx.client
        .disk_delete()
        .organization_name(ctx.org_name.clone())
        .project_name(ctx.project_name.clone())
        .disk_name(disk_name)
        .send()
        .await?;
    ctx.cleanup().await?;

    eprintln!("let's roll.");
    Ok(())
}

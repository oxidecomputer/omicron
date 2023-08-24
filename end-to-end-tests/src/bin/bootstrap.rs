use anyhow::Result;
use end_to_end_tests::helpers::ctx::{build_client, Context};
use end_to_end_tests::helpers::{generate_name, get_system_ip_pool};
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oxide_client::types::{
    ByteCount, DiskCreate, DiskSource, IpRange, Ipv4Range,
};
use oxide_client::{
    ClientDisksExt, ClientProjectsExt, ClientSystemNetworkingExt,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let client = build_client().await?;

    // ===== ENSURE NEXUS IS UP ===== //
    eprintln!("waiting for nexus to come up...");
    wait_for_condition(
        || async {
            client
                .project_list()
                .send()
                .await
                .map_err(|_| CondCheckError::<oxide_client::Error>::NotYet)
        },
        &Duration::from_secs(1),
        &Duration::from_secs(300),
    )
    .await?;

    let (first, last) = get_system_ip_pool().await?;

    // ===== CREATE IP POOL ===== //
    eprintln!("creating IP pool... {:?} - {:?}", first, last);
    client
        .ip_pool_range_add()
        .pool("default")
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
                .project(ctx.project_name.clone())
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
        .project(ctx.project_name.clone())
        .disk(disk_name)
        .send()
        .await?;
    ctx.cleanup().await?;

    eprintln!("let's roll.");
    Ok(())
}

use anyhow::Result;
use end_to_end_tests::helpers::ctx::{build_client, Context};
use end_to_end_tests::helpers::{generate_name, get_system_ip_pool};
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oxide_client::types::{
    ByteCount, DiskCreate, DiskSource, IpRange, Ipv4Range, NameOrId,
};
use oxide_client::{ClientDisksExt, ClientOrganizationsExt, ClientSystemExt};
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
    let (first, last) = get_system_ip_pool()?;
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
    let org_name = NameOrId::Name(ctx.org_name.clone());
    let project_name = NameOrId::Name(ctx.project_name.clone());
    wait_for_condition(
        || async {
            ctx.client
                .disk_create()
                .organization(org_name.clone())
                .project(project_name.clone())
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
        .organization(org_name.clone())
        .project(project_name.clone())
        .disk(NameOrId::Name(disk_name))
        .send()
        .await?;
    ctx.cleanup().await?;

    eprintln!("let's roll.");
    Ok(())
}

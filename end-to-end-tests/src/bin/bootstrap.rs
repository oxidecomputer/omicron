use anyhow::Result;
use end_to_end_tests::helpers::ctx::{ClientParams, Context};
use end_to_end_tests::helpers::{
    generate_name, get_system_ip_pool, try_create_ip_range,
};
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use oxide_client::types::{
    ByteCount, DeviceAccessTokenRequest, DeviceAuthRequest, DeviceAuthVerify,
    DiskBackend, DiskCreate, DiskSource, IpPoolCreate, IpPoolLinkSilo,
    IpPoolType, IpVersion, NameOrId, SiloQuotasUpdate,
};
use oxide_client::{
    ClientConsoleAuthExt, ClientDisksExt, ClientProjectsExt,
    ClientSystemIpPoolsExt, ClientSystemSilosExt,
};
use serde::{Deserialize, de::DeserializeOwned};
use std::time::Duration;
use uuid::Uuid;

fn main() -> Result<()> {
    oxide_tokio_rt::run(run_test())
}

async fn run_test() -> Result<()> {
    let params = ClientParams::new()?;
    let client = params.build_client().await?;

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
    let ip_version =
        if first.is_ipv4() { IpVersion::V4 } else { IpVersion::V6 };
    eprintln!("creating IP{} IP pool... {:?} - {:?}", ip_version, first, last);
    let pool_name = "default";
    client
        .ip_pool_create()
        .body(IpPoolCreate {
            name: pool_name.parse().unwrap(),
            description: "Default IP pool".to_string(),
            ip_version,
            pool_type: IpPoolType::Unicast,
        })
        .send()
        .await?;
    client
        .ip_pool_silo_link()
        .pool(pool_name)
        .body(IpPoolLinkSilo {
            silo: NameOrId::Name(params.silo_name().parse().unwrap()),
            is_default: true,
        })
        .send()
        .await?;
    client
        .ip_pool_range_add()
        .pool(pool_name)
        .body(try_create_ip_range(first, last)?)
        .send()
        .await?;

    // ===== SET UP QUOTAS ===== //
    eprintln!("setting up quotas...");
    client
        .silo_quotas_update()
        .silo("recovery")
        .body(SiloQuotasUpdate {
            cpus: Some(16),
            memory: Some(ByteCount(1024 * 1024 * 1024 * 10)),
            storage: Some(ByteCount(1024 * 1024 * 1024 * 1024)),
        })
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
                    disk_backend: DiskBackend::Virtual(DiskSource::Blank {
                        block_size: 512.try_into().unwrap(),
                    }),
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

    // ===== PRINT CLI ENVIRONMENT ===== //
    let client_id = Uuid::new_v4();
    let DeviceAuthResponse { device_code, user_code } =
        deserialize_byte_stream(
            ctx.client
                .device_auth_request()
                .body(DeviceAuthRequest { client_id, ttl_seconds: None })
                .send()
                .await?,
        )
        .await?;
    ctx.client
        .device_auth_confirm()
        .body(DeviceAuthVerify { user_code })
        .send()
        .await?;
    let DeviceAccessTokenGrant { access_token } = deserialize_byte_stream(
        ctx.client
            .device_access_token()
            .body(DeviceAccessTokenRequest {
                client_id,
                device_code,
                grant_type: "urn:ietf:params:oauth:grant-type:device_code"
                    .to_string(),
            })
            .send()
            .await?,
    )
    .await?;

    println!("OXIDE_HOST={}", params.base_url());
    println!("OXIDE_RESOLVE={}", params.resolve_nexus().await?);
    println!("OXIDE_TOKEN={}", access_token);

    ctx.cleanup().await?;
    eprintln!("let's roll.");
    Ok(())
}

async fn deserialize_byte_stream<T: DeserializeOwned>(
    response: oxide_client::ResponseValue<oxide_client::ByteStream>,
) -> Result<T> {
    use bytes::buf::Buf;
    use futures::TryStreamExt;

    let bytes =
        response.into_inner_stream().try_collect::<bytes::BytesMut>().await?;
    Ok(serde_json::from_reader(bytes.reader())?)
}

#[derive(Deserialize)]
struct DeviceAuthResponse {
    device_code: String,
    user_code: String,
}

#[derive(Deserialize)]
struct DeviceAccessTokenGrant {
    access_token: String,
}

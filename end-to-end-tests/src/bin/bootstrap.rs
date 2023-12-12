use anyhow::Result;
use end_to_end_tests::helpers::ctx::{ClientParams, Context};
use end_to_end_tests::helpers::{generate_name, get_system_ip_pool};
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oxide_client::types::{
    ByteCount, DeviceAccessTokenRequest, DeviceAuthRequest, DeviceAuthVerify,
    DiskCreate, DiskSource, IpRange, Ipv4Range,
};
use oxide_client::{
    ClientDisksExt, ClientHiddenExt, ClientProjectsExt,
    ClientSystemNetworkingExt,
};
use serde::{de::DeserializeOwned, Deserialize};
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
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

    // ===== PRINT CLI ENVIRONMENT ===== //
    let client_id = Uuid::new_v4();
    let DeviceAuthResponse { device_code, user_code } =
        deserialize_byte_stream(
            ctx.client
                .device_auth_request()
                .body(DeviceAuthRequest { client_id })
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
    let body = hyper::Body::wrap_stream(response.into_inner_stream());
    let bytes = hyper::body::to_bytes(body).await?;
    Ok(serde_json::from_slice(&bytes)?)
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

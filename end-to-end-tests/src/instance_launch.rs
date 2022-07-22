#![cfg(test)]

use crate::helpers::{ctx::Context, generate_name, try_loop};
use anyhow::{ensure, Context as _, Result};
use oxide_client::types::{
    ByteCount, DiskCreate, DiskSource, Distribution, ExternalIpCreate,
    GlobalImageCreate, ImageSource, InstanceCpuCount, InstanceCreate,
    InstanceDiskAttachment, InstanceNetworkInterfaceAttachment,
};
use oxide_client::{ClientDisksExt, ClientImagesGlobalExt, ClientInstancesExt};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn instance_launch() -> Result<()> {
    let ctx = Context::new().await.unwrap();

    let image_id = ctx
        .client
        .image_global_create()
        .body(GlobalImageCreate {
            name: generate_name("debian")?,
            description: String::new(),
            block_size: 512.try_into().map_err(anyhow::Error::msg)?,
            distribution: Distribution {
                name: "debian".try_into().map_err(anyhow::Error::msg)?,
                version: "propolis-blob".into(),
            },
            source: ImageSource::Url {
                url:
                    "http://[fd00:1122:3344:101::1]:54321/debian-11-genericcloud-amd64.raw"
                        .into(),
            },
        })
        .send()
        .await?
        .id;

    let disk_name = ctx
        .client
        .disk_create()
        .organization_name(ctx.org_name.clone())
        .project_name(ctx.project_name.clone())
        .body(DiskCreate {
            name: generate_name("disk")?,
            description: String::new(),
            disk_source: DiskSource::GlobalImage { image_id },
            size: ByteCount(2048 * 1024 * 1024),
        })
        .send()
        .await?
        .name
        .clone();

    let instance = ctx
        .client
        .instance_create()
        .organization_name(ctx.org_name.clone())
        .project_name(ctx.project_name.clone())
        .body(InstanceCreate {
            name: generate_name("instance")?,
            description: String::new(),
            hostname: "localshark".into(), // 🦈
            memory: ByteCount(1024 * 1024 * 1024),
            ncpus: InstanceCpuCount(2),
            disks: vec![InstanceDiskAttachment::Attach { name: disk_name }],
            network_interfaces: InstanceNetworkInterfaceAttachment::Default,
            external_ips: vec![ExternalIpCreate::Ephemeral { pool_name: None }],
            user_data: String::new(),
        })
        .send()
        .await?;

    // poll serial for login prompt, waiting 1 min max
    let serial = try_loop(
        || async {
            sleep(Duration::from_secs(5)).await;
            let data = String::from_utf8_lossy(
                &ctx.client
                    .instance_serial_console()
                    .organization_name(ctx.org_name.clone())
                    .project_name(ctx.project_name.clone())
                    .instance_name(instance.name.clone())
                    .from_start(0)
                    .max_bytes(10 * 1024 * 1024)
                    .send()
                    .await?
                    .data,
            )
            .into_owned();
            ensure!(data.contains("localshark login:"), "not yet booted");
            Ok(data)
        },
        Duration::from_secs(300),
    )
    .await?;

    let host_keys = serial
        .split_once("-----BEGIN SSH HOST KEY KEYS-----")
        .and_then(|(_, s)| s.split_once("-----END SSH HOST KEY KEYS-----"))
        .map(|(s, _)| s.trim())
        .context("failed to get SSH host keys from serial console")?;
    println!("{}", host_keys);

    // tear-down
    ctx.client
        .instance_stop()
        .organization_name(ctx.org_name.clone())
        .project_name(ctx.project_name.clone())
        .instance_name(instance.name.clone())
        .send()
        .await?;
    ctx.cleanup().await
}

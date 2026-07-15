// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![cfg(test)]

use crate::helpers::{ctx::Context, generate_name};
use anyhow::{Context as _, Result, ensure};
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use oxide_client::types::{
    ByteCount, DiskBackend, DiskCreate, DiskSource, ExternalIp,
    ExternalIpCreate, InstanceCpuCount, InstanceCreate, InstanceDiskAttachment,
    InstanceNetworkInterfaceAttachment, InstanceState, IpVersion, PoolSelector,
    SshKeyCreate,
};
use oxide_client::{ClientCurrentUserExt, ClientDisksExt, ClientInstancesExt};
use russh::keys::PrivateKeyWithHashAlg;
use russh::{ChannelMsg, Disconnect};
use ssh_key::PublicKey;
use ssh_key::private::Ed25519Keypair;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn instance_launch() -> Result<()> {
    let ctx = Context::new().await?;

    eprintln!("generate SSH key");
    let key = Arc::new(Ed25519Keypair::random(&mut rand_010::rng()).into());
    let key = PrivateKeyWithHashAlg::new(key, None);
    let public_key_str = key.public_key().to_openssh()?;
    eprintln!("create SSH key: {}", public_key_str);
    let ssh_key_name = generate_name("key")?;
    ctx.client
        .current_user_ssh_key_create()
        .body(SshKeyCreate {
            name: ssh_key_name.clone(),
            description: String::new(),
            public_key: public_key_str,
        })
        .send()
        .await?;

    eprintln!("create disk");
    let disk_name = generate_name("disk")?;
    let disk_name = ctx
        .client
        .disk_create()
        .project(ctx.project_name.clone())
        .body(DiskCreate {
            name: disk_name.clone(),
            description: String::new(),
            disk_backend: DiskBackend::Distributed(DiskSource::Image {
                image_id: ctx.get_silo_image_id("debian11").await?,
                read_only: false,
            }),
            size: ByteCount(2048 * 1024 * 1024),
        })
        .send()
        .await?
        .name
        .clone();

    eprintln!("create instance");
    let instance = ctx
        .client
        .instance_create()
        .project(ctx.project_name.clone())
        .body(InstanceCreate {
            name: generate_name("instance")?,
            description: String::new(),
            hostname: "localshark".parse().unwrap(), // 🦈
            memory: ByteCount(1024 * 1024 * 1024),
            ncpus: InstanceCpuCount(2),
            boot_disk: Some(InstanceDiskAttachment::Attach {
                name: disk_name.clone(),
            }),
            disks: Vec::new(),
            network_interfaces:
                InstanceNetworkInterfaceAttachment::DefaultDualStack,
            external_ips: vec![ExternalIpCreate::Ephemeral {
                pool_selector: PoolSelector::Auto {
                    ip_version: Some(IpVersion::V4),
                },
            }],
            user_data: String::new(),
            ssh_public_keys: Some(vec![oxide_client::types::NameOrId::Name(
                ssh_key_name.clone(),
            )]),
            start: true,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
            cpu_platform: None,
            multicast_groups: Vec::new(),
            enable_jumbo_frames: false,
        })
        .send()
        .await?;

    let ip_addr = ctx
        .client
        .instance_external_ip_list()
        .project(ctx.project_name.clone())
        .instance(instance.name.clone())
        .send()
        .await?
        .items
        .iter()
        .find(|eip| matches!(eip, ExternalIp::Ephemeral { .. }))
        .context("no external IPs")?
        .clone();

    let ExternalIp::Ephemeral { ip: ip_addr, .. } = ip_addr else {
        anyhow::bail!("IP bound to instance was not ephemeral as required.")
    };
    eprintln!("instance external IP: {}", ip_addr);

    // poll serial for login prompt, waiting 5 min max
    // (pulling disk blocks over HTTP is slow)
    eprintln!("waiting for serial console");
    let serial = wait_for_condition(
        || async {
            type Error =
                CondCheckError<oxide_client::Error<oxide_client::types::Error>>;

            let instance_state = ctx
                .client
                .instance_view()
                .project(ctx.project_name.clone())
                .instance(instance.name.clone())
                .send()
                .await?
                .run_state;

            if instance_state == InstanceState::Starting {
                return Err(Error::NotYet { status: None });
            }

            let data = String::from_utf8_lossy(
                &ctx.client
                    .instance_serial_console()
                    .project(ctx.project_name.clone())
                    .instance(instance.name.clone())
                    .from_start(0)
                    .max_bytes(10 * 1024 * 1024)
                    .send()
                    .await?
                    .data,
            )
            .into_owned();
            if data.contains("-----END SSH HOST KEY KEYS-----") {
                Ok(data)
            } else {
                Err(Error::NotYet { status: None })
            }
        },
        &Duration::from_secs(5),
        &Duration::from_secs(300),
    )
    .await?;

    let host_key = serial
        .split_once("-----BEGIN SSH HOST KEY KEYS-----")
        .and_then(|(_, s)| s.split_once("-----END SSH HOST KEY KEYS-----"))
        .and_then(|(lines, _)| {
            lines.trim().lines().find(|line| line.starts_with("ssh-ed25519"))
        })
        .context("failed to get SSH host key from serial console")?;
    eprintln!("host key: {}", host_key);
    let host_key = PublicKey::from_openssh(host_key)?;

    eprintln!("connecting ssh");
    let mut session = russh::client::connect(
        Default::default(),
        (ip_addr, 22),
        SshClient { host_key },
    )
    .await?;
    eprintln!("authenticating ssh");
    ensure!(
        session.authenticate_publickey("debian", key).await?.success(),
        "authentication failed"
    );

    eprintln!("open session");
    let mut channel = session.channel_open_session().await?;
    eprintln!("exec");
    channel.exec(true, "echo 'Hello, Oxide!' | sudo tee /dev/ttyS0").await?;
    while let Some(msg) = channel.wait().await {
        eprintln!("msg: {:?}", msg);
        match msg {
            ChannelMsg::Data { data } => {
                ensure!(
                    data.as_ref() == b"Hello, Oxide!\n",
                    "wrong output: {:?}",
                    data
                );
            }
            ChannelMsg::ExitStatus { exit_status } => {
                ensure!(exit_status == 0, "exit status {}", exit_status);
                break;
            }
            _ => {}
        }
    }

    // sign off
    eprintln!("disconnecting ssh");
    channel.eof().await?;
    session.disconnect(Disconnect::ByApplication, "cya", "en").await?;

    // check that we saw it on the console
    eprintln!("waiting for serial console");

    let data = wait_for_condition(
        || async {
            type Error =
                CondCheckError<oxide_client::Error<oxide_client::types::Error>>;

            let instance_state = ctx
                .client
                .instance_view()
                .project(ctx.project_name.clone())
                .instance(instance.name.clone())
                .send()
                .await?
                .run_state;

            if instance_state == InstanceState::Starting {
                return Err(Error::NotYet { status: None });
            }

            let data = String::from_utf8_lossy(
                &ctx.client
                    .instance_serial_console()
                    .project(ctx.project_name.clone())
                    .instance(instance.name.clone())
                    .most_recent(1024 * 1024)
                    .max_bytes(1024 * 1024)
                    .send()
                    .await
                    .map_err(|_e| Error::NotYet { status: None })?
                    .data,
            )
            .into_owned();
            if data.contains("-----END SSH HOST KEY KEYS-----") {
                Ok(data)
            } else {
                Err(Error::NotYet { status: None })
            }
        },
        &Duration::from_secs(5),
        &Duration::from_secs(300),
    )
    .await?;

    ensure!(
        data.contains("Hello, Oxide!"),
        "string not seen on console\n{}",
        data
    );

    // tear-down
    eprintln!("stopping instance");
    ctx.client
        .instance_stop()
        .project(ctx.project_name.clone())
        .instance(instance.name.clone())
        .send()
        .await?;

    eprintln!("deleting instance");
    wait_for_condition(
        || async {
            ctx.client
                .instance_delete()
                .project(ctx.project_name.clone())
                .instance(instance.name.clone())
                .send()
                .await
                .map_err(|_| CondCheckError::<oxide_client::Error>::NotYet {
                    status: None,
                })
        },
        &Duration::from_secs(1),
        &Duration::from_secs(60),
    )
    .await?;

    eprintln!("deleting disk");
    wait_for_condition(
        || async {
            ctx.client
                .disk_delete()
                .project(ctx.project_name.clone())
                .disk(disk_name.clone())
                .send()
                .await
                .map_err(|_| CondCheckError::<oxide_client::Error>::NotYet {
                    status: None,
                })
        },
        &Duration::from_secs(1),
        &Duration::from_secs(60),
    )
    .await?;

    ctx.cleanup().await
}

#[derive(Debug)]
struct SshClient {
    host_key: PublicKey,
}

impl russh::client::Handler for SshClient {
    type Error = anyhow::Error;

    fn check_server_key(
        &mut self,
        server_public_key: &PublicKey,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        futures::future::ready(Ok(
            self.host_key.key_data() == server_public_key.key_data()
        ))
    }
}

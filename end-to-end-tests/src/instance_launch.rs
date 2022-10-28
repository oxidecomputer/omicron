#![cfg(test)]

use crate::helpers::{ctx::Context, generate_name};
use anyhow::{ensure, Context as _, Result};
use futures::future::Ready;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oxide_client::types::{
    ByteCount, DiskCreate, DiskSource, Distribution, ExternalIpCreate,
    GlobalImageCreate, ImageSource, InstanceCpuCount, InstanceCreate,
    InstanceDiskAttachment, InstanceNetworkInterfaceAttachment, SshKeyCreate,
};
use oxide_client::{
    ClientDisksExt, ClientInstancesExt, ClientSessionExt, ClientSystemExt,
};
use std::sync::Arc;
use std::time::Duration;
use thrussh::{client::Session, ChannelMsg, Disconnect};
use thrussh_keys::key::{KeyPair, PublicKey};
use thrussh_keys::PublicKeyBase64;
use tokio::time::sleep;

#[tokio::test]
async fn instance_launch() -> Result<()> {
    let ctx = Context::new().await?;

    eprintln!("generate SSH key");
    let key =
        Arc::new(KeyPair::generate_ed25519().context("key generation failed")?);
    let public_key_str = format!("ssh-ed25519 {}", key.public_key_base64());
    eprintln!("create SSH key: {}", public_key_str);
    ctx.client
        .session_sshkey_create()
        .body(SshKeyCreate {
            name: generate_name("key")?,
            description: String::new(),
            public_key: public_key_str,
        })
        .send()
        .await?;

    eprintln!("create system image");
    let image_id = ctx
        .client
        .system_image_create()
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

    eprintln!("create disk");
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

    eprintln!("create instance");
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
            start: true,
        })
        .send()
        .await?;

    let ip_addr = ctx
        .client
        .instance_external_ip_list()
        .organization_name(ctx.org_name.clone())
        .project_name(ctx.project_name.clone())
        .instance_name(instance.name.clone())
        .send()
        .await?
        .items
        .first()
        .context("no external IPs")?
        .ip;
    eprintln!("instance external IP: {}", ip_addr);

    // poll serial for login prompt, waiting 5 min max
    // (pulling disk blocks over HTTP is slow)
    eprintln!("waiting for serial console");
    let serial = wait_for_condition(
        || async {
            type Error =
                CondCheckError<oxide_client::Error<oxide_client::types::Error>>;

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
            if data.contains("localshark login:") {
                Ok(data)
            } else {
                Err(Error::NotYet)
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
        .and_then(|line| line.split_whitespace().nth(1))
        .context("failed to get SSH host key from serial console")?;
    eprintln!("host key: ssh-ed25519 {}", host_key);
    let host_key =
        PublicKey::parse(b"ssh-ed25519", &base64::decode(host_key)?)?;

    eprintln!("connecting ssh");
    let mut session = thrussh::client::connect(
        Default::default(),
        (ip_addr, 22),
        SshClient { host_key },
    )
    .await?;
    eprintln!("authenticating ssh");
    ensure!(
        session.authenticate_publickey("debian", key).await?,
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
    sleep(Duration::from_secs(5)).await;
    let data = String::from_utf8_lossy(
        &ctx.client
            .instance_serial_console()
            .organization_name(ctx.org_name.clone())
            .project_name(ctx.project_name.clone())
            .instance_name(instance.name.clone())
            .most_recent(1024 * 1024)
            .max_bytes(1024 * 1024)
            .send()
            .await?
            .data,
    )
    .into_owned();
    ensure!(
        data.contains("Hello, Oxide!"),
        "string not seen on console\n{}",
        data
    );

    // tear-down
    eprintln!("stopping instance");
    ctx.client
        .instance_stop()
        .organization_name(ctx.org_name.clone())
        .project_name(ctx.project_name.clone())
        .instance_name(instance.name.clone())
        .send()
        .await?;

    eprintln!("deleting instance");
    wait_for_condition(
        || async {
            ctx.client
                .instance_delete()
                .organization_name(ctx.org_name.clone())
                .project_name(ctx.project_name.clone())
                .instance_name(instance.name.clone())
                .send()
                .await
                .map_err(|_| CondCheckError::<oxide_client::Error>::NotYet)
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

impl thrussh::client::Handler for SshClient {
    type Error = anyhow::Error;
    type FutureUnit = Ready<Result<(Self, Session)>>;
    type FutureBool = Ready<Result<(Self, bool)>>;

    fn finished_bool(self, b: bool) -> Self::FutureBool {
        futures::future::ready(Ok((self, b)))
    }

    fn finished(self, session: Session) -> Self::FutureUnit {
        futures::future::ready(Ok((self, session)))
    }

    fn check_server_key(
        self,
        server_public_key: &PublicKey,
    ) -> Self::FutureBool {
        let b = &self.host_key == server_public_key;
        self.finished_bool(b)
    }
}

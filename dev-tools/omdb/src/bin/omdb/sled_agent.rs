// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update specific Sleds

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use crate::helpers::CONNECTION_OPTIONS_HEADING;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use base64::Engine;
use bootstrap_agent_lockstep_client::types::ReplicatedNetworkConfig;
use bootstrap_agent_lockstep_client::types::ReplicatedNetworkConfigContents;
use clap::Args;
use clap::Subcommand;
use sled_agent_client::types::NodeStatus;
use sled_agent_client::types::OperatorSwitchZonePolicy;

/// Arguments to the "omdb sled-agent" subcommand
#[derive(Debug, Args)]
pub struct SledAgentArgs {
    /// URL of the Sled internal API
    ///
    /// This is typically port 12345 on a sled's underlay network IP address.
    #[clap(
        long,
        env = "OMDB_SLED_AGENT_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    sled_agent_url: Option<String>,

    /// URL of the sled bootstrap-agent-lockstep API
    ///
    /// This is typically port 8080 on a sled's bootstrap network IP address.
    #[clap(
        long,
        env = "OMDB_BOOTSTRAP_LOCKSTEP_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    bootstrap_lockstep_url: Option<String>,

    #[command(subcommand)]
    command: SledAgentCommands,
}

/// Subcommands for the "omdb sled-agent" subcommand
#[derive(Debug, Subcommand)]
enum SledAgentCommands {
    /// print information about zones
    #[clap(subcommand)]
    Zones(ZoneCommands),

    /// control the switch zone policy
    #[clap(subcommand)]
    SwitchZonePolicy(SwitchZonePolicyCommands),

    /// print information about the replicated network config
    #[clap(subcommand)]
    NetworkConfig(NetworkConfigCommands),

    /// print information about the local bootstore node
    #[clap(subcommand)]
    Bootstore(BootstoreCommands),

    /// print information about the local trust quorum node
    #[clap(subcommand)]
    TrustQuorum(TrustQuorumCommands),
}

#[derive(Debug, Subcommand)]
enum ZoneCommands {
    /// print list of all running control plane zones
    List,
}

#[derive(Debug, Subcommand)]
enum SwitchZonePolicyCommands {
    /// get the current policy
    Get,

    /// enable starting the switch zone if a switch is present
    Enable,

    /// disable the switch zone
    ///
    /// This is extremely dangerous! If used incorrectly, it can render the rack
    /// inaccessible.
    DangerDangerDisable,
}

#[derive(Debug, Subcommand)]
enum NetworkConfigCommands {
    /// show the current contents of the replicated network config
    Show,
}

#[derive(Debug, Subcommand)]
enum BootstoreCommands {
    /// show the internal state of the local bootstore node
    Status,
}

#[derive(Debug, Subcommand)]
enum TrustQuorumCommands {
    /// show the status of the local trust quorum node
    Status,
    /// show the status of a trust quorum node via proxy
    ProxyStatus(TrustQuorumProxyStatusArgs),
}

#[derive(Debug, Args)]
struct TrustQuorumProxyStatusArgs {
    /// Oxide part number of the target sled
    #[clap(long)]
    part_number: String,
    /// Serial number of the target sled
    #[clap(long)]
    serial_number: String,
}

impl SledAgentArgs {
    /// Run a `omdb sled-agent` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        // We require either a sled_agent_url or a bootstrap_lockstep_url, but
        // we don't know which one until we match on `self.command` below. Wrap
        // the conversion from URL-to-client in a closure so we only bail out if
        // we're missing a URL argument we actually need.

        // Helper to create a sled-agent client.
        let make_sa_client = || -> anyhow::Result<_> {
            let Some(sled_agent_url) = &self.sled_agent_url else {
                bail!(
                    "sled URL must be specified with --sled-agent-url or \
                     OMDB_SLED_AGENT_URL"
                );
            };
            Ok(sled_agent_client::Client::new(sled_agent_url, log.clone()))
        };

        // Helper to create a bootstrap-agent-lockstep client.
        let make_ba_lockstep_client = || -> anyhow::Result<_> {
            let Some(bootstrap_lockstep_url) = &self.bootstrap_lockstep_url
            else {
                bail!(
                    "bootstrap lockstep URL must be specified with \
                     --bootstrap-lockstep-url or OMDB_BOOTSTRAP_LOCKSTEP_URL"
                );
            };
            Ok(bootstrap_agent_lockstep_client::Client::new(
                bootstrap_lockstep_url,
                log.clone(),
            ))
        };

        match &self.command {
            SledAgentCommands::Zones(ZoneCommands::List) => {
                cmd_zones_list(&make_sa_client()?).await
            }
            SledAgentCommands::SwitchZonePolicy(
                SwitchZonePolicyCommands::Get,
            ) => cmd_switch_zone_policy_get(&make_sa_client()?).await,
            SledAgentCommands::SwitchZonePolicy(
                SwitchZonePolicyCommands::Enable,
            ) => {
                let token = omdb.check_allow_destructive()?;
                cmd_switch_zone_policy_put(
                    &make_sa_client()?,
                    OperatorSwitchZonePolicy::StartIfSwitchPresent,
                    token,
                )
                .await
            }
            SledAgentCommands::SwitchZonePolicy(
                SwitchZonePolicyCommands::DangerDangerDisable,
            ) => {
                let token = omdb.check_allow_destructive()?;
                cmd_switch_zone_policy_put(
                    &make_sa_client()?,
                    OperatorSwitchZonePolicy::StopDespiteSwitchPresence,
                    token,
                )
                .await
            }
            SledAgentCommands::NetworkConfig(NetworkConfigCommands::Show) => {
                cmd_network_config_show(&make_ba_lockstep_client()?).await
            }
            SledAgentCommands::Bootstore(BootstoreCommands::Status) => {
                cmd_bootstore_status(&make_sa_client()?).await
            }
            SledAgentCommands::TrustQuorum(TrustQuorumCommands::Status) => {
                cmd_trust_quorum_status(&make_sa_client()?).await
            }
            SledAgentCommands::TrustQuorum(
                TrustQuorumCommands::ProxyStatus(args),
            ) => cmd_trust_quorum_proxy_status(&make_sa_client()?, args).await,
        }
    }
}

/// Runs `omdb sled-agent zones list`
async fn cmd_zones_list(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let response = client.zones_list().await.context("listing zones")?;
    let zones = response.into_inner();
    let zones: Vec<_> = zones.into_iter().collect();

    println!("zones:");
    if zones.is_empty() {
        println!("    <none>");
    }
    for zone in &zones {
        println!("    {:?}", zone);
    }

    Ok(())
}

/// Runs `omdb sled-agent switch-zone-policy get`
async fn cmd_switch_zone_policy_get(
    client: &sled_agent_client::Client,
) -> anyhow::Result<()> {
    let response = client
        .debug_operator_switch_zone_policy_get()
        .await
        .context("getting policy")?;
    let policy = response.into_inner();

    match policy {
        OperatorSwitchZonePolicy::StartIfSwitchPresent => {
            println!("switch zone will start if a switch is present (default)");
        }
        OperatorSwitchZonePolicy::StopDespiteSwitchPresence => {
            println!(
                "switch zone DISABLED and will not start even if a \
                 switch is present"
            );
        }
    }

    Ok(())
}

/// Runs `omdb sled-agent switch-zone-policy {enable,danger-danger-disable}`
async fn cmd_switch_zone_policy_put(
    client: &sled_agent_client::Client,
    policy: OperatorSwitchZonePolicy,
    _destruction_token: DestructiveOperationToken,
) -> anyhow::Result<()> {
    client
        .debug_operator_switch_zone_policy_put(policy)
        .await
        .context("setting policy")?;
    cmd_switch_zone_policy_get(client).await
}

/// Runs `omdb sled-agent network-config show`
async fn cmd_network_config_show(
    client: &bootstrap_agent_lockstep_client::Client,
) -> anyhow::Result<()> {
    let ReplicatedNetworkConfig { contents } = client
        .network_config_contents_for_debug()
        .await
        .context("failed to fetch network config contents")?
        .into_inner();
    let Some(contents) = contents else {
        println!(
            "no network contents available yet - \
             this is normal if RSS has not yet started"
        );
        return Ok(());
    };

    let ReplicatedNetworkConfigContents { base64_blob, generation } = contents;
    println!("network config generation: {generation}");

    let blob = {
        let mut output = Vec::new();
        match base64::engine::general_purpose::STANDARD
            .decode_vec(&base64_blob, &mut output)
        {
            Ok(()) => output,
            Err(err) => {
                println!("raw data (expected base64): {base64_blob}");
                return Err(
                    anyhow!(err).context("failed to decode base64 blob")
                );
            }
        }
    };

    let decoded: serde_json::Value = match serde_json::from_slice(&blob) {
        Ok(value) => value,
        Err(err) => {
            println!(
                "raw blob (expected JSON): {}",
                String::from_utf8_lossy(&blob)
            );
            return Err(anyhow!(err).context("failed to decode blob as JSON"));
        }
    };

    println!("network config contents:");
    println!("{decoded:#}"); // `:#` format induces pretty-printing

    Ok(())
}

/// Runs `omdb sled-agent bootstore status`
async fn cmd_bootstore_status(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let status = client.bootstore_status().await.context("bootstore status")?;
    println!("fsm ledger generation: {}", status.fsm_ledger_generation);
    println!(
        "network config ledger generation: {:?}",
        status.network_config_ledger_generation
    );
    println!("fsm state: {}", status.fsm_state);
    println!("peers (found by ddmd):");
    if status.peers.is_empty() {
        println!("    <none>");
    }
    for peer in status.peers.iter() {
        println!("    {peer}");
    }
    println!("established connections:");
    if status.established_connections.is_empty() {
        println!("    <none>");
    }
    for c in status.established_connections.iter() {
        println!("     {:?} : {}", c.baseboard, c.addr);
    }
    println!("accepted connections:");
    if status.accepted_connections.is_empty() {
        println!("    <none>");
    }
    for addr in status.accepted_connections.iter() {
        println!("    {addr}");
    }
    println!("negotiating connections:");
    if status.negotiating_connections.is_empty() {
        println!("    <none>");
    }
    for addr in status.negotiating_connections.iter() {
        println!("    {addr}");
    }

    Ok(())
}

/// Runs `omdb sled-agent trust-quorum status`
async fn cmd_trust_quorum_status(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let status = client
        .trust_quorum_status()
        .await
        .context("trust quorum status")?
        .into_inner();

    print_trust_quorum_status(status);

    Ok(())
}

/// Runs `omdb sled-agent trust-quorum proxy-status`
async fn cmd_trust_quorum_proxy_status(
    client: &sled_agent_client::Client,
    args: &TrustQuorumProxyStatusArgs,
) -> Result<(), anyhow::Error> {
    let status = client
        .trust_quorum_proxy_status(&args.part_number, &args.serial_number)
        .await
        .context("trust quorum proxy status")?
        .into_inner();

    print_trust_quorum_status(status);

    Ok(())
}

fn print_trust_quorum_status(status: NodeStatus) {
    println!("connected peers:");
    if status.connected_peers.is_empty() {
        println!("    <none>");
    }
    for peer in status.connected_peers.iter() {
        println!("    {peer}");
    }

    println!("alarms:");
    if status.alarms.is_empty() {
        println!("    <none>");
    }
    for alarm in status.alarms.iter() {
        println!("    {:?}", alarm);
    }

    println!("persistent state:");
    println!("    has lrtq share: {}", status.persistent_state.has_lrtq_share);
    println!("    configs: {:?}", status.persistent_state.configs);
    println!("    shares: {:?}", status.persistent_state.shares);
    println!("    commits: {:?}", status.persistent_state.commits);
    println!("    expunged: {:?}", status.persistent_state.expunged);

    println!("proxied requests: {}", status.proxied_requests);
}

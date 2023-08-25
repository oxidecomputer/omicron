// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use display_error_chain::DisplayErrorChain;
use dpd_client::types::LinkCreate;
use dpd_client::types::LinkId;
use dpd_client::types::LinkSettings;
use dpd_client::types::LinkState;
use dpd_client::types::PortFec as DpdPortFec;
use dpd_client::types::PortId;
use dpd_client::types::PortSettings;
use dpd_client::types::PortSpeed as DpdPortSpeed;
use dpd_client::types::RouteSettingsV4;
use dpd_client::Client as DpdClient;
use dpd_client::ClientState as DpdClientState;
use illumos_utils::zone::SVCCFG;
use illumos_utils::PFEXEC;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::api::internal::shared::PortFec as OmicronPortFec;
use omicron_common::api::internal::shared::PortSpeed as OmicronPortSpeed;
use omicron_common::api::internal::shared::RackNetworkConfig;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_common::api::internal::shared::UplinkConfig;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use slog::error;
use slog::o;
use slog::Logger;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use thiserror::Error;
use tokio::process::Command;
use tokio::sync::mpsc;
use trust_dns_resolver::config::NameServerConfigGroup;
use trust_dns_resolver::config::ResolverConfig;
use trust_dns_resolver::config::ResolverOpts;
use trust_dns_resolver::error::ResolveErrorKind;
use trust_dns_resolver::AsyncResolver;
use update_engine::StepSpec;

const DNS_PORT: u16 = 53;

// DNS name we'll use to query DNS servers. We don't care to actually get a
// useful answer back (e.g., if we're talking to a DNS server that is not
// connected to the internet); _any_ response is sufficient to check that our
// connectivity to the server is ok.
const DNS_NAME_TO_QUERY: &str = "oxide.computer.";

const UPLINK_SMF_NAME: &str = "svc:/oxide/uplink";
const UPLINK_DEFAULT_SMF_NAME: &str = "svc:/oxide/uplink:default";

const WICKETD_TAG: &str = "wicketd-preflight";

const CHRONYD: &str = "/usr/sbin/chronyd";
const IPADM: &str = "/usr/sbin/ipadm";
const ROUTE: &str = "/usr/sbin/route";

const DPD_DEFAULT_IPV4_CIDR: &str = "0.0.0.0/0";

pub(super) async fn run_local_uplink_preflight_check(
    network_config: RackNetworkConfig,
    dns_servers: Vec<IpAddr>,
    ntp_servers: Vec<String>,
    our_switch_location: SwitchLocation,
    dns_name_to_query: Option<String>,
    event_buffer: Arc<Mutex<Option<EventBuffer>>>,
    log: &Logger,
) {
    let dpd_client = DpdClient::new(
        &format!("http://[::1]:{DENDRITE_PORT}"),
        DpdClientState {
            tag: WICKETD_TAG.to_string(),
            log: log.new(o!("component" => "DpdClient")),
        },
    );

    // Build the update executor.
    let (sender, mut receiver) = mpsc::channel(128);
    let mut engine = UpdateEngine::new(log, sender);

    for uplink in network_config
        .uplinks
        .iter()
        .filter(|uplink| uplink.switch == our_switch_location)
    {
        add_steps_for_single_local_uplink_preflight_check(
            &mut engine,
            &dpd_client,
            uplink,
            &dns_servers,
            &ntp_servers,
            dns_name_to_query.as_deref(),
        );
    }

    // Spawn a task to accept all events from the executing engine.
    let event_receiving_task = tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            if let Some(event_buffer) = &mut *event_buffer.lock().unwrap() {
                event_buffer.add_event(event);
            }
        }
    });

    // Execute the update engine.
    match engine.execute().await {
        Ok(_cx) => (),
        Err(err) => {
            error!(
                log, "uplink preflight check failed";
                "err" => %DisplayErrorChain::new(&err),
            );
        }
    }

    // Wait for all events to be received and written to the update log.
    event_receiving_task.await.expect("event receiving task panicked");
}

fn add_steps_for_single_local_uplink_preflight_check<'a>(
    engine: &mut UpdateEngine<'a>,
    dpd_client: &'a DpdClient,
    uplink: &'a UplinkConfig,
    dns_servers: &'a [IpAddr],
    ntp_servers: &'a [String],
    dns_name_to_query: Option<&'a str>,
) {
    // Total time we're willing to wait for an L1 link up.
    const L1_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

    // Time between polls to dpd waiting for L1 link up.
    const L1_RETRY_DELAY: Duration = Duration::from_millis(500);

    // Total time we're willing to wait for the `uplink` service to create an IP
    // address after we `svccfg refresh` it.
    const UPLINK_SVC_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

    // Time between polling `ipadm` to see if the `uplink` service has created
    // the IP address we requested.
    const UPLINK_SVC_RETRY_DELAY: Duration = Duration::from_millis(500);

    // Timeout we give to chronyd during the NTP check, in seconds.
    const CHRONYD_CHECK_TIMEOUT_SECS: &str = "30";

    let registrar = engine.for_component(uplink.uplink_port.clone());

    let prev_step = registrar
        .new_step(
            UplinkPreflightStepId::ConfigureSwitch,
            "Configuring switch",
            |_cx| async {
                // Check that the port name is valid and that it has no links
                // configured already.
                let port_id = PortId::from_str(&uplink.uplink_port)
                    .map_err(UplinkPreflightTerminalError::InvalidPortName)?;
                let links = dpd_client
                    .link_list(&port_id)
                    .await
                    .map_err(UplinkPreflightTerminalError::GetCurrentConfig)?
                    .into_inner();
                if !links.is_empty() {
                    return Err(
                        UplinkPreflightTerminalError::UplinkAlreadyConfigured,
                    );
                }

                // We must use `port_settings_apply` to create and configure the
                // link + gateway in one go instead of using `link_create` +
                // `link_enabled_set` + `link_ipv4_create` + `route_ipv4_create`
                // due to https://github.com/oxidecomputer/dendrite/issues/588.
                // That means we have to conjure up the link ID that we want;
                // because we just checked that this port has no config, we
                // will use link id 0.
                let link_id = LinkId(0);
                let port_settings = build_port_settings(uplink, &link_id);

                // Create and configure the link.
                match dpd_client
                    .port_settings_apply(&port_id, &port_settings)
                    .await
                {
                    Ok(_response) => {
                        let metadata = vec![format!(
                            "configured {}/{}: ip {}, gateway {}",
                            *port_id,
                            link_id.0,
                            uplink.uplink_cidr,
                            uplink.gateway_ip
                        )];
                        StepSuccess::new((port_id, link_id))
                            .with_metadata(metadata)
                            .into()
                    }
                    Err(err) => {
                        Err(UplinkPreflightTerminalError::ConfigurePort {
                            port_id,
                            err,
                        })
                    }
                }
            },
        )
        .register();

    // -----------------------------------------------------------
    // We have now created a link. From this point on we no longer
    // return terminal errors: instead, if we encounter an error,
    // we'll skip to the cleanup step, which must remove the link we
    // just created, as well as any additional cleanup required from
    // subsequent successful steps. If something outside our control
    // goes wrong (e.g., wicketd crashes), support intervention will
    // be required to clean up any leftover state.
    // -----------------------------------------------------------

    let prev_step = registrar
        .new_step(
            UplinkPreflightStepId::WaitForL1Link,
            "Waiting for L1 link up",
            move |cx| async move {
                let (port_id, link_id) = prev_step.into_value(cx.token()).await;

                // Wait for link up.
                let start_waiting_l1 = Instant::now();
                loop {
                    let not_up_message = match dpd_client
                        .link_get(&port_id, &link_id)
                        .await
                        .map(|response| response.into_inner())
                    {
                        Ok(link) => match link.link_state {
                            LinkState::Up => {
                                return StepSuccess::new(Ok(L1Success {
                                    port_id,
                                    link_id,
                                }))
                                .into();
                            }
                            state @ (LinkState::Down
                            | LinkState::Unknown
                            | LinkState::Faulted(_)) => {
                                format!("link state: {state:?}")
                            }
                        },
                        Err(err) => {
                            format!(
                                "failed to get link state: {}",
                                DisplayErrorChain::new(&err)
                            )
                        }
                    };

                    if start_waiting_l1.elapsed() < L1_WAIT_TIMEOUT {
                        cx.send_progress(StepProgress::Progress {
                            progress: None,
                            metadata: not_up_message,
                        })
                        .await;
                        tokio::time::sleep(L1_RETRY_DELAY).await;
                    } else {
                        return StepWarning::new(
                            Err(L1Failure::WaitForLinkUp(port_id, link_id)),
                            not_up_message,
                        )
                        .into();
                    }
                }
            },
        )
        .register();

    let prev_step = registrar
        .new_step(
            UplinkPreflightStepId::ConfigureAddress,
            "Configuring IP address in host OS",
            move |cx| async move {
                let level1 = match prev_step.into_value(cx.token()).await {
                    Ok(level1) => level1,
                    Err(failure) => {
                        return StepSkipped::new(
                            Err(L2Failure::L1(failure)),
                            "link not up due to earlier failure",
                        )
                        .into()
                    }
                };

                // Tell the `uplink` service about the IP address we created on
                // the switch when configuring the uplink.
                let uplink_property =
                    UplinkProperty(format!("uplinks/{}_0", uplink.uplink_port));
                let uplink_cidr = uplink.uplink_cidr.to_string();

                if let Err(err) = execute_command(&[
                    SVCCFG,
                    "-s",
                    UPLINK_SMF_NAME,
                    "addpropvalue",
                    &uplink_property.0,
                    "astring:",
                    &uplink_cidr,
                ])
                .await
                {
                    return StepWarning::new(
                        Err(L2Failure::UplinkAddProperty(level1)),
                        format!("could not add uplink property: {err}"),
                    )
                    .into();
                };

                if let Err(err) = execute_command(&[
                    SVCCFG,
                    "-s",
                    UPLINK_DEFAULT_SMF_NAME,
                    "refresh",
                ])
                .await
                {
                    return StepWarning::new(
                        Err(L2Failure::UplinkRefresh(level1, uplink_property)),
                        format!("could not add uplink property: {err}"),
                    )
                    .into();
                };

                // Wait for the `uplink` service to create the IP address.
                let start_waiting_addr = Instant::now();
                'waiting_for_addr: loop {
                    let ipadm_out = match execute_command(&[
                        IPADM,
                        "show-addr",
                        "-p",
                        "-o",
                        "addr",
                    ])
                    .await
                    {
                        Ok(stdout) => stdout,
                        Err(err) => {
                            return StepWarning::new(
                                Err(L2Failure::RunIpadm(
                                    level1,
                                    uplink_property,
                                )),
                                format!("failed running ipadm: {err}"),
                            )
                            .into();
                        }
                    };

                    for line in ipadm_out.split('\n') {
                        if line == uplink_cidr {
                            break 'waiting_for_addr;
                        }
                    }

                    // We did not find `uplink_cidr` in the output of ipadm;
                    // sleep a bit and try again, unless we've been waiting too
                    // long already.
                    if start_waiting_addr.elapsed() < UPLINK_SVC_WAIT_TIMEOUT {
                        tokio::time::sleep(UPLINK_SVC_RETRY_DELAY).await;
                    } else {
                        return StepWarning::new(
                            Err(L2Failure::WaitingForHostAddr(
                                level1,
                                uplink_property,
                            )),
                            format!(
                                "timed out waiting for `uplink` to \
                                 create {uplink_cidr}"
                            ),
                        )
                        .into();
                    }
                }

                let metadata =
                    vec![format!("configured {}", uplink_property.0)];
                StepSuccess::new(Ok(L2Success { level1, uplink_property }))
                    .with_metadata(metadata)
                    .into()
            },
        )
        .register();

    let prev_step = registrar
        .new_step(
            UplinkPreflightStepId::ConfigureRouting,
            "Configuring routing in host OS",
            move |cx| async move {
                let level2 = match prev_step.into_value(cx.token()).await {
                    Ok(level2) => level2,
                    Err(failure) => {
                        return StepSkipped::new(
                            Err(RoutingFailure::L2(failure)),
                            "addresses not configured due to earlier failure",
                        )
                        .into();
                    }
                };

                // Add the gateway as the default route in illumos.
                if let Err(err) = execute_command(&[
                    ROUTE,
                    "add",
                    "-inet",
                    "default",
                    &uplink.gateway_ip.to_string(),
                ])
                .await
                {
                    return StepWarning::new(
                        Err(RoutingFailure::HostDefaultRoute(level2)),
                        format!("could not add default route: {err}"),
                    )
                    .into();
                };

                StepSuccess::new(Ok(RoutingSuccess { level2 }))
                    .with_metadata(vec![format!(
                        "added default route to {}",
                        uplink.gateway_ip
                    )])
                    .into()
            },
        )
        .register();

    // -----------------------------------------------------------
    // All switch and OS configuration is complete; we now do the _actual_
    // checks we care about: can we contact the DNS and NTP servers listed in
    // the RSS config?
    // -----------------------------------------------------------

    let prev_step = registrar
        .new_step(
            UplinkPreflightStepId::CheckExternalDnsConnectivity,
            "Checking for external DNS connectivity",
            move |cx| async move {
                let level2 = match prev_step.into_value(cx.token()).await {
                    Ok(level2) => level2,
                    Err(err) => {
                        return StepSkipped::new(
                            Err(err),
                            "skipped due to previous failure",
                        )
                        .into();
                    }
                };

                let step_impl = DnsLookupStep::new(
                    dns_servers,
                    ntp_servers,
                    dns_name_to_query,
                );
                let ntp_ips_result = step_impl.run(&cx).await;

                Ok(ntp_ips_result.map(|ntp_ips| Ok((level2, ntp_ips))))
            },
        )
        .register();

    let prev_step = registrar
        .new_step(
            UplinkPreflightStepId::CheckExternalNtpConnectivity,
            "Checking for external NTP connectivity",
            move |cx| async move {
                let (level2, ntp_ips) =
                    match prev_step.into_value(cx.token()).await {
                        Ok((level2, ntp_ips)) => (level2, ntp_ips),
                        Err(err) => {
                            return StepSkipped::new(
                                Err(err),
                                "skipped due to previous failure",
                            )
                            .into();
                        }
                    };

                if ntp_ips.is_empty() {
                    return StepSkipped::new(
                        Ok(level2),
                        "no NTP IP addresses known (DNS failure?)",
                    )
                    .into();
                }

                let mut messages = vec![];
                let mut warnings = vec![];

                for ntp_ip in ntp_ips {
                    // The directive here `pool ...` should match what we
                    // use in `chrony.conf.boundary` for boundary NTP zones.
                    let directive = format!(
                        "pool {ntp_ip} iburst maxdelay 0.1 maxsources 16"
                    );
                    let output = match execute_command_ignoring_status(&[
                        CHRONYD,
                        "-t",
                        CHRONYD_CHECK_TIMEOUT_SECS,
                        "-Q",
                        &directive,
                    ])
                    .await
                    {
                        Ok(output) => output,
                        Err(err) => {
                            // We're ignoring exit statuses from chronyd itself,
                            // so this can only fail if we can't execute it at
                            // all. Break out of the loop: changing server IPs
                            // is not going to make it executable.
                            warnings.push(format!(
                                "failed to execute chronyd: {err}"
                            ));
                            break;
                        }
                    };

                    let mut parsed_result = false;
                    for line in output.stderr.split('\n') {
                        if line.contains("Timeout reached") {
                            parsed_result = true;
                            warnings.push(format!(
                                "failed to contact NTP server {ntp_ip}"
                            ));
                            break;
                        } else if line.contains("System clock wrong by") {
                            parsed_result = true;
                            messages.push(format!(
                                "successfully contacted NTP server {ntp_ip}"
                            ));
                            break;
                        }
                    }

                    if !parsed_result {
                        warnings.push(format!(
                            "failed to determine success or failure for \
                             {ntp_ip} from chronyd output: stdout={} stderr={}",
                            output.stdout, output.stderr
                        ));
                    }
                }

                if warnings.is_empty() {
                    StepSuccess::new(Ok(level2)).with_metadata(messages).into()
                } else {
                    StepWarning::new(Ok(level2), warnings.join("\n"))
                        .with_metadata(messages)
                        .into()
                }
            },
        )
        .register();

    // -----------------------------------------------------------
    // Our checks are complete; we now need to _undo_ all of the
    // setup we did, in reverse order. Any failure from this point
    // on is bad news: we'll leave things partially configured, so
    // we'll raise the issue to the operator that support needs to
    // get involved to help clean up.
    // -----------------------------------------------------------

    let prev_step = registrar
        .new_step(
            UplinkPreflightStepId::CleanupRouting,
            "Cleaning up host OS routing configuration",
            move |cx| async move {
                let remove_host_route: bool;
                let level2 = match prev_step.into_value(cx.token()).await {
                    Ok(RoutingSuccess { level2 }) => {
                        remove_host_route = true;
                        level2
                    }
                    Err(RoutingFailure::HostDefaultRoute(level2)) => {
                        // We succeeded in setting up the dpd route but failed
                        // setting up the host route; only remove the dpd route.
                        remove_host_route = false;
                        level2
                    }
                    Err(RoutingFailure::L2(level2_failure)) => {
                        return StepSkipped::new(
                            Err(level2_failure),
                            "routing was not configured due to earlier failure",
                        )
                        .into();
                    }
                };

                if remove_host_route {
                    execute_command(&[
                        ROUTE,
                        "delete",
                        "-inet",
                        "default",
                        &uplink.gateway_ip.to_string(),
                    ])
                    .await
                    .map_err(|err| {
                        UplinkPreflightTerminalError::RemoveHostRoute {
                            err,
                            gateway_ip: uplink.gateway_ip,
                        }
                    })?;
                }

                StepSuccess::new(Ok(level2)).into()
            },
        )
        .register();

    let prev_step =
        registrar
            .new_step(
                UplinkPreflightStepId::CleanupAddress,
                "Cleaning up host OS IP address configuration",
                move |cx| async move {
                    let mut uplink_property_to_remove = None;
                    let level1 = match prev_step.into_value(cx.token()).await {
                        Ok(L2Success { level1, uplink_property }) => {
                            uplink_property_to_remove = Some(uplink_property);
                            level1
                        }
                        // These error cases all occurred after we reconfigured
                        // the uplink SMF properties, so we need to remove the
                        // property.
                        Err(
                            L2Failure::UplinkRefresh(level1, uplink_property)
                            | L2Failure::RunIpadm(level1, uplink_property)
                            | L2Failure::WaitingForHostAddr(
                                level1,
                                uplink_property,
                            ),
                        ) => {
                            uplink_property_to_remove = Some(uplink_property);
                            level1
                        }
                        Err(L2Failure::UplinkAddProperty(level1)) => {
                            // We failed to add the property to `uplink`'s smf
                            // config, so we leave `uplink_property_to_remove`
                            // None.
                            level1
                        }
                        Err(L2Failure::L1(level1_failure)) => {
                            return StepSkipped::new(
                                Err(level1_failure),
                                "addresses were not configured due to \
                                 earlier failure",
                            )
                            .into();
                        }
                    };

                    if let Some(uplink_property) = uplink_property_to_remove {
                        execute_command(&[
                            SVCCFG,
                            "-s",
                            UPLINK_SMF_NAME,
                            "delprop",
                            &uplink_property.0,
                        ])
                        .await
                        .map_err(|err| {
                            UplinkPreflightTerminalError::RemoveSmfProperty {
                                property: uplink_property.0,
                                err,
                            }
                        })?;

                        execute_command(&[
                            SVCCFG,
                            "-s",
                            UPLINK_DEFAULT_SMF_NAME,
                            "refresh",
                        ])
                        .await
                        .map_err(
                            UplinkPreflightTerminalError::RefreshUplinkSmf,
                        )?;
                    }

                    StepSuccess::new(Ok(level1)).into()
                },
            )
            .register();

    registrar
        .new_step(
            UplinkPreflightStepId::CleanupL1,
            "Cleaning up switch configuration",
            move |cx| async move {
                let (port_id, _link_id) =
                    match prev_step.into_value(cx.token()).await {
                        Ok(L1Success { port_id, link_id })
                        | Err(L1Failure::WaitForLinkUp(port_id, link_id)) => {
                            (port_id, link_id)
                        }
                    };

                dpd_client
                    .port_settings_apply(
                        &port_id,
                        &PortSettings {
                            tag: WICKETD_TAG.to_string(),
                            links: HashMap::new(),
                            v4_routes: HashMap::new(),
                            v6_routes: HashMap::new(),
                        },
                    )
                    .await
                    .map_err(|err| {
                        UplinkPreflightTerminalError::UnconfigurePort {
                            port_id,
                            err,
                        }
                    })?;

                StepSuccess::new(()).into()
            },
        )
        .register();
}

fn build_port_settings(
    uplink: &UplinkConfig,
    link_id: &LinkId,
) -> PortSettings {
    // Map from omicron_common types to dpd_client types
    let fec = match uplink.uplink_port_fec {
        OmicronPortFec::Firecode => DpdPortFec::Firecode,
        OmicronPortFec::None => DpdPortFec::None,
        OmicronPortFec::Rs => DpdPortFec::Rs,
    };
    let speed = match uplink.uplink_port_speed {
        OmicronPortSpeed::Speed0G => DpdPortSpeed::Speed0G,
        OmicronPortSpeed::Speed1G => DpdPortSpeed::Speed1G,
        OmicronPortSpeed::Speed10G => DpdPortSpeed::Speed10G,
        OmicronPortSpeed::Speed25G => DpdPortSpeed::Speed25G,
        OmicronPortSpeed::Speed40G => DpdPortSpeed::Speed40G,
        OmicronPortSpeed::Speed50G => DpdPortSpeed::Speed50G,
        OmicronPortSpeed::Speed100G => DpdPortSpeed::Speed100G,
        OmicronPortSpeed::Speed200G => DpdPortSpeed::Speed200G,
        OmicronPortSpeed::Speed400G => DpdPortSpeed::Speed400G,
    };

    let mut port_settings = PortSettings {
        tag: WICKETD_TAG.to_string(),
        links: HashMap::new(),
        v4_routes: HashMap::new(),
        v6_routes: HashMap::new(),
    };

    port_settings.links.insert(
        link_id.to_string(),
        LinkSettings {
            addrs: vec![IpAddr::V4(uplink.uplink_cidr.ip())],
            params: LinkCreate {
                // TODO we should take these parameters too
                // https://github.com/oxidecomputer/omicron/issues/3061
                autoneg: false,
                kr: false,
                fec,
                speed,
            },
        },
    );

    port_settings.v4_routes.insert(
        DPD_DEFAULT_IPV4_CIDR.parse().unwrap(),
        RouteSettingsV4 {
            link_id: link_id.0,
            nexthop: Some(uplink.gateway_ip),
            vid: uplink.uplink_vid,
        },
    );

    port_settings
}

async fn execute_command(args: &[&str]) -> Result<String, String> {
    let mut command = Command::new(PFEXEC);
    command.env_clear().args(args);
    let output = command
        .output()
        .await
        .map_err(|err| format!("failed to execute command: {err}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);

    if output.status.success() {
        Ok(stdout.to_string())
    } else {
        Err(format!(
            "command failed (status {}); stdout={stdout:?} stderr={:?}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        ))
    }
}

struct CommandOutput {
    stdout: String,
    stderr: String,
}

async fn execute_command_ignoring_status(
    args: &[&str],
) -> Result<CommandOutput, String> {
    let mut command = Command::new(PFEXEC);
    command.env_clear().args(args);
    let output = command
        .output()
        .await
        .map_err(|err| format!("failed to execute command: {err}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    Ok(CommandOutput { stdout: stdout.to_string(), stderr: stderr.to_string() })
}

enum L1Failure {
    WaitForLinkUp(PortId, LinkId),
}

struct L1Success {
    port_id: PortId,
    link_id: LinkId,
}

struct UplinkProperty(String);

enum L2Failure {
    L1(L1Failure),
    UplinkAddProperty(L1Success),
    UplinkRefresh(L1Success, UplinkProperty),
    RunIpadm(L1Success, UplinkProperty),
    WaitingForHostAddr(L1Success, UplinkProperty),
}

struct L2Success {
    level1: L1Success,
    uplink_property: UplinkProperty,
}

enum RoutingFailure {
    L2(L2Failure),
    HostDefaultRoute(L2Success),
}

struct RoutingSuccess {
    level2: L2Success,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "id", rename_all = "snake_case")]
pub(super) enum UplinkPreflightStepId {
    ConfigureSwitch,
    WaitForL1Link,
    ConfigureAddress,
    ConfigureRouting,
    CheckExternalDnsConnectivity,
    CheckExternalNtpConnectivity,
    CleanupRouting,
    CleanupAddress,
    CleanupL1,
}

type DpdError = dpd_client::Error<dpd_client::types::Error>;

#[derive(Debug, Error)]
pub(crate) enum UplinkPreflightTerminalError {
    #[error("invalid port name: {0}")]
    InvalidPortName(&'static str),
    #[error("failed to connect to dpd to check for current configuration")]
    GetCurrentConfig(#[source] DpdError),
    #[error("uplink already configured - is rack already initialized?")]
    UplinkAlreadyConfigured,
    #[error("failed to create port {port_id:?}")]
    ConfigurePort {
        #[source]
        err: DpdError,
        port_id: PortId,
    },
    #[error("failed to remove host OS route to gateway {gateway_ip}: {err}")]
    RemoveHostRoute { err: String, gateway_ip: Ipv4Addr },
    #[error("failed to remove uplink SMF property {property:?}: {err}")]
    RemoveSmfProperty { property: String, err: String },
    #[error("failed to refresh uplink service config: {0}")]
    RefreshUplinkSmf(String),
    #[error("failed to clear settings for port {port_id:?}")]
    UnconfigurePort {
        #[source]
        err: DpdError,
        port_id: PortId,
    },
}

impl update_engine::AsError for UplinkPreflightTerminalError {
    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self
    }
}

#[derive(JsonSchema)]
pub(super) enum UplinkPreflightCheckSpec {}

impl StepSpec for UplinkPreflightCheckSpec {
    type Component = String;
    type StepId = UplinkPreflightStepId;
    type StepMetadata = ();
    type ProgressMetadata = String;
    type CompletionMetadata = Vec<String>;
    type SkippedMetadata = ();
    type Error = UplinkPreflightTerminalError;
}

update_engine::define_update_engine!(pub(super) UplinkPreflightCheckSpec);

// If we have been given DNS names for the NTP servers, this step serves a dual
// purpose: check connectivity to DNS servers, and use them to resolve the NTP
// servers. If we've been given IPs for the NTP servers, we only need to check
// connectivity to the DNS servers, and will query for a stock DNS name instead.
struct DnsLookupStep<'a> {
    dns_servers: &'a [IpAddr],
    ntp_names_to_resolve: Vec<&'a str>,
    dns_name_to_query: Option<&'a str>,

    // Usable output of this step: IP addrs of the NTP servers to use.
    ntp_ips: BTreeSet<IpAddr>,
}

impl<'a> DnsLookupStep<'a> {
    fn new(
        dns_servers: &'a [IpAddr],
        ntp_servers: &'a [String],
        dns_name_to_query: Option<&'a str>,
    ) -> Self {
        let mut ntp_ips = BTreeSet::new();
        let mut ntp_names_to_resolve = Vec::new();
        for ntp_server in ntp_servers {
            match ntp_server.parse::<IpAddr>() {
                Ok(ip) => {
                    ntp_ips.insert(ip);
                }
                Err(_) => {
                    ntp_names_to_resolve.push(ntp_server.as_str());
                }
            }
        }
        Self { dns_servers, ntp_names_to_resolve, dns_name_to_query, ntp_ips }
    }

    async fn run(mut self, cx: &StepContext) -> StepResult<Vec<IpAddr>> {
        let mut messages = vec![];
        let mut warnings = vec![];

        for &dns_ip in self.dns_servers {
            let resolver = match AsyncResolver::tokio(
                ResolverConfig::from_parts(
                    None,
                    vec![],
                    NameServerConfigGroup::from_ips_clear(
                        &[dns_ip],
                        DNS_PORT,
                        true,
                    ),
                ),
                ResolverOpts::default(),
            ) {
                Ok(resolver) => resolver,
                Err(err) => {
                    warnings.push(format!(
                        "failed to create resolver for {dns_ip}: {}",
                        DisplayErrorChain::new(&err)
                    ));
                    continue;
                }
            };

            // Attempt to resolve any NTP servers that aren't IP addresses.
            for &ntp_name in &self.ntp_names_to_resolve {
                match resolver.lookup_ip(ntp_name).await {
                    Ok(ips) => {
                        for ip in ips {
                            let message = format!(
                                "resolved {ntp_name} to {ip} \
                                 via DNS server {dns_ip}"
                            );
                            messages.push(message.clone());

                            cx.send_progress(StepProgress::Progress {
                                progress: None,
                                metadata: message,
                            })
                            .await;
                            self.ntp_ips.insert(ip);
                        }
                    }
                    Err(err) => {
                        warnings.push(format!(
                            "failed to look up {ntp_name} via DNS server \
                             {dns_ip}: {}",
                            DisplayErrorChain::new(&err)
                        ));
                    }
                }
            }

            // If we were given a DNS name to query, we'll query that too.
            if let Some(dns_name) = self.dns_name_to_query {
                match resolver.lookup_ip(dns_name).await {
                    Ok(ips) => {
                        for ip in ips {
                            let message = format!(
                                "resolved {dns_name} to {ip} \
                                 via DNS server {dns_ip}"
                            );
                            messages.push(message.clone());
                            cx.send_progress(StepProgress::Progress {
                                progress: None,
                                metadata: message,
                            })
                            .await;
                        }
                    }
                    Err(err) => {
                        warnings.push(format!(
                            "failed to look up {dns_name} via DNS server \
                             {dns_ip}: {}",
                            DisplayErrorChain::new(&err)
                        ));
                    }
                }
            }

            // If all the NTP servers are IP addresses and we weren't given an
            // explicit DNS name to query, we'll attempt a DNS query of an
            // arbitrarily-chosen DNS name (`oxide.computer`). This may fail at
            // the DNS level if the server we're talking to does not handle
            // general public DNS names; we treat such a failure at the DNS
            // level as successes, because we're trying to check uplink
            // connectivity.
            if self.ntp_names_to_resolve.is_empty()
                && self.dns_name_to_query.is_none()
            {
                match resolver.lookup_ip(DNS_NAME_TO_QUERY).await {
                    Ok(ips) => {
                        for ip in ips {
                            let message = format!(
                                "resolved {DNS_NAME_TO_QUERY} to {ip} \
                                 via DNS server {dns_ip}"
                            );
                            messages.push(message.clone());
                            cx.send_progress(StepProgress::Progress {
                                progress: None,
                                metadata: message,
                            })
                            .await;
                        }
                    }
                    Err(err) => match err.kind() {
                        ResolveErrorKind::NoRecordsFound { .. } => {
                            let message = format!(
                                "no DNS records found for {DNS_NAME_TO_QUERY}; \
                                 connectivity to DNS server appears good"
                            );
                            messages.push(message.clone());
                            cx.send_progress(StepProgress::Progress {
                                progress: None,
                                metadata: message,
                            })
                            .await;
                        }
                        _ => {
                            warnings.push(format!(
                                "failed to look up {DNS_NAME_TO_QUERY} via \
                                 DNS server {dns_ip}: {}",
                                DisplayErrorChain::new(&err)
                            ));
                        }
                    },
                }
            }
        }

        let ntp_ips = self.ntp_ips.into_iter().collect();
        if warnings.is_empty() {
            StepSuccess::new(ntp_ips).with_metadata(messages).build()
        } else {
            StepWarning::new(ntp_ips, warnings.join("\n"))
                .with_metadata(messages)
                .build()
        }
    }
}

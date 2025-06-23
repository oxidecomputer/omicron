// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use display_error_chain::DisplayErrorChain;
use dpd_client::Client as DpdClient;
use dpd_client::ClientState as DpdClientState;
use dpd_client::types::LinkCreate;
use dpd_client::types::LinkId;
use dpd_client::types::LinkSettings;
use dpd_client::types::LinkState;
use dpd_client::types::PortFec as DpdPortFec;
use dpd_client::types::PortId;
use dpd_client::types::PortSettings;
use dpd_client::types::PortSpeed as DpdPortSpeed;
use either::Either;
use hickory_resolver::TokioResolver;
use hickory_resolver::config::NameServerConfigGroup;
use hickory_resolver::config::ResolveHosts;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::config::ResolverOpts;
use hickory_resolver::name_server::TokioConnectionProvider;
use illumos_utils::PFEXEC;
use illumos_utils::zone::SVCCFG;
use omicron_common::OMICRON_DPD_TAG;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::api::internal::shared::PortFec as OmicronPortFec;
use omicron_common::api::internal::shared::PortSpeed as OmicronPortSpeed;
use omicron_common::api::internal::shared::SwitchLocation;
use oxnet::IpNet;
use slog::Logger;
use slog::error;
use slog::o;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use tokio::process::Command;
use wicket_common::preflight_check::EventBuffer;
use wicket_common::preflight_check::StepContext;
use wicket_common::preflight_check::StepProgress;
use wicket_common::preflight_check::StepResult;
use wicket_common::preflight_check::StepSkipped;
use wicket_common::preflight_check::StepSuccess;
use wicket_common::preflight_check::StepWarning;
use wicket_common::preflight_check::UpdateEngine;
use wicket_common::preflight_check::UplinkPreflightStepId;
use wicket_common::preflight_check::UplinkPreflightTerminalError;
use wicket_common::rack_setup::UserSpecifiedPortConfig;
use wicket_common::rack_setup::UserSpecifiedRackNetworkConfig;

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

pub(super) async fn run_local_uplink_preflight_check(
    network_config: UserSpecifiedRackNetworkConfig,
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
    let (sender, mut receiver) = update_engine::channel();
    let mut engine = UpdateEngine::new(log, sender);

    for (port, uplink) in network_config.port_map(our_switch_location) {
        add_steps_for_single_local_uplink_preflight_check(
            &mut engine,
            &dpd_client,
            port,
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
    port: &'a str,
    uplink: &'a UserSpecifiedPortConfig,
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

    let registrar = engine.for_component(port.to_owned());

    let prev_step = registrar
        .new_step(
            UplinkPreflightStepId::ConfigureSwitch,
            "Configuring switch",
            |_cx| async {
                // Check that the port name is valid and that it has no links
                // configured already.
                let port_id = PortId::from_str(port).map_err(|_| {
                    UplinkPreflightTerminalError::InvalidPortName(
                        port.to_owned(),
                    )
                })?;
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
                    .port_settings_apply(
                        &port_id,
                        Some(OMICRON_DPD_TAG),
                        &port_settings,
                    )
                    .await
                {
                    Ok(_response) => {
                        let metadata = vec![format!(
                            "configured {:?}/{}: ips {:#?}, routes {:#?}",
                            port_id, link_id.0, uplink.addresses, uplink.routes
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
            async move |cx| {
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
                            LinkState::ConfigError(e) => {
                                format!("link config error: {e:?}")
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
            async move |cx| {
                let level1 = match prev_step.into_value(cx.token()).await {
                    Ok(level1) => level1,
                    Err(failure) => {
                        return StepSkipped::new(
                            Err(L2Failure::L1(failure)),
                            "link not up due to earlier failure",
                        )
                        .into();
                    }
                };

                // Tell the `uplink` service about the IP address we created on
                // the switch when configuring the uplink.
                let uplink_property =
                    UplinkProperty(format!("uplinks/{}_0", port));

                for addr in &uplink.addresses {
                    // This includes the CIDR only
                    let uplink_cidr = addr.address.to_string();
                    // This includes the VLAN ID, if any
                    let uplink_cfg = addr.to_string();
                    if let Err(err) = execute_command(&[
                        SVCCFG,
                        "-s",
                        UPLINK_SMF_NAME,
                        "addpropvalue",
                        &uplink_property.0,
                        "astring:",
                        &uplink_cfg,
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
                            Err(L2Failure::UplinkRefresh(
                                level1,
                                uplink_property,
                            )),
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
                        if start_waiting_addr.elapsed()
                            < UPLINK_SVC_WAIT_TIMEOUT
                        {
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
            async move |cx| {
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

                for r in &uplink.routes {
                    // Add the gateway as the default route in illumos.
                    if let Err(err) = execute_command(&[
                        ROUTE,
                        "add",
                        "-inet",
                        &r.destination.to_string(),
                        &r.nexthop.to_string(),
                    ])
                    .await
                    {
                        return StepWarning::new(
                            Err(RoutingFailure::HostDefaultRoute(level2)),
                            format!("could not add default route: {err}"),
                        )
                        .into();
                    };
                }

                StepSuccess::new(Ok(RoutingSuccess { level2 }))
                    .with_metadata(vec![format!(
                        "added routes {:#?}",
                        uplink.routes,
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
            async move |cx| {
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

                let ntp_ips_result = DnsLookupStep::default()
                    .run(dns_servers, ntp_servers, dns_name_to_query, &cx)
                    .await;

                Ok(ntp_ips_result.map(|ntp_ips| Ok((level2, ntp_ips))))
            },
        )
        .register();

    let prev_step = registrar
        .new_step(
            UplinkPreflightStepId::CheckExternalNtpConnectivity,
            "Checking for external NTP connectivity",
            async move |cx| {
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
            async move |cx| {
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

                for r in &uplink.routes {
                    if remove_host_route {
                        execute_command(&[
                            ROUTE,
                            "delete",
                            "-inet",
                            &r.destination.to_string(),
                            &r.nexthop.to_string(),
                        ])
                        .await
                        .map_err(|err| {
                            UplinkPreflightTerminalError::RemoveHostRoute {
                                err,
                                destination: r.destination,
                                nexthop: r.nexthop,
                            }
                        })?;
                    }
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
                async move |cx| {
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
            async move |cx| {
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
                        Some(OMICRON_DPD_TAG),
                        &PortSettings { links: HashMap::new() },
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
    uplink: &UserSpecifiedPortConfig,
    link_id: &LinkId,
) -> PortSettings {
    // Map from omicron_common types to dpd_client types
    let fec = uplink.uplink_port_fec.map(|fec| match fec {
        OmicronPortFec::Firecode => DpdPortFec::Firecode,
        OmicronPortFec::None => DpdPortFec::None,
        OmicronPortFec::Rs => DpdPortFec::Rs,
    });
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

    let mut port_settings = PortSettings { links: HashMap::new() };

    let addrs = uplink.addresses.iter().map(|a| a.addr()).collect();

    port_settings.links.insert(
        link_id.to_string(),
        LinkSettings {
            addrs,
            params: LinkCreate {
                autoneg: uplink.autoneg,
                kr: false, //NOTE: kr does not apply to user configurable links
                fec,
                speed,
                lane: Some(LinkId(0)),
                tx_eq: None,
            },
        },
    );

    for r in &uplink.routes {
        if let (IpNet::V4(_dst), IpAddr::V4(_nexthop)) =
            (r.destination, r.nexthop)
        {
            // TODO: do we need to create config for mgd?
        }
    }

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

#[derive(Debug, Default)]
struct DnsLookupStep {
    // Usable output of this step: IP addrs of the NTP servers to use.
    ntp_ips: BTreeSet<IpAddr>,

    messages: Vec<String>,
    warnings: Vec<String>,
}

impl DnsLookupStep {
    /// If we have been given DNS names for the NTP servers, this step serves a
    /// dual purpose: check connectivity to DNS servers, and use them to resolve
    /// the NTP servers. If we've been given IPs for the NTP servers, we only
    /// need to check connectivity to the DNS servers, and will query for a
    /// stock DNS name (or the provided DNS name) instead.
    async fn run(
        mut self,
        dns_servers: &[IpAddr],
        ntp_servers: &[String],
        dns_name_to_query: Option<&str>,
        cx: &StepContext,
    ) -> StepResult<Vec<IpAddr>> {
        // Partition `ntp_servers` into IP addrs (added to `self.ntp_ips`) and
        // DNS names (added to `ntp_names_to_resolve`).
        let mut ntp_names_to_resolve = Vec::new();
        for ntp_server in ntp_servers {
            match ntp_server.parse::<IpAddr>() {
                Ok(ip) => {
                    self.ntp_ips.insert(ip);
                }
                Err(_) => {
                    ntp_names_to_resolve.push(ntp_server.as_str());
                }
            }
        }

        // When looking up NTP servers, we need to find a record and we need to
        // record the resulting IP(s) in `self.ntp_ips`.
        let ntp_name_options = DnsLookupOptions {
            is_ntp_server: true,
            is_no_records_found_okay: false,
        };

        // If we were given an explicity `--query-dns some.host`, we expect it
        // to resolve, but it's not an NTP server.
        let dns_name_to_query_options = DnsLookupOptions {
            is_ntp_server: false,
            is_no_records_found_okay: false,
        };

        // If we don't have any NTP server names or an explicit `--query-dns`,
        // we will try querying for a fixed name. This will only resolve if the
        // server can respond to public DNS entries, so we don't treat a failure
        // to find a record as a failure of this step.
        let fallback_name_to_query_options = DnsLookupOptions {
            is_ntp_server: false,
            is_no_records_found_okay: true,
        };

        'dns_servers: for &dns_ip in dns_servers {
            let resolver = self.build_resolver(dns_ip);

            // Attempt to resolve any NTP servers that aren't IP addresses.
            for &ntp_name in &ntp_names_to_resolve {
                match self
                    .lookup_ip(
                        dns_ip,
                        &resolver,
                        ntp_name,
                        ntp_name_options,
                        cx,
                    )
                    .await
                {
                    Ok(()) => (),
                    Err(StopTryingServer) => continue 'dns_servers,
                }
            }

            // If we were given a DNS name to query, we'll query that too.
            if let Some(dns_name) = dns_name_to_query {
                match self
                    .lookup_ip(
                        dns_ip,
                        &resolver,
                        dns_name,
                        dns_name_to_query_options,
                        cx,
                    )
                    .await
                {
                    Ok(()) => (),
                    Err(StopTryingServer) => continue 'dns_servers,
                }
            }

            // If all the NTP servers are IP addresses and we weren't given an
            // explicit DNS name to query, we'll attempt a DNS query of an
            // arbitrarily-chosen DNS name (`oxide.computer`). This may fail at
            // the DNS level if the server we're talking to does not handle
            // general public DNS names; we treat such a failure at the DNS
            // level as successes, because we're trying to check uplink
            // connectivity.
            if ntp_names_to_resolve.is_empty() && dns_name_to_query.is_none() {
                match self
                    .lookup_ip(
                        dns_ip,
                        &resolver,
                        DNS_NAME_TO_QUERY,
                        fallback_name_to_query_options,
                        cx,
                    )
                    .await
                {
                    Ok(()) => (),
                    Err(StopTryingServer) => continue 'dns_servers,
                }
            }
        }

        let ntp_ips = self.ntp_ips.into_iter().collect();
        if self.warnings.is_empty() {
            StepSuccess::new(ntp_ips).with_metadata(self.messages).build()
        } else {
            StepWarning::new(ntp_ips, self.warnings.join("\n"))
                .with_metadata(self.messages)
                .build()
        }
    }

    /// Attempt to look up `name` via `resolver`.
    ///
    /// Based on the options and result of the query, we may add an IP to
    /// `self.ntp_ips` (if we succeed and `options` indicates this is the name
    /// of an NTP server) and will either:
    ///
    /// * append the results of the query to `self.messages` (if we succeed)
    /// * append the results of the query to `self.warnings` (if we fail)
    ///
    /// If we fail in a way that indicates we shouldn't bother trying to contact
    /// this DNS server any more, we return `Err(StopTryingServer)`. We may
    /// return `Ok(_)` even if the specific DNS query we performed failed (e.g.,
    /// if a record wasn't found) as long as we were able to contact the server.
    async fn lookup_ip(
        &mut self,
        dns_ip: IpAddr,
        resolver: &TokioResolver,
        name: &str,
        options: DnsLookupOptions,
        cx: &StepContext,
    ) -> Result<(), StopTryingServer> {
        // How long are we willing to retry in the face of errors?
        const RETRY_TIMEOUT: Duration = Duration::from_secs(30);

        // We may have a mix of failures followed by success, so we don't know
        // (as we're looping below) whether we want to append to `self.messages`
        // or `self.warnings`. We'll append to this local vec for now, then move
        // its contents into one or the other when we're done.
        let mut query_results = Vec::new();
        let start = Instant::now();

        // Query for an A record first; we'll switch to AAAA if we get a
        // `NoRecordsFound` error from our A query.
        let mut query_ipv4 = true;
        let mut attempt = 0;

        loop {
            attempt += 1;

            let (query_type, result) = if query_ipv4 {
                (
                    "A",
                    resolver.ipv4_lookup(name).await.map(|records| {
                        Either::Left(
                            records.into_iter().map(|x| IpAddr::V4(x.into())),
                        )
                    }),
                )
            } else {
                (
                    "AAAA",
                    resolver.ipv6_lookup(name).await.map(|records| {
                        Either::Right(
                            records.into_iter().map(|x| IpAddr::V6(x.into())),
                        )
                    }),
                )
            };

            let err = match result {
                // If our query succeeded, we're done.
                Ok(records) => {
                    for ip in records {
                        let message = format!(
                            "DNS server {dns_ip} \
                             {query_type} query attempt {attempt}: \
                             resolved {name} to {ip}"
                        );
                        query_results.push(message.clone());

                        if options.is_ntp_server {
                            self.ntp_ips.insert(ip);
                        }

                        cx.send_progress(StepProgress::Progress {
                            progress: None,
                            metadata: message,
                        })
                        .await;
                    }
                    self.messages.append(&mut query_results);
                    return Ok(());
                }
                Err(err) => err,
            };

            if err.is_no_records_found() && options.is_no_records_found_okay {
                let message = format!(
                    "DNS server {dns_ip} \
                     {query_type} query attempt {attempt}: \
                     no record found for {name}; \
                     connectivity to DNS server appears good"
                );
                query_results.push(message.clone());
                cx.send_progress(StepProgress::Progress {
                    progress: None,
                    metadata: message,
                })
                .await;

                self.messages.append(&mut query_results);
                return Ok(());
            } else if err.is_no_records_found() {
                // If we did not find records but that is not an acceptable
                // result for the lookup. Switch to AAAA queries (if this was A)
                // or we're done (and failed).
                let message = format!(
                    "DNS server {dns_ip} \
                     {query_type} query attempt {attempt}: \
                     failed to look up {name}: {}",
                    DisplayErrorChain::new(&err)
                );
                query_results.push(message.clone());
                cx.send_progress(StepProgress::Progress {
                    progress: None,
                    metadata: message,
                })
                .await;

                if query_ipv4 {
                    query_ipv4 = false;
                    attempt = 0;
                    continue;
                } else {
                    self.warnings.append(&mut query_results);
                    return Ok(());
                }
            } else {
                let message = format!(
                    "DNS server {dns_ip} \
                     {query_type} query attempt {attempt}: \
                     failed to look up {name}: {}",
                    DisplayErrorChain::new(&err)
                );
                query_results.push(message.clone());
                cx.send_progress(StepProgress::Progress {
                    progress: None,
                    metadata: message,
                })
                .await;

                if start.elapsed() < RETRY_TIMEOUT {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                } else {
                    self.warnings.append(&mut query_results);
                    return Err(StopTryingServer);
                }
            }
        }
    }

    /// Returns a `TokioResolver` if we're able to build one.
    ///
    /// If building it fails, we'll append to our internal `warnings` and return
    /// `None`.
    fn build_resolver(&mut self, dns_ip: IpAddr) -> TokioResolver {
        let mut options = ResolverOpts::default();

        // Enable edns for potentially larger records
        options.edns0 = true;

        // We will retry ourselves; we don't want the resolver
        // retrying internally too.
        options.attempts = 1;

        // We're only using this resolver temporarily; we don't want
        // or need it to cache anything.
        options.cache_size = 0;

        // We only want to query the DNS server, not /etc/hosts.
        options.use_hosts_file = ResolveHosts::Never;

        // This is currently the default for `ResolverOpts`, but it
        // doesn't hurt to specify it in case that changes.
        options.timeout = Duration::from_secs(5);

        // We're only specifying one server, so this setting
        // _probably_ doesn't matter.
        options.num_concurrent_reqs = 1;

        TokioResolver::builder_with_config(
            ResolverConfig::from_parts(
                None,
                vec![],
                NameServerConfigGroup::from_ips_clear(
                    &[dns_ip],
                    DNS_PORT,
                    true,
                ),
            ),
            TokioConnectionProvider::default(),
        )
        .with_options(options)
        .build()
    }
}

struct StopTryingServer;

#[derive(Debug, Clone, Copy)]
struct DnsLookupOptions {
    // Is this DNS name an NTP server? If so, we'll add any IPs found to
    // `self.ntp_ips`.
    is_ntp_server: bool,

    // If we're looking up our hard-coded `DNS_NAME_TO_QUERY`, we don't know
    // whether or not we expect to get an actual answer; it depends on whether
    // the server can handle requests for public DNS records. If this is true,
    // we'll treat `NoRecordsFound` as a success instead of a failure.
    is_no_records_found_okay: bool,
}

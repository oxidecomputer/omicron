// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8Path;
use camino::Utf8PathBuf;
use dropshot::test_util::LogContext;
use gateway_messages::SpPort;
use omicron_gateway::MgsArguments;
use omicron_gateway::SpIdentifier;
use omicron_gateway::SpType;
use omicron_gateway::SwitchPortConfig;
use omicron_gateway::SwitchPortDescription;
pub use omicron_gateway::metrics::MetricsConfig;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_uuid_kinds::RackUuid;
use qorb::resolver::AllBackends;
use qorb::resolver::Resolver;
use qorb::resolvers::fixed::FixedResolver;
use slog_error_chain::InlineErrorChain;
use sp_sim::NUM_IGNITION_TARGETS;
use sp_sim::SimRack;
use sp_sim::SimulatedSp;
use sp_sim::config::EreportConfig;
use sp_sim::config::GimletConfig;
use sp_sim::config::SpCommonConfig;
use std::collections::HashSet;
use std::convert::Infallible;
use std::future;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

// TODO this exact value is copy/pasted from `nexus/test-utils` - should we
// import it or have our own?
const RACK_UUID: &str = "c19a698f-c6f9-4a17-ae30-20d711b8f7dc";
pub const DEFAULT_SP_SIM_CONFIG: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/configs/sp_sim_config.test.toml");
const SP_SIM_GIMLET_TEMPLATE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/configs/sp_sim_gimlet_template.test.toml"
);

pub struct GatewayTestContext {
    pub client: gateway_client::Client,
    pub server: omicron_gateway::Server,
    port: u16,
    pub simrack: GatewaySimRack,
    pub logctx: LogContext,
    pub gateway_id: Uuid,
    resolver: FixedResolver,
    resolver_backends: watch::Receiver<AllBackends>,
}

impl GatewayTestContext {
    pub fn address(&self) -> SocketAddrV6 {
        SocketAddrV6::new(Ipv6Addr::LOCALHOST, self.port, 0, 0)
    }

    pub fn mgs_backends(&self) -> watch::Receiver<AllBackends> {
        self.resolver_backends.clone()
    }

    pub async fn teardown(mut self) {
        self.resolver.terminate().await;
        self.server.close().await.unwrap();
        match self.simrack {
            GatewaySimRack::Owned(simrack) => simrack
                .teardown()
                .await
                .expect("simulated SP tasks ran until teardown"),
            GatewaySimRack::Shared(_) => {}
        }
        self.logctx.cleanup_successful();
    }
}

/// Whether the simulated rack is owned or shared with other MGS instances.
///
/// This determines responsibility for tearing down the rack.
pub enum GatewaySimRack {
    /// The rack was started by this MGS, and it is responsible for tearing
    /// it down.
    Owned(SimRack),
    /// The rack is shared with other MGS instances. The caller is responsible
    /// for tearing it down.
    Shared(Arc<SimRack>),
}

impl Deref for GatewaySimRack {
    type Target = SimRack;

    fn deref(&self) -> &SimRack {
        match self {
            GatewaySimRack::Owned(simrack) => simrack,
            GatewaySimRack::Shared(simrack) => simrack,
        }
    }
}

pub fn load_test_config(
    sp_sim_config_file: Utf8PathBuf,
) -> (omicron_gateway::Config, sp_sim::Config) {
    (load_mgs_test_config(), load_sp_sim_config(&sp_sim_config_file))
}

pub fn load_mgs_test_config() -> omicron_gateway::Config {
    // The test configs are located relative to the directory this file is in.
    // TODO: embed these with include_str! instead?
    let manifest_dir = Utf8Path::new(env!("CARGO_MANIFEST_DIR"));
    let config_path = manifest_dir.join("configs/config.test.toml");
    omicron_gateway::Config::from_file(&config_path).unwrap_or_else(|e| {
        panic!(
            "failed to load MGS config from {config_path}: {}",
            InlineErrorChain::new(&e)
        )
    })
}

pub fn load_sp_sim_config(sp_sim_config_file: &Utf8Path) -> sp_sim::Config {
    match sp_sim::Config::from_file(sp_sim_config_file) {
        Ok(config) => config,
        Err(e) => panic!(
            "failed to load SP simulator config: {}",
            InlineErrorChain::new(&e)
        ),
    }
}

/// Returns the serial number of the simulated gimlet SP in sled slot `slot`.
pub fn sim_gimlet_serial(slot: u16) -> String {
    format!("SimGimlet{slot:02}")
}

/// Records the gimlet SPs [`add_simulated_gimlet_sps`] appended to an sp-sim
/// config.
///
/// This is used by [`add_simulated_gimlet_ports`] to give every MGS config its
/// matching sled ports.
#[derive(Clone, Copy, Debug)]
#[must_use]
pub struct AddedSimGimlets {
    /// The ignition target number of gimlet slot 0.
    ///
    /// Due to the way ignition target numbering works, this is the same as the
    /// number of sidecars.
    first_gimlet_ignition_target: u8,
    /// The `SpIdentifier` index of the first added gimlet.
    first_added_slot: u8,
    count: u8,
}

/// Appends the specified number of simulated Gimlet SPs to the sp-sim config.
///
/// Each new SP is a copy of the template defined by
/// `configs/sp_sim_gimlet_template.test.toml`, with unique values substituted
/// for the serial number and the ereport restart ID.
///
/// # Panics
///
/// If `count` is 0, this returns without checking anything. Otherwise, panics
/// if:
///
/// * The total number of simulated SPs after adding the new ones would exceed
///   sp-sim's [`sp_sim::NUM_IGNITION_TARGETS`] ignition targets.
/// * The gimlet template (`configs/sp_sim_gimlet_template.test.toml`) can't
///   be loaded.
pub fn add_simulated_gimlet_sps(
    sp_sim_config: &mut sp_sim::Config,
    count: usize,
) -> AddedSimGimlets {
    let sps = &mut sp_sim_config.simulated_sps;
    let sidecars = u8::try_from(sps.sidecar.len())
        .expect("the sp-sim config's sidecar count fits in a u8");
    let first_added_slot = u8::try_from(sps.gimlet.len())
        .expect("the sp-sim config's gimlet count fits in a u8");
    if count == 0 {
        return AddedSimGimlets {
            // Due to the way ignition target numbering works, this is the same
            // as the number of sidecars.
            first_gimlet_ignition_target: sidecars,
            first_added_slot,
            count: 0,
        };
    }

    let total_sps = usize::from(sidecars)
        + usize::from(first_added_slot)
        + count
        + sps.psc.len();
    assert!(
        total_sps <= NUM_IGNITION_TARGETS,
        "adding {count} gimlet SPs makes {total_sps} simulated SPs, but \
         sp-sim has only {NUM_IGNITION_TARGETS} ignition targets",
    );
    let added = AddedSimGimlets {
        first_gimlet_ignition_target: sidecars,
        first_added_slot,
        count: u8::try_from(count).expect(
            "the added gimlet count was checked against sp-sim's limit",
        ),
    };

    let template =
        match GimletConfig::from_file(Utf8Path::new(SP_SIM_GIMLET_TEMPLATE)) {
            Ok(template) => template,
            Err(e) => panic!(
                "failed to load SP simulator gimlet template: {}",
                InlineErrorChain::new(&e)
            ),
        };
    for slot in added.first_added_slot..added.first_added_slot + added.count {
        sps.gimlet.push(GimletConfig {
            // The only fields that are different per added gimlet are:
            //
            // - The serial number, to make each SP unique.
            // - The EreportConfig, to ensure each SP has a fresh restart UUID.
            //
            // All other fields are copied verbatim. In the config, all
            // addresses are port 0, and duplicated per-device identity (cert
            // seeds, component VPD serials) is fine because nothing relies on
            // it being unique. (But, if either of these changes in the future,
            // we may need to update this.)
            common: SpCommonConfig {
                serial_number: sim_gimlet_serial(u16::from(slot)),
                ereport_config: EreportConfig::default(),
                ..template.common.clone()
            },
        });
    }

    added
}

/// Appends MGS sled ports for added simulated Gimlet SPs.
///
/// Call this with the return value of [`add_simulated_gimlet_sps`].
///
/// `sp-sim`'s simulated ignition controller targets are numbered in a fixed
/// order: sidecars, then gimlets, then PSCs. This means that when extra gimlets
/// are added, the PSC ports are incremented by the number of gimlets added. For
/// example, let's say the original config had 2 sidecars, 2 gimlets and 1 PSC.
/// If 2 gimlets are added:
///
/// |          | before | after                    |
/// |----------|--------|--------------------------|
/// | sidecars | 0, 1   | 0, 1                     |
/// | gimlets  | 2, 3   | 2, 3, **4, 5** (added)   |
/// | PSC      | **4**  | **6**                    |
///
/// # Panics
///
/// If no gimlets were added, this returns without checking anything.
/// Otherwise, panics if the MGS config's switch ports don't follow sp-sim's
/// ignition target numbering (sidecars, then gimlets, then PSCs), or if the
/// added gimlets would push them past sp-sim's
/// [`sp_sim::NUM_IGNITION_TARGETS`] ignition targets.
pub fn add_simulated_gimlet_ports(
    server_config: &mut omicron_gateway::Config,
    added: AddedSimGimlets,
) {
    let AddedSimGimlets {
        first_gimlet_ignition_target,
        first_added_slot,
        count,
    } = added;
    if count == 0 {
        return;
    }

    assert_switch_port_layout(
        &server_config.switch,
        usize::from(first_gimlet_ignition_target),
        usize::from(first_added_slot),
    );

    let first_added_ignition_target =
        first_gimlet_ignition_target + first_added_slot;
    for port in &mut server_config.switch.port {
        if port.ignition_target >= first_added_ignition_target {
            port.ignition_target += count;
        }
    }

    let localhost_port_0 = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
    let switch = &mut server_config.switch;
    for slot in first_added_slot..first_added_slot + count {
        switch.port.push(SwitchPortDescription {
            config: SwitchPortConfig::Simulated {
                fake_interface: format!("fake-sled{slot}"),
                addr: localhost_port_0,
                ereport_addr: localhost_port_0,
            },
            ignition_target: first_gimlet_ignition_target + slot,
            location: switch
                .location
                .description
                .iter()
                .map(|description| {
                    (
                        description.name.clone(),
                        SpIdentifier::new(SpType::Sled, u16::from(slot)),
                    )
                })
                .collect(),
        });
    }

    let max_ignition_target =
        switch.port.iter().map(|port| port.ignition_target).max().expect(
            "the MGS config has switch ports, since sled ports were added",
        );
    assert!(
        usize::from(max_ignition_target) < NUM_IGNITION_TARGETS,
        "after adding {count} gimlet SPs, the highest MGS ignition target is \
         {max_ignition_target}, but sp-sim has only {NUM_IGNITION_TARGETS} \
         ignition targets",
    );
}

/// Checks that the MGS switch ports follow sp-sim's ignition numbering as
/// described above.
///
/// Panics on any mismatches.
fn assert_switch_port_layout(
    switch: &omicron_gateway::SwitchConfig,
    sidecars: usize,
    gimlets: usize,
) {
    let mut mismatches = Vec::new();

    let mut targets: Vec<usize> = switch
        .port
        .iter()
        .map(|port| usize::from(port.ignition_target))
        .collect();
    targets.sort_unstable();
    let mut deduped_targets = targets.clone();
    deduped_targets.dedup();
    if deduped_targets != targets {
        mismatches.push(format!(
            "more than one switch port has the same ignition target \
             (targets: {targets:?})"
        ));
    }

    for port in &switch.port {
        let target = usize::from(port.ignition_target);
        let mut location: Vec<_> = port.location.iter().collect();
        location.sort_unstable_by_key(|(name, _)| *name);
        for (name, sp_id) in location {
            let slot = usize::from(sp_id.slot);
            let expected = match sp_id.typ {
                SpType::Switch => {
                    if slot < sidecars && target < sidecars {
                        Ok(())
                    } else {
                        Err(format!("slot and target both below {sidecars}"))
                    }
                }
                SpType::Sled => {
                    if slot < gimlets && target == sidecars + slot {
                        Ok(())
                    } else {
                        Err(format!(
                            "slot below {gimlets} and target {}",
                            sidecars + slot,
                        ))
                    }
                }
                SpType::Power => {
                    let expected_target = sidecars + gimlets + slot;
                    if target == expected_target {
                        Ok(())
                    } else {
                        Err(format!("target {expected_target}"))
                    }
                }
            };
            if let Err(expected) = expected {
                mismatches.push(format!(
                    "at {name}, ignition target {target} is {:?} {slot}, \
                     but expected {expected}",
                    sp_id.typ,
                ));
            }
        }
    }

    let expected_sled_slots: Vec<usize> = (0..gimlets).collect();
    for description in &switch.location.description {
        let name = &description.name;
        let mut sled_slots: Vec<usize> = switch
            .port
            .iter()
            .filter_map(|port| port.location.get(name))
            .filter_map(|sp_id| match sp_id.typ {
                SpType::Sled => Some(usize::from(sp_id.slot)),
                SpType::Switch | SpType::Power => None,
            })
            .collect();
        sled_slots.sort_unstable();
        if sled_slots != expected_sled_slots {
            mismatches.push(format!(
                "at {name}, the sled ports are for slots {sled_slots:?}, \
                 but expected one per existing gimlet SP \
                 ({expected_sled_slots:?})"
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "adding gimlet SPs needs the MGS switch ports to follow sp-sim's \
         ignition target numbering ({sidecars} sidecars, then {gimlets} \
         gimlets, then PSCs), but:\n{}",
        mismatches.join("\n"),
    );
}

pub async fn test_setup(
    test_name: &str,
    sp_port: SpPort,
) -> GatewayTestContext {
    let (server_config, sp_sim_config) =
        load_test_config(DEFAULT_SP_SIM_CONFIG.into());
    test_setup_with_config(
        test_name,
        sp_port,
        server_config,
        &sp_sim_config,
        None,
    )
    .await
}

pub async fn test_setup_metrics_disabled(
    test_name: &str,
    sp_port: SpPort,
) -> GatewayTestContext {
    let (mut server_config, sp_sim_config) =
        load_test_config(DEFAULT_SP_SIM_CONFIG.into());
    match server_config.metrics.as_mut() {
        Some(cfg) => {
            cfg.disabled = true;
        }
        None => {
            server_config.metrics =
                Some(MetricsConfig { disabled: true, ..Default::default() });
        }
    }
    test_setup_with_config(
        test_name,
        sp_port,
        server_config,
        &sp_sim_config,
        None,
    )
    .await
}

pub async fn test_setup_local_reset_disabled(
    test_name: &str,
    sp_port: SpPort,
) -> GatewayTestContext {
    let (mut server_config, sp_sim_config) =
        load_test_config(DEFAULT_SP_SIM_CONFIG.into());
    for description in server_config.switch.location.description.iter_mut() {
        description.allow_local_sp_reset = false;
    }
    test_setup_with_config(
        test_name,
        sp_port,
        server_config,
        &sp_sim_config,
        None,
    )
    .await
}

fn expected_location(
    config: &omicron_gateway::Config,
    sp_port: SpPort,
) -> String {
    let config = &config.switch.location;
    let mut locations = config
        .description
        .iter()
        .map(|d| d.name.as_str())
        .collect::<HashSet<_>>();

    for determination in &config.determination {
        let refined = match sp_port {
            SpPort::One => &determination.sp_port_1,
            SpPort::Two => &determination.sp_port_2,
        };

        locations.retain(|name| refined.iter().any(|s| s == name));
    }

    assert_eq!(locations.len(), 1);
    locations.into_iter().next().unwrap().to_string()
}

pub async fn test_setup_with_config(
    test_name: &str,
    sp_port: SpPort,
    server_config: omicron_gateway::Config,
    sp_sim_config: &sp_sim::Config,
    listen_addr: Option<SocketAddrV6>,
) -> GatewayTestContext {
    // Use log settings from the server config and ignore log settings in
    // sp_sim_config; we'll give it the same logger as the server
    let logctx = LogContext::new(test_name, &server_config.log);

    // Start fake rack of simulated SPs
    let simrack = GatewaySimRack::Owned(
        SimRack::start(sp_sim_config, &logctx.log).await.unwrap(),
    );

    test_setup_with_simrack_impl(
        logctx,
        sp_port,
        server_config,
        simrack,
        listen_addr,
    )
    .await
}

/// Starts MGS for an existing simulated rack, communicating with the SP
/// simulator over `sp_port`.
pub async fn test_setup_with_simrack(
    test_name: &str,
    sp_port: SpPort,
    server_config: omicron_gateway::Config,
    simrack: Arc<SimRack>,
    listen_addr: Option<SocketAddrV6>,
) -> GatewayTestContext {
    let logctx = LogContext::new(test_name, &server_config.log);
    test_setup_with_simrack_impl(
        logctx,
        sp_port,
        server_config,
        GatewaySimRack::Shared(simrack),
        listen_addr,
    )
    .await
}

async fn test_setup_with_simrack_impl(
    logctx: LogContext,
    sp_port: SpPort,
    mut server_config: omicron_gateway::Config,
    simrack: GatewaySimRack,
    listen_addr: Option<SocketAddrV6>,
) -> GatewayTestContext {
    // Can't be `const` because `SocketAddrV6::new()` isn't const yet
    let localhost_port_0 = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
    let mut addresses = vec![localhost_port_0];
    if let Some(addr) = listen_addr {
        addresses.push(addr);
    };

    let log = &logctx.log;

    let expected_location = expected_location(&server_config, sp_port);

    // Update discovery addrs of `server_config` to point to the SP ports that
    // will identify us as the expected location
    for port_description in &mut server_config.switch.port {
        // we need to know whether this port points to a switch or sled; for now
        // assume that matches whether we end up as `switch0` or `switch1`
        let target_sp =
            port_description.location.get(&expected_location).unwrap();
        let (sp_addr, sp_ereport_addr) = match target_sp.typ {
            SpType::Switch => {
                let switch = &simrack.sidecars[usize::from(target_sp.slot)];
                (switch.local_addr(sp_port), switch.local_ereport_addr(sp_port))
            }
            SpType::Sled => {
                let sled = &simrack.gimlets[usize::from(target_sp.slot)];
                (sled.local_addr(sp_port), sled.local_ereport_addr(sp_port))
            }
            SpType::Power => {
                let psc = &simrack.pscs[usize::from(target_sp.slot)];
                (psc.local_addr(sp_port), psc.local_ereport_addr(sp_port))
            }
        };
        match &mut port_description.config {
            SwitchPortConfig::Simulated { addr, ereport_addr, .. } => {
                *addr = sp_addr.unwrap();
                *ereport_addr =
                    sp_ereport_addr.expect("no ereport addr configured");
            }
            SwitchPortConfig::SwitchZoneInterface { .. } => {
                panic!("test config using `switch-zone-interface` config")
            }
        }
    }

    // Start gateway server
    let rack_id = Some(RACK_UUID.parse::<RackUuid>().unwrap());
    let gateway_id = Uuid::new_v4();
    let args = MgsArguments { id: gateway_id, addresses, rack_id };
    let server = omicron_gateway::Server::start(
        server_config.clone(),
        args,
        log.clone(),
    )
    .await
    .unwrap();

    // Build a list of all SPs defined in our config
    let mut all_sp_ids = Vec::new();
    let mut local_switch = None;
    for port_config in &server_config.switch.port {
        let sp_id =
            port_config.location.get(&expected_location).copied().unwrap();
        all_sp_ids.push(sp_id);

        // Note the ID of our local ignition controller for use later in
        // checking that our self-location-discovery was correct.
        if port_config.config.interface()
            == server_config.switch.local_ignition_controller_interface
        {
            assert!(local_switch.is_none(), "ignition controller listed twice");
            local_switch = Some(sp_id);
        }
    }

    // Wait until the server has figured out the socket address of all those SPs
    let mgmt_switch = server.management_switch();
    poll::wait_for_condition::<(), Infallible, _, _>(
        || {
            let result = if mgmt_switch.is_discovery_complete()
                && all_sp_ids.iter().all(|&id| {
                    // All ids are valid; unwrap finding the handle to each one.
                    let sp = mgmt_switch.sp(id).unwrap();

                    // Have we found this SP?
                    sp.sp_addr_watch().borrow().is_some()
                }) {
                Ok(())
            } else {
                Err(CondCheckError::NotYet { status: None })
            };
            future::ready(result)
        },
        &Duration::from_millis(100),
        // This seems like a pretty long time to wait for MGS to discover the
        // simulated SPs, but we've seen tests fail due to timeouts here in the
        // past, so we may as well be generous:
        // https://github.com/oxidecomputer/omicron/issues/6877
        &Duration::from_secs(30),
    )
    .await
    .unwrap();

    // Make sure it discovered the location we expect
    assert_eq!(mgmt_switch.local_switch().unwrap(), local_switch.unwrap());

    let server_addr = server
        .dropshot_server_for_address(localhost_port_0)
        .unwrap()
        .local_addr();
    let client = gateway_client::Client::new(
        &format!("http://{server_addr}"),
        logctx.log.new(slog::o!("component" => "MgsClient")),
    );

    let mut resolver = FixedResolver::new(std::iter::once(server_addr));
    let resolver_backends = resolver.monitor();

    GatewayTestContext {
        client,
        server,
        port: server_addr.port(),
        simrack,
        logctx,
        gateway_id,
        resolver,
        resolver_backends,
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use assert_matches::assert_matches;
use gateway_messages::SpPort;
use mg_admin_client::types::Neighbor as MgdNeighbor;
use mg_api_types::bgp::config::Origin4 as MgdOrigin4;
use mg_api_types::bgp::history::Origin6 as MgdOrigin6;
use proptest::collection::btree_map;
use proptest::collection::vec;
use proptest::prelude::Strategy;
use proptest::prelude::any;
use proptest::prelude::proptest as proptest_macro;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkPorts;
use std::collections::BTreeMap;
use test_strategy::proptest;
use tokio::task::block_in_place;

/// Build a minimal `PortConfig` on the given switch with the given BGP peers.
fn port_config(
    switch: SwitchSlot,
    port: &str,
    bgp_peers: Vec<BgpPeerConfig>,
) -> PortConfig {
    PortConfig {
        routes: Vec::new(),
        addresses: Vec::new(),
        switch,
        port: port.to_string(),
        uplink_port_speed: LinkSpeed::Speed100G,
        uplink_port_fec: None,
        bgp_peers,
        autoneg: false,
        lldp: None,
        tx_eq: None,
    }
}

fn rack_config(
    ports: Vec<PortConfig>,
    bgp: Vec<BgpConfig>,
) -> RackNetworkConfig {
    RackNetworkConfig {
        rack_subnet: "fd00::/48".parse().unwrap(),
        infra_ip_first: "10.0.0.1".parse().unwrap(),
        infra_ip_last: "10.0.0.100".parse().unwrap(),
        ports: UplinkPorts::new(ports).unwrap(),
        bgp,
        bfd: Vec::new(),
    }
}

// Description of a single desired BGP configuration. This attempts to be more
// or less valid, but certainly has some nonsense fields (e.g., port names are
// arbitrary ascii strings and not related to any physical ports).
//
// Because in practice we only support a single ASN per mgd instance today, this
// input only describes a single ASN.
#[derive(Debug, Clone, test_strategy::Arbitrary)]
struct TestInput {
    asn: u32,

    #[strategy(any::<BgpConfig>().prop_map(move |mut config| {
        config.asn = #asn;
        config
    }))]
    bgp_config: BgpConfig,

    #[strategy(
        vec(any::<(RouterPeerIpAddr, BgpPeerConfig)>(), 1..=8)
            .prop_map(move |items| {
                items
                    .into_iter()
                    .map(|(ip, mut peer)| {
                        peer.asn = #asn;
                        peer.addr = RouterPeerType::Numbered { ip };
                        (ip, peer)
                    })
                    .collect()
            })
    )]
    numbered_peers: BTreeMap<RouterPeerIpAddr, BgpPeerConfig>,

    #[strategy(
        btree_map(
            "[0-9a-zA-Z]{0,16}",
            any::<(RouterLifetimeConfig, BgpPeerConfig)>()
                .prop_map(move |(router_lifetime, mut peer)| {
                    peer.asn = #asn;
                    peer.addr = RouterPeerType::Unnumbered {
                        router_lifetime,
                    };
                    peer
                }),
            1..=8
        )
    )]
    unnumbered_peers: BTreeMap<String, BgpPeerConfig>,
}

impl TestInput {
    fn rack_network_config(&self) -> RackNetworkConfig {
        // port name for numbered peers doesn't matter - see
        // https://github.com/oxidecomputer/maghemite/issues/768
        let mut qsfp0 =
            self.numbered_peers.values().cloned().collect::<Vec<_>>();
        if let Some(unnumbered_qsfp0) = self.unnumbered_peers.get("qsfp0") {
            qsfp0.push(unnumbered_qsfp0.clone());
        }
        let mut all_ports =
            vec![port_config(SwitchSlot::Switch0, "qsfp0", qsfp0)];
        for (port, config) in &self.unnumbered_peers {
            if port != "qsfp0" {
                all_ports.push(port_config(
                    SwitchSlot::Switch0,
                    port,
                    vec![config.clone()],
                ));
            }
        }
        rack_config(all_ports, vec![self.bgp_config.clone()])
    }

    fn expected_origin4(&self) -> BTreeSet<Ipv4Net> {
        self.bgp_config
            .originate
            .iter()
            .filter_map(|net| match net {
                IpNet::V4(net) => Some(*net),
                IpNet::V6(_) => None,
            })
            .collect()
    }

    fn expected_origin6(&self) -> BTreeSet<Ipv6Net> {
        self.bgp_config
            .originate
            .iter()
            .filter_map(|net| match net {
                IpNet::V6(net) => Some(*net),
                IpNet::V4(_) => None,
            })
            .collect()
    }
}

// Simple proptest to confirm our `Arbitrary` impl for `TestInput` produces
// valid desired configurations
#[proptest]
fn proptest_valid_input(input: TestInput) {
    let logctx =
        omicron_test_utils::dev::test_setup_log("proptest_valid_input");

    match DiffableBgpConfig::from_desired_config(
        &input.rack_network_config(),
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    ) {
        Ok(_) => (),
        Err(err) => panic!("invalid config: {err:#}"),
    }

    logctx.cleanup_successful();
}

// Helper trait we implement for both `MgdNeighbor` and `MgdUnnumberedNeighbor`
// so we can write a single "compare the MGD state against our expected config"
// function below (`check_mgd_state_against_expected_config()`).
trait MgdNeighborKind {
    fn asn(&self) -> u32;
    fn hold_time(&self) -> u64;
    fn idle_hold_time(&self) -> u64;
    fn delay_open(&self) -> u64;
    fn connect_retry(&self) -> u64;
    fn keepalive(&self) -> u64;
    fn remote_asn(&self) -> Option<u32>;
    fn min_ttl(&self) -> Option<u8>;
    fn md5_auth_key(&self) -> Option<&str>;
    fn multi_exit_discriminator(&self) -> Option<u32>;
    fn communities(&self) -> &[u32];
    fn local_pref(&self) -> Option<u32>;
    fn enforce_first_as(&self) -> bool;
    fn ipv4_unicast(&self) -> Option<&MgdIpv4UnicastConfig>;
    fn ipv6_unicast(&self) -> Option<&MgdIpv6UnicastConfig>;
    fn vlan_id(&self) -> Option<u16>;

    fn peer_type(&self) -> RouterPeerType;
}

impl MgdNeighborKind for MgdNeighbor {
    fn asn(&self) -> u32 {
        self.asn
    }
    fn hold_time(&self) -> u64 {
        self.hold_time
    }
    fn idle_hold_time(&self) -> u64 {
        self.idle_hold_time
    }
    fn delay_open(&self) -> u64 {
        self.delay_open
    }
    fn connect_retry(&self) -> u64 {
        self.connect_retry
    }
    fn keepalive(&self) -> u64 {
        self.keepalive
    }
    fn remote_asn(&self) -> Option<u32> {
        self.remote_asn
    }
    fn min_ttl(&self) -> Option<u8> {
        self.min_ttl
    }
    fn md5_auth_key(&self) -> Option<&str> {
        self.md5_auth_key.as_deref()
    }
    fn multi_exit_discriminator(&self) -> Option<u32> {
        self.multi_exit_discriminator
    }
    fn communities(&self) -> &[u32] {
        &self.communities
    }
    fn local_pref(&self) -> Option<u32> {
        self.local_pref
    }
    fn enforce_first_as(&self) -> bool {
        self.enforce_first_as
    }
    fn ipv4_unicast(&self) -> Option<&MgdIpv4UnicastConfig> {
        self.ipv4_unicast.as_ref()
    }
    fn ipv6_unicast(&self) -> Option<&MgdIpv6UnicastConfig> {
        self.ipv6_unicast.as_ref()
    }
    fn vlan_id(&self) -> Option<u16> {
        self.vlan_id
    }
    fn peer_type(&self) -> RouterPeerType {
        RouterPeerType::Numbered { ip: self.host.ip().try_into().unwrap() }
    }
}

impl MgdNeighborKind for MgdUnnumberedNeighbor {
    fn asn(&self) -> u32 {
        self.asn
    }
    fn hold_time(&self) -> u64 {
        self.hold_time
    }
    fn idle_hold_time(&self) -> u64 {
        self.idle_hold_time
    }
    fn delay_open(&self) -> u64 {
        self.delay_open
    }
    fn connect_retry(&self) -> u64 {
        self.connect_retry
    }
    fn keepalive(&self) -> u64 {
        self.keepalive
    }
    fn remote_asn(&self) -> Option<u32> {
        self.remote_asn
    }
    fn min_ttl(&self) -> Option<u8> {
        self.min_ttl
    }
    fn md5_auth_key(&self) -> Option<&str> {
        self.md5_auth_key.as_deref()
    }
    fn multi_exit_discriminator(&self) -> Option<u32> {
        self.multi_exit_discriminator
    }
    fn communities(&self) -> &[u32] {
        &self.communities
    }
    fn local_pref(&self) -> Option<u32> {
        self.local_pref
    }
    fn enforce_first_as(&self) -> bool {
        self.enforce_first_as
    }
    fn ipv4_unicast(&self) -> Option<&MgdIpv4UnicastConfig> {
        self.ipv4_unicast.as_ref()
    }
    fn ipv6_unicast(&self) -> Option<&MgdIpv6UnicastConfig> {
        self.ipv6_unicast.as_ref()
    }
    fn vlan_id(&self) -> Option<u16> {
        self.vlan_id
    }
    fn peer_type(&self) -> RouterPeerType {
        RouterPeerType::Unnumbered {
            router_lifetime: RouterLifetimeConfig::new(
                self.act_as_a_default_ipv6_router,
            )
            .unwrap(),
        }
    }
}

fn check_mgd_state_against_expected_config<T: MgdNeighborKind>(
    neighbor: &T,
    config: &BgpPeerConfig,
) {
    let BgpPeerConfig {
        asn,
        // TODO-correctness The reconciler doesn't use this field, which means
        // mgd has no way of echoing it back to us.
        port: _,
        addr,
        hold_time,
        idle_hold_time,
        delay_open,
        connect_retry,
        keepalive,
        remote_asn,
        min_ttl,
        md5_auth_key,
        multi_exit_discriminator,
        communities,
        local_pref,
        enforce_first_as,
        allowed_import,
        allowed_export,
        vlan_id,
    } = config;

    assert_eq!(neighbor.asn(), *asn);
    assert_eq!(neighbor.peer_type(), *addr);

    // MGD fills in defaults for many fields if we don't specify a desired
    // value. This macro checks that if we set a value, MGD's value matches. If
    // we didn't set a value, we don't check that field.
    macro_rules! check_if_set {
        ($name:ident) => {
            if let Some(expected) = *$name {
                assert_eq!(
                    neighbor.$name(),
                    expected,
                    concat!("unexpected ", stringify!($name)),
                );
            }
        };
    }
    check_if_set!(hold_time);
    check_if_set!(idle_hold_time);
    check_if_set!(delay_open);
    check_if_set!(connect_retry);
    check_if_set!(keepalive);

    // These fields can be compared directly.
    assert_eq!(neighbor.remote_asn(), *remote_asn);
    assert_eq!(neighbor.min_ttl(), *min_ttl);
    assert_eq!(neighbor.md5_auth_key(), md5_auth_key.as_deref());
    assert_eq!(neighbor.multi_exit_discriminator(), *multi_exit_discriminator);
    assert_eq!(neighbor.communities(), *communities);
    assert_eq!(neighbor.local_pref(), *local_pref);
    assert_eq!(neighbor.enforce_first_as(), *enforce_first_as);
    assert_eq!(neighbor.vlan_id(), *vlan_id);

    // Checking import/export policy is more complicated.
    fn check_policy(
        config_policy: &ImportExportPolicy,
        ipv4_unicast: &MgdImportExportPolicy4,
        ipv6_unicast: &MgdImportExportPolicy6,
    ) {
        match config_policy {
            ImportExportPolicy::NoFiltering => {
                assert_matches!(
                    ipv4_unicast,
                    MgdImportExportPolicy4::NoFiltering
                );
                assert_matches!(
                    ipv6_unicast,
                    MgdImportExportPolicy6::NoFiltering
                );
            }
            ImportExportPolicy::Allow(nets) => {
                let nets4 = nets
                    .iter()
                    .filter_map(|x| match x {
                        IpNet::V4(p) => Some(*p),
                        IpNet::V6(_) => None,
                    })
                    .collect::<BTreeSet<_>>();
                let nets6 = nets
                    .iter()
                    .filter_map(|x| match x {
                        IpNet::V6(p) => Some(*p),
                        IpNet::V4(_) => None,
                    })
                    .collect::<BTreeSet<_>>();

                assert_matches!(
                    ipv4_unicast,
                    MgdImportExportPolicy4::Allow(allowed) if *allowed == nets4
                );
                assert_matches!(
                    ipv6_unicast,
                    MgdImportExportPolicy6::Allow(allowed) if *allowed == nets6
                );
            }
        }
    }
    check_policy(
        allowed_import,
        &neighbor.ipv4_unicast().unwrap().import_policy,
        &neighbor.ipv6_unicast().unwrap().import_policy,
    );
    check_policy(
        allowed_export,
        &neighbor.ipv4_unicast().unwrap().export_policy,
        &neighbor.ipv6_unicast().unwrap().export_policy,
    );
}

// Main implementation of our proptest.
//
// Unlike many proptests, we _don't_ try to clear the state of the mgd we're
// talking to before attempting to reconcile to the config described by `input`.
// Instead, we (ab)use the fact that the state changes between each input as
// part of our test: any reconciliation should succeed regardless of any state
// left behind by a prior test invocation. This messes with shrinking, but is a
// cheap and easy way to put the reconciler through its paces.
async fn run_one_proptest_input(
    input: &TestInput,
    client: &Client,
    log: &Logger,
) {
    // Perform reconciliation and ensure it had no errors.
    let rack_network_config = input.rack_network_config();
    match reconcile(
        &client,
        &rack_network_config,
        ThisSledSwitchSlot::TEST_FAKE,
        &log,
    )
    .await
    {
        MgdBgpReconcilerStatus::Success { .. } => {}
        status => {
            panic!("unexpected status: {status:?}")
        }
    }

    // --- post-reconciliation checks of mgd state ---

    // confirm no other routers exist
    let routers = client.read_routers().await.unwrap().into_inner();
    assert_eq!(routers.len(), 1);
    assert!(
        routers.iter().find(|r| r.asn == input.asn).is_some(),
        "unexpected routers: {routers:?}"
    );

    // confirm originate prefixes
    let MgdOrigin4 { asn: _, prefixes } =
        client.read_origin4(input.asn).await.unwrap().into_inner();
    assert_eq!(
        prefixes.into_iter().collect::<BTreeSet<_>>(),
        input.expected_origin4()
    );
    let MgdOrigin6 { asn: _, prefixes } =
        client.read_origin6(input.asn).await.unwrap().into_inner();
    assert_eq!(
        prefixes.into_iter().collect::<BTreeSet<_>>(),
        input.expected_origin6()
    );

    // confirm shaper/checker
    if let Some(shaper) = &input.bgp_config.shaper {
        let MgdShaperSource { asn: _, code } =
            client.read_shaper(input.asn).await.unwrap().into_inner();
        assert_eq!(code, *shaper);
    } else {
        let err = client.read_shaper(input.asn).await.unwrap_err();
        assert_eq!(err.status(), Some(http::StatusCode::NOT_FOUND));
    }
    if let Some(checker) = &input.bgp_config.checker {
        let MgdCheckerSource { asn: _, code } =
            client.read_checker(input.asn).await.unwrap().into_inner();
        assert_eq!(code, *checker);
    } else {
        let err = client.read_checker(input.asn).await.unwrap_err();
        assert_eq!(err.status(), Some(http::StatusCode::NOT_FOUND));
    }

    // confirm numbered peers
    let numbered_neighbors =
        client.read_neighbors(input.asn).await.unwrap().into_inner();
    for neighbor in &numbered_neighbors {
        let addr: RouterPeerIpAddr = neighbor.name.parse().unwrap();
        let config = input.numbered_peers.get(&addr).unwrap();
        check_mgd_state_against_expected_config(neighbor, config);
    }
    assert_eq!(numbered_neighbors.len(), input.numbered_peers.len());

    // confirm unnumbered peers
    let unnumbered_neighbors =
        client.read_unnumbered_neighbors(input.asn).await.unwrap().into_inner();
    for neighbor in &unnumbered_neighbors {
        let Some(config) = input.unnumbered_peers.get(&neighbor.group) else {
            panic!(
                "mgd has unnumbered peer {neighbor:?} that is not present \
                 in our input"
            );
        };
        check_mgd_state_against_expected_config(neighbor, config);
    }
    assert_eq!(unnumbered_neighbors.len(), input.unnumbered_peers.len());

    // ----
    // After the successful reconciliation above, reconciling again should do
    // nothing. To check this, we partially reimplement `reconcile()` only up
    // through the construction of the `ReconciliationPlan`, then confirm that
    // plan is empty.
    // ----
    let current_config =
        DiffableBgpConfig::fetch_current(client).await.unwrap();
    let desired_config = DiffableBgpConfig::from_desired_config(
        &rack_network_config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .unwrap();
    let ReconciliationPlan {
        set_max_paths,
        routers_to_delete,
        routers_to_update,
        routers_to_create,
        originate4_to_update,
        originate6_to_update,
        originate4_to_create,
        originate6_to_create,
        shapers_to_delete,
        shapers_to_update,
        shapers_to_create,
        checkers_to_delete,
        checkers_to_update,
        checkers_to_create,
        numbered_peers_to_delete,
        numbered_peers_to_update,
        numbered_peers_to_create,
        unnumbered_peers_to_delete,
        unnumbered_peers_to_update,
        unnumbered_peers_to_create,
    } = ReconciliationPlan::new(&current_config, &desired_config);
    assert!(set_max_paths.is_none());
    assert!(routers_to_delete.is_empty());
    assert!(routers_to_update.is_empty());
    assert!(routers_to_create.is_empty());
    assert!(originate4_to_update.is_empty());
    assert!(originate6_to_update.is_empty());
    assert!(originate4_to_create.is_empty());
    assert!(originate6_to_create.is_empty());
    assert!(shapers_to_delete.is_empty());
    assert!(shapers_to_update.is_empty());
    assert!(shapers_to_create.is_empty());
    assert!(checkers_to_delete.is_empty());
    assert!(checkers_to_update.is_empty());
    assert!(checkers_to_create.is_empty());
    assert!(numbered_peers_to_delete.is_empty());
    assert!(numbered_peers_to_update.is_empty());
    assert!(numbered_peers_to_create.is_empty());
    assert!(unnumbered_peers_to_delete.is_empty());
    assert!(unnumbered_peers_to_update.is_empty());
    assert!(unnumbered_peers_to_create.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn proptest_full_reconciliation() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("proptest_full_reconciliation");
    let mgsctx = gateway_test_utils::setup::test_setup(
        "proptest_full_reconciliation",
        SpPort::One,
    )
    .await;
    let mut mgdctx = omicron_test_utils::dev::maghemite::MgdInstance::start(
        0,
        mgsctx.address().into(),
    )
    .await
    .expect("started mgd");
    let client = Client::new(
        &format!("http://{}", mgdctx.address()),
        logctx.log.clone(),
    );
    let rt = tokio::runtime::Handle::current();

    proptest_macro!(|(input: TestInput)| {
        // Do a little dance to call our async `one_test_invocation` within the
        // non-async `proptest_macro!()` context.
        block_in_place(|| rt.block_on(run_one_proptest_input(
            &input,
            &client,
            &logctx.log,
        )));
    });

    mgdctx.cleanup().await.expect("mgd cleanup succeeded");
    mgsctx.teardown().await;
    logctx.cleanup_successful();
}

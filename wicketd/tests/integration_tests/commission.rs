// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeSet;
use std::time::Duration;

use super::setup::{
    Cond, WicketdTestContext, assert_client_error, assert_client_error_message,
    wait_for_sled0_progress,
};
use camino_tempfile::Utf8TempDir;
use clap::Parser;
use gateway_messages::SpPort;
use gateway_test_utils::setup as gateway_setup;
use http::StatusCode;
use iddqd::IdOrdMap;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use wicket_common::example::ExampleRackSetupData;
use wicket_common::rack_setup::{BgpAuthKey, CurrentRssUserConfigInsensitive};
use wicketd_client::types::PutBgpAuthKeyBody;
use wicketd_commission_types::rack_setup::PutRssUserConfigInsensitive;
use wicketd_commission_types_versions::latest::inventory::{
    IgnitionFaults, LocationInfo, PowerState, SpIdentifier, SpIgnitionInfo,
    SpInfo, SpInventoryParams, SpStateInfo, SpType, SwitchSlot,
};
use wicketd_commission_types_versions::latest::rack_setup::{
    NewPasswordHash, PutRecoveryUserPasswordHash,
};
use wicketd_commission_types_versions::latest::update::{
    StartUpdateOptions, StartUpdateParams, UpdateState, UpdateTargets,
};

/// Wait for the SP inventory to become ready.
async fn wait_for_sp_inventory(
    ctx: &WicketdTestContext,
    ready: impl Fn(&IdOrdMap<SpInfo>) -> bool,
) -> IdOrdMap<SpInfo> {
    wait_for_condition(
        || async {
            let result: Cond<IdOrdMap<SpInfo>> = match ctx
                .commission_client
                .get_sp_inventory(&SpInventoryParams::default())
                .await
            {
                Ok(resp) => {
                    let sps = resp.into_inner().sps;
                    if ready(&sps) {
                        Ok(sps)
                    } else {
                        Err(CondCheckError::NotYet { status: None })
                    }
                }
                // The main retryable error here is 503, meaning MGS
                // inventory isn't available yet.
                Err(err) if err.is_retryable() => {
                    Err(CondCheckError::NotYet { status: None })
                }
                Err(err) => Err(CondCheckError::Failed(err)),
            };
            result
        },
        &Duration::from_millis(100),
        &Duration::from_secs(30),
    )
    .await
    .expect("sp inventory became available")
}

/// Test inventory on the commissioning API.
#[tokio::test]
async fn test_commission_inventory() {
    let gateway =
        gateway_setup::test_setup("test_commission_inventory", SpPort::One)
            .await;
    let ctx = WicketdTestContext::setup(gateway).await;

    // Wait for MGS inventory and ignition to become available.
    let sps = wait_for_sp_inventory(&ctx, |sps| {
        [0u16, 1].iter().all(|slot| {
            sps.get(&SpIdentifier { typ: SpType::Sled, slot: *slot })
                .is_some_and(|sp| {
                    sp.state.is_some() && sp.ignition != SpIgnitionInfo::NotRead
                })
        })
    })
    .await;

    assert_eq!(sps.len(), 4, "four simulated SPs");

    let sled0 = sps
        .get(&SpIdentifier { typ: SpType::Sled, slot: 0 })
        .expect("sled 0 present");
    assert_eq!(
        sled0.state,
        Some(sim_gimlet_state("SimGimlet00")),
        "sled 0 state as reported by sp-sim"
    );

    let sled1 = sps
        .get(&SpIdentifier { typ: SpType::Sled, slot: 1 })
        .expect("sled 1 present");
    assert_eq!(
        sled1.state,
        Some(sim_gimlet_state("SimGimlet01")),
        "sled 1 state as reported by sp-sim"
    );

    for sled in [&sled0, &sled1] {
        assert_eq!(
            sled.ignition,
            sim_ignition_present(),
            "sp-sim sleds report ignition present with no faults: {sled:?}"
        );
    }

    // Force a refresh.
    let force_refresh: Vec<_> = sps.iter().map(|sp| sp.id).collect();
    let refreshed = ctx
        .commission_client
        .get_sp_inventory(&SpInventoryParams { force_refresh })
        .await
        .expect("get_sp_inventory with force_refresh succeeded")
        .into_inner();
    assert!(
        refreshed.mgs_last_seen < Duration::from_secs(30),
        "a just-completed forced refresh implies MGS was seen recently, \
         but mgs_last_seen is {:?}",
        refreshed.mgs_last_seen,
    );
    assert_eq!(
        refreshed.sps.len(),
        4,
        "four simulated SPs after forced refresh"
    );
    let refreshed_sled0 = refreshed
        .sps
        .get(&SpIdentifier { typ: SpType::Sled, slot: 0 })
        .expect("sled 0 present after forced refresh");
    assert_eq!(
        refreshed_sled0.state,
        Some(sim_gimlet_state("SimGimlet00")),
        "sled 0 state after forced refresh: {refreshed:?}"
    );

    let err = ctx
        .commission_client
        .get_sp_inventory(&SpInventoryParams {
            force_refresh: vec![SpIdentifier { typ: SpType::Sled, slot: 99 }],
        })
        .await
        .expect_err("force_refresh of a nonexistent SP is rejected");
    assert_client_error_message(&err, StatusCode::BAD_REQUEST, "sled 99");

    let bootstrap = ctx
        .commission_client
        .get_bootstrap_sleds()
        .await
        .expect("get_bootstrap_sleds succeeded")
        .into_inner();
    let ids: Vec<_> = bootstrap.iter().map(|s| s.id).collect();
    assert_eq!(
        ids,
        vec![
            SpIdentifier { typ: SpType::Sled, slot: 0 },
            SpIdentifier { typ: SpType::Sled, slot: 1 },
        ],
    );
    let serial_numbers: Vec<_> =
        bootstrap.iter().map(|s| s.serial_number.clone()).collect();
    assert_eq!(serial_numbers, vec!["SimGimlet00", "SimGimlet01"]);
    assert!(
        bootstrap.iter().all(|s| s.ip.is_none()),
        "no bootstrap IPs discovered in the test environment"
    );

    let location = wait_for_condition(
        || async {
            let result: Cond<LocationInfo> =
                match ctx.commission_client.get_location().await {
                    Ok(resp) => Ok(resp.into_inner()),
                    Err(err) if err.is_retryable() => {
                        Err(CondCheckError::NotYet { status: None })
                    }
                    Err(err) => Err(CondCheckError::Failed(err)),
                };
            result
        },
        &Duration::from_millis(100),
        &Duration::from_secs(30),
    )
    .await
    .expect("location became available");

    assert_eq!(
        location.switch_slot,
        SwitchSlot::Switch0,
        "cabled to switch slot 0"
    );
    assert_eq!(
        location.switch_serial.as_deref(),
        Some("SimSidecar0"),
        "switch 0 serial reported by sp-sim"
    );
    assert_eq!(
        location.sled_serial, None,
        "the harness sets no baseboard, so there is no sled serial"
    );

    ctx.teardown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_commission_start_update() {
    let gateway =
        gateway_setup::test_setup("test_commission_start_update", SpPort::One)
            .await;
    let ctx = WicketdTestContext::setup(gateway).await;
    let log = ctx.log();

    // Since a repository hasn't been uploaded yet, the described system version
    // is absent.
    let repo = ctx
        .commission_client
        .get_repository()
        .await
        .expect("get_repository succeeded")
        .into_inner();
    assert_eq!(
        repo.system_version, None,
        "no repository uploaded yet, so no system version"
    );

    // Wait until sled 0's SP is populated.
    wait_for_sp_inventory(&ctx, |sps| {
        sps.get(&SpIdentifier { typ: SpType::Sled, slot: 0 })
            .is_some_and(|sp| sp.state.is_some())
    })
    .await;

    // A sled absent from inventory is rejected with a 400 that names the
    // missing inventory state.
    let absent = StartUpdateParams {
        targets: UpdateTargets::single(SpIdentifier {
            typ: SpType::Sled,
            slot: 30,
        }),
        options: StartUpdateOptions::default(),
    };
    let err = ctx
        .commission_client
        .post_start_update(&absent)
        .await
        .expect_err("updating a sled absent from inventory is rejected");
    assert_client_error_message(
        &err,
        StatusCode::BAD_REQUEST,
        "no inventory state present",
    );

    // Now upload a fake TUF repository so a real update can start.
    let temp_dir = Utf8TempDir::new().expect("temp dir created");
    let archive_path = temp_dir.path().join("archive.zip");
    let args = tufaceous::Args::try_parse_from([
        "tufaceous",
        "assemble",
        "../update-common/manifests/fake.toml",
        archive_path.as_str(),
    ])
    .expect("args parsed correctly");
    args.exec(log).await.expect("assemble command completed successfully");
    let zip_bytes =
        fs_err::read(&archive_path).expect("archive read correctly");
    ctx.commission_client
        .put_repository(zip_bytes)
        .await
        .expect("put_repository succeeded");

    let params = StartUpdateParams {
        targets: UpdateTargets::single(SpIdentifier {
            typ: SpType::Sled,
            slot: 0,
        }),
        options: StartUpdateOptions::default(),
    };
    ctx.commission_client
        .post_start_update(&params)
        .await
        .expect("post_start_update on sled 0 succeeded");

    let entry = wait_for_sled0_progress(
        &ctx,
        "sled 0 reached Running with a running step",
        |p| {
            p.progress.state == UpdateState::Running
                && p.progress.innermost_running_steps().next().is_some()
        },
    )
    .await;
    assert_eq!(
        entry.progress.state,
        UpdateState::Running,
        "sled 0 rolls up to Running",
    );
    assert!(
        !entry.progress.steps.is_empty(),
        "at least one top-level step: {:?}",
        entry.progress.steps,
    );
    let running: Vec<_> = entry.progress.innermost_running_steps().collect();
    assert!(!running.is_empty(), "at least one running step: {running:?}");
    assert!(
        running.iter().all(|step| !step.description.is_empty()),
        "each running step has a description: {running:?}",
    );

    ctx.teardown().await;
}

// Test setting up RSS via the commissioning client.
#[tokio::test]
async fn test_commission_rss_config() {
    let gateway =
        gateway_setup::test_setup("test_commission_rss_config", SpPort::One)
            .await;
    let ctx = WicketdTestContext::setup(gateway).await;

    // We can't actually start RSS since we don't have a bootstrap agent in the
    // backend, but at least ensure that the endpoints return a 503.
    let err = ctx
        .commission_client
        .get_rack_setup_state()
        .await
        .expect_err("get_rack_setup_state fails without a bootstrap agent");
    assert_client_error_message(
        &err,
        StatusCode::SERVICE_UNAVAILABLE,
        "bootstrap agent address not yet known",
    );

    let err = ctx
        .commission_client
        .post_run_rack_setup()
        .await
        .expect_err("post_run_rack_setup fails without a bootstrap agent");
    assert_client_error_message(
        &err,
        StatusCode::SERVICE_UNAVAILABLE,
        "bootstrap agent address not yet known",
    );

    // Upload a certificate first -- wicketd will wait for the corresponding
    // key.
    let response = ctx
        .commission_client
        .post_rss_config_cert("a garbage certificate")
        .await
        .expect("post_rss_config_cert succeeded")
        .into_inner();
    assert_eq!(
        response,
        wicketd_commission_types_versions::latest::rack_setup::CertificateUploadResponse::WaitingOnKey,
    );

    // Upload the matching key. Validation will reject the garbage pair with a
    // 400.
    let err = ctx
        .commission_client
        .post_rss_config_key("a garbage key")
        .await
        .expect_err("post_rss_config_key rejects an invalid pair");
    assert_client_error(&err, StatusCode::BAD_REQUEST);

    // An invalid recovery password hash is rejected with a 400.
    let err = ctx
        .commission_client
        .put_rss_config_recovery_user_password_hash(&recovery_hash(
            "not-a-phc-string",
        ))
        .await
        .expect_err("invalid password hash rejected");
    assert_client_error(&err, StatusCode::BAD_REQUEST);

    // A valid PHC hash is accepted.
    ctx.commission_client
        .put_rss_config_recovery_user_password_hash(&recovery_hash(
            VALID_PHC_HASH,
        ))
        .await
        .expect("valid password hash accepted");

    // Clear out the user password hash.
    ctx.commission_client
        .delete_rss_config()
        .await
        .expect("delete_rss_config succeeded");

    // Wait until both simulated sleds' SPs are available.
    wait_for_sp_inventory(&ctx, |sps| {
        [0u16, 1].iter().all(|slot| {
            sps.get(&SpIdentifier { typ: SpType::Sled, slot: *slot })
                .is_some_and(|sp| sp.state.is_some())
        })
    })
    .await;

    // Test that a config with an unknown sled is rejected.
    let mut config = example_put_config();
    config.bootstrap_sleds = std::iter::once(200u16).collect();
    let err = ctx
        .commission_client
        .put_rss_config(&config)
        .await
        .expect_err("config referencing an unknown sled is rejected");
    assert_client_error(&err, StatusCode::BAD_REQUEST);

    // Ensure that roundtripping the RSS config works.
    let mut internal = ExampleRackSetupData::non_empty().put_insensitive;
    internal.bootstrap_sleds = [0u16, 1].into_iter().collect();

    ctx.commission_client
        .put_rss_config(&internal)
        .await
        .expect("config with sleds present in inventory accepted");

    let current = ctx
        .wicketd_client
        .get_rss_config()
        .await
        .expect("get_rss_config succeeded")
        .into_inner();

    let PutRssUserConfigInsensitive {
        bootstrap_sleds: expected_slots,
        ntp_servers: expected_ntp_servers,
        dns_servers: expected_dns_servers,
        internal_services_ip_pool_ranges: expected_pool_ranges,
        external_dns_ips: expected_external_dns_ips,
        external_dns_zone_name: expected_dns_zone_name,
        rack_network_config: expected_rack_network_config,
        allowed_source_ips: expected_allowed_source_ips,
        external_jumbo_frames_opt_in_enabled: expected_jumbo_frames,
    } = internal;

    let CurrentRssUserConfigInsensitive {
        bootstrap_sleds,
        ntp_servers,
        dns_servers,
        internal_services_ip_pool_ranges,
        external_dns_ips,
        external_dns_zone_name,
        rack_network_config,
        allowed_source_ips,
        external_jumbo_frames_opt_in_enabled,
    } = current.insensitive;

    let stored_slots: BTreeSet<u16> =
        bootstrap_sleds.iter().map(|sled| sled.id.slot).collect();
    assert_eq!(stored_slots, expected_slots, "bootstrap sled slots stored");
    assert_eq!(ntp_servers, expected_ntp_servers, "ntp_servers stored");
    assert_eq!(dns_servers, expected_dns_servers, "dns_servers stored");
    assert_eq!(
        internal_services_ip_pool_ranges, expected_pool_ranges,
        "internal_services_ip_pool_ranges stored"
    );
    assert_eq!(
        external_dns_ips, expected_external_dns_ips,
        "external_dns_ips stored"
    );
    assert_eq!(
        external_dns_zone_name, expected_dns_zone_name,
        "external_dns_zone_name stored"
    );
    assert_eq!(
        rack_network_config,
        Some(expected_rack_network_config),
        "rack_network_config round-tripped through the v1 conversions"
    );
    assert_eq!(
        allowed_source_ips,
        Some(expected_allowed_source_ips),
        "allowed_source_ips round-tripped through the v1 conversions"
    );
    assert_eq!(
        external_jumbo_frames_opt_in_enabled, expected_jumbo_frames,
        "external_jumbo_frames_opt_in_enabled stored"
    );

    let key_body = PutBgpAuthKeyBody {
        key: BgpAuthKey::TcpMd5 { key: "md5-secret".to_owned() },
    };
    let status = ctx
        .wicketd_client
        .put_bgp_auth_key(
            &"bgp-key-1".parse().expect("bgp-key-1 is a valid key ID"),
            &key_body,
        )
        .await
        .expect("valid BGP auth key ID accepted")
        .into_inner()
        .status;
    match status {
        wicketd_client::types::SetBgpAuthKeyStatus::Added => {}
        other => panic!("expected the key to be added, got {other:?}"),
    }

    ctx.teardown().await;
}

fn recovery_hash(hash: &str) -> PutRecoveryUserPasswordHash {
    PutRecoveryUserPasswordHash { hash: NewPasswordHash(hash.to_string()) }
}

fn sim_gimlet_state(serial_number: &str) -> SpStateInfo {
    SpStateInfo {
        serial_number: serial_number.to_string(),
        // sp-sim gimlets start in power state A0 (see sp-sim/src/gimlet.rs).
        power_state: PowerState::A0,
    }
}

fn sim_ignition_present() -> SpIgnitionInfo {
    // sp-sim's initial ignition state is powered on with no faults
    // (see sp-sim/src/sidecar.rs, initial_ignition_state).
    SpIgnitionInfo::Present {
        power: true,
        faults: IgnitionFaults { a3: false, a2: false, rot: false, sp: false },
    }
}

// A hash for the password "oxide". This is (obviously) only intended for
// transient deployments in development with no sensitive data or resources. You
// can change this value to any other supported hash. The only thing that needs
// to be changed with this hash are the instructions given to individuals
// running this program who then want to log in as this user. For more on what's
// supported, see the API docs for this type and the specific constraints in the
// nexus-passwords crate.
//
// The hash was generated via:
// `cargo run --example argon2 -- --input oxide`.
const VALID_PHC_HASH: &str = "$argon2id$v=19$m=98304,t=23,p=1$Effh/p6M2ZKdnpJFeGqtGQ$ZtUwcVODAvUAVK6EQ5FJMv+GMlUCo9PQQsy9cagL+EU";

fn example_put_config() -> PutRssUserConfigInsensitive {
    ExampleRackSetupData::non_empty().put_insensitive
}

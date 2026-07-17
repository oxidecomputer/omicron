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
    LocationInfo, SpIdentifier, SpInfo, SpType,
};
use wicketd_commission_types_versions::latest::update::{
    SpUpdateProgress, StartUpdateOptions, StartUpdateParams,
};

/// Wait for the SP inventory to become ready.
async fn wait_for_sp_inventory(
    ctx: &WicketdTestContext,
    ready: impl Fn(&IdOrdMap<SpInfo>) -> bool,
) -> IdOrdMap<SpInfo> {
    wait_for_condition(
        || async {
            let result: Cond<IdOrdMap<SpInfo>> =
                match ctx.commission_client.get_sp_inventory(None).await {
                    Ok(resp) => {
                        let sps = resp.into_inner();
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

    // Wait for MGS inventory to become available.
    // TODO-RAINCLAUDE: also wait for ignition so the fidelity assertions
    // TODO-RAINCLAUDE: below are deterministic.
    let sps = wait_for_sp_inventory(&ctx, |sps| {
        [0u16, 1].iter().all(|slot| {
            sps.get(&SpIdentifier { typ: SpType::Sled, slot: *slot })
                .is_some_and(|sp| {
                    sp.serial_number.is_some() && sp.ignition_present.is_some()
                })
        })
    })
    .await;

    assert_eq!(sps.len(), 4, "four simulated SPs");

    let sled0 = sps
        .get(&SpIdentifier { typ: SpType::Sled, slot: 0 })
        .expect("sled 0 present");
    assert_eq!(sled0.serial_number.as_deref(), Some("SimGimlet00"));

    let sled1 = sps
        .get(&SpIdentifier { typ: SpType::Sled, slot: 1 })
        .expect("sled 1 present");
    assert_eq!(sled1.serial_number.as_deref(), Some("SimGimlet01"));

    // TODO-RAINCLAUDE: conversion-fidelity guards for sp_info_to_v1's
    // TODO-RAINCLAUDE: hand-mapped fields: power_state travels in lockstep
    // TODO-RAINCLAUDE: with serial_number, and sp-sim reports ignition.
    for sled in [&sled0, &sled1] {
        assert!(
            sled.power_state.is_some(),
            "power_state read in lockstep with serial_number: {sled:?}"
        );
        assert_eq!(
            sled.ignition_present,
            Some(true),
            "sp-sim sleds report ignition present"
        );
    }

    // TODO-RAINCLAUDE: exercise the force_refresh branch through the
    // TODO-RAINCLAUDE: commission client.
    let refreshed = ctx
        .commission_client
        .get_sp_inventory(Some(true))
        .await
        .expect("get_sp_inventory with force_refresh succeeded")
        .into_inner();
    assert_eq!(refreshed.len(), 4, "four simulated SPs after forced refresh");
    let refreshed_sled0 = refreshed
        .get(&SpIdentifier { typ: SpType::Sled, slot: 0 })
        .expect("sled 0 present after forced refresh");
    assert_eq!(
        refreshed_sled0.serial_number.as_deref(),
        Some("SimGimlet00"),
        "sled 0 serial after forced refresh: {refreshed:?}"
    );

    let bootstrap = ctx
        .commission_client
        .get_bootstrap_sleds()
        .await
        .expect("get_bootstrap_sleds succeeded")
        .into_inner();
    // TODO-RAINCLAUDE: IdOrdMap iterates in key order (baseboard serial), so the
    // TODO-RAINCLAUDE: identifiers come back sorted without an explicit sort.
    let identifiers: Vec<_> =
        bootstrap.iter().map(|s| s.identifier.clone()).collect();
    assert_eq!(identifiers, vec!["SimGimlet00", "SimGimlet01"]);
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

    assert_eq!(location.switch_slot, 0, "cabled to switch slot 0");
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

// TODO-RAINCLAUDE: exercises post_start_update validation and a real start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_commission_start_update() {
    let gateway =
        gateway_setup::test_setup("test_commission_start_update", SpPort::One)
            .await;
    let ctx = WicketdTestContext::setup(gateway).await;
    let log = ctx.log();

    // TODO-RAINCLAUDE: with no repository uploaded yet, the described system
    // TODO-RAINCLAUDE: version is absent.
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

    // TODO-RAINCLAUDE: Wait until sled 0 has SP state so it is a valid target.
    wait_for_sp_inventory(&ctx, |sps| {
        sps.get(&SpIdentifier { typ: SpType::Sled, slot: 0 })
            .is_some_and(|sp| sp.serial_number.is_some())
    })
    .await;

    // TODO-RAINCLAUDE: Empty targets are rejected with a 400.
    let empty = StartUpdateParams {
        targets: BTreeSet::new(),
        options: StartUpdateOptions::default(),
    };
    let err = ctx
        .commission_client
        .post_start_update(&empty)
        .await
        .expect_err("empty update targets are rejected");
    assert_client_error_message(
        &err,
        StatusCode::BAD_REQUEST,
        "No update targets specified",
    );

    // TODO-RAINCLAUDE: A sled slot absent from inventory is rejected with a 400
    // TODO-RAINCLAUDE: that names the missing inventory state.
    let absent = StartUpdateParams {
        targets: std::iter::once(SpIdentifier { typ: SpType::Sled, slot: 30 })
            .collect(),
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

    // TODO-RAINCLAUDE: Upload a fake TUF repository so a real update can start.
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

    // TODO-RAINCLAUDE: A valid start on sled 0 succeeds and drives the update
    // TODO-RAINCLAUDE: into progress; stop once it reports InProgress.
    let params = StartUpdateParams {
        targets: std::iter::once(SpIdentifier { typ: SpType::Sled, slot: 0 })
            .collect(),
        options: StartUpdateOptions::default(),
    };
    ctx.commission_client
        .post_start_update(&params)
        .await
        .expect("post_start_update on sled 0 succeeded");

    let entry =
        wait_for_sled0_progress(&ctx, "sled 0 reached InProgress", |p| {
            matches!(p, SpUpdateProgress::InProgress { .. })
        })
        .await;
    match entry.progress {
        SpUpdateProgress::InProgress { total_steps, description, .. } => {
            assert!(total_steps >= 1, "at least one top-level step");
            assert!(
                !description.is_empty(),
                "the running step has a description"
            );
        }
        other => panic!("expected InProgress, got {other:?}"),
    }

    ctx.teardown().await;
}

// TODO-RAINCLAUDE: exercises the RSS cert/key/password cycle and boundary validation errors.
#[tokio::test]
async fn test_commission_rss_config() {
    let gateway =
        gateway_setup::test_setup("test_commission_rss_config", SpPort::One)
            .await;
    let ctx = WicketdTestContext::setup(gateway).await;

    // TODO-RAINCLAUDE: the test harness runs no bootstrap-agent lockstep server
    // TODO-RAINCLAUDE: and discovers no bootstrap peers, so both rack-setup
    // TODO-RAINCLAUDE: endpoints surface the retryable 503 from the lockstep
    // TODO-RAINCLAUDE: client before any RSS-specific logic (status projection or
    // TODO-RAINCLAUDE: config validation) runs. This still exercises that the
    // TODO-RAINCLAUDE: endpoints are wired up and propagate the lockstep error.
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

    // TODO-RAINCLAUDE: Wait until both simulated sleds have SP state (serials read):
    // TODO-RAINCLAUDE: put_rss_config resolves bootstrap slots against the sled inventory,
    // TODO-RAINCLAUDE: which only contains SPs whose state has been read from MGS.
    wait_for_sp_inventory(&ctx, |sps| {
        [0u16, 1].iter().all(|slot| {
            sps.get(&SpIdentifier { typ: SpType::Sled, slot: *slot })
                .is_some_and(|sp| sp.serial_number.is_some())
        })
    })
    .await;

    // TODO-RAINCLAUDE: submit a config referencing a nonexistent sled slot: the shape is
    // TODO-RAINCLAUDE: valid (the body deserializes) but the sled is not in inventory, so
    // TODO-RAINCLAUDE: this fails with a 400.
    let mut config = example_put_config();
    config.bootstrap_sleds = std::iter::once(200u16).collect();
    let err = ctx
        .commission_client
        .put_rss_config(&config)
        .await
        .expect_err("config referencing an unknown sled is rejected");
    assert_client_error(&err, StatusCode::BAD_REQUEST);

    // TODO-RAINCLAUDE: positive-path conversion fidelity: PUT a config whose bootstrap
    // TODO-RAINCLAUDE: slots match the sp-sim inventory, then read the stored config back
    // TODO-RAINCLAUDE: through the unstable wicketd API and deep-compare it field-by-field
    // TODO-RAINCLAUDE: against the wicket-common original; a swapped or dropped field in
    // TODO-RAINCLAUDE: the manual conversions fails here.
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

    // TODO-RAINCLAUDE: a valid, Name-conformant key ID referenced by the
    // TODO-RAINCLAUDE: uploaded config is accepted.
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

// TODO-RAINCLAUDE: a manual port config with a misspelled field must be
// TODO-RAINCLAUDE: rejected outright, not silently accepted as a DDM-auto port.
// TODO-RAINCLAUDE: This uses a raw request because the typed client cannot
// TODO-RAINCLAUDE: express the malformed body; the body is rejected at parse
// TODO-RAINCLAUDE: time (deny_unknown_fields), before any inventory lookup.
#[tokio::test]
async fn test_commission_rss_config_rejects_unknown_port_field() {
    use dropshot::test_util::ClientTestContext;

    let gateway = gateway_setup::test_setup(
        "test_commission_rss_config_rejects_unknown_port_field",
        SpPort::One,
    )
    .await;
    let ctx = WicketdTestContext::setup(gateway).await;

    let raw = ClientTestContext::new(
        std::net::SocketAddr::V6(ctx.commission_addr),
        ctx.log().new(slog::o!("component" => "commission raw client")),
    );

    // TODO-RAINCLAUDE: start from a valid config, then misspell `routes` as
    // TODO-RAINCLAUDE: `route` in a manual port config.
    let mut body = serde_json::to_value(example_put_config())
        .expect("example config serializes to JSON");
    let port = body["rack_network_config"]["switch0"]["port0"]
        .as_object_mut()
        .expect("switch0 port0 is a manual port object");
    let routes = port.remove("routes").expect("manual port has routes");
    port.insert("route".to_owned(), routes);

    let request = http::Request::builder()
        .method(http::Method::PUT)
        .uri(raw.url("/rack-setup/config"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(omicron_common::api::VERSION_HEADER, "1.0.0")
        .body(serde_json::to_string(&body).unwrap().into())
        .expect("request builds");

    let error = raw
        .make_request_with_request(request, StatusCode::BAD_REQUEST)
        .await
        .expect_err("a misspelled manual port field is rejected with a 400");
    assert!(
        error.message.contains("unknown field `route`"),
        "the 400 names the misspelled manual port-config field: {:?}",
        error.message,
    );

    // TODO-RAINCLAUDE: version negotiation runs during routing, before the
    // TODO-RAINCLAUDE: handler (and any MGS 503), so both rejections are
    // TODO-RAINCLAUDE: deterministic and need no polling. A version newer than
    // TODO-RAINCLAUDE: the server supports is rejected with a 400.
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(raw.url("/repository"))
        .header(omicron_common::api::VERSION_HEADER, "99.0.0")
        .body(String::new().into())
        .expect("request builds");
    let error = raw
        .make_request_with_request(request, StatusCode::BAD_REQUEST)
        .await
        .expect_err("an unsupported API version is rejected with a 400");
    assert!(
        error.message.contains("server does not support this API version"),
        "the 400 names the unsupported API version: {:?}",
        error.message,
    );

    // TODO-RAINCLAUDE: with no version header at all, the server rejects the
    // TODO-RAINCLAUDE: request rather than assuming a version (no on_missing is
    // TODO-RAINCLAUDE: configured).
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(raw.url("/repository"))
        .body(String::new().into())
        .expect("request builds");
    let error = raw
        .make_request_with_request(request, StatusCode::BAD_REQUEST)
        .await
        .expect_err("a missing API version header is rejected with a 400");
    assert!(
        error.message.contains("missing expected header"),
        "the 400 names the missing version header: {:?}",
        error.message,
    );

    ctx.teardown().await;
}

fn recovery_hash(
    hash: &str,
) -> wicketd_commission_types_versions::latest::rack_setup::PutRecoveryUserPasswordHash
{
    use wicketd_commission_types_versions::latest::rack_setup::{
        NewPasswordHash, PutRecoveryUserPasswordHash,
    };
    PutRecoveryUserPasswordHash { hash: NewPasswordHash(hash.to_string()) }
}

// TODO-RAINCLAUDE: A valid Argon2id PHC hash with secure parameters, taken from
// TODO-RAINCLAUDE: sled-agent's simulated-server recovery-user fixture
// TODO-RAINCLAUDE: (sled-agent/src/sim/server.rs).
const VALID_PHC_HASH: &str = "$argon2id$v=19$m=98304,t=23,p=1$Effh/p6M2ZKdnpJFeGqtGQ$ZtUwcVODAvUAVK6EQ5FJMv+GMlUCo9PQQsy9cagL+EU";

fn example_put_config() -> PutRssUserConfigInsensitive {
    ExampleRackSetupData::non_empty().put_insensitive
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for the simulated sled agent's Helios RoT.

use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use dice_verifier::helios_rot::verify_attestation;
use omicron_sled_agent::sim;
use omicron_test_utils::dev::test_setup_log;
use omicron_uuid_kinds::{RackUuid, SledUuid};
use sled_agent_types::rot::{Attestation, QualifyingData, Rot};
use slog::Logger;
use std::sync::Arc;
use x509_cert::{Certificate, der::DecodePem};

/// Write a self-signed P-384 certificate and its PKCS#8 private key to `dir`,
/// returning a config for the simulated Helios RoT that uses them.
fn gen_helios_rot_config(dir: &Utf8Path) -> sim::ConfigHeliosRot {
    let mut params =
        rcgen::CertificateParams::new(vec!["sim-helios-rot".to_string()]);
    params.alg = &rcgen::PKCS_ECDSA_P384_SHA384;
    let cert = rcgen::Certificate::from_params(params).unwrap();

    let priv_key = dir.join("helios-alias.key.pem");
    let cert_chain = dir.join("helios-alias.certlist.pem");
    std::fs::write(&priv_key, cert.serialize_private_key_pem()).unwrap();
    std::fs::write(&cert_chain, cert.serialize_pem().unwrap()).unwrap();

    sim::ConfigHeliosRot { priv_key, cert_chain }
}

fn sim_config(helios_rot: Option<sim::ConfigHeliosRot>) -> sim::Config {
    sim::Config {
        helios_rot,
        ..sim::Config::for_testing(
            SledUuid::new_v4(),
            RackUuid::new_v4(),
            sim::SimMode::Explicit,
            None,
            sim::ZpoolConfig::None,
            sim::SledCpuFamily::AmdMilan,
        )
    }
}

async fn start(
    config: &sim::Config,
    log: &Logger,
) -> anyhow::Result<sim::Server> {
    let simulated_upstairs = Arc::new(sim::SimulatedUpstairs::new(log.clone()));
    sim::Server::start(
        config,
        log,
        sim::NexusRegistration::Manual,
        &simulated_upstairs,
        0,
    )
    .await
}

fn client(server: &sim::Server, log: &Logger) -> sled_agent_client::Client {
    sled_agent_client::Client::new(
        &format!("http://{}", server.http_server.local_addr()),
        log.clone(),
    )
}

fn assert_unimplemented<T>(
    result: Result<
        T,
        sled_agent_client::Error<sled_agent_client::types::Error>,
    >,
) {
    let err = result.err().expect("request should fail");
    assert_eq!(
        err.status(),
        Some(http::StatusCode::METHOD_NOT_ALLOWED),
        "unexpected error: {err}"
    );
}

/// With a Helios RoT configured, the simulated sled agent serves the Helios
/// RoT endpoints using the mock RoT.
#[tokio::test]
async fn sim_helios_rot_attestation() {
    let logctx = test_setup_log("sim_helios_rot_attestation");
    let log = &logctx.log;
    let tmp = Utf8TempDir::new().unwrap();
    let config = sim_config(Some(gen_helios_rot_config(tmp.path())));
    let server = start(&config, log).await.expect("started sim sled agent");
    let client = client(&server, log);

    let log_response = client
        .rot_measurement_log(&Rot::Helios)
        .await
        .expect("got measurement log")
        .into_inner();
    assert!(log_response.log.is_none(), "Helios RoT has no measurement log");

    let chain = client
        .rot_certificate_chain(&Rot::Helios)
        .await
        .expect("got certificate chain")
        .into_inner();
    assert_eq!(chain.0.len(), 1);
    let alias = Certificate::from_pem(&chain.0[0]).unwrap();

    let qdata = [7u8; 48];
    let attestation = client
        .rot_attest(&Rot::Helios, &QualifyingData::Q48(qdata))
        .await
        .expect("got attestation")
        .into_inner();
    let Attestation::P384(sig) = attestation else {
        panic!("expected a P-384 attestation");
    };
    verify_attestation(
        &alias,
        &helios_rot::Attestation::P384(helios_rot::Array(sig.0)),
        &helios_rot::QualifyingData::Q48(helios_rot::Array(qdata)),
    )
    .expect("attestation should verify against the alias cert");

    // Only 48 bytes of qualifying data are accepted by the Helios RoT.
    let err = client
        .rot_attest(&Rot::Helios, &QualifyingData::Q32([7u8; 32]))
        .await
        .err()
        .expect("32 bytes of qualifying data should be rejected");
    assert_eq!(err.status(), Some(http::StatusCode::BAD_REQUEST));

    // The Oxide RoT is not simulated.
    assert_unimplemented(client.rot_certificate_chain(&Rot::Oxide).await);

    server.http_server.close().await.expect("closed sim sled agent");
    logctx.cleanup_successful();
}

/// Without a Helios RoT configured, the RoT endpoints remain unimplemented.
#[tokio::test]
async fn sim_helios_rot_unconfigured() {
    let logctx = test_setup_log("sim_helios_rot_unconfigured");
    let log = &logctx.log;
    let server =
        start(&sim_config(None), log).await.expect("started sim sled agent");
    let client = client(&server, log);

    for rot in [Rot::Helios, Rot::Oxide] {
        assert_unimplemented(client.rot_measurement_log(&rot).await);
    }
    assert_unimplemented(client.rot_certificate_chain(&Rot::Helios).await);
    assert_unimplemented(
        client.rot_attest(&Rot::Helios, &QualifyingData::Q48([7u8; 48])).await,
    );

    server.http_server.close().await.expect("closed sim sled agent");
    logctx.cleanup_successful();
}

/// A Helios RoT config that can't be loaded prevents the sim from starting.
#[tokio::test]
async fn sim_helios_rot_bad_config() {
    let logctx = test_setup_log("sim_helios_rot_bad_config");
    let tmp = Utf8TempDir::new().unwrap();
    let config = sim_config(Some(sim::ConfigHeliosRot {
        priv_key: tmp.path().join("missing.key.pem"),
        cert_chain: tmp.path().join("missing.certlist.pem"),
    }));
    let Err(err) = start(&config, &logctx.log).await else {
        panic!("sim sled agent should fail to start");
    };
    assert!(
        format!("{err:#}").contains("missing.certlist.pem"),
        "unexpected error: {err:#}"
    );
    logctx.cleanup_successful();
}

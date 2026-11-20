// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for the RoT endpoints of the simulated sled agent.

use attest_mock::MockData;
use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use dice_verifier::oxide_rot::verify_attestation;
use omicron_sled_agent::sim::{
    Config, ConfigRot, NexusRegistration, Server, SimMode, SimulatedUpstairs,
    SledCpuFamily, ZpoolConfig,
};
use omicron_test_utils::dev::test_setup_log;
use omicron_uuid_kinds::{RackUuid, SledUuid};
use sled_agent_types::rot::{Attestation, Measurement, Nonce, Rot};
use slog::Logger;
use sprockets_tls_test_utils::{
    OutputFileExistsBehavior, alias_prefix, certlist_path, private_key_path,
};
use std::sync::Arc;
use x509_cert::{Certificate, der::DecodePem};

// An arbitrary digest for the single measurement in the mock log.
const DIGEST: &str =
    "be4df4e085175f3de0c8ac4837e1c2c9a34e8983209dac6b549e94154f7cdd9c";

/// Generate the artifacts required to mock the Oxide RoT in `dir`
fn gen_rot_config(dir: &Utf8Path) -> ConfigRot {
    // tests in this module need a single cert chain to represent the
    // attestation signer
    const LEAF_COUNT: usize = 1;

    let behavior = OutputFileExistsBehavior::Overwrite;

    let doc = sprockets_tls_test_utils::generate_config(LEAF_COUNT);
    doc.write_key_pairs(dir.to_owned(), behavior).unwrap();
    doc.write_certificates(dir.to_owned(), behavior).unwrap();
    doc.write_certificate_lists(dir.to_owned(), behavior).unwrap();

    let log_kdl = format!(
        r#"
        measurement {{
            algorithm "sha3-256"
            digest "{DIGEST}"
        }}
        "#
    );

    let log = attest_mock::MockLog::parse("log.kdl", &log_kdl)
        .unwrap()
        .to_bytes()
        .unwrap();
    std::fs::write(dir.join("log.bin"), &log).unwrap();

    // sprockets test utils / pki-playground append a positive integer to
    // the file name of the generated leaf. We've only created a single
    // leaf & cert chain so this must be `1`
    let alias = alias_prefix(LEAF_COUNT);

    ConfigRot {
        alias_key: private_key_path(dir.to_owned(), &alias),
        cert_chain: certlist_path(dir.to_owned(), &alias),
        log: dir.join("log.bin"),
    }
}

/// Start a simulated sled agent with the provided RoT config
async fn start_sim(
    log: &Logger,
    rot: Option<ConfigRot>,
) -> Result<Server, anyhow::Error> {
    let config = Config {
        rot,
        ..Config::for_testing(
            SledUuid::new_v4(),
            RackUuid::new_v4(),
            SimMode::Explicit,
            None,
            ZpoolConfig::None,
            SledCpuFamily::AmdMilan,
        )
    };
    let upstairs = Arc::new(SimulatedUpstairs::new(log.clone()));

    // We don't need Nexus, use `Background` registration policy to avoid
    // having to setup one up.
    // NOTE: This may be an abuse of `Background`. According to the docs this
    // will cause the server to retry in the background. This may have side
    // effects.
    Server::start(&config, log, NexusRegistration::Background, &upstairs, 0)
        .await
}

fn client(log: &Logger, server: &Server) -> sled_agent_client::Client {
    sled_agent_client::Client::new(
        &format!("http://{}", server.http_server.local_addr()),
        log.clone(),
    )
}

/// Get and appraise an attestation from the mock RoT through the simulated
/// sled agent's HTTP API
#[tokio::test]
async fn sim_rot_verify_attestation() {
    let logctx = test_setup_log("sim_rot_verify_attestation");
    let tmp = Utf8TempDir::new().unwrap();
    let rot_config = gen_rot_config(tmp.path());

    let server = start_sim(&logctx.log, Some(rot_config)).await.unwrap();
    let client = client(&logctx.log, &server);

    // get attestation artifacts from the RoT
    let log =
        client.rot_measurement_log(&Rot::Oxide).await.unwrap().into_inner();
    let chain =
        client.rot_certificate_chain(&Rot::Oxide).await.unwrap().into_inner();
    let nonce = [7u8; 32];
    let attestation = client
        .rot_attest(&Rot::Oxide, &Nonce::N32(nonce))
        .await
        .unwrap()
        .into_inner();

    // ensure the artifacts we got back are (generally) in the expected form
    assert_eq!(log.0.len(), 1);
    let Measurement::Sha3_256(digest) = &log.0[0];
    assert_eq!(hex::encode(digest.0), DIGEST);

    assert!(!chain.0.is_empty());

    // Convert the API types back to those understood by the verifier
    let mut rot_log = oxide_rot::Log::default();
    for Measurement::Sha3_256(digest) in log.0 {
        assert!(
            rot_log.push(oxide_rot::Measurement::Sha3_256(digest.0.into()))
        );
    }
    let Attestation::Ed25519(sig) = attestation;
    let rot_attestation = oxide_rot::Attestation::Ed25519(sig.0.into());
    let qdata = oxide_rot::QualifyingData::try_from(&nonce[..]).unwrap();

    // The first cert in the chain is the leaf. This cert holds the public
    // part of the key used to sign the attestation.
    let alias = Certificate::from_pem(&chain.0[0]).unwrap();
    verify_attestation(&alias, &rot_attestation, &rot_log, &qdata)
        .expect("attestation should verify against the alias cert");

    logctx.cleanup_successful();
}

/// Without RoT artifacts the RoT endpoints remain unimplemented
#[tokio::test]
async fn sim_no_rot() {
    let logctx = test_setup_log("sim_no_rot");

    let server = start_sim(&logctx.log, None).await.unwrap();
    let client = client(&logctx.log, &server);

    let not_allowed = Some(http::StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        client
            .rot_measurement_log(&Rot::Oxide)
            .await
            .err()
            .expect("request should fail")
            .status(),
        not_allowed
    );
    assert_eq!(
        client
            .rot_certificate_chain(&Rot::Oxide)
            .await
            .err()
            .expect("request should fail")
            .status(),
        not_allowed
    );
    assert_eq!(
        client
            .rot_attest(&Rot::Oxide, &Nonce::N32([7u8; 32]))
            .await
            .err()
            .expect("request should fail")
            .status(),
        not_allowed
    );

    logctx.cleanup_successful();
}

/// The simulated sled agent fails to start if the RoT artifacts can't be
/// loaded
#[tokio::test]
async fn sim_rot_missing_artifact() {
    let logctx = test_setup_log("sim_rot_missing_artifact");
    let tmp = Utf8TempDir::new().unwrap();
    let mut rot_config = gen_rot_config(tmp.path());
    rot_config.log = tmp.path().join("does-not-exist.bin");

    let Err(err) = start_sim(&logctx.log, Some(rot_config)).await else {
        panic!("sim started with a missing RoT artifact");
    };
    assert!(
        format!("{err:#}").contains("creating mock RoT"),
        "unexpected error: {err:#}"
    );

    logctx.cleanup_successful();
}

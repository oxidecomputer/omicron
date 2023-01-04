// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for operating on certificates

use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_certificate;
use nexus_test_utils::resource_helpers::delete_certificate;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views::Certificate;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::external_api::params;
use std::io::Write;
use std::sync::Arc;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Utility structure for making a test certificate
struct CertificateChain {
    root_cert: rustls::Certificate,
    intermediate_cert: rustls::Certificate,
    end_cert: rustls::Certificate,
    end_keypair: rcgen::Certificate,
}

impl CertificateChain {
    fn new() -> Self {
        let mut root_params = rcgen::CertificateParams::new(vec![]);
        root_params.is_ca =
            rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let root_keypair = rcgen::Certificate::from_params(root_params)
            .expect("failed to generate root keys");

        let mut intermediate_params = rcgen::CertificateParams::new(vec![]);
        intermediate_params.is_ca =
            rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let intermediate_keypair =
            rcgen::Certificate::from_params(intermediate_params)
                .expect("failed to generate intermediate keys");

        let params = rcgen::CertificateParams::new(vec!["localhost".into()]);
        let end_keypair = rcgen::Certificate::from_params(params)
            .expect("failed to generate end-entity keys");

        let root_cert = rustls::Certificate(
            root_keypair
                .serialize_der()
                .expect("failed to serialize root cert"),
        );
        let intermediate_cert = rustls::Certificate(
            intermediate_keypair
                .serialize_der_with_signer(&root_keypair)
                .expect("failed to serialize intermediate cert"),
        );
        let end_cert = rustls::Certificate(
            end_keypair
                .serialize_der_with_signer(&intermediate_keypair)
                .expect("failed to serialize end-entity cert"),
        );

        Self { root_cert, intermediate_cert, end_cert, end_keypair }
    }

    fn end_cert_private_key(&self) -> rustls::PrivateKey {
        rustls::PrivateKey(self.end_keypair.serialize_private_key_der())
    }

    fn cert_chain(&self) -> Vec<rustls::Certificate> {
        vec![
            self.end_cert.clone(),
            self.intermediate_cert.clone(),
            self.root_cert.clone(),
        ]
    }

    // Issues a GET request using the certificate chain.
    async fn do_request(
        &self,
        client: &dropshot::test_util::ClientTestContext,
        scheme: http::uri::Scheme,
    ) -> Result<(), hyper::Error> {
        let address = client.bind_address;
        let port = address.port();
        let uri: hyper::Uri =
            format!("{scheme}://localhost:{port}/").parse().unwrap();
        let request = hyper::Request::builder()
            .method(http::method::Method::GET)
            .uri(&uri)
            .body(hyper::Body::empty())
            .unwrap();

        match scheme.as_str() {
            "http" => {
                let http_client = hyper::Client::builder().build_http();
                http_client.request(request).await.map(|_| ())
            }
            "https" => {
                let make_pki_verifier = || {
                    let mut root_store =
                        rustls::RootCertStore { roots: vec![] };
                    root_store.add(&self.root_cert).expect("adding root cert");
                    rustls::client::WebPkiVerifier::new(root_store, None)
                };

                let tls_config = rustls::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_custom_certificate_verifier(Arc::new(
                        make_pki_verifier(),
                    ))
                    .with_no_client_auth();
                let https_connector =
                    hyper_rustls::HttpsConnectorBuilder::new()
                        .with_tls_config(tls_config)
                        .https_only()
                        .enable_http1()
                        .build();
                let https_client =
                    hyper::Client::builder().build(https_connector);
                https_client.request(request).await.map(|_| ())
            }
            _ => panic!("Unsupported scheme"),
        }
    }
}

fn tls_cert_to_pem(certs: &Vec<rustls::Certificate>) -> Vec<u8> {
    let mut serialized_certs = vec![];
    let mut cert_writer = std::io::BufWriter::new(&mut serialized_certs);
    for cert in certs {
        let encoded_cert = pem::encode(&pem::Pem {
            tag: "CERTIFICATE".to_string(),
            contents: cert.0.clone(),
        });
        cert_writer
            .write(encoded_cert.as_bytes())
            .expect("failed to serialize cert");
    }
    drop(cert_writer);
    serialized_certs
}

fn tls_key_to_pem(key: &rustls::PrivateKey) -> Vec<u8> {
    let mut serialized_key = vec![];
    let mut key_writer = std::io::BufWriter::new(&mut serialized_key);
    let encoded_key = pem::encode(&pem::Pem {
        tag: "PRIVATE KEY".to_string(),
        contents: key.0.clone(),
    });
    key_writer.write(encoded_key.as_bytes()).expect("failed to serialize key");
    drop(key_writer);
    serialized_key
}

const CERTS_URL: &str = "/system/certificates";
const CERT_NAME: &str = "my-certificate";

async fn certs_list(client: &ClientTestContext) -> Vec<Certificate> {
    NexusRequest::iter_collection_authn(client, CERTS_URL, "", None)
        .await
        .expect("failed to list certificates")
        .all_items
}

async fn cert_get(client: &ClientTestContext, cert_name: &str) -> Certificate {
    let cert_url = format!("{CERTS_URL}/{}", cert_name);
    NexusRequest::object_get(client, &cert_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to issue GET requst for certificate")
        .parsed_body()
        .expect("failed to parse response for certificate GET request")
}

async fn cert_get_expect_not_found(client: &ClientTestContext) {
    let cert_url = format!("{CERTS_URL}/{}", CERT_NAME);
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, &cert_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        format!("not found: certificate with name \"{}\"", CERT_NAME)
    );
}

async fn cert_create_expect_error(
    client: &ClientTestContext,
    cert: Vec<u8>,
    key: Vec<u8>,
) -> String {
    let url = CERTS_URL.to_string();
    let params = params::CertificateCreate {
        identity: IdentityMetadataCreateParams {
            name: CERT_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        cert,
        key,
    };

    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        &url,
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .expect("failed to parse error response")
    .message
}

async fn cert_delete_expect_not_found(client: &ClientTestContext) {
    let cert_url = format!("{CERTS_URL}/{}", CERT_NAME);
    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::DELETE,
        &cert_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!("not found: certificate with name \"{}\"", CERT_NAME),
    );
}

#[nexus_test]
async fn test_not_found_before_creation(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let certs = certs_list(&client).await;
    assert!(certs.is_empty());

    // Make sure we get a 404 if we fetch one.
    cert_get_expect_not_found(&client).await;

    // We should also get a 404 if we delete one.
    cert_delete_expect_not_found(&client).await;
}

#[nexus_test]
async fn test_crud(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let certs = certs_list(&client).await;
    assert!(certs.is_empty());

    let chain = CertificateChain::new();
    let (cert, key) = (chain.cert_chain(), chain.end_cert_private_key());
    let (cert, key) = (tls_cert_to_pem(&cert), tls_key_to_pem(&key));

    // We can create a new certificate
    create_certificate(&client, CERT_NAME, cert.clone(), key.clone()).await;

    // The certificate can be listed or accessed directly
    let list = certs_list(&client).await;
    assert_eq!(list.len(), 1, "Expected exactly one certificate");
    assert_eq!(list[0].identity.name, CERT_NAME);

    let fetched_cert = cert_get(&client, CERT_NAME).await;
    assert_eq!(fetched_cert.identity.name, CERT_NAME);

    // We can delete the certificate, and it no longer appears in the API
    delete_certificate(&client, CERT_NAME).await;
    let certs = certs_list(&client).await;
    assert!(certs.is_empty());
    cert_get_expect_not_found(&client).await;
    cert_delete_expect_not_found(&client).await;
}

#[tokio::test]
async fn test_refresh() {
    // Load the default config, but force the external dropshot server to use
    // HTTPS.
    let mut config = nexus_test_utils::load_test_config();

    let chain = CertificateChain::new();
    let (cert, key) = (chain.cert_chain(), chain.end_cert_private_key());
    let (cert_u8, key_u8) = (tls_cert_to_pem(&cert), tls_key_to_pem(&key));

    config.deployment.dropshot_external.push(dropshot::ConfigDropshot {
        tls: Some(dropshot::ConfigTls::AsBytes {
            certs: cert_u8.clone(),
            key: key_u8.clone(),
        }),
        ..Default::default()
    });

    // Actually do the test setup typically implied by "nexus_test".
    let cptestctx = nexus_test_utils::test_setup_with_config::<
        omicron_nexus::Server,
    >("test_refresh", &mut config)
    .await;

    let clients = &cptestctx.external_clients().await;
    let http_client = &clients[0];
    let https_client = &clients[1];

    let mut root_certs = rustls::RootCertStore::empty();
    root_certs.add(&chain.root_cert).expect("Failed to add certificate");

    // We can use HTTP on the HTTP interface, and HTTPS on the HTTPS
    // interface...
    chain.do_request(&http_client, http::uri::Scheme::HTTP).await.unwrap();
    chain.do_request(&https_client, http::uri::Scheme::HTTPS).await.unwrap();

    // ... but not vice-versa.
    chain.do_request(&http_client, http::uri::Scheme::HTTPS).await.unwrap_err();
    chain.do_request(&https_client, http::uri::Scheme::HTTP).await.unwrap_err();

    // Assert the auto-loaded certificate was given a default name.
    cert_get(&http_client, "default-1").await;

    // Remove the default certificate, add a new one.
    //
    // NOTE: We're doing this on the "HTTP client" interface only because dropshot
    // makes a hard-coded assumption that the test client is not using HTTPS:
    // https://docs.rs/dropshot/0.8.0/src/dropshot/test_util.rs.html#106
    let chain2 = CertificateChain::new();
    let (cert, key) = (chain2.cert_chain(), chain2.end_cert_private_key());
    let (cert_u8, key_u8) = (tls_cert_to_pem(&cert), tls_key_to_pem(&key));
    delete_certificate(&http_client, "default-1").await;
    create_certificate(
        &http_client,
        CERT_NAME,
        cert_u8.clone(),
        key_u8.clone(),
    )
    .await;

    // Requests through the old certificate chain fail -- it was removed.
    chain
        .do_request(&https_client, http::uri::Scheme::HTTPS)
        .await
        .unwrap_err();
    // Requests through the new certificate chain succeed.
    chain2.do_request(&https_client, http::uri::Scheme::HTTPS).await.unwrap();

    cptestctx.teardown().await;
}

#[nexus_test]
async fn test_cannot_create_certificate_with_bad_key(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let chain = CertificateChain::new();
    let (cert, key) = (chain.cert_chain(), chain.end_cert_private_key());
    let (cert, mut key) = (tls_cert_to_pem(&cert), tls_key_to_pem(&key));

    for i in 0..key.len() {
        key[i] = !key[i];
    }
    cert_create_expect_error(&client, cert, key).await;
}

#[nexus_test]
async fn test_cannot_create_certificate_with_bad_cert(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let chain = CertificateChain::new();
    let (cert, key) = (chain.cert_chain(), chain.end_cert_private_key());
    let (mut cert, key) = (tls_cert_to_pem(&cert), tls_key_to_pem(&key));

    for i in 0..cert.len() {
        cert[i] = !cert[i];
    }
    cert_create_expect_error(&client, cert, key).await;
}

// TODO: Follow-up work:
// - Does deleting a cert cause a refresh? Should it?
// - Do we add an internal Nexus API to notify that a refresh should happen?

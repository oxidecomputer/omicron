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
pub struct CertificateChain {
    root_cert: rustls::Certificate,
    intermediate_cert: rustls::Certificate,
    end_cert: rustls::Certificate,
    end_keypair: rcgen::Certificate,
}

impl CertificateChain {
    pub fn new() -> Self {
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

    pub fn end_cert_private_key_as_pem(&self) -> Vec<u8> {
        tls_key_to_pem(&self.end_cert_private_key())
    }

    fn cert_chain(&self) -> Vec<rustls::Certificate> {
        vec![
            self.end_cert.clone(),
            self.intermediate_cert.clone(),
            self.root_cert.clone(),
        ]
    }

    pub fn cert_chain_as_pem(&self) -> Vec<u8> {
        tls_cert_to_pem(&self.cert_chain())
    }

    fn make_pki_verifier(&self) -> rustls::client::WebPkiVerifier {
        let mut root_store = rustls::RootCertStore { roots: vec![] };
        root_store.add(&self.root_cert).expect("adding root cert");
        rustls::client::WebPkiVerifier::new(root_store, None)
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
                let tls_config = rustls::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_custom_certificate_verifier(Arc::new(
                        self.make_pki_verifier(),
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
const CERT_NAME2: &str = "my-other-certificate";

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
    let error = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &cert_url,
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
    name: &str,
    cert: Vec<u8>,
    key: Vec<u8>,
    status: StatusCode,
) -> String {
    let url = CERTS_URL.to_string();
    let params = params::CertificateCreate {
        identity: IdentityMetadataCreateParams {
            name: name.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        cert,
        key,
    };

    NexusRequest::expect_failure_with_body(
        client,
        status,
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
    let (cert, key) =
        (chain.cert_chain_as_pem(), chain.end_cert_private_key_as_pem());

    // We can create a new certificate
    create_certificate(&client, CERT_NAME, cert.clone(), key.clone()).await;

    // The certificate can be listed or accessed directly
    let list = certs_list(&client).await;
    assert_eq!(list.len(), 1, "Expected exactly one certificate");
    assert_eq!(list[0].identity.name, CERT_NAME);
    let fetched_cert = cert_get(&client, CERT_NAME).await;
    assert_eq!(fetched_cert.identity.name, CERT_NAME);

    // Cannot create a certificate with the same name twice.
    cert_create_expect_error(
        &client,
        CERT_NAME,
        cert.clone(),
        key.clone(),
        StatusCode::CONFLICT,
    )
    .await;

    // However, we can create a certificate with a different name.
    create_certificate(&client, CERT_NAME2, cert.clone(), key.clone()).await;
    let list = certs_list(&client).await;
    assert_eq!(list.len(), 2, "Expected exactly two certificates");

    // We can delete the certificates, and they no longer appear in the API
    delete_certificate(&client, CERT_NAME2).await;
    delete_certificate(&client, CERT_NAME).await;
    let certs = certs_list(&client).await;
    assert!(certs.is_empty());
    cert_get_expect_not_found(&client).await;
    cert_delete_expect_not_found(&client).await;
}

#[nexus_test]
async fn test_refresh(cptestctx: &ControlPlaneTestContext) {
    let chain = CertificateChain::new();
    let (cert, key) =
        (chain.cert_chain_as_pem(), chain.end_cert_private_key_as_pem());

    create_certificate(
        &cptestctx.external_client,
        CERT_NAME,
        cert.clone(),
        key.clone(),
    )
    .await;

    let http_client = &cptestctx.external_http_client().await;
    let https_client = &cptestctx.external_https_client().await;

    let mut root_certs = rustls::RootCertStore::empty();
    root_certs.add(&chain.root_cert).expect("Failed to add certificate");

    // We can use HTTP on the HTTP interface, and HTTPS on the HTTPS
    // interface...
    chain.do_request(&http_client, http::uri::Scheme::HTTP).await.unwrap();
    chain.do_request(&https_client, http::uri::Scheme::HTTPS).await.unwrap();

    // ... but not vice-versa.
    chain.do_request(&http_client, http::uri::Scheme::HTTPS).await.unwrap_err();
    chain.do_request(&https_client, http::uri::Scheme::HTTP).await.unwrap_err();

    // Remove the default certificate, add a new one.
    //
    // NOTE: We're doing this on the "HTTP client" interface only because dropshot
    // makes a hard-coded assumption that the test client is not using HTTPS:
    // https://docs.rs/dropshot/0.8.0/src/dropshot/test_util.rs.html#106
    let chain2 = CertificateChain::new();
    let (cert, key) =
        (chain2.cert_chain_as_pem(), chain2.end_cert_private_key_as_pem());
    create_certificate(
        &http_client,
        "my-other-certificate",
        cert.clone(),
        key.clone(),
    )
    .await;
    delete_certificate(&http_client, CERT_NAME).await;

    // (Test config) Refresh the clients -- the port for the HTTPS interface
    // probably changed.
    let https_client = &cptestctx.external_https_client().await;

    // Requests through the old certificate chain fail -- it was removed.
    chain
        .do_request(&https_client, http::uri::Scheme::HTTPS)
        .await
        .unwrap_err();
    // Requests through the new certificate chain succeed.
    chain2.do_request(&https_client, http::uri::Scheme::HTTPS).await.unwrap();
}

#[nexus_test]
async fn test_cannot_create_certificate_with_bad_key(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let chain = CertificateChain::new();
    let (cert, mut key) =
        (chain.cert_chain_as_pem(), chain.end_cert_private_key_as_pem());

    for i in 0..key.len() {
        key[i] = !key[i];
    }
    cert_create_expect_error(
        &client,
        CERT_NAME,
        cert,
        key,
        StatusCode::BAD_REQUEST,
    )
    .await;
}

#[nexus_test]
async fn test_cannot_create_certificate_with_bad_cert(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let chain = CertificateChain::new();
    let (mut cert, key) =
        (chain.cert_chain_as_pem(), chain.end_cert_private_key_as_pem());

    for i in 0..cert.len() {
        cert[i] = !cert[i];
    }
    cert_create_expect_error(
        &client,
        CERT_NAME,
        cert,
        key,
        StatusCode::BAD_REQUEST,
    )
    .await;
}

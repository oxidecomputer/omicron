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
use std::io::Write;

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

        let end_keypair = rcgen::Certificate::from_params(
            rcgen::CertificateParams::new(vec!["localhost".into()]),
        )
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
}

fn tls_cert_to_buffer(certs: &Vec<rustls::Certificate>) -> Vec<u8> {
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

fn tls_key_to_buffer(key: &rustls::PrivateKey) -> Vec<u8> {
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

const CERT_NAME: &str = "my-certificate";

async fn certs_list(
    client: &ClientTestContext,
    list_url: &str,
) -> Vec<Certificate> {
    NexusRequest::iter_collection_authn(client, list_url, "", None)
        .await
        .expect("failed to list certificates")
        .all_items
}

async fn cert_get(client: &ClientTestContext, cert_url: &str) -> Certificate {
    NexusRequest::object_get(client, &cert_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to issue GET requst for certificate")
        .parsed_body()
        .expect("failed to parse response for certificate GET request")
}

async fn cert_get_expect_not_found(client: &ClientTestContext) {
    let cert_url = format!("/system/certificates/{}", CERT_NAME);
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

async fn cert_delete_expect_not_found(client: &ClientTestContext) {
    let cert_url = format!("/system/certificates/{}", CERT_NAME);
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

    let certs_url = "/system/certificates";
    let certs = certs_list(&client, certs_url).await;
    assert!(certs.is_empty());

    // Make sure we get a 404 if we fetch one.
    cert_get_expect_not_found(&client).await;

    // We should also get a 404 if we delete one.
    cert_delete_expect_not_found(&client).await;
}

#[nexus_test]
async fn test_crud(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let certs_url = "/system/certificates";
    let cert_url = format!("{}/{}", certs_url, CERT_NAME);
    let certs = certs_list(&client, certs_url).await;
    assert!(certs.is_empty());

    let chain = CertificateChain::new();
    let (cert, key) = (chain.cert_chain(), chain.end_cert_private_key());
    let (cert, key) = (tls_cert_to_buffer(&cert), tls_key_to_buffer(&key));

    // We can create a new certificate
    create_certificate(&client, CERT_NAME, cert.clone(), key.clone()).await;

    // The certificate can be listed or accessed directly
    let list = certs_list(&client, &certs_url).await;
    assert_eq!(list.len(), 1, "Expected exactly one certificate");
    assert_eq!(list[0].identity.name, CERT_NAME);

    let fetched_cert = cert_get(&client, &cert_url).await;
    assert_eq!(fetched_cert.identity.name, CERT_NAME);

    // We can delete the certificate, and it no longer appears in the API
    delete_certificate(&client, CERT_NAME).await;
    let certs = certs_list(&client, certs_url).await;
    assert!(certs.is_empty());
    cert_get_expect_not_found(&client).await;
    cert_delete_expect_not_found(&client).await;
}

#[nexus_test]
async fn test_refresh(_cptestctx: &ControlPlaneTestContext) {
    todo!();
}

#[nexus_test]
async fn test_validation(_cptestctx: &ControlPlaneTestContext) {
    // TODO: Pass bad certs?
    todo!();
}

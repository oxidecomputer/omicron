// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for operating on certificates

use display_error_chain::ErrorChainExt;
use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use futures::TryStreamExt;
use http::StatusCode;
use http::method::Method;
use internal_dns_types::names::DNS_ZONE_EXTERNAL_TESTING;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::load_test_config;
use nexus_test_utils::resource_helpers::create_certificate;
use nexus_test_utils::resource_helpers::delete_certificate;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::views::Certificate;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::internal::nexus::Certificate as InternalCertificate;
use omicron_test_utils::certificates::CertificateChain;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use oxide_client::ClientCurrentUserExt;
use oxide_client::ClientSilosExt;
use oxide_client::ClientSystemSilosExt;
use oxide_client::CustomDnsResolver;
use std::sync::Arc;
use std::time::Duration;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const CERTS_URL: &str = "/v1/certificates";
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
    cert: String,
    key: String,
) -> String {
    let url = CERTS_URL.to_string();
    let params = params::CertificateCreate {
        identity: IdentityMetadataCreateParams {
            name: name.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        cert,
        key,
        service: shared::ServiceUsingCertificate::ExternalApi,
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

    let chain = CertificateChain::new(cptestctx.wildcard_silo_dns_name());
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
    let message =
        cert_create_expect_error(&client, CERT_NAME, cert.clone(), key.clone())
            .await;
    assert_eq!(message, format!("already exists: certificate \"{CERT_NAME}\""));

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
async fn test_cannot_create_certificate_with_bad_key(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let chain = CertificateChain::new(cptestctx.wildcard_silo_dns_name());
    let (cert, der_key) =
        (chain.cert_chain_as_pem(), chain.end_cert_private_key_as_der());

    let key = String::from_utf8_lossy(&der_key).to_string();

    // Cannot create a certificate with a bad key (e.g. not PEM encoded)
    let error = cert_create_expect_error(&client, CERT_NAME, cert, key).await;
    let expected = "Failed to parse private key";
    assert!(
        error.contains(expected),
        "{error:?} does not contain {expected:?}"
    );
}

#[nexus_test]
async fn test_cannot_create_certificate_with_mismatched_key(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let chain1 = CertificateChain::new(cptestctx.wildcard_silo_dns_name());
    let cert1 = chain1.cert_chain_as_pem();

    let chain2 = CertificateChain::new(cptestctx.wildcard_silo_dns_name());
    let key2 = chain2.end_cert_private_key_as_pem();

    // Cannot create a certificate with a key that doesn't match the cert
    let error = cert_create_expect_error(&client, CERT_NAME, cert1, key2).await;
    let expected = "Certificate and private key do not match";
    assert!(
        error.contains(expected),
        "{error:?} does not contain {expected:?}"
    );
}

#[nexus_test]
async fn test_cannot_create_certificate_with_bad_cert(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let chain = CertificateChain::new(cptestctx.wildcard_silo_dns_name());
    let (cert, key) =
        (chain.cert_chain_as_pem(), chain.end_cert_private_key_as_pem());

    let tmp =
        cert.as_bytes().into_iter().map(|c| c ^ 0x7f).collect::<Vec<u8>>();

    let cert = String::from_utf8(tmp).unwrap();

    let error = cert_create_expect_error(&client, CERT_NAME, cert, key).await;

    // TODO-correctness It's suprising this is the error we get back instead of
    // "Failed to parse certificate". Why?
    let expected = "Certificate exists, but is empty";
    assert!(
        error.contains(expected),
        "{error:?} does not contain {expected:?}"
    );
}

#[nexus_test]
async fn test_cannot_create_certificate_with_expired_cert(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let mut params =
        rcgen::CertificateParams::new(vec![cptestctx.wildcard_silo_dns_name()]);
    params.not_after = std::time::SystemTime::UNIX_EPOCH.into();

    let chain = CertificateChain::with_params(params);
    let (cert, key) =
        (chain.cert_chain_as_pem(), chain.end_cert_private_key_as_pem());

    let error = cert_create_expect_error(&client, CERT_NAME, cert, key).await;
    let expected = "Certificate exists, but is expired";
    assert!(
        error.contains(expected),
        "{error:?} does not contain {expected:?}"
    );
}

#[nexus_test]
async fn test_cannot_create_certificate_with_incorrect_subject_alt_name(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let bad_subject_alt_name =
        format!("some-other-silo.sys.{}", cptestctx.external_dns_zone_name);

    let chain = CertificateChain::new(&bad_subject_alt_name);
    let (cert, key) =
        (chain.cert_chain_as_pem(), chain.end_cert_private_key_as_pem());

    let error = cert_create_expect_error(&client, CERT_NAME, cert, key).await;
    for expected in [
        "Certificate not valid for".to_string(),
        format!("SANs: {bad_subject_alt_name}"),
    ] {
        assert!(
            error.contains(&expected),
            "{error:?} does not contain {expected:?}"
        );
    }
}

#[tokio::test]
async fn test_silo_certificates() {
    // The goal of this test is to verify that Nexus serves the correct TLS
    // certificate for each incoming connection.  The correct TLS certificate
    // for a connection depends on the certificates configured for the Silo
    // whose endpoint the client used to reach Nexus.  So in order to test this,
    // we need to do a few things:
    //
    // 1. Stand up Nexus in a mode that will run its HTTP server with TLS.
    //    Easy enough: this just involves a slight tweak to the configuration.
    //
    //    Except that this means Nexus _won't_ be listening over HTTP.  That
    //    means we need to connect to it using a TLS-capable client.  Our usual
    //    test client is not TLS-capable.  Fortunately, the oxide-client is.
    //
    //    We also need to generate a TLS certificate, have Nexus use that, and
    //    then have our client trust it.  Again, because Nexus is not listening
    //    on HTTP, we can't use the usual APIs to add the certificate after
    //    Nexus is up.  We need to provide the certificate when initializing the
    //    rack.
    //
    //    All of this is required just to be able to have our test be able to
    //    talk to Nexus using HTTP/TLS.
    //
    // 2. Having done all that, we'll create two more certificates for two more
    //    Silos, create users in those Silos so that we can make requests, and
    //    then make requests as those users to list certificates and verify that
    //    the correct certificate (and only that certificate) appears in each
    //    Silo.
    //
    // 3. We'll do all this using separate reqwest clients for each Silo
    //    endpoint.  Each client will trust only its own Silo's certificate.  We
    //    will verify that these clients _can't_ make requests to the other Silo
    //    because it doesn't trust the certificate.

    // Create the certificates and metadata for the three Silos that we're going
    // to test with.
    let silo1 = SiloCert::new("test-suite-silo".parse().unwrap());
    let silo2 = SiloCert::new("silo2".parse().unwrap());
    let silo3 = SiloCert::new("silo3".parse().unwrap());

    // Start Nexus with a TLS server instead of its usual HTTP server.
    let cptestctx = {
        let mut config = load_test_config();
        config.deployment.dropshot_external.tls = true;
        nexus_test_utils::test_setup_with_config::<omicron_nexus::Server>(
            "test_silo_certificates",
            &mut config,
            omicron_sled_agent::sim::SimMode::Explicit,
            Some(silo1.cert.clone()),
            0,
        )
        .await
    };

    let nexus_port = cptestctx.external_client.bind_address.port();

    // Log into silo1 (the Silo created during rack initialization) as the user
    // that was created when that Silo was created.  We'll use this session to
    // create the other Silos and their users.
    let resolver = Arc::new(
        CustomDnsResolver::new(
            cptestctx.external_dns.dns_server.local_address(),
        )
        .unwrap(),
    );
    let session_token = oxide_client::login(
        silo1.reqwest_client().dns_resolver(resolver.clone()),
        &silo1.login_url(nexus_port),
        cptestctx.user_name.as_ref().parse().unwrap(),
        "oxide".parse().unwrap(),
    )
    .await
    .expect("failed to log into recovery silo");

    let silo1_client = silo1.oxide_client(
        silo1.reqwest_client(),
        resolver.clone(),
        AuthnMode::Session(session_token),
        nexus_port,
    );

    // Using that user, create a second Silo.
    let silo2_cert = oxide_client::types::CertificateCreate::builder()
        .name(silo2.cert_name.clone())
        .description("")
        .cert(silo2.cert.cert.clone())
        .key(silo2.cert.key.clone())
        .service(oxide_client::types::ServiceUsingCertificate::ExternalApi);
    silo1_client
        .silo_create()
        .body(
            oxide_client::types::SiloCreate::builder()
                .name(silo2.silo_name.clone())
                .description("")
                .discoverable(false)
                .quotas(oxide_client::types::SiloQuotasCreate {
                    cpus: 0,
                    memory: oxide_client::types::ByteCount(0),
                    storage: oxide_client::types::ByteCount(0),
                })
                .identity_mode(oxide_client::types::SiloIdentityMode::LocalOnly)
                .tls_certificates(vec![silo2_cert.try_into().unwrap()]),
        )
        .send()
        .await
        .expect("failed to create Silo");

    // Create a local user in that Silo.
    let silo2_user = silo1_client
        .local_idp_user_create()
        .silo(silo2.silo_name.clone())
        .body(
            oxide_client::types::UserCreate::builder()
                .external_id("testuser-silo2")
                .password(oxide_client::types::UserPassword::LoginDisallowed),
        )
        .send()
        .await
        .expect("failed to create user")
        .into_inner()
        .id;

    // Grant that user admin privileges on that Silo.
    let mut silo2_policy = silo1_client
        .silo_policy_view()
        .silo(silo2.silo_name.clone())
        .send()
        .await
        .unwrap()
        .into_inner();
    silo2_policy.role_assignments.push(
        oxide_client::types::SiloRoleRoleAssignment::builder()
            .identity_id(silo2_user)
            .identity_type(oxide_client::types::IdentityType::SiloUser)
            .role_name(oxide_client::types::SiloRole::Admin)
            .try_into()
            .unwrap(),
    );
    silo1_client
        .silo_policy_update()
        .silo(silo2.silo_name.clone())
        .body(silo2_policy)
        .send()
        .await
        .expect("failed to update Silo policy");

    // Create another Silo with an admin user in it.
    let silo3_cert = oxide_client::types::CertificateCreate::builder()
        .name(silo3.cert_name.clone())
        .description("")
        .cert(silo3.cert.cert.clone())
        .key(silo3.cert.key.clone())
        .service(oxide_client::types::ServiceUsingCertificate::ExternalApi);
    silo1_client
        .silo_create()
        .body(
            oxide_client::types::SiloCreate::builder()
                .name(silo3.silo_name.clone())
                .description("")
                .discoverable(false)
                .quotas(oxide_client::types::SiloQuotasCreate {
                    cpus: 0,
                    memory: oxide_client::types::ByteCount(0),
                    storage: oxide_client::types::ByteCount(0),
                })
                .identity_mode(oxide_client::types::SiloIdentityMode::LocalOnly)
                .tls_certificates(vec![silo3_cert.try_into().unwrap()]),
        )
        .send()
        .await
        .expect("failed to create Silo");
    let silo3_user = silo1_client
        .local_idp_user_create()
        .silo(silo3.silo_name.clone())
        .body(
            oxide_client::types::UserCreate::builder()
                .external_id("testuser-silo3")
                .password(oxide_client::types::UserPassword::LoginDisallowed),
        )
        .send()
        .await
        .expect("failed to create user")
        .into_inner()
        .id;

    // Grant that user admin privileges on that Silo.
    let mut silo3_policy = silo1_client
        .silo_policy_view()
        .silo(silo3.silo_name.clone())
        .send()
        .await
        .unwrap()
        .into_inner();
    silo3_policy.role_assignments.push(
        oxide_client::types::SiloRoleRoleAssignment::builder()
            .identity_id(silo3_user)
            .identity_type(oxide_client::types::IdentityType::SiloUser)
            .role_name(oxide_client::types::SiloRole::Admin)
            .try_into()
            .unwrap(),
    );
    silo1_client
        .silo_policy_update()
        .silo(silo3.silo_name.clone())
        .body(silo3_policy)
        .send()
        .await
        .expect("failed to update Silo policy");

    // Verify that the user in silo 1 (the one created by the test suite) cannot
    // see any of the three certificates that we created in the other Silos.
    let certs = silo1_client
        .certificate_list()
        .stream()
        .try_collect::<Vec<_>>()
        .await
        .expect("failed to list certificates");
    assert_eq!(certs.len(), 1);
    // This is the name created during rack initialization.
    assert_eq!(certs[0].name, "default-1".parse().unwrap());

    // Similarly, create clients for the users in the other two Silos.  Note
    // that loading of the right certificates is asynchronous.  Wait for them to
    // show up.  (We only have to wait for silo3's because they will be
    // propagated in sequential order.)
    let silo2_client = silo2.oxide_client(
        silo2.reqwest_client(),
        resolver.clone(),
        AuthnMode::SiloUser(silo2_user),
        nexus_port,
    );
    let silo3_client = silo3.oxide_client(
        silo3.reqwest_client(),
        resolver.clone(),
        AuthnMode::SiloUser(silo3_user),
        nexus_port,
    );
    wait_for_condition(
        || async {
            match silo3_client.current_user_view().send().await {
                Ok(result) => {
                    assert_eq!(result.into_inner().id, silo3_user);
                    Ok(())
                }
                Err(oxide_client::Error::CommunicationError(error))
                    if error.is_connect() =>
                {
                    Err(CondCheckError::NotYet)
                }
                Err(e) => Err(CondCheckError::Failed(e)),
            }
        },
        &Duration::from_millis(50),
        &Duration::from_secs(30),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "failed to connect to silo3's endpoint within timeout: {:#}",
            error
        )
    });

    // Now make sure each client can see only the certificate for its own Silo.
    // This also exercises that Nexus is _serving_ the right certificate.
    // That's because each client is configured to trust only its own client's
    // certificate.
    let certs = silo2_client
        .certificate_list()
        .stream()
        .try_collect::<Vec<_>>()
        .await
        .expect("failed to list certificates");
    assert_eq!(certs.len(), 1);
    assert_eq!(certs[0].name, silo2.cert_name);

    let certs = silo3_client
        .certificate_list()
        .stream()
        .try_collect::<Vec<_>>()
        .await
        .expect("failed to list certificates");
    assert_eq!(certs.len(), 1);
    assert_eq!(certs[0].name, silo3.cert_name);

    // For good measure, to make sure we got the certificate stuff right, let's
    // try to use the wrong certificate to reach each endpoint and confirm that
    // we get a TLS error.
    let silo2_client_wrong_cert = silo2.oxide_client(
        silo3.reqwest_client(),
        resolver.clone(),
        AuthnMode::SiloUser(silo2_user),
        nexus_port,
    );
    let error =
        silo2_client_wrong_cert.current_user_view().send().await.expect_err(
            "unexpectedly connected with wrong certificate trusted",
        );
    if let oxide_client::Error::CommunicationError(error) = error {
        assert!(error.is_connect());
        assert!(error.chain().to_string().contains("self-signed certificate"));
    } else {
        panic!(
            "unexpected error connecting with wrong certificate: {:#}",
            error
        );
    }
    let silo3_client_wrong_cert = silo3.oxide_client(
        silo2.reqwest_client(),
        resolver.clone(),
        AuthnMode::SiloUser(silo2_user),
        nexus_port,
    );
    let error =
        silo3_client_wrong_cert.current_user_view().send().await.expect_err(
            "unexpectedly connected with wrong certificate trusted",
        );
    if let oxide_client::Error::CommunicationError(error) = error {
        assert!(error.is_connect());
        assert!(error.chain().to_string().contains("self-signed certificate"));
    } else {
        panic!(
            "unexpected error connecting with wrong certificate: {:#}",
            error
        );
    }

    cptestctx.teardown().await;
}

/// Helper for the certificate test
///
/// This structure keeps track of various metadata about a Silo to ensure
/// that we operate on it consistently.
struct SiloCert {
    silo_name: oxide_client::types::Name,
    dns_name: String,
    cert_name: oxide_client::types::Name,
    cert: InternalCertificate,
}

impl SiloCert {
    /// Given just the silo name, construct the DNS name, a new certificate
    /// chain for that DNS name, and a name for that certificate.
    fn new(silo_name: oxide_client::types::Name) -> SiloCert {
        let dns_name =
            format!("{}.sys.{}", silo_name.as_str(), DNS_ZONE_EXTERNAL_TESTING);
        let chain =
            CertificateChain::with_params(rcgen::CertificateParams::new(vec![
                dns_name.clone(),
            ]));
        let cert_name = format!("cert-{}", silo_name.as_str()).parse().unwrap();
        let cert = InternalCertificate {
            cert: chain.cert_chain_as_pem(),
            key: chain.end_cert_private_key_as_pem(),
        };
        SiloCert { silo_name, dns_name, cert_name, cert }
    }

    /// Returns the base URL of the HTTPS endpoint for this Silo
    fn base_url(&self, port: u16) -> String {
        format!("https://{}:{}", &self.dns_name, port)
    }

    /// Returns the full URL to the login endpoint for this Silo
    fn login_url(&self, port: u16) -> String {
        format!(
            "{}/v1/login/{}/local",
            &self.base_url(port),
            &self.silo_name.as_str()
        )
    }

    /// Returns a new `ReqwestClientBuilder` that's configured to trust this
    /// client's certificate
    fn reqwest_client(&self) -> reqwest::ClientBuilder {
        let rustls_cert =
            reqwest::tls::Certificate::from_pem(&self.cert.cert.as_bytes())
                .unwrap();
        reqwest::ClientBuilder::new().add_root_certificate(rustls_cert)
    }

    /// Returns an `oxide_client::Client` that's configured to talk to this
    /// Silo's endpoint
    ///
    /// You provide the underlying `reqwest_client`.  To actually use this
    /// Silo's endpoint, use `self.reqwest_client()` to make sure the client
    /// will trust this Silo's certificate.  These methods are separated so
    /// that we can test what happens when we _don't_ trust this client's
    /// certificate.
    fn oxide_client(
        &self,
        reqwest_client: reqwest::ClientBuilder,
        resolver: Arc<CustomDnsResolver>,
        authn_mode: AuthnMode,
        port: u16,
    ) -> oxide_client::Client {
        let (header_name, header_value) = authn_mode.authn_header().unwrap();
        let mut headers = http::header::HeaderMap::new();
        headers.append(header_name, header_value);
        let reqwest_client = reqwest_client
            .default_headers(headers)
            .dns_resolver(resolver)
            .build()
            .unwrap();
        let base_url = self.base_url(port);
        oxide_client::Client::new_with_client(&base_url, reqwest_client)
    }
}

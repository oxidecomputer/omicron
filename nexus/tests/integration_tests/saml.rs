// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::authn::silos::{
    IdentityProviderType, SamlIdentityProvider, SamlLoginPost,
};
use omicron_nexus::external_api::console_api;
use omicron_nexus::external_api::{params, shared};
use omicron_nexus::external_api::views::{self, Silo};
use omicron_nexus::TestInterfaces;

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::resource_helpers::{create_silo, object_create};

use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

use httptest::{matchers::*, responders::*, Expectation, Server};

// Valid SAML IdP entity descriptor from https://en.wikipedia.org/wiki/SAML_metadata#Identity_provider_metadata
// note: no signing keys
pub const SAML_IDP_DESCRIPTOR: &str = include_str!("data/saml_idp_descriptor.xml");

// Create a SAML IdP
#[nexus_test]
async fn test_create_a_saml_idp(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Fixed)
        .await;

    let silo: Silo =
        NexusRequest::object_get(&client, &format!("/silos/{}", SILO_NAME,))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to make request")
            .parsed_body()
            .unwrap();

    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR;

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    let silo_saml_idp: views::SamlIdentityProvider = object_create(
        client,
        &format!("/silos/{}/saml_identity_providers", SILO_NAME),
        &params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Url {
                url: server.url("/descriptor").to_string(),
            },

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,
        },
    )
    .await;

    // Assert external authenticator opctx can read it
    let nexus = &cptestctx.server.apictx.nexus;

    let _retrieved_silo_nexus = nexus
        .silo_fetch(
            &nexus.opctx_external_authn(),
            &omicron_common::api::external::Name::try_from(
                SILO_NAME.to_string(),
            )
            .unwrap()
            .into(),
        )
        .await
        .unwrap();

    let (.., retrieved_silo_idp_from_nexus) = IdentityProviderType::lookup(
        &nexus.datastore(),
        &nexus.opctx_external_authn(),
        &omicron_common::api::external::Name::try_from(SILO_NAME.to_string())
            .unwrap()
            .into(),
        &omicron_common::api::external::Name::try_from(
            "some-totally-real-saml-provider".to_string(),
        )
        .unwrap()
        .into(),
    )
    .await
    .unwrap();

    match retrieved_silo_idp_from_nexus {
        IdentityProviderType::Saml(_) => {
            // ok
        }
    }

    // Expect the SSO redirect when trying to log in unauthenticated
    let result = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!(
                "/login/{}/{}",
                silo.identity.name, silo_saml_idp.identity.name
            ),
        )
        .expect_status(Some(StatusCode::FOUND)),
    )
    .execute()
    .await
    .expect("expected success");

    assert!(result.headers["Location"]
        .to_str()
        .unwrap()
        .to_string()
        .starts_with(
            "https://idp.example.org/SAML2/SSO/Redirect?SAMLRequest=",
        ));
}

// Fail to create a SAML IdP out of an invalid descriptor
#[nexus_test]
async fn test_create_a_saml_idp_invalid_descriptor_truncated(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Fixed)
        .await;

    let saml_idp_descriptor = {
        let mut saml_idp_descriptor = SAML_IDP_DESCRIPTOR.to_string();
        saml_idp_descriptor.truncate(100);
        saml_idp_descriptor
    };

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/silos/{}/saml_identity_providers", SILO_NAME),
        )
        .body(Some(&params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Url {
                url: server.url("/descriptor").to_string(),
            },

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success");
}

// Fail to create a SAML IdP out of a descriptor with no SSO redirect binding url
#[nexus_test]
async fn test_create_a_saml_idp_invalid_descriptor_no_redirect_binding(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Fixed)
        .await;

    let saml_idp_descriptor = {
        let saml_idp_descriptor = SAML_IDP_DESCRIPTOR.to_string();
        saml_idp_descriptor
            .lines()
            .filter(|x| {
                !x.contains(
                    "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
                )
            })
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join("\n")
    };

    assert!(!saml_idp_descriptor
        .contains("urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"));

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/silos/{}/saml_identity_providers", SILO_NAME),
        )
        .body(Some(&params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Url {
                url: server.url("/descriptor").to_string(),
            },

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success");
}

// Create a hidden Silo with a SAML IdP
#[nexus_test]
async fn test_create_a_hidden_silo_saml_idp(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_silo(&client, "hidden", false, shared::UserProvisionType::Fixed)
        .await;

    // Valid IdP descriptor
    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR.to_string();

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    let silo_saml_idp: views::SamlIdentityProvider = object_create(
        client,
        "/silos/hidden/saml_identity_providers",
        &params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Url {
                url: server.url("/descriptor").to_string(),
            },

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,
        },
    )
    .await;

    // Expect the SSO redirect when trying to log in
    let result = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!("/login/hidden/{}", silo_saml_idp.identity.name),
        )
        .expect_status(Some(StatusCode::FOUND)),
    )
    .execute()
    .await
    .expect("expected success");

    assert!(result.headers["Location"]
        .to_str()
        .unwrap()
        .to_string()
        .starts_with(
            "https://idp.example.org/SAML2/SSO/Redirect?SAMLRequest=",
        ));
}

// Can't create a SAML IdP if the metadata URL returns something that's not 200
#[nexus_test]
async fn test_saml_idp_metadata_url_404(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Fixed)
        .await;

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(404).body("no descriptor found")),
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/silos/{}/saml_identity_providers", SILO_NAME),
        )
        .body(Some(&params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Url {
                url: server.url("/descriptor").to_string(),
            },

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success");
}

// Can't create a SAML IdP if the metadata URL isn't a URL
#[nexus_test]
async fn test_saml_idp_metadata_url_invalid(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Fixed)
        .await;

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/silos/{}/saml_identity_providers", SILO_NAME),
        )
        .body(Some(&params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Url {
                url: "htttps://fake.url".to_string(),
            },

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success");
}

// TODO samael does not support signing with ECDSA yet, add tests when it does
const RSA_KEY_1_PUBLIC: &str = include_str!("data/rsa-key-1-public.b64");
const RSA_KEY_1_PRIVATE: &str = include_str!("data/rsa-key-1-private.b64");
const RSA_KEY_2_PUBLIC: &str = include_str!("data/rsa-key-2-public.b64");
const RSA_KEY_2_PRIVATE: &str = include_str!("data/rsa-key-2-private.b64");

#[nexus_test]
async fn test_saml_idp_reject_keypair(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR;

    // Spin up a server but expect it never to be accessed
    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .times(0)
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Fixed)
        .await;

    let test_cases = vec![
        // Reject signing keypair if the certificate or key is not base64
        // encoded
        params::DerEncodedKeyPair {
            public_cert: "regular string".to_string(),
            private_key: RSA_KEY_1_PRIVATE.to_string(),
        },
        params::DerEncodedKeyPair {
            public_cert: RSA_KEY_1_PUBLIC.to_string(),
            private_key: "regular string".to_string(),
        },
        // Reject signing keypair if the certificate or key is base64 encoded
        // but not valid
        params::DerEncodedKeyPair {
            public_cert: base64::encode("not a cert"),
            private_key: RSA_KEY_1_PRIVATE.to_string(),
        },
        params::DerEncodedKeyPair {
            public_cert: RSA_KEY_1_PUBLIC.to_string(),
            private_key: base64::encode("not a cert"),
        },
        // Reject signing keypair if cert and key are swapped
        params::DerEncodedKeyPair {
            public_cert: RSA_KEY_1_PRIVATE.to_string(),
            private_key: RSA_KEY_1_PUBLIC.to_string(),
        },
        // Reject signing keypair if the keys do not match
        params::DerEncodedKeyPair {
            public_cert: RSA_KEY_1_PUBLIC.to_string(),
            private_key: RSA_KEY_2_PRIVATE.to_string(),
        },
        params::DerEncodedKeyPair {
            public_cert: RSA_KEY_2_PUBLIC.to_string(),
            private_key: RSA_KEY_1_PRIVATE.to_string(),
        },
    ];

    for test_case in test_cases {
        NexusRequest::new(
            RequestBuilder::new(
                client,
                Method::POST,
                &format!("/silos/{}/saml_identity_providers", SILO_NAME),
            )
            .body(Some(&params::SamlIdentityProviderCreate {
                identity: IdentityMetadataCreateParams {
                    name: "some-totally-real-saml-provider"
                        .to_string()
                        .parse()
                        .unwrap(),
                    description: "a demo provider".to_string(),
                },

                idp_metadata_source: params::IdpMetadataSource::Url {
                    url: server.url("/descriptor").to_string(),
                },

                idp_entity_id: "entity_id".to_string(),
                sp_client_id: "client_id".to_string(),
                acs_url: "http://acs".to_string(),
                slo_url: "http://slo".to_string(),
                technical_contact_email: "technical@fake".to_string(),

                signing_keypair: Some(test_case),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("unexpected success");
    }
}

// Test that a RSA keypair works
#[nexus_test]
async fn test_saml_idp_rsa_keypair_ok(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR;

    // Spin up a server but expect it never to be accessed
    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .times(1)
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Fixed)
        .await;

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/silos/{}/saml_identity_providers", SILO_NAME),
        )
        .body(Some(&params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Url {
                url: server.url("/descriptor").to_string(),
            },

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: Some(params::DerEncodedKeyPair {
                public_cert: RSA_KEY_1_PUBLIC.to_string(),
                private_key: RSA_KEY_1_PRIVATE.to_string(),
            }),
        }))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure");
}

pub const SAML_RESPONSE_IDP_DESCRIPTOR: &str =
    include_str!("data/saml_response_idp_descriptor.xml");
pub const SAML_RESPONSE: &str = include_str!("data/saml_response.xml");
pub const SAML_RESPONSE_IDP_DESCRIPTOR_ECDSA_SHA256: &str =
    include_str!("data/saml_response_idp_descriptor_ecdsa_sha256.xml");
pub const SAML_RESPONSE_SIGNED_WITH_ECDSA_SHA256: &str =
    include_str!("data/saml_response_signed_with_ecdsa_sha256.xml");
pub const SAML_RESPONSE_ONLY_ASSERTION_SIGNED: &str =
    include_str!("data/saml_response_only_assertion_signed.xml");
pub const SAML_RESPONSE_UNSIGNED: &str =
    include_str!("data/saml_response_unsigned.xml");
pub const SAML_RESPONSE_WITH_COMMENT: &str =
    include_str!("data/saml_response_with_comment.xml");
pub const SAML_RESPONSE_WITH_GROUPS: &str =
    include_str!("data/saml_response_with_groups.xml");

// Test receiving a correct SAML response
#[test]
fn test_correct_saml_response() {
    let silo_saml_identity_provider = SamlIdentityProvider {
        idp_metadata_document_string: SAML_RESPONSE_IDP_DESCRIPTOR.to_string(),

        idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
        sp_client_id: "https://customer.site/oxide_rack/saml".to_string(),
        acs_url: "https://customer.site/oxide_rack/saml".to_string(),
        slo_url: "http://slo".to_string(),
        technical_contact_email: "technical@fake".to_string(),

        public_cert: None,
        private_key: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::encode(&SAML_RESPONSE),
        relay_state: None,
    })
    .unwrap();

    let (authenticated_subject, relay_state) = silo_saml_identity_provider
        .authenticated_subject(
            &body_bytes,
            // Set max_issue_delay so that SAMLResponse is valid
            Some(
                chrono::Utc::now()
                    - "2022-05-04T15:36:12.631Z"
                        .parse::<chrono::DateTime<chrono::Utc>>()
                        .unwrap()
                    + chrono::Duration::seconds(60),
            ),
        )
        .unwrap();

    assert_eq!(
        authenticated_subject.external_id,
        "some@customer.com".to_string()
    );
    assert_eq!(relay_state, None);
}

// Test receiving a correct SAML response, signed with ecdsa-sha256
#[test]
fn test_correct_saml_response_ecdsa_sha256() {
    let silo_saml_identity_provider = SamlIdentityProvider {
        idp_metadata_document_string: SAML_RESPONSE_IDP_DESCRIPTOR_ECDSA_SHA256
            .to_string(),

        idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
        sp_client_id: "https://customer.site/oxide_rack/saml".to_string(),
        acs_url: "https://customer.site/oxide_rack/saml".to_string(),
        slo_url: "http://slo".to_string(),
        technical_contact_email: "technical@fake".to_string(),

        public_cert: None,
        private_key: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::encode(&SAML_RESPONSE_SIGNED_WITH_ECDSA_SHA256),
        relay_state: None,
    })
    .unwrap();

    let (authenticated_subject, relay_state) = silo_saml_identity_provider
        .authenticated_subject(
            &body_bytes,
            // Set max_issue_delay so that SAMLResponse is valid
            Some(
                chrono::Utc::now()
                    - "2022-05-04T15:36:12.631Z"
                        .parse::<chrono::DateTime<chrono::Utc>>()
                        .unwrap()
                    + chrono::Duration::seconds(60),
            ),
        )
        .unwrap();

    assert_eq!(
        authenticated_subject.external_id,
        "some@customer.com".to_string()
    );
    assert_eq!(relay_state, None);
}

// Test a SAML response with only the assertion signed
#[test]
fn test_accept_saml_response_only_assertion_signed() {
    let silo_saml_identity_provider = SamlIdentityProvider {
        idp_metadata_document_string: SAML_RESPONSE_IDP_DESCRIPTOR.to_string(),

        idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
        sp_client_id: "https://customer.site/oxide_rack/saml".to_string(),
        acs_url: "https://customer.site/oxide_rack/saml".to_string(),
        slo_url: "http://slo".to_string(),
        technical_contact_email: "technical@fake".to_string(),

        public_cert: None,
        private_key: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::encode(&SAML_RESPONSE_ONLY_ASSERTION_SIGNED),
        relay_state: None,
    })
    .unwrap();

    let _result = silo_saml_identity_provider
        .authenticated_subject(
            &body_bytes,
            // Set max_issue_delay so that SAMLResponse is valid
            Some(
                chrono::Utc::now()
                    - "2022-05-04T15:36:12.631Z"
                        .parse::<chrono::DateTime<chrono::Utc>>()
                        .unwrap()
                    + chrono::Duration::seconds(60),
            ),
        )
        .unwrap();
}

// Test rejecting an unsigned SAML response
#[test]
fn test_reject_unsigned_saml_response() {
    let silo_saml_identity_provider = SamlIdentityProvider {
        idp_metadata_document_string: SAML_RESPONSE_IDP_DESCRIPTOR.to_string(),

        idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
        sp_client_id: "https://customer.site/oxide_rack/saml".to_string(),
        acs_url: "https://customer.site/oxide_rack/saml".to_string(),
        slo_url: "http://slo".to_string(),
        technical_contact_email: "technical@fake".to_string(),

        public_cert: None,
        private_key: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::encode(&SAML_RESPONSE_UNSIGNED),
        relay_state: None,
    })
    .unwrap();

    let result = silo_saml_identity_provider.authenticated_subject(
        &body_bytes,
        // Set max_issue_delay so that SAMLResponse is valid
        Some(
            chrono::Utc::now()
                - "2022-05-04T15:36:12.631Z"
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .unwrap()
                + chrono::Duration::seconds(60),
        ),
    );

    assert!(result.is_err());
}

// Test rejecting a correct SAML response that contains a XML comment in
// saml:NameID.
//
// See: https://duo.com/blog/duo-finds-saml-vulnerabilities-affecting-multiple-implementations
#[test]
fn test_reject_saml_response_with_xml_comment() {
    let silo_saml_identity_provider = SamlIdentityProvider {
        idp_metadata_document_string: SAML_RESPONSE_IDP_DESCRIPTOR.to_string(),

        idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
        sp_client_id: "https://customer.site/oxide_rack/saml".to_string(),
        acs_url: "https://customer.site/oxide_rack/saml".to_string(),
        slo_url: "http://slo".to_string(),
        technical_contact_email: "technical@fake".to_string(),

        public_cert: None,
        private_key: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::encode(&SAML_RESPONSE_WITH_COMMENT),
        relay_state: None,
    })
    .unwrap();

    let result = silo_saml_identity_provider.authenticated_subject(
        &body_bytes,
        // Set max_issue_delay so that SAMLResponse is valid
        Some(
            chrono::Utc::now()
                - "2022-05-04T15:36:12.631Z"
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .unwrap()
                + chrono::Duration::seconds(60),
        ),
    );

    assert!(result.is_err());
}

// Test receiving a correct SAML response that has group attributes
#[test]
fn test_correct_saml_response_with_group_attributes() {
    let silo_saml_identity_provider = SamlIdentityProvider {
        idp_metadata_document_string: SAML_RESPONSE_IDP_DESCRIPTOR.to_string(),

        idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
        sp_client_id: "https://customer.site/oxide_rack/saml".to_string(),
        acs_url: "https://customer.site/oxide_rack/saml".to_string(),
        slo_url: "http://slo".to_string(),
        technical_contact_email: "technical@fake".to_string(),

        public_cert: None,
        private_key: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::encode(&SAML_RESPONSE_WITH_GROUPS),
        relay_state: None,
    })
    .unwrap();

    let (authenticated_subject, relay_state) = silo_saml_identity_provider
        .authenticated_subject(
            &body_bytes,
            // Set max_issue_delay so that SAMLResponse is valid
            Some(
                chrono::Utc::now()
                    - "2022-05-04T15:36:12.631Z"
                        .parse::<chrono::DateTime<chrono::Utc>>()
                        .unwrap()
                    + chrono::Duration::seconds(60),
            ),
        )
        .unwrap();

    assert_eq!(
        authenticated_subject.external_id,
        "some@customer.com".to_string()
    );
    assert_eq!(
        authenticated_subject.groups,
        vec!["SRE".to_string(), "Admins".to_string()]
    );
    assert_eq!(relay_state, None);
}

// Test getting redirected with correct SAML response, and asking whoami
#[nexus_test]
async fn test_post_saml_response(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Jit).await;

    let _silo_saml_idp: views::SamlIdentityProvider = object_create(
        client,
        &format!("/silos/{}/saml_identity_providers", SILO_NAME),
        &params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Base64EncodedXml {
                data: base64::encode(SAML_RESPONSE_IDP_DESCRIPTOR),
            },

            idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "https://customer.site/oxide_rack/saml".to_string(),
            slo_url: "https://customer.site/oxide_rack/saml".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,
        },
    )
    .await;

    let nexus = &cptestctx.server.apictx.nexus;
    nexus.set_samael_max_issue_delay(
        chrono::Utc::now()
            - "2022-05-04T15:36:12.631Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
            + chrono::Duration::seconds(60),
    );

    let result = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/login/{}/some-totally-real-saml-provider", SILO_NAME),
        )
        .raw_body(Some(
            serde_urlencoded::to_string(SamlLoginPost {
                saml_response: base64::encode(SAML_RESPONSE),
                relay_state: None,
            })
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::FOUND)),
    )
    .execute()
    .await
    .expect("expected success");

    assert!(result.headers["Location"]
        .to_str()
        .unwrap()
        .to_string()
        .ends_with("/organizations"));

    // ask whoami
    NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/session/me")
            .expect_status(Some(StatusCode::UNAUTHORIZED)),
    )
    .execute()
    .await
    .expect("expected success");

    let _session_user: views::SessionUser = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/session/me")
            .header(
                http::header::COOKIE,
                result.headers["Set-Cookie"].to_str().unwrap().to_string(),
            )
            .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected success")
    .parsed_body()
    .unwrap();
}

// Test correct SAML response with relay state
#[nexus_test]
async fn test_post_saml_response_with_relay_state(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Jit).await;

    let _silo_saml_idp: views::SamlIdentityProvider = object_create(
        client,
        &format!("/silos/{}/saml_identity_providers", SILO_NAME),
        &params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Base64EncodedXml {
                data: base64::encode(SAML_RESPONSE_IDP_DESCRIPTOR),
            },

            idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "https://customer.site/oxide_rack/saml".to_string(),
            slo_url: "https://customer.site/oxide_rack/saml".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,
        },
    )
    .await;

    let nexus = &cptestctx.server.apictx.nexus;
    nexus.set_samael_max_issue_delay(
        chrono::Utc::now()
            - "2022-05-04T15:36:12.631Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
            + chrono::Duration::seconds(60),
    );

    let result = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/login/{}/some-totally-real-saml-provider", SILO_NAME),
        )
        .raw_body(Some(
            serde_urlencoded::to_string(SamlLoginPost {
                saml_response: base64::encode(SAML_RESPONSE),
                relay_state: Some(
                    console_api::RelayState {
                        referer: Some("/some/actual/nexus/url".to_string()),
                    }
                    .to_encoded()
                    .unwrap(),
                ),
            })
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::FOUND)),
    )
    .execute()
    .await
    .expect("expected success");

    assert!(result.headers["Location"]
        .to_str()
        .unwrap()
        .to_string()
        .ends_with("/some/actual/nexus/url"));
}

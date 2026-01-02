// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_queries::authn::silos::{
    IdentityProviderType, SamlIdentityProvider, SamlLoginPost,
};
use nexus_test_utils::assert_same_items;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{create_silo, object_create};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::shared::RelayState;
use nexus_types::external_api::views::{self, Silo};
use nexus_types::external_api::{params, shared};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::TestInterfaces;
use omicron_uuid_kinds::SiloGroupUuid;

use base64::Engine;
use dropshot::ResultsPage;
use http::StatusCode;
use http::method::Method;
use httptest::{Expectation, Server, matchers::*, responders::*};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Valid SAML IdP entity descriptor from https://en.wikipedia.org/wiki/SAML_metadata#Identity_provider_metadata
// note: no signing keys
pub const SAML_IDP_DESCRIPTOR: &str =
    include_str!("data/saml_idp_descriptor.xml");
pub const SAML_IDP_DESCRIPTOR_ENCRYPTION_KEY_ONLY: &str =
    include_str!("data/saml_idp_descriptor_encryption_key_only.xml");
pub const SAML_IDP_DESCRIPTOR_NO_KEYS: &str =
    include_str!("data/saml_idp_descriptor_no_keys.xml");

// Create a SAML IdP
#[nexus_test]
async fn test_create_a_saml_idp(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
        .await;
    let silo: Silo = NexusRequest::object_get(
        &client,
        &format!("/v1/system/silos/{}", SILO_NAME,),
    )
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
        &format!("/v1/system/identity-providers/saml?silo={}", SILO_NAME),
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

            group_attribute_name: None,
        },
    )
    .await;

    // Assert external authenticator opctx can read it
    let nexus = &cptestctx.server.server_context().nexus;
    let (.., _retrieved_silo_nexus) = nexus
        .silo_lookup(
            &nexus.opctx_external_authn(),
            omicron_common::api::external::Name::try_from(
                SILO_NAME.to_string(),
            )
            .unwrap()
            .into(),
        )
        .unwrap()
        .fetch()
        .await
        .unwrap();

    let (.., retrieved_silo_idp_from_nexus) = nexus
        .datastore()
        .identity_provider_lookup(
            &nexus.opctx_external_authn(),
            &omicron_common::api::external::Name::try_from(
                SILO_NAME.to_string(),
            )
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
                "/login/{}/saml/{}/redirect",
                silo.identity.name, silo_saml_idp.identity.name
            ),
        )
        .expect_status(Some(StatusCode::FOUND)),
    )
    .execute()
    .await
    .expect("expected success");

    assert!(
        result.headers["Location"].to_str().unwrap().to_string().starts_with(
            "https://idp.example.org/SAML2/SSO/Redirect?SAMLRequest=",
        )
    );
}

// Fail to create a SAML IdP out of an invalid descriptor
#[nexus_test]
async fn test_create_a_saml_idp_invalid_descriptor_truncated(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
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
            &format!("/v1/system/identity-providers/saml?silo={}", SILO_NAME),
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

            group_attribute_name: None,
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
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
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

    assert!(
        !saml_idp_descriptor
            .contains("urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect")
    );

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/v1/system/identity-providers/saml?silo={}", SILO_NAME),
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

            group_attribute_name: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success");
}

// Fail to create a SAML IdP from a metadata document that only has encryption
// keys
#[nexus_test]
async fn test_create_a_saml_idp_metadata_only_encryption_keys(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
        .await;

    let saml_idp_descriptor =
        SAML_IDP_DESCRIPTOR_ENCRYPTION_KEY_ONLY.to_string();

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/v1/system/identity-providers/saml?silo={}", SILO_NAME),
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

            group_attribute_name: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success");
}

// Fail to create a SAML IdP from a metadata document that has no keys
#[nexus_test]
async fn test_create_a_saml_idp_metadata_no_keys(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
        .await;

    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR_NO_KEYS.to_string();

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/v1/system/identity-providers/saml?silo={}", SILO_NAME),
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

            group_attribute_name: None,
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

    create_silo(&client, "hidden", false, shared::SiloIdentityMode::SamlJit)
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
        "/v1/system/identity-providers/saml?silo=hidden",
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

            group_attribute_name: None,
        },
    )
    .await;

    // Expect the SSO redirect when trying to log in
    let result = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!(
                "/login/hidden/saml/{}/redirect",
                silo_saml_idp.identity.name
            ),
        )
        .expect_status(Some(StatusCode::FOUND)),
    )
    .execute()
    .await
    .expect("expected success");

    assert!(
        result.headers["Location"].to_str().unwrap().to_string().starts_with(
            "https://idp.example.org/SAML2/SSO/Redirect?SAMLRequest=",
        )
    );
}

// Can't create a SAML IdP if the metadata URL returns something that's not 200
#[nexus_test]
async fn test_saml_idp_metadata_url_404(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
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
            &format!("/v1/system/identity-providers/saml?silo={}", SILO_NAME),
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

            group_attribute_name: None,
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
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
        .await;

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/v1/system/identity-providers/saml?silo={}", SILO_NAME),
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

            group_attribute_name: None,
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
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
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
            public_cert: base64::engine::general_purpose::STANDARD
                .encode("not a cert"),
            private_key: RSA_KEY_1_PRIVATE.to_string(),
        },
        params::DerEncodedKeyPair {
            public_cert: RSA_KEY_1_PUBLIC.to_string(),
            private_key: base64::engine::general_purpose::STANDARD
                .encode("not a cert"),
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
                &format!(
                    "/v1/system/identity-providers/saml?silo={}",
                    SILO_NAME
                ),
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

                group_attribute_name: None,
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
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
        .await;

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/v1/system/identity-providers/saml?silo={}", SILO_NAME),
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

            group_attribute_name: None,
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

        group_attribute_name: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::engine::general_purpose::STANDARD
            .encode(&SAML_RESPONSE),
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

        group_attribute_name: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::engine::general_purpose::STANDARD
            .encode(&SAML_RESPONSE_SIGNED_WITH_ECDSA_SHA256),
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

// Test rejecting a SAML response signed with a key that doesn't match the silo
// identity provider's key
#[test]
fn test_reject_saml_response_signed_with_other_key() {
    let silo_saml_identity_provider = SamlIdentityProvider {
        idp_metadata_document_string: SAML_RESPONSE_IDP_DESCRIPTOR.to_string(),

        idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
        sp_client_id: "https://customer.site/oxide_rack/saml".to_string(),
        acs_url: "https://customer.site/oxide_rack/saml".to_string(),
        slo_url: "http://slo".to_string(),
        technical_contact_email: "technical@fake".to_string(),

        public_cert: None,
        private_key: None,

        group_attribute_name: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::engine::general_purpose::STANDARD
            .encode(&SAML_RESPONSE_SIGNED_WITH_ECDSA_SHA256),
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

        group_attribute_name: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::engine::general_purpose::STANDARD
            .encode(&SAML_RESPONSE_ONLY_ASSERTION_SIGNED),
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

        group_attribute_name: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::engine::general_purpose::STANDARD
            .encode(&SAML_RESPONSE_UNSIGNED),
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

// Test accepting a correct SAML response that contains a XML comment in
// saml:NameID, and ensuring that the full text node is extracted (and not a
// substring).
//
// This used to be a test that _rejected_ such responses, but a change to an
// upstream dependency (quick-xml) caused the behavior around text nodes with
// embedded comments to change. Specifically, consider:
//
// <saml:NameId>user@example.com<!--comment-->.evil.com</saml:NameId>
//
// What should the text node for this element be?
//
// * Some XML parsing libraries just return "user@example.com". That leads to a
//   vulnerability, where an attacker can get a response signed with a
//   different email address than intended.
// * Some XML libraries return "user@example.com.evil.com". This is safe,
//   because the text after the comment hasn't been dropped. This is the behavior
//   with quick-xml 0.30, and the one that we're testing here.
// * Some XML libraries are unable to deserialize the document. This is also
//   safe (and not particularly problematic because typically SAML responses
//   aren't going to contain comments), and was the behavior with quick-xml
//   0.23.
//
// See:
// https://duo.com/blog/duo-finds-saml-vulnerabilities-affecting-multiple-implementations
#[test]
fn test_handle_saml_response_with_xml_comment() {
    let silo_saml_identity_provider = SamlIdentityProvider {
        idp_metadata_document_string: SAML_RESPONSE_IDP_DESCRIPTOR.to_string(),

        idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
        sp_client_id: "https://customer.site/oxide_rack/saml".to_string(),
        acs_url: "https://customer.site/oxide_rack/saml".to_string(),
        slo_url: "http://slo".to_string(),
        technical_contact_email: "technical@fake".to_string(),

        public_cert: None,
        private_key: None,

        group_attribute_name: None,
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::engine::general_purpose::STANDARD
            .encode(&SAML_RESPONSE_WITH_COMMENT),
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

    let (authenticated_subject, _) =
        result.expect("expected validation to succeed");
    assert_eq!(authenticated_subject.external_id, "some@customer.com");
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

        group_attribute_name: Some("groups".into()),
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::engine::general_purpose::STANDARD
            .encode(&SAML_RESPONSE_WITH_GROUPS),
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

// Test receiving a correct SAML response that has group attributes but not the
// same group_attribute_name
#[test]
fn test_correct_saml_response_with_group_attributes_wrong_attribute_name() {
    let silo_saml_identity_provider = SamlIdentityProvider {
        idp_metadata_document_string: SAML_RESPONSE_IDP_DESCRIPTOR.to_string(),

        idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
        sp_client_id: "https://customer.site/oxide_rack/saml".to_string(),
        acs_url: "https://customer.site/oxide_rack/saml".to_string(),
        slo_url: "http://slo".to_string(),
        technical_contact_email: "technical@fake".to_string(),

        public_cert: None,
        private_key: None,

        group_attribute_name: Some("something".into()),
    };

    let body_bytes = serde_urlencoded::to_string(SamlLoginPost {
        saml_response: base64::engine::general_purpose::STANDARD
            .encode(&SAML_RESPONSE_WITH_GROUPS),
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
    assert!(authenticated_subject.groups.is_empty());
    assert_eq!(relay_state, None);
}

// Test getting redirected with correct SAML response, and asking whoami
#[nexus_test]
async fn test_post_saml_response(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
        .await;

    let _silo_saml_idp: views::SamlIdentityProvider = object_create(
        client,
        &format!("/v1/system/identity-providers/saml?silo={}", SILO_NAME),
        &params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Base64EncodedXml {
                data: base64::engine::general_purpose::STANDARD
                    .encode(SAML_RESPONSE_IDP_DESCRIPTOR),
            },

            idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "https://customer.site/oxide_rack/saml".to_string(),
            slo_url: "https://customer.site/oxide_rack/saml".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,

            group_attribute_name: Some("groups".into()),
        },
    )
    .await;

    let nexus = &cptestctx.server.server_context().nexus;
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
            &format!(
                "/login/{}/saml/some-totally-real-saml-provider",
                SILO_NAME
            ),
        )
        .raw_body(Some(
            serde_urlencoded::to_string(SamlLoginPost {
                saml_response: base64::engine::general_purpose::STANDARD
                    .encode(SAML_RESPONSE_WITH_GROUPS),
                relay_state: None,
            })
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::SEE_OTHER)),
    )
    .execute()
    .await
    .expect("expected success");

    assert_eq!(result.headers["Location"].to_str().unwrap(), "/");

    // ask whoami
    NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me")
            .expect_status(Some(StatusCode::UNAUTHORIZED)),
    )
    .execute()
    .await
    .expect("expected success");

    let session_cookie_value =
        result.headers["Set-Cookie"].to_str().unwrap().to_string();

    let groups: ResultsPage<views::Group> = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/groups")
            .header(http::header::COOKIE, session_cookie_value.clone())
            .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected success")
    .parsed_body()
    .unwrap();

    let silo_group_names: Vec<&str> =
        groups.items.iter().map(|g| g.display_name.as_str()).collect();
    let silo_group_ids: Vec<SiloGroupUuid> =
        groups.items.iter().map(|g| g.id).collect();

    assert_same_items(silo_group_names, vec!["SRE", "Admins"]);

    let session_me = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me")
            .header(http::header::COOKIE, session_cookie_value.clone())
            .expect_status(Some(StatusCode::OK)),
    )
    .execute_and_parse_unwrap::<views::CurrentUser>()
    .await;

    assert_eq!(session_me.user.display_name, "some@customer.com");

    let groups: ResultsPage<views::Group> = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me/groups")
            .header(http::header::COOKIE, session_cookie_value)
            .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected success")
    .parsed_body()
    .unwrap();

    let session_me_group_ids =
        groups.items.iter().map(|g| g.id).collect::<Vec<_>>();

    assert_same_items(session_me_group_ids, silo_group_ids);
}

// Test correct SAML response with relay state
#[nexus_test]
async fn test_post_saml_response_with_relay_state(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
        .await;

    let _silo_saml_idp: views::SamlIdentityProvider = object_create(
        client,
        &format!("/v1/system/identity-providers/saml?silo={}", SILO_NAME),
        &params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Base64EncodedXml {
                data: base64::engine::general_purpose::STANDARD
                    .encode(SAML_RESPONSE_IDP_DESCRIPTOR),
            },

            idp_entity_id: "https://some.idp.test/oxide_rack/".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "https://customer.site/oxide_rack/saml".to_string(),
            slo_url: "https://customer.site/oxide_rack/saml".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,

            group_attribute_name: None,
        },
    )
    .await;

    let nexus = &cptestctx.server.server_context().nexus;
    nexus.set_samael_max_issue_delay(
        chrono::Utc::now()
            - "2022-05-04T15:36:12.631Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
            + chrono::Duration::seconds(60),
    );

    let result_with_relay_state = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!(
                "/login/{}/saml/some-totally-real-saml-provider",
                SILO_NAME
            ),
        )
        .raw_body(Some(
            serde_urlencoded::to_string(SamlLoginPost {
                saml_response: base64::engine::general_purpose::STANDARD
                    .encode(SAML_RESPONSE),
                relay_state: Some(
                    RelayState {
                        redirect_uri: Some(
                            "/some/actual/nexus/url".parse().unwrap(),
                        ),
                    }
                    .to_encoded()
                    .unwrap(),
                ),
            })
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::SEE_OTHER)),
    )
    .execute()
    .await
    .expect("expected success");

    assert!(
        result_with_relay_state.headers["Location"]
            .to_str()
            .unwrap()
            .to_string()
            .ends_with("/some/actual/nexus/url")
    );

    let result_with_invalid_relay_state = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!(
                "/login/{}/saml/some-totally-real-saml-provider",
                SILO_NAME
            ),
        )
        .raw_body(Some(
            serde_urlencoded::to_string(SamlLoginPost {
                saml_response: base64::engine::general_purpose::STANDARD
                    .encode(SAML_RESPONSE),
                relay_state: Some("some-idp-set-value".to_string()),
            })
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::SEE_OTHER)),
    )
    .execute()
    .await
    .expect("expected success");

    assert_eq!(
        result_with_invalid_relay_state.headers["Location"].to_str().unwrap(),
        "/"
    );
}

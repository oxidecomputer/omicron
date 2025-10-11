// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::integration_tests::saml::SAML_IDP_DESCRIPTOR;
use crate::integration_tests::saml::SAML_RESPONSE_IDP_DESCRIPTOR;
use crate::integration_tests::saml::SAML_RESPONSE_WITH_GROUPS;
use async_bb8_diesel::AsyncRunQueryDsl;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use http::method::Method;
use nexus_db_queries::authn::USER_TEST_PRIVILEGED;
use nexus_db_queries::authn::silos::{IdentityProviderType, SamlLoginPost};
use nexus_db_queries::db::model::ScimClientBearerToken;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::object_create_no_body;
use nexus_test_utils::resource_helpers::object_delete;
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views::{self, Silo};
use nexus_types::external_api::{params, shared};
use nexus_types::identity::Asset;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::TestInterfaces;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Create a SAML+SCIM Silo, test we can create a SAML IDP for it
#[nexus_test]
async fn test_create_a_saml_scim_silo(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;
    let silo: Silo = NexusRequest::object_get(
        &client,
        &format!("/v1/system/silos/{SILO_NAME}"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request")
    .parsed_body()
    .unwrap();

    // Assert we can create a SAML IDP for this identity type

    let silo_saml_idp: views::SamlIdentityProvider = object_create(
        client,
        &format!("/v1/system/identity-providers/saml?silo={SILO_NAME}"),
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
                    .encode(SAML_IDP_DESCRIPTOR),
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

// Test that users are not JITed for SamlScim silos
#[nexus_test]
async fn test_no_jit_for_saml_scim_silos(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    let _silo_saml_idp: views::SamlIdentityProvider = object_create(
        client,
        &format!("/v1/system/identity-providers/saml?silo={SILO_NAME}"),
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

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/login/{SILO_NAME}/saml/some-totally-real-saml-provider"),
        )
        .raw_body(Some(
            serde_urlencoded::to_string(SamlLoginPost {
                saml_response: base64::engine::general_purpose::STANDARD
                    .encode(SAML_RESPONSE_WITH_GROUPS),
                relay_state: None,
            })
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::UNAUTHORIZED)),
    )
    .execute()
    .await
    .expect("expected 401");
}

#[nexus_test]
async fn test_scim_client_token_crud(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a Silo, then grant the PrivilegedUser the Admin role on it

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Initially, there should be no tokens created during silo create.

    let tokens: Vec<views::ScimClientBearerToken> =
        object_get(client, &format!("/v1/system/scim/tokens?silo={SILO_NAME}"))
            .await;

    assert!(tokens.is_empty());

    // Fleet admins can create SCIM client tokens

    let created_token_1: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={SILO_NAME}"),
        )
        .await;

    // Now there's one!

    let tokens: Vec<views::ScimClientBearerToken> =
        object_get(client, &format!("/v1/system/scim/tokens?silo={SILO_NAME}"))
            .await;

    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].id, created_token_1.id);

    // Get that specific token

    let token: views::ScimClientBearerToken = object_get(
        client,
        &format!(
            "/v1/system/scim/tokens/{}?silo={SILO_NAME}",
            created_token_1.id,
        ),
    )
    .await;

    assert_eq!(token.id, created_token_1.id);

    // Create a new token

    let created_token_2: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={SILO_NAME}"),
        )
        .await;

    // Now there's two!

    let tokens: Vec<views::ScimClientBearerToken> =
        object_get(client, &format!("/v1/system/scim/tokens?silo={SILO_NAME}"))
            .await;

    assert_eq!(tokens.len(), 2);
    assert!(tokens.iter().any(|token| token.id == created_token_1.id));
    assert!(tokens.iter().any(|token| token.id == created_token_2.id));

    // Create one more

    let created_token_3: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={SILO_NAME}"),
        )
        .await;

    let tokens: Vec<views::ScimClientBearerToken> =
        object_get(client, &format!("/v1/system/scim/tokens?silo={SILO_NAME}"))
            .await;

    assert_eq!(tokens.len(), 3);
    assert!(tokens.iter().any(|token| token.id == created_token_1.id));
    assert!(tokens.iter().any(|token| token.id == created_token_2.id));
    assert!(tokens.iter().any(|token| token.id == created_token_3.id));

    // Delete one

    object_delete(
        client,
        &format!(
            "/v1/system/scim/tokens/{}?silo={SILO_NAME}",
            created_token_1.id,
        ),
    )
    .await;

    // Check there's two

    let tokens: Vec<views::ScimClientBearerToken> =
        object_get(client, &format!("/v1/system/scim/tokens?silo={SILO_NAME}"))
            .await;

    assert_eq!(tokens.len(), 2);
    assert!(tokens.iter().any(|token| token.id == created_token_2.id));
    assert!(tokens.iter().any(|token| token.id == created_token_3.id));
}

#[nexus_test]
async fn test_scim_client_token_tenancy(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create two Silos, then grant the PrivilegedUser the Admin role on both

    const SILO_1_NAME: &str = "saml-scim-silo-1";
    const SILO_2_NAME: &str = "saml-scim-silo-2";

    create_silo(&client, SILO_1_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    create_silo(&client, SILO_2_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_1_NAME}"),
        shared::SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_2_NAME}"),
        shared::SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Initially, there should be no tokens created during silo create.

    let tokens: Vec<views::ScimClientBearerToken> = object_get(
        client,
        &format!("/v1/system/scim/tokens?silo={SILO_1_NAME}"),
    )
    .await;

    assert!(tokens.is_empty());

    let tokens: Vec<views::ScimClientBearerToken> = object_get(
        client,
        &format!("/v1/system/scim/tokens?silo={SILO_2_NAME}"),
    )
    .await;

    assert!(tokens.is_empty());

    // Create a token in one of the Silos

    let _created_token_1: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={SILO_1_NAME}"),
        )
        .await;

    // Now there's one but only in the first Silo

    let tokens: Vec<views::ScimClientBearerToken> = object_get(
        client,
        &format!("/v1/system/scim/tokens?silo={SILO_1_NAME}"),
    )
    .await;

    assert!(!tokens.is_empty());

    let tokens: Vec<views::ScimClientBearerToken> = object_get(
        client,
        &format!("/v1/system/scim/tokens?silo={SILO_2_NAME}"),
    )
    .await;

    assert!(tokens.is_empty());
}

#[nexus_test]
async fn test_scim_client_token_bearer_auth(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a Silo, then grant the PrivilegedUser the Admin role on it

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a token

    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={SILO_NAME}"),
        )
        .await;

    // Check that we can get a SCIM provider using that token
    // XXX this will 500 until the final impl PR, but it should not 401

    RequestBuilder::new(client, Method::GET, "/scim/v2/Users")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer {}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::INTERNAL_SERVER_ERROR))
        .execute()
        .await
        .expect("expected 500");
}

#[nexus_test]
async fn test_scim_client_no_auth_with_expired_token(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    // Create a Silo, then insert an expired token into it

    const SILO_NAME: &str = "saml-scim-silo";

    let silo = create_silo(
        &client,
        SILO_NAME,
        true,
        shared::SiloIdentityMode::SamlScim,
    )
    .await;

    // Manually create an expired token

    {
        let now = Utc::now();

        let new_token = ScimClientBearerToken {
            id: Uuid::new_v4(),
            time_created: now,
            time_deleted: None,
            time_expires: Some(now),
            silo_id: silo.identity.id,
            bearer_token: String::from("testpost"),
        };

        let conn = nexus.datastore().pool_connection_for_tests().await.unwrap();

        use nexus_db_schema::schema::scim_client_bearer_token::dsl;
        diesel::insert_into(dsl::scim_client_bearer_token)
            .values(new_token.clone())
            .execute_async(&*conn)
            .await
            .unwrap();
    }

    // This should 401

    RequestBuilder::new(client, Method::GET, "/scim/v2/Users")
        .header(http::header::AUTHORIZATION, String::from("Bearer testpost"))
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("expected 401");
}

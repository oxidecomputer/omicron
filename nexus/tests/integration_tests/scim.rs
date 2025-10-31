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
use nexus_auth::context::OpContext;
use nexus_db_queries::authn::USER_TEST_PRIVILEGED;
use nexus_db_queries::authn::silos::{IdentityProviderType, SamlLoginPost};
use nexus_db_queries::db::model::ScimClientBearerToken;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::create_silo_with_admin_group_name;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::grant_iam_for_group;
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
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloGroupUuid;
use uuid::Uuid;

use scim2_test_client::Tester;

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

    RequestBuilder::new(client, Method::GET, "/scim/v2/Users")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::OK))
        .execute()
        .await
        .expect("expected 200");
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
        .header(
            http::header::AUTHORIZATION,
            String::from("Bearer oxide-scim-testpost"),
        )
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("expected 401");
}

#[nexus_test]
async fn test_scim2_crate_self_test(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    // Create a Silo, then grant the PrivilegedUser the Admin role on it

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        opctx.authn.actor().unwrap().silo_user_id().unwrap(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a token

    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={}", SILO_NAME,),
        )
        .await;

    // Point the scim2-rs crate's self tester at Nexus

    let tester = Tester::new(
        client.url("/scim/v2").to_string(),
        Some(format!("oxide-scim-{}", created_token.bearer_token)),
    )
    .unwrap();

    tester.run().await.unwrap();
}

// Test that disabling a SCIM user means they can no longer log in, and it
// invalidates all their sessions.
#[nexus_test]
async fn test_disabling_scim_user(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    // Create the Silo

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    // Create a SAML IDP

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

    nexus.set_samael_max_issue_delay(
        chrono::Utc::now()
            - "2022-05-04T15:36:12.631Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
            + chrono::Duration::seconds(60),
    );

    // The user isn't created yet so we should see a 401.

    NexusRequest::new(
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
        .expect_status(Some(StatusCode::UNAUTHORIZED)),
    )
    .execute()
    .await
    .expect("expected 401");

    // Grant permissions on this silo for the PrivilegedUser

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        opctx.authn.actor().unwrap().silo_user_id().unwrap(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a token

    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={}", SILO_NAME,),
        )
        .await;

    // Using this SCIM token, create a user with a name matching the saml:NameID
    // email in SAML_RESPONSE_WITH_GROUPS.

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Users")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                    "userName": "some@customer.com",
                    "externalId": "some@customer.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201");

    // Now the user can log in and create a valid session

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
    .expect("expected 303");

    let session_cookie_value =
        result.headers["Set-Cookie"].to_str().unwrap().to_string();

    let me: views::CurrentUser = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me")
            .header(http::header::COOKIE, session_cookie_value.clone())
            .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected success")
    .parsed_body()
    .unwrap();

    assert_eq!(me.user.display_name, String::from("some@customer.com"));

    // Disable the user by asetting active = false

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PATCH,
            &format!("/scim/v2/Users/{}", me.user.id),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .raw_body(Some(
            serde_json::to_string(&serde_json::json!(
                {
                  "schemas": [
                    "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                  ],
                  "Operations": [
                    {
                      "op": "replace",
                      "value": {
                        "active": false
                      }
                    }
                  ]
                }
            ))
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200");

    // The same session should not work anymore.

    NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me")
            .header(http::header::COOKIE, session_cookie_value.clone())
            .expect_status(Some(StatusCode::UNAUTHORIZED)),
    )
    .execute()
    .await
    .expect("expected 401");

    // And they can no longer log in

    NexusRequest::new(
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
        .expect_status(Some(StatusCode::UNAUTHORIZED)),
    )
    .execute()
    .await
    .expect("expected 401");
}

// Test that searching for a SCIM user works and is case insensitive
#[nexus_test]
async fn test_scim_user_search(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    // Create the Silo

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    // Grant permissions on this silo for the PrivilegedUser

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        opctx.authn.actor().unwrap().silo_user_id().unwrap(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a token

    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={}", SILO_NAME,),
        )
        .await;

    // Using this SCIM token, create two users.

    let _mike: scim2_rs::User = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Users")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        "userName": "mscott",
                        "externalId": "mscott@dundermifflin.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created user");

    let created_user: scim2_rs::User = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Users")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        // Note that the name uses upper case!
                        "userName": "JHALPERT",
                        "externalId": "jhalpert@dundermifflin.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created user");

    // Now search for that user

    let response: scim2_rs::ListResponse = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            "/scim/v2/Users?filter=username%20eq%20%22JHALPERT%22",
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200")
    .parsed_body()
    .expect("list of users");

    // `response.resources` is a Vec of generic resources that have the type
    // `serde_json::Map<String, serde_json::Value>`. This to_value -> from_value
    // converts it into a User without a trip though string serialization +
    // deserialization.
    let users: Vec<scim2_rs::User> = serde_json::from_value(
        serde_json::to_value(&response.resources).unwrap(),
    )
    .unwrap();

    assert_eq!(users.len(), 1);
    assert_eq!(users[0].id, created_user.id);

    // Case insensitive search should also work

    let response: scim2_rs::ListResponse = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            "/scim/v2/Users?filter=username%20eq%20%22JhaLpErT%22",
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200")
    .parsed_body()
    .expect("list of users");

    let users: Vec<scim2_rs::User> = serde_json::from_value(
        serde_json::to_value(&response.resources).unwrap(),
    )
    .unwrap();

    assert_eq!(users.len(), 1);
    assert_eq!(users[0].id, created_user.id);

    // Searching for a non-existent user should return nothing

    let response: scim2_rs::ListResponse = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            "/scim/v2/Users?filter=username%20eq%20%22dschrute%22",
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200")
    .parsed_body()
    .expect("list of users");

    assert_eq!(response.total_results, 0);
}

// Test that searching for a SCIM group works and is case insensitive
#[nexus_test]
async fn test_scim_group_search(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    // Create the Silo

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    // Grant permissions on this silo for the PrivilegedUser

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        opctx.authn.actor().unwrap().silo_user_id().unwrap(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a token

    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={}", SILO_NAME,),
        )
        .await;

    // Using this SCIM token, create two groups.

    let _regional_managers: scim2_rs::Group = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Groups")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        "displayName": "regional_managers",
                        "externalId": "regional_managers@dundermifflin.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created group");

    let created_group: scim2_rs::Group = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Groups")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        // Note that the name uses upper case!
                        "displayName": "ASSISTANT_TO_REGIONAL_MANAGER",
                        "externalId": "arm@dundermifflin.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created group");

    // Now search for that group

    let response: scim2_rs::ListResponse = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!(
                "/scim/v2/Groups?filter=displayname%20eq%20%22{}%22",
                "ASSISTANT_TO_REGIONAL_MANAGER",
            ),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200")
    .parsed_body()
    .expect("list of groups");

    // `response.resources` is a Vec of generic resources that have the type
    // `serde_json::Map<String, serde_json::Value>`. This to_value -> from_value
    // converts it into a Group without a trip though string serialization +
    // deserialization.
    let groups: Vec<scim2_rs::Group> = serde_json::from_value(
        serde_json::to_value(&response.resources).unwrap(),
    )
    .unwrap();

    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].id, created_group.id);

    // Case insensitive search should also work

    let response: scim2_rs::ListResponse = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!(
                "/scim/v2/Groups?filter=displayname%20eq%20%22{}%22",
                "AsSIStANT_TO_regIOnAL_mANaGER",
            ),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200")
    .parsed_body()
    .expect("list of groups");

    let groups: Vec<scim2_rs::Group> = serde_json::from_value(
        serde_json::to_value(&response.resources).unwrap(),
    )
    .unwrap();

    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].id, created_group.id);

    // Searching for a non-existent group should return nothing

    let response: scim2_rs::ListResponse = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!(
                "/scim/v2/Groups?filter=displayname%20eq%20%22{}%22",
                "assistant_to_assistant_to_regional_manager",
            ),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200")
    .parsed_body()
    .expect("list of groups");

    assert_eq!(response.total_results, 0);
}

// Test that for SCIM users, userName is unique (even if case is different)
#[nexus_test]
async fn test_scim_user_unique(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    // Create the Silo

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    // Grant permissions on this silo for the PrivilegedUser

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        opctx.authn.actor().unwrap().silo_user_id().unwrap(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a token

    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={}", SILO_NAME,),
        )
        .await;

    // Using this SCIM token, try to create two "identical" users.

    let _mike: scim2_rs::User = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Users")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        "userName": "mscott",
                        "externalId": "mscott@dundermifflin.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created user");

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Users")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        "userName": "MscOtT",
                        "externalId": "mscott@dundermifflin.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CONFLICT)),
    )
    .execute()
    .await
    .expect("expected 409");

    // Now, create a different user, then try to PUT so that the name matches.
    // This should fail with a 409 as well.

    let mike_scarn: scim2_rs::User = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Users")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        "userName": "mscarn",
                        "externalId": "mscarn@dundermifflin.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created user");

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PUT,
            &format!("/scim/v2/Users/{}", mike_scarn.id),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .raw_body(Some(
            serde_json::to_string(&serde_json::json!(
                {
                    "userName": "mscott",
                    "externalId": "mscott@dundermifflin.com",
                }
            ))
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::CONFLICT)),
    )
    .execute()
    .await
    .expect("expected 409");
}

// Test that for SCIM groups, displayName is unique (even if case is different)
#[nexus_test]
async fn test_scim_group_unique(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    // Create the Silo

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    // Grant permissions on this silo for the PrivilegedUser

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        opctx.authn.actor().unwrap().silo_user_id().unwrap(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a token

    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={}", SILO_NAME,),
        )
        .await;

    // Using this SCIM token, try to create two "identical" groups

    let _sales: scim2_rs::Group = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Groups")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        "displayName": "Sales",
                        "externalId": "sales@dundermifflin.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created group");

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Groups")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        "displayName": "SALES",
                        "externalId": "sales@dundermifflin.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CONFLICT)),
    )
    .execute()
    .await
    .expect("expected 409");

    // Now, create a different group, then try to PUT so that the name matches.
    // This should fail with a 409 as well.

    let accounting: scim2_rs::Group = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Groups")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        "displayName": "accounting",
                        "externalId": "accounting@dundermifflin.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created group");

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PUT,
            &format!("/scim/v2/Groups/{}", accounting.id),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .raw_body(Some(
            serde_json::to_string(&serde_json::json!(
                {
                    "displayName": "sales",
                    "externalId": "sales@dundermifflin.com",
                }
            ))
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::CONFLICT)),
    )
    .execute()
    .await
    .expect("expected 409");
}

// Test that a group with the silo admin group name confers admin privileges
#[nexus_test]
async fn test_scim_user_admin_group_priv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    // Create the Silo

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo_with_admin_group_name(
        &client,
        SILO_NAME,
        true,
        shared::SiloIdentityMode::SamlScim,
        Some(String::from("scranton_admins")),
    )
    .await;

    // Create a SAML IDP

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

    nexus.set_samael_max_issue_delay(
        chrono::Utc::now()
            - "2022-05-04T15:36:12.631Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
            + chrono::Duration::seconds(60),
    );

    // Grant permissions on this silo for the PrivilegedUser

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        opctx.authn.actor().unwrap().silo_user_id().unwrap(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a token

    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={}", SILO_NAME,),
        )
        .await;

    // Using this SCIM token, create a user with a name matching the saml:NameID
    // email in SAML_RESPONSE_WITH_GROUPS.

    let user: scim2_rs::User = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Users")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                    "userName": "some@customer.com",
                    "externalId": "some@customer.com",
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created user");

    // Login with that user

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
    .expect("expected 303");

    let session_cookie_value =
        result.headers["Set-Cookie"].to_str().unwrap().to_string();

    // Initially this user should _not_ have the silo admin role, they are not
    // part of any group.

    let me: views::CurrentUser = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me")
            .header(http::header::COOKIE, session_cookie_value.clone())
            .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected success")
    .parsed_body()
    .unwrap();

    assert!(!me.silo_admin);

    // Creating the group with a name that matches the silo admin group name but
    // with no members does not change the existing user's role assignment.

    let admin_group: scim2_rs::Group = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Groups")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        "displayName": "scranton_admins",
                        "externalId": "scranton_admins",
                        "members": [],
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created group");

    let me: views::CurrentUser = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me")
            .header(http::header::COOKIE, session_cookie_value.clone())
            .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected success")
    .parsed_body()
    .unwrap();

    assert!(!me.silo_admin);

    // Then, add the user to the silo admin group via a PATCH - they should gain
    // the admin role

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PATCH,
            &format!("/scim/v2/Groups/{}", admin_group.id),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .raw_body(Some(
            serde_json::to_string(&serde_json::json!(
                {
                  "schemas": [
                    "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                  ],
                  "Operations": [
                    {
                      "op": "add",
                      "path": "members",
                      "value": [
                        {
                          "value": user.id
                        }
                      ]
                    }
                  ]
                }
            ))
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200");

    let me: views::CurrentUser = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me")
            .header(http::header::COOKIE, session_cookie_value.clone())
            .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected success")
    .parsed_body()
    .unwrap();

    assert!(me.silo_admin);

    // Renaming the group to _not_ the silo admin group name means they should
    // lose the admin role

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PATCH,
            &format!("/scim/v2/Groups/{}", admin_group.id),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .raw_body(Some(
            serde_json::to_string(&serde_json::json!(
                {
                  "schemas": [
                    "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                  ],
                  "Operations": [
                    {
                      "op": "replace",
                      "value": {
                        "id": admin_group.id,
                        "displayName": "scranton_the_electric_city", // WHAT?
                      }
                    }
                  ]
                }
            ))
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200");

    let me: views::CurrentUser = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me")
            .header(http::header::COOKIE, session_cookie_value.clone())
            .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected success")
    .parsed_body()
    .unwrap();

    assert!(!me.silo_admin);

    // Renaming it back to what it was should mean they gain the admin role on
    // their silo

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PATCH,
            &format!("/scim/v2/Groups/{}", admin_group.id),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .raw_body(Some(
            serde_json::to_string(&serde_json::json!(
                {
                  "schemas": [
                    "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                  ],
                  "Operations": [
                    {
                      "op": "replace",
                      "value": {
                        "id": admin_group.id,
                        "displayName": "scranton_admins",
                      }
                    }
                  ]
                }
            ))
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200");

    let me: views::CurrentUser = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me")
            .header(http::header::COOKIE, session_cookie_value.clone())
            .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected success")
    .parsed_body()
    .unwrap();

    assert!(me.silo_admin);

    // Removing them from the group means they lose the admin role on their silo

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PATCH,
            &format!("/scim/v2/Groups/{}", admin_group.id),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .raw_body(Some(
            serde_json::to_string(&serde_json::json!(
                {
                  "schemas": [
                    "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                  ],
                  "Operations": [
                    {
                      "op": "remove",
                      "path": "members",
                      "value": [
                        {
                          "value": user.id,
                        }
                      ]
                    }
                  ]
                }
            ))
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200");

    let me: views::CurrentUser = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/v1/me")
            .header(http::header::COOKIE, session_cookie_value.clone())
            .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected success")
    .parsed_body()
    .unwrap();

    assert!(!me.silo_admin);
}

// Test that if a group already has the silo admin role, renaming it to the silo
// admin group name won't error with a conflict.
#[nexus_test]
async fn test_scim_user_admin_group_priv_conflict(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    // Create the Silo

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo_with_admin_group_name(
        &client,
        SILO_NAME,
        true,
        shared::SiloIdentityMode::SamlScim,
        Some(String::from("assistant_to_assistant_to_regional_manager")),
    )
    .await;

    // Grant permissions on this silo for the PrivilegedUser

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        opctx.authn.actor().unwrap().silo_user_id().unwrap(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a token

    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={}", SILO_NAME,),
        )
        .await;

    // Create the group with a name that does not match the silo admin group
    // name.

    let group: scim2_rs::Group = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Groups")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!(
                    {
                        "displayName": "assistant_to_regional_manager",
                        "externalId": "assistant_to_regional_manager",
                        "members": [],
                    }
                ))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute()
    .await
    .expect("expected 201")
    .parsed_body()
    .expect("created group");

    // Create a role assignment of silo admin for this group

    grant_iam_for_group(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        SiloGroupUuid::from_untyped_uuid(group.id.parse().unwrap()),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Rename the group to match the silo's admin group name - this should not
    // error out

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PATCH,
            &format!("/scim/v2/Groups/{}", group.id),
        )
        .header(http::header::CONTENT_TYPE, "application/scim+json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer oxide-scim-{}", created_token.bearer_token),
        )
        .allow_non_dropshot_errors()
        .raw_body(Some(
            serde_json::to_string(&serde_json::json!(
                {
                  "schemas": [
                    "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                  ],
                  "Operations": [
                    {
                      "op": "replace",
                      "value": {
                        "id": group.id,
                        "displayName":
                            "assistant_to_assistant_to_regional_manager",
                      }
                    }
                  ]
                }
            ))
            .unwrap(),
        ))
        .expect_status(Some(StatusCode::OK)),
    )
    .execute()
    .await
    .expect("expected 200");
}

#[nexus_test]
async fn test_scim_list_users_with_groups(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    const SILO_NAME: &str = "saml-scim-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlScim)
        .await;

    grant_iam(
        client,
        &format!("/v1/system/silos/{SILO_NAME}"),
        shared::SiloRole::Admin,
        opctx.authn.actor().unwrap().silo_user_id().unwrap(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    let created_token: views::ScimClientBearerTokenValue =
        object_create_no_body(
            client,
            &format!("/v1/system/scim/tokens?silo={}", SILO_NAME),
        )
        .await;

    // Create 5 users
    let mut users = Vec::new();
    for i in 1..=5 {
        let user: scim2_rs::User = NexusRequest::new(
            RequestBuilder::new(client, Method::POST, "/scim/v2/Users")
                .header(http::header::CONTENT_TYPE, "application/scim+json")
                .header(
                    http::header::AUTHORIZATION,
                    format!("Bearer oxide-scim-{}", created_token.bearer_token),
                )
                .allow_non_dropshot_errors()
                .raw_body(Some(
                    serde_json::to_string(&serde_json::json!({
                        "userName": format!("user{}", i),
                        "externalId": format!("user{}@example.com", i),
                    }))
                    .unwrap(),
                ))
                .expect_status(Some(StatusCode::CREATED)),
        )
        .execute_and_parse_unwrap()
        .await;
        users.push(user);
    }

    // Create 3 groups with various membership patterns:
    // - group1: user1, user2, user3
    // - group2: user1, user4
    // - group3: no members
    let group1: scim2_rs::Group = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Groups")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!({
                    "displayName": "group1",
                    "externalId": "group1@example.com",
                    "members": [
                        {"value": users[0].id},
                        {"value": users[1].id},
                        {"value": users[2].id},
                    ],
                }))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute_and_parse_unwrap()
    .await;

    let group2: scim2_rs::Group = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Groups")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!({
                    "displayName": "group2",
                    "externalId": "group2@example.com",
                    "members": [
                        {"value": users[0].id},
                        {"value": users[3].id},
                    ],
                }))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute_and_parse_unwrap()
    .await;

    let _group3: scim2_rs::Group = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/scim/v2/Groups")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .raw_body(Some(
                serde_json::to_string(&serde_json::json!({
                    "displayName": "group3",
                    "externalId": "group3@example.com",
                }))
                .unwrap(),
            ))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .execute_and_parse_unwrap()
    .await;

    // List all users and verify group memberships
    let response: scim2_rs::ListResponse = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, "/scim/v2/Users")
            .header(http::header::CONTENT_TYPE, "application/scim+json")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer oxide-scim-{}", created_token.bearer_token),
            )
            .allow_non_dropshot_errors()
            .expect_status(Some(StatusCode::OK)),
    )
    .execute_and_parse_unwrap()
    .await;

    let returned_users: Vec<scim2_rs::User> = serde_json::from_value(
        serde_json::to_value(&response.resources).unwrap(),
    )
    .unwrap();

    // Find our created users in the response
    let find_user = |user_id: &str| {
        returned_users
            .iter()
            .find(|u| u.id == user_id)
            .expect("user should be in list")
    };

    // user1 should be in group1 and group2
    let user1 = find_user(&users[0].id);
    assert!(user1.groups.is_some());
    let user1_groups = user1.groups.as_ref().unwrap();
    assert_eq!(user1_groups.len(), 2);
    let user1_group_ids: std::collections::HashSet<_> = user1_groups
        .iter()
        .map(|g| g.value.as_ref().unwrap().as_str())
        .collect();
    assert!(user1_group_ids.contains(group1.id.as_str()));
    assert!(user1_group_ids.contains(group2.id.as_str()));

    // user2 should be in group1 only
    let user2 = find_user(&users[1].id);
    assert!(user2.groups.is_some());
    let user2_groups = user2.groups.as_ref().unwrap();
    assert_eq!(user2_groups.len(), 1);
    assert_eq!(user2_groups[0].value.as_ref().unwrap(), &group1.id);

    // user3 should be in group1 only
    let user3 = find_user(&users[2].id);
    assert!(user3.groups.is_some());
    let user3_groups = user3.groups.as_ref().unwrap();
    assert_eq!(user3_groups.len(), 1);
    assert_eq!(user3_groups[0].value.as_ref().unwrap(), &group1.id);

    // user4 should be in group2 only
    let user4 = find_user(&users[3].id);
    assert!(user4.groups.is_some());
    let user4_groups = user4.groups.as_ref().unwrap();
    assert_eq!(user4_groups.len(), 1);
    assert_eq!(user4_groups[0].value.as_ref().unwrap(), &group2.id);

    // user5 should have no groups
    let user5 = find_user(&users[4].id);
    assert!(user5.groups.is_none());
}

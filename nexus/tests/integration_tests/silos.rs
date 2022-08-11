// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use omicron_common::api::external::{IdentityMetadataCreateParams, Name};
use omicron_nexus::authn::silos::{AuthenticatedSubject, IdentityProviderType};
use omicron_nexus::context::OpContext;
use omicron_nexus::db::lookup::LookupPath;
use omicron_nexus::external_api::views::{
    self, IdentityProvider, Organization, SamlIdentityProvider, Silo,
};
use omicron_nexus::external_api::{params, shared};
use omicron_nexus::TestInterfaces as _;
use std::collections::HashSet;

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::resource_helpers::{
    create_organization, create_silo, grant_iam, object_create,
    objects_list_page_authz,
};

use crate::integration_tests::saml::SAML_IDP_DESCRIPTOR;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_nexus::authz::{self, SiloRole};
use uuid::Uuid;

use httptest::{matchers::*, responders::*, Expectation, Server};
use omicron_nexus::authn::{USER_TEST_PRIVILEGED, USER_TEST_UNPRIVILEGED};
use omicron_nexus::db::fixed_data::silo::SILO_ID;
use omicron_nexus::db::identity::Asset;

#[nexus_test]
async fn test_silos(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create two silos: one discoverable, one not
    create_silo(
        &client,
        "discoverable",
        true,
        shared::UserProvisionType::Fixed,
    )
    .await;
    create_silo(&client, "hidden", false, shared::UserProvisionType::Fixed)
        .await;

    // Verify GET /silos/{silo} works for both discoverable and not
    let discoverable_url = "/silos/discoverable";
    let hidden_url = "/silos/hidden";

    let silo: Silo = NexusRequest::object_get(&client, &discoverable_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body()
        .unwrap();
    assert_eq!(silo.identity.name, "discoverable");

    let silo: Silo = NexusRequest::object_get(&client, &hidden_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body()
        .unwrap();
    assert_eq!(silo.identity.name, "hidden");

    // Verify 404 if silo doesn't exist
    NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &"/silos/testpost",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Verify GET /silos only returns discoverable silos
    let silos = objects_list_page_authz::<Silo>(client, "/silos").await.items;
    assert_eq!(silos.len(), 1);
    assert_eq!(silos[0].identity.name, "discoverable");

    // Create a new user in the discoverable silo
    let new_silo_user_id =
        "6922f0b2-9a92-659b-da6b-93ad4955a3a3".parse().unwrap();
    nexus
        .silo_user_create(
            silos[0].identity.id, /* silo id */
            new_silo_user_id,
            "some_silo_user".into(),
        )
        .await
        .unwrap();

    // Grant the user "admin" privileges on that Silo.
    grant_iam(
        client,
        "/silos/discoverable",
        SiloRole::Admin,
        new_silo_user_id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // TODO-coverage, TODO-security: Add test for Silo-local session
    // when we can use users in another Silo.

    let authn_opctx = nexus.opctx_external_authn();

    // Create organization with built-in user auth
    // Note: this currently goes to the built-in silo!
    let org_name: Name = "someorg".parse().unwrap();
    let new_org_in_default_silo =
        create_organization(&client, org_name.as_str()).await;

    // Create an Organization of the same name in a different Silo to verify
    // that's possible.
    let new_org_in_our_silo = NexusRequest::objects_post(
        client,
        "/organizations",
        &params::OrganizationCreate {
            identity: IdentityMetadataCreateParams {
                name: org_name.clone(),
                description: String::new(),
            },
        },
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to create same-named Organization in a different Silo")
    .parsed_body::<views::Organization>()
    .expect("failed to parse new Organization");
    assert_eq!(
        new_org_in_default_silo.identity.name,
        new_org_in_our_silo.identity.name
    );
    assert_ne!(
        new_org_in_default_silo.identity.id,
        new_org_in_our_silo.identity.id
    );
    // Delete it so that we can delete the Silo later.
    NexusRequest::object_delete(
        client,
        &format!("/organizations/{}", org_name),
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to delete test Organization");

    // Verify GET /organizations works with built-in user auth
    let organizations =
        objects_list_page_authz::<Organization>(client, "/organizations")
            .await
            .items;
    assert_eq!(organizations.len(), 1);
    assert_eq!(organizations[0].identity.name, "someorg");

    // TODO: uncomment when silo users can have role assignments
    /*
    // Verify GET /organizations doesn't list anything if authing under
    // different silo.
    let organizations =
        objects_list_page_authz_with_session::<Organization>(
            client, "/organizations", &session,
        )
        .await
        .items;
    assert_eq!(organizations.len(), 0);
    */

    // Verify DELETE doesn't work if organizations exist
    // TODO: put someorg in discoverable silo, not built-in
    NexusRequest::expect_failure(
        &client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &"/silos/default-silo",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Delete organization
    NexusRequest::object_delete(&client, &"/organizations/someorg")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");

    // Verify silo DELETE works
    NexusRequest::object_delete(&client, &"/silos/discoverable")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");

    // Verify silo user was also deleted
    nexus
        .silo_user_fetch(authn_opctx, new_silo_user_id)
        .await
        .expect_err("unexpected success");
}

// Test that admin group is created if admin_group_name is applied.
#[nexus_test]
async fn test_silo_admin_group(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    let silo: Silo = object_create(
        client,
        "/silos",
        &params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: "silo-name".parse().unwrap(),
                description: "a silo".to_string(),
            },
            discoverable: false,
            user_provision_type: shared::UserProvisionType::Jit,
            admin_group_name: Some("administrator".into()),
        },
    )
    .await;
    grant_iam(
        &client,
        "/silos/silo-name",
        SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    let authn_opctx = nexus.opctx_external_authn();

    let (authz_silo, db_silo) =
        LookupPath::new(&authn_opctx, &nexus.datastore())
            .silo_name(&silo.identity.name.into())
            .fetch()
            .await
            .unwrap();

    assert!(nexus
        .datastore()
        .silo_group_optional_lookup(
            &authn_opctx,
            &authz_silo,
            "administrator".into(),
        )
        .await
        .unwrap()
        .is_some());

    // Test that a user is granted privileges from their group membership
    let admin_group_user = nexus
        .silo_user_from_authenticated_subject(
            &authn_opctx,
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "adminuser@company.com".into(),
                groups: vec!["administrator".into()],
            },
        )
        .await
        .unwrap()
        .unwrap();

    let group_memberships = nexus
        .datastore()
        .silo_group_membership_for_user(
            &authn_opctx,
            &authz_silo,
            admin_group_user.id(),
        )
        .await
        .unwrap();

    assert_eq!(group_memberships.len(), 1);

    // Create an organization
    let _org = NexusRequest::objects_post(
        client,
        "/organizations",
        &params::OrganizationCreate {
            identity: IdentityMetadataCreateParams {
                name: "myorg".parse().unwrap(),
                description: "some org".into(),
            },
        },
    )
    .authn_as(AuthnMode::SiloUser(admin_group_user.id()))
    .execute()
    .await
    .expect("failed to create Organization")
    .parsed_body::<views::Organization>()
    .expect("failed to parse as Organization");
}

// Test listing providers
#[nexus_test]
async fn test_listing_identity_providers(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // List providers - should be none
    let providers = objects_list_page_authz::<IdentityProvider>(
        client,
        "/silos/default-silo/identity-providers",
    )
    .await
    .items;

    assert_eq!(providers.len(), 0);

    // Add some providers
    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR;

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .times(1..)
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    let silo_saml_idp_1: SamlIdentityProvider = object_create(
        client,
        &"/silos/default-silo/saml-identity-providers",
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

    let silo_saml_idp_2: SamlIdentityProvider = object_create(
        client,
        &"/silos/default-silo/saml-identity-providers",
        &params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "another-totally-real-saml-provider"
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

    // List providers again - expect 2
    let providers = objects_list_page_authz::<IdentityProvider>(
        client,
        "/silos/default-silo/identity-providers",
    )
    .await
    .items;

    assert_eq!(providers.len(), 2);

    let provider_name_set =
        providers.into_iter().map(|x| x.identity.name).collect::<HashSet<_>>();
    assert!(provider_name_set.contains(&silo_saml_idp_1.identity.name));
    assert!(provider_name_set.contains(&silo_saml_idp_2.identity.name));
}

// Test that deleting the silo deletes the idp
#[nexus_test]
async fn test_deleting_a_silo_deletes_the_idp(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "default-silo";

    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR;

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    let silo_saml_idp: SamlIdentityProvider = object_create(
        client,
        &format!("/silos/{}/saml-identity-providers", SILO_NAME),
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

    // Delete the silo
    NexusRequest::object_delete(&client, &format!("/silos/{}", SILO_NAME))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");

    // Expect that the silo is gone
    let nexus = &cptestctx.server.apictx.nexus;

    let response = IdentityProviderType::lookup(
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
    .await;

    assert!(response.is_err());
    match response.err().unwrap() {
        omicron_common::api::external::Error::ObjectNotFound {
            type_name,
            lookup_type: _,
        } => {
            assert_eq!(
                type_name,
                omicron_common::api::external::ResourceType::Silo
            );
        }

        _ => {
            assert!(false);
        }
    }

    // No SSO redirect expected
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!("/login/{}/{}", SILO_NAME, silo_saml_idp.identity.name),
        )
        .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .execute()
    .await
    .expect("expected success");
}

// Create a Silo with a SAML IdP document string
#[nexus_test]
async fn test_saml_idp_metadata_data_valid(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_silo(&client, "blahblah", true, shared::UserProvisionType::Fixed)
        .await;
    grant_iam(
        &client,
        "/silos/blahblah",
        SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    let silo_saml_idp: SamlIdentityProvider = object_create(
        client,
        "/silos/blahblah/saml-identity-providers",
        &params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Base64EncodedXml {
                data: base64::encode(SAML_IDP_DESCRIPTOR.to_string()),
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
            &format!("/login/blahblah/{}", silo_saml_idp.identity.name),
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

// Fail to create a Silo with a SAML IdP document string that isn't valid
#[nexus_test]
async fn test_saml_idp_metadata_data_truncated(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_silo(&client, "blahblah", true, shared::UserProvisionType::Fixed)
        .await;
    grant_iam(
        &client,
        "/silos/blahblah",
        SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            "/silos/blahblah/saml-identity-providers",
        )
        .body(Some(&params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Base64EncodedXml {
                data: base64::encode({
                    let mut saml_idp_descriptor =
                        SAML_IDP_DESCRIPTOR.to_string();
                    saml_idp_descriptor.truncate(100);
                    saml_idp_descriptor
                }),
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

// Can't create a SAML IdP from bad base64 data
#[nexus_test]
async fn test_saml_idp_metadata_data_invalid(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Fixed)
        .await;
    grant_iam(
        &client,
        &format!("/silos/{}", SILO_NAME),
        SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/silos/{}/saml-identity-providers", SILO_NAME),
        )
        .body(Some(&params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Base64EncodedXml {
                data: "bad data".to_string(),
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

struct TestSiloUserProvisionTypes {
    provision_type: shared::UserProvisionType,
    existing_silo_user: bool,
    expect_user: bool,
}

#[nexus_test]
async fn test_silo_user_provision_types(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    let test_cases: Vec<TestSiloUserProvisionTypes> = vec![
        // A silo configured with a "fixed" user provision type should fetch a
        // user if it exists already.
        TestSiloUserProvisionTypes {
            provision_type: shared::UserProvisionType::Fixed,
            existing_silo_user: true,
            expect_user: true,
        },
        // A silo configured with a "fixed" user provision type should not create a
        // user if one does not exist already.
        TestSiloUserProvisionTypes {
            provision_type: shared::UserProvisionType::Fixed,
            existing_silo_user: false,
            expect_user: false,
        },
        // A silo configured with a "JIT" user provision type should fetch a
        // user if it exists already.
        TestSiloUserProvisionTypes {
            provision_type: shared::UserProvisionType::Jit,
            existing_silo_user: true,
            expect_user: true,
        },
        // A silo configured with a "JIT" user provision type should create a user
        // if one does not exist already.
        TestSiloUserProvisionTypes {
            provision_type: shared::UserProvisionType::Jit,
            existing_silo_user: false,
            expect_user: true,
        },
    ];

    for test_case in test_cases {
        let silo =
            create_silo(&client, "test-silo", true, test_case.provision_type)
                .await;

        if test_case.existing_silo_user {
            nexus
                .silo_user_create(
                    silo.identity.id,
                    Uuid::new_v4(),
                    "external@id.com".into(),
                )
                .await
                .unwrap();
        }

        let authn_opctx = nexus.opctx_external_authn();

        let (authz_silo, db_silo) =
            LookupPath::new(&authn_opctx, &nexus.datastore())
                .silo_name(&silo.identity.name.into())
                .fetch()
                .await
                .unwrap();

        let existing_silo_user = nexus
            .silo_user_from_authenticated_subject(
                &authn_opctx,
                &authz_silo,
                &db_silo,
                &AuthenticatedSubject {
                    external_id: "external@id.com".into(),
                    groups: vec![],
                },
            )
            .await
            .unwrap();

        if test_case.expect_user {
            assert!(existing_silo_user.is_some());
        } else {
            assert!(existing_silo_user.is_none());
        }

        NexusRequest::object_delete(&client, &"/silos/test-silo")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to make request");
    }
}

#[nexus_test]
async fn test_silo_user_fetch_by_external_id(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    let silo = create_silo(
        &client,
        "test-silo",
        true,
        shared::UserProvisionType::Fixed,
    )
    .await;
    grant_iam(
        &client,
        "/silos/test-silo",
        SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    let (authz_silo, _) = LookupPath::new(&opctx, &nexus.datastore())
        .silo_name(&Name::try_from("test-silo".to_string()).unwrap().into())
        .fetch_for(authz::Action::Read)
        .await
        .unwrap();

    // Create a user
    nexus
        .silo_user_create(
            silo.identity.id,
            uuid::Uuid::new_v4(),
            "5513e049dac9468de5bdff36ab17d04f".into(),
        )
        .await
        .unwrap();

    // Fetching by external id that's not in the db should be Ok(None)
    let result = nexus
        .datastore()
        .silo_user_fetch_by_external_id(&opctx, &authz_silo, "123".into())
        .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Fetching by external id that is should be Ok(Some)
    let result = nexus
        .datastore()
        .silo_user_fetch_by_external_id(
            &opctx,
            &authz_silo,
            "5513e049dac9468de5bdff36ab17d04f".into(),
        )
        .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[nexus_test]
async fn test_silo_users_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    let initial_silo_users: Vec<views::User> =
        NexusRequest::iter_collection_authn(client, "/users", "", None)
            .await
            .expect("failed to list silo users (1)")
            .all_items;

    // In the built-in Silo, we expect the test-privileged and test-unprivileged
    // users.
    assert_eq!(
        initial_silo_users,
        vec![
            views::User {
                id: USER_TEST_PRIVILEGED.id(),
                display_name: USER_TEST_PRIVILEGED.external_id.clone()
            },
            views::User {
                id: USER_TEST_UNPRIVILEGED.id(),
                display_name: USER_TEST_UNPRIVILEGED.external_id.clone()
            },
        ]
    );

    // Now create another user and make sure we can see them.  While we're at
    // it, use a small limit to check that pagination is really working.
    let new_silo_user_id =
        "bd75d207-37f3-4769-b808-677ae04eaf23".parse().unwrap();
    let new_silo_user_external_id = "can_we_see_them?";
    nexus
        .silo_user_create(
            *SILO_ID,
            new_silo_user_id,
            new_silo_user_external_id.into(),
        )
        .await
        .unwrap();

    let silo_users: Vec<views::User> =
        NexusRequest::iter_collection_authn(client, "/users", "", Some(1))
            .await
            .expect("failed to list silo users (2)")
            .all_items;
    assert_eq!(
        silo_users,
        vec![
            views::User {
                id: USER_TEST_PRIVILEGED.id(),
                display_name: USER_TEST_PRIVILEGED.external_id.clone()
            },
            views::User {
                id: USER_TEST_UNPRIVILEGED.id(),
                display_name: USER_TEST_UNPRIVILEGED.external_id.clone()
            },
            views::User {
                id: new_silo_user_id,
                display_name: new_silo_user_external_id.into(),
            },
        ]
    );

    // Create another Silo with a Silo administrator.  That user should not be
    // able to see the users in the first Silo.

    let silo =
        create_silo(client, "silo2", true, shared::UserProvisionType::Fixed)
            .await;
    let new_silo_user_id =
        "6922f0b2-9a92-659b-da6b-93ad4955a3a3".parse().unwrap();
    let new_silo_user_name = String::from("some_silo_user");
    nexus
        .silo_user_create(
            silo.identity.id,
            new_silo_user_id,
            new_silo_user_name.clone(),
        )
        .await
        .unwrap();
    grant_iam(
        client,
        "/silos/silo2",
        SiloRole::Admin,
        new_silo_user_id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let silo2_users: dropshot::ResultsPage<views::User> =
        NexusRequest::object_get(client, "/users")
            .authn_as(AuthnMode::SiloUser(new_silo_user_id))
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(
        silo2_users.items,
        vec![views::User {
            id: new_silo_user_id,
            display_name: new_silo_user_name,
        }]
    );

    // The "test-privileged" user also shouldn't see the user in this other
    // Silo.
    let new_silo_users: Vec<views::User> =
        NexusRequest::iter_collection_authn(client, "/users", "", Some(1))
            .await
            .expect("failed to list silo users (2)")
            .all_items;
    assert_eq!(silo_users, new_silo_users,);

    // TODO-coverage When we have a way to remove or invalidate Silo Users, we
    // should test that doing so causes them to stop appearing in the list.
}

#[nexus_test]
async fn test_silo_groups_jit(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    let silo =
        create_silo(&client, "test-silo", true, shared::UserProvisionType::Jit)
            .await;

    // Create a user in advance
    let silo_user_id = Uuid::new_v4();
    nexus
        .silo_user_create(
            silo.identity.id,
            silo_user_id,
            "external@id.com".into(),
        )
        .await
        .unwrap();

    let authn_opctx = nexus.opctx_external_authn();

    let (authz_silo, db_silo) =
        LookupPath::new(&authn_opctx, &nexus.datastore())
            .silo_name(&silo.identity.name.into())
            .fetch()
            .await
            .unwrap();

    // Should create two groups from the authenticated subject
    let existing_silo_user = nexus
        .silo_user_from_authenticated_subject(
            &authn_opctx,
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "external@id.com".into(),
                groups: vec!["a-group".into(), "b-group".into()],
            },
        )
        .await
        .unwrap()
        .unwrap();

    let group_memberships = nexus
        .datastore()
        .silo_group_membership_for_user(
            &authn_opctx,
            &authz_silo,
            existing_silo_user.id(),
        )
        .await
        .unwrap();

    assert_eq!(group_memberships.len(), 2);

    let mut group_names = vec![];

    for group_membership in &group_memberships {
        let (.., db_group) = LookupPath::new(&authn_opctx, nexus.datastore())
            .silo_group_id(group_membership.silo_group_id)
            .fetch()
            .await
            .unwrap();

        group_names.push(db_group.external_id);
    }

    assert!(group_names.contains(&"a-group".to_string()));
    assert!(group_names.contains(&"b-group".to_string()));
}

#[nexus_test]
async fn test_silo_groups_fixed(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    let silo = create_silo(
        &client,
        "test-silo",
        true,
        shared::UserProvisionType::Fixed,
    )
    .await;

    // Create a user in advance
    let silo_user_id = Uuid::new_v4();
    nexus
        .silo_user_create(
            silo.identity.id,
            silo_user_id,
            "external@id.com".into(),
        )
        .await
        .unwrap();

    let authn_opctx = nexus.opctx_external_authn();

    let (authz_silo, db_silo) =
        LookupPath::new(&authn_opctx, &nexus.datastore())
            .silo_name(&silo.identity.name.into())
            .fetch()
            .await
            .unwrap();

    // Should not create groups from the authenticated subject
    let existing_silo_user = nexus
        .silo_user_from_authenticated_subject(
            &authn_opctx,
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "external@id.com".into(),
                groups: vec!["a-group".into(), "b-group".into()],
            },
        )
        .await
        .unwrap()
        .unwrap();

    let group_memberships = nexus
        .datastore()
        .silo_group_membership_for_user(
            &authn_opctx,
            &authz_silo,
            existing_silo_user.id(),
        )
        .await
        .unwrap();

    assert_eq!(group_memberships.len(), 0);
}

#[nexus_test]
async fn test_silo_groups_remove_from_one_group(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    let silo =
        create_silo(&client, "test-silo", true, shared::UserProvisionType::Jit)
            .await;

    // Create a user in advance
    let silo_user_id = Uuid::new_v4();
    nexus
        .silo_user_create(
            silo.identity.id,
            silo_user_id,
            "external@id.com".into(),
        )
        .await
        .unwrap();

    let authn_opctx = nexus.opctx_external_authn();

    let (authz_silo, db_silo) =
        LookupPath::new(&authn_opctx, &nexus.datastore())
            .silo_name(&silo.identity.name.into())
            .fetch()
            .await
            .unwrap();

    // Add to two groups
    let existing_silo_user = nexus
        .silo_user_from_authenticated_subject(
            &authn_opctx,
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "external@id.com".into(),
                groups: vec!["a-group".into(), "b-group".into()],
            },
        )
        .await
        .unwrap()
        .unwrap();

    // Check those groups were created and the user was added
    let group_memberships = nexus
        .datastore()
        .silo_group_membership_for_user(
            &authn_opctx,
            &authz_silo,
            existing_silo_user.id(),
        )
        .await
        .unwrap();

    assert_eq!(group_memberships.len(), 2);

    let mut group_names = vec![];

    for group_membership in &group_memberships {
        let (.., db_group) = LookupPath::new(&authn_opctx, nexus.datastore())
            .silo_group_id(group_membership.silo_group_id)
            .fetch()
            .await
            .unwrap();

        group_names.push(db_group.external_id);
    }

    assert!(group_names.contains(&"a-group".to_string()));
    assert!(group_names.contains(&"b-group".to_string()));

    // Then remove their membership from one group
    let existing_silo_user = nexus
        .silo_user_from_authenticated_subject(
            &authn_opctx,
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "external@id.com".into(),
                groups: vec!["b-group".into()],
            },
        )
        .await
        .unwrap()
        .unwrap();

    let group_memberships = nexus
        .datastore()
        .silo_group_membership_for_user(
            &authn_opctx,
            &authz_silo,
            existing_silo_user.id(),
        )
        .await
        .unwrap();

    assert_eq!(group_memberships.len(), 1);

    let mut group_names = vec![];

    for group_membership in &group_memberships {
        let (.., db_group) = LookupPath::new(&authn_opctx, nexus.datastore())
            .silo_group_id(group_membership.silo_group_id)
            .fetch()
            .await
            .unwrap();

        group_names.push(db_group.external_id);
    }

    assert!(group_names.contains(&"b-group".to_string()));
}

#[nexus_test]
async fn test_silo_groups_remove_from_both_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    let silo =
        create_silo(&client, "test-silo", true, shared::UserProvisionType::Jit)
            .await;

    // Create a user in advance
    let silo_user_id = Uuid::new_v4();
    nexus
        .silo_user_create(
            silo.identity.id,
            silo_user_id,
            "external@id.com".into(),
        )
        .await
        .unwrap();

    let authn_opctx = nexus.opctx_external_authn();

    let (authz_silo, db_silo) =
        LookupPath::new(&authn_opctx, &nexus.datastore())
            .silo_name(&silo.identity.name.into())
            .fetch()
            .await
            .unwrap();

    // Add to two groups
    let existing_silo_user = nexus
        .silo_user_from_authenticated_subject(
            &authn_opctx,
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "external@id.com".into(),
                groups: vec!["a-group".into(), "b-group".into()],
            },
        )
        .await
        .unwrap()
        .unwrap();

    // Check those groups were created and the user was added
    let group_memberships = nexus
        .datastore()
        .silo_group_membership_for_user(
            &authn_opctx,
            &authz_silo,
            existing_silo_user.id(),
        )
        .await
        .unwrap();

    assert_eq!(group_memberships.len(), 2);

    let mut group_names = vec![];

    for group_membership in &group_memberships {
        let (.., db_group) = LookupPath::new(&authn_opctx, nexus.datastore())
            .silo_group_id(group_membership.silo_group_id)
            .fetch()
            .await
            .unwrap();

        group_names.push(db_group.external_id);
    }

    assert!(group_names.contains(&"a-group".to_string()));
    assert!(group_names.contains(&"b-group".to_string()));

    // Then remove from both groups, and add to a new one
    let existing_silo_user = nexus
        .silo_user_from_authenticated_subject(
            &authn_opctx,
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "external@id.com".into(),
                groups: vec!["c-group".into()],
            },
        )
        .await
        .unwrap()
        .unwrap();

    let group_memberships = nexus
        .datastore()
        .silo_group_membership_for_user(
            &authn_opctx,
            &authz_silo,
            existing_silo_user.id(),
        )
        .await
        .unwrap();

    assert_eq!(group_memberships.len(), 1);

    let mut group_names = vec![];

    for group_membership in &group_memberships {
        let (.., db_group) = LookupPath::new(&authn_opctx, nexus.datastore())
            .silo_group_id(group_membership.silo_group_id)
            .fetch()
            .await
            .unwrap();

        group_names.push(db_group.external_id);
    }

    assert!(group_names.contains(&"c-group".to_string()));
}

// Test that silo delete cleans up associated groups
#[nexus_test]
async fn test_silo_delete_clean_up_groups(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create a silo
    let silo =
        create_silo(&client, "test-silo", true, shared::UserProvisionType::Jit)
            .await;
    grant_iam(
        &client,
        "/silos/test-silo",
        SiloRole::Admin,
        USER_TEST_PRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    let (authz_silo, db_silo) = LookupPath::new(&opctx, &nexus.datastore())
        .silo_name(&silo.identity.name.into())
        .fetch()
        .await
        .unwrap();

    // Add a user with a group membership
    let silo_user = nexus
        .silo_user_from_authenticated_subject(
            &nexus.opctx_external_authn(),
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "user@company.com".into(),
                groups: vec!["sre".into()],
            },
        )
        .await
        .expect("silo_user_from_authenticated_subject")
        .unwrap();

    // Delete the silo
    NexusRequest::object_delete(&client, &"/silos/test-silo")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");

    // Expect the group is gone
    assert!(nexus
        .datastore()
        .silo_group_optional_lookup(&opctx, &authz_silo, "a-group".into(),)
        .await
        .expect("silo_group_optional_lookup")
        .is_none());

    // Expect the group membership is gone
    let memberships = nexus
        .datastore()
        .silo_group_membership_for_user(&opctx, &authz_silo, silo_user.id())
        .await
        .expect("silo_group_membership_for_user");

    assert!(memberships.is_empty());

    // Expect the user is gone
    LookupPath::new(&opctx, &nexus.datastore())
        .silo_user_id(silo_user.id())
        .fetch()
        .await
        .expect_err("user found");
}

// Test ensuring the same group from different users
#[nexus_test]
async fn test_ensure_same_silo_group(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create a silo
    let silo =
        create_silo(&client, "test-silo", true, shared::UserProvisionType::Jit)
            .await;

    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    let (authz_silo, db_silo) = LookupPath::new(&opctx, &nexus.datastore())
        .silo_name(&silo.identity.name.into())
        .fetch()
        .await
        .unwrap();

    // Add the first user with a group membership
    let _silo_user_1 = nexus
        .silo_user_from_authenticated_subject(
            &nexus.opctx_external_authn(),
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "user1@company.com".into(),
                groups: vec!["sre".into()],
            },
        )
        .await
        .expect("silo_user_from_authenticated_subject 1")
        .unwrap();

    // Add the first user with a group membership
    let _silo_user_2 = nexus
        .silo_user_from_authenticated_subject(
            &nexus.opctx_external_authn(),
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "user2@company.com".into(),
                groups: vec!["sre".into()],
            },
        )
        .await
        .expect("silo_user_from_authenticated_subject 2")
        .unwrap();
}

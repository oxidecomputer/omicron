// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::integration_tests::saml::SAML_IDP_DESCRIPTOR;
use dropshot::ResultsPage;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::authn::silos::AuthenticatedSubject;
use nexus_db_queries::authn::{USER_TEST_PRIVILEGED, USER_TEST_UNPRIVILEGED};
use nexus_db_queries::authz::{self};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_db_queries::db::identity::Asset;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_ip_pool, create_local_user, create_project, create_silo, grant_iam,
    link_ip_pool, object_create, object_delete, objects_list_page_authz,
    projects_list, test_params,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views::Certificate;
use nexus_types::external_api::views::{
    self, IdentityProvider, Project, SamlIdentityProvider, Silo,
};
use nexus_types::external_api::{params, shared};
use nexus_types::silo::DEFAULT_SILO_ID;
use omicron_common::address::{IpRange, Ipv4Range};
use omicron_common::api::external::{
    IdentityMetadataCreateParams, LookupType, Name,
};
use omicron_common::api::external::{ObjectIdentity, UserId};
use omicron_test_utils::certificates::CertificateChain;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::SiloUserUuid;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Write;
use std::str::FromStr;

use base64::Engine;
use hickory_resolver::ResolveErrorKind;
use hickory_resolver::proto::ProtoErrorKind;
use http::StatusCode;
use http::method::Method;
use httptest::{Expectation, Server, matchers::*, responders::*};
use nexus_types::external_api::shared::{FleetRole, SiloRole};
use std::convert::Infallible;
use std::net::Ipv4Addr;
use std::time::Duration;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_silos(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    // Verify that we cannot create a name with the same name as the recovery
    // Silo that was created during rack initialization.
    let error: dropshot::HttpErrorResponseBody =
        NexusRequest::expect_failure_with_body(
            client,
            StatusCode::BAD_REQUEST,
            Method::POST,
            "/v1/system/silos",
            &params::SiloCreate {
                identity: IdentityMetadataCreateParams {
                    name: cptestctx.silo_name.clone(),
                    description: "a silo".to_string(),
                },
                quotas: params::SiloQuotasCreate::empty(),
                discoverable: false,
                identity_mode: shared::SiloIdentityMode::LocalOnly,
                admin_group_name: None,
                tls_certificates: vec![],
                mapped_fleet_roles: Default::default(),
                restrict_network_actions: None,
            },
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();
    assert_eq!(error.message, "already exists: silo \"test-suite-silo\"");

    // Create two silos: one discoverable, one not
    create_silo(
        &client,
        "discoverable",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;
    create_silo(&client, "hidden", false, shared::SiloIdentityMode::LocalOnly)
        .await;

    // Verify that an external DNS name was propagated for these Silos.
    verify_silo_dns_name(cptestctx, "discoverable", true).await;
    verify_silo_dns_name(cptestctx, "hidden", true).await;

    // Verify GET /v1/system/silos/{silo} works for both discoverable and not
    let discoverable_url = "/v1/system/silos/discoverable";
    let hidden_url = "/v1/system/silos/hidden";

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
        &"/v1/system/silos/testpost",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Verify GET /v1/system/silos only returns discoverable silos
    let silos =
        objects_list_page_authz::<Silo>(client, "/v1/system/silos").await.items;
    assert_eq!(silos.len(), 1);
    assert_eq!(silos[0].identity.name, "discoverable");

    // Create a new user in the discoverable silo
    let new_silo_user_id = create_local_user(
        client,
        &silos[0],
        &"some-silo-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;

    // Grant the user "admin" privileges on that Silo.
    grant_iam(
        client,
        discoverable_url,
        SiloRole::Admin,
        new_silo_user_id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // TODO-coverage  TODO-security Add test for Silo-local session
    // when we can use users in another Silo.

    let authn_opctx = nexus.opctx_external_authn();

    // Create project with built-in user auth
    // Note: this currently goes to the built-in silo!
    let project_name = "someproj";
    let new_proj_in_default_silo = create_project(&client, project_name).await;

    // default silo project shows up in default silo
    let projects_in_default_silo =
        projects_list(client, "/v1/projects", "", None).await;
    assert_eq!(projects_in_default_silo.len(), 1);

    // default silo project does not show up in our silo
    let projects_in_our_silo = NexusRequest::object_get(client, "/v1/projects")
        .authn_as(AuthnMode::SiloUser(new_silo_user_id))
        .execute_and_parse_unwrap::<dropshot::ResultsPage<Project>>()
        .await;
    assert_eq!(projects_in_our_silo.items.len(), 0);

    // Create a Project of the same name in a different Silo to verify
    // that's possible.
    let new_proj_in_our_silo = NexusRequest::objects_post(
        client,
        "/v1/projects",
        &params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: project_name.parse().unwrap(),
                description: String::new(),
            },
        },
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to create same-named Project in a different Silo")
    .parsed_body::<views::Project>()
    .expect("failed to parse new Project");
    assert_eq!(
        new_proj_in_default_silo.identity.name,
        new_proj_in_our_silo.identity.name
    );
    assert_ne!(
        new_proj_in_default_silo.identity.id,
        new_proj_in_our_silo.identity.id
    );
    // delete default subnet from VPC so we can delete the VPC
    NexusRequest::object_delete(
        client,
        &format!(
            "/v1/vpc-subnets/default?project={}&vpc=default",
            project_name
        ),
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to delete test Vpc");
    // delete VPC from project so we can delete the project later
    NexusRequest::object_delete(
        client,
        &format!("/v1/vpcs/default?project={}", project_name),
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to delete test Vpc");

    // Verify GET /v1/projects works with built-in user auth
    let projects = projects_list(client, "/v1/projects", "", None).await;
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].identity.name, "someproj");

    // Deleting discoverable silo fails because there's still a project in it
    NexusRequest::expect_failure(
        &client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &discoverable_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Delete project
    NexusRequest::object_delete(&client, &"/v1/projects/someproj")
        .authn_as(AuthnMode::SiloUser(new_silo_user_id))
        .execute()
        .await
        .expect("failed to make request");

    // Verify silo DELETE now works
    NexusRequest::object_delete(&client, &discoverable_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");

    // Verify the DNS name was removed.
    verify_silo_dns_name(cptestctx, "discoverable", false).await;

    // Verify silo user was also deleted
    LookupPath::new(&authn_opctx, nexus.datastore())
        .silo_user_id(new_silo_user_id)
        .fetch()
        .await
        .expect_err("unexpected success");
}

// Test that admin group is created if admin_group_name is applied.
#[nexus_test]
async fn test_silo_admin_group(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    let silo: Silo = object_create(
        client,
        "/v1/system/silos",
        &params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: "silo-name".parse().unwrap(),
                description: "a silo".to_string(),
            },
            quotas: params::SiloQuotasCreate::empty(),
            discoverable: false,
            identity_mode: shared::SiloIdentityMode::SamlJit,
            admin_group_name: Some("administrator".into()),
            tls_certificates: vec![],
            mapped_fleet_roles: Default::default(),
            restrict_network_actions: None,
        },
    )
    .await;

    let authn_opctx = nexus.opctx_external_authn();

    let (authz_silo, db_silo) =
        LookupPath::new(&authn_opctx, nexus.datastore())
            .silo_name(&silo.identity.name.into())
            .fetch()
            .await
            .unwrap();

    assert!(
        nexus
            .datastore()
            .silo_group_optional_lookup(
                &authn_opctx,
                &authz_silo,
                "administrator".into(),
            )
            .await
            .unwrap()
            .is_some()
    );

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

    // Create a project
    let _org = NexusRequest::objects_post(
        client,
        "/v1/projects",
        &params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: "myproj".parse().unwrap(),
                description: "some proj".into(),
            },
        },
    )
    .authn_as(AuthnMode::SiloUser(admin_group_user.id()))
    .execute()
    .await
    .expect("failed to create Project")
    .parsed_body::<views::Project>()
    .expect("failed to parse as Project");
}

// Test listing providers
#[nexus_test]
async fn test_listing_identity_providers(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    create_silo(&client, "test-silo", true, shared::SiloIdentityMode::SamlJit)
        .await;

    // List providers - should be none
    let providers = objects_list_page_authz::<IdentityProvider>(
        client,
        "/v1/system/identity-providers?silo=test-silo",
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
        &"/v1/system/identity-providers/saml?silo=test-silo",
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
        &"/v1/system/identity-providers/saml?silo=test-silo",
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
        "/v1/system/identity-providers?silo=test-silo",
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

    const SILO_NAME: &str = "test-silo";
    create_silo(&client, SILO_NAME, true, shared::SiloIdentityMode::SamlJit)
        .await;

    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR;

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    let silo_saml_idp: SamlIdentityProvider = object_create(
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

    // Delete the silo
    NexusRequest::object_delete(
        &client,
        &format!("/v1/system/silos/{}", SILO_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Expect that the silo is gone
    let nexus = &cptestctx.server.server_context().nexus;

    let response = nexus
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
            &format!(
                "/login/{}/saml/{}/redirect",
                SILO_NAME, silo_saml_idp.identity.name
            ),
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

    create_silo(&client, "blahblah", true, shared::SiloIdentityMode::SamlJit)
        .await;

    let silo_saml_idp: SamlIdentityProvider = object_create(
        client,
        "/v1/system/identity-providers/saml?silo=blahblah",
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

    // Expect the SSO redirect when trying to log in
    let result = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!(
                "/login/blahblah/saml/{}/redirect",
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

// Fail to create a Silo with a SAML IdP document string that isn't valid
#[nexus_test]
async fn test_saml_idp_metadata_data_truncated(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_silo(&client, "blahblah", true, shared::SiloIdentityMode::SamlJit)
        .await;

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            "/v1/system/identity-providers/saml?silo=blahblah",
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
                data: base64::engine::general_purpose::STANDARD.encode({
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
    identity_mode: shared::SiloIdentityMode,
    existing_silo_user: bool,
    expect_user: bool,
}

#[nexus_test]
async fn test_silo_user_provision_types(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let test_cases: Vec<TestSiloUserProvisionTypes> = vec![
        // A silo configured with a "ApiOnly" user provision type should fetch a
        // user if it exists already.
        TestSiloUserProvisionTypes {
            identity_mode: shared::SiloIdentityMode::LocalOnly,
            existing_silo_user: true,
            expect_user: true,
        },
        // A silo configured with a "ApiOnly" user provision type should not
        // create a user if one does not exist already.
        TestSiloUserProvisionTypes {
            identity_mode: shared::SiloIdentityMode::LocalOnly,
            existing_silo_user: false,
            expect_user: false,
        },
        // A silo configured with a "JIT" user provision type should fetch a
        // user if it exists already.
        TestSiloUserProvisionTypes {
            identity_mode: shared::SiloIdentityMode::SamlJit,
            existing_silo_user: true,
            expect_user: true,
        },
        // A silo configured with a "JIT" user provision type should create a
        // user if one does not exist already.
        TestSiloUserProvisionTypes {
            identity_mode: shared::SiloIdentityMode::SamlJit,
            existing_silo_user: false,
            expect_user: true,
        },
    ];

    for test_case in test_cases {
        let silo =
            create_silo(&client, "test-silo", true, test_case.identity_mode)
                .await;

        if test_case.existing_silo_user {
            match test_case.identity_mode {
                shared::SiloIdentityMode::SamlJit => {
                    create_jit_user(datastore, &silo, "external-id-com").await;
                }
                shared::SiloIdentityMode::LocalOnly => {
                    create_local_user(
                        client,
                        &silo,
                        &"external-id-com".parse().unwrap(),
                        test_params::UserPassword::LoginDisallowed,
                    )
                    .await;
                }
            };
        }

        let authn_opctx = nexus.opctx_external_authn();

        let (authz_silo, db_silo) =
            LookupPath::new(&authn_opctx, nexus.datastore())
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
                    external_id: "external-id-com".into(),
                    groups: vec![],
                },
            )
            .await;

        if test_case.expect_user {
            assert!(existing_silo_user.is_ok());
        } else {
            assert!(existing_silo_user.is_err());
        }

        NexusRequest::object_delete(&client, &"/v1/system/silos/test-silo")
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
    let nexus = &cptestctx.server.server_context().nexus;

    let silo = create_silo(
        &client,
        "test-silo",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    let opctx_external_authn = nexus.opctx_external_authn();
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    let (authz_silo, _) = LookupPath::new(&opctx, nexus.datastore())
        .silo_name(&Name::try_from("test-silo".to_string()).unwrap().into())
        .fetch_for(authz::Action::Read)
        .await
        .unwrap();

    // Create a user
    create_local_user(
        client,
        &silo,
        &"f5513e049dac9468de5bdff36ab17d04f".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    // Fetching by external id that's not in the db should be Ok(None)
    let result = nexus
        .datastore()
        .silo_user_fetch_by_external_id(
            &opctx_external_authn,
            &authz_silo,
            "123",
        )
        .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Fetching by external id that is should be Ok(Some)
    let result = nexus
        .datastore()
        .silo_user_fetch_by_external_id(
            &opctx_external_authn,
            &authz_silo,
            "f5513e049dac9468de5bdff36ab17d04f",
        )
        .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[nexus_test]
async fn test_silo_users_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let initial_silo_users: Vec<views::User> =
        NexusRequest::iter_collection_authn(client, "/v1/users", "", None)
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
                display_name: USER_TEST_PRIVILEGED.external_id.clone(),
                silo_id: DEFAULT_SILO_ID,
            },
            views::User {
                id: USER_TEST_UNPRIVILEGED.id(),
                display_name: USER_TEST_UNPRIVILEGED.external_id.clone(),
                silo_id: DEFAULT_SILO_ID,
            },
        ]
    );

    // Now create another user and make sure we can see them.  While we're at
    // it, use a small limit to check that pagination is really working.
    let new_silo_user_external_id = "can-we-see-them";
    let new_silo_user_id = create_local_user(
        client,
        &views::Silo::try_from(DEFAULT_SILO.clone()).unwrap(),
        &new_silo_user_external_id.parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;

    let mut silo_users: Vec<views::User> =
        NexusRequest::iter_collection_authn(client, "/v1/users", "", Some(1))
            .await
            .expect("failed to list silo users (2)")
            .all_items;
    silo_users.sort_by(|u1, u2| u1.display_name.cmp(&u2.display_name));
    assert_eq!(
        silo_users,
        vec![
            views::User {
                id: new_silo_user_id,
                display_name: new_silo_user_external_id.into(),
                silo_id: DEFAULT_SILO_ID,
            },
            views::User {
                id: USER_TEST_PRIVILEGED.id(),
                display_name: USER_TEST_PRIVILEGED.external_id.clone(),
                silo_id: DEFAULT_SILO_ID,
            },
            views::User {
                id: USER_TEST_UNPRIVILEGED.id(),
                display_name: USER_TEST_UNPRIVILEGED.external_id.clone(),
                silo_id: DEFAULT_SILO_ID,
            },
        ]
    );

    // Create another Silo with a Silo administrator.  That user should not be
    // able to see the users in the first Silo.

    let silo =
        create_silo(client, "silo2", true, shared::SiloIdentityMode::LocalOnly)
            .await;

    let new_silo_user_name = String::from("some-silo-user");
    let new_silo_user_id = create_local_user(
        client,
        &silo,
        &new_silo_user_name.parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;
    grant_iam(
        client,
        "/v1/system/silos/silo2",
        SiloRole::Admin,
        new_silo_user_id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let silo2_users: dropshot::ResultsPage<views::User> =
        NexusRequest::object_get(client, "/v1/users")
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
            silo_id: silo.identity.id,
        }]
    );

    // The "test-privileged" user also shouldn't see the user in this other
    // Silo.
    let mut new_silo_users: Vec<views::User> =
        NexusRequest::iter_collection_authn(client, "/v1/users", "", Some(1))
            .await
            .expect("failed to list silo users (2)")
            .all_items;
    new_silo_users.sort_by(|u1, u2| u1.display_name.cmp(&u2.display_name));
    assert_eq!(silo_users, new_silo_users,);

    // TODO-coverage When we have a way to remove or invalidate Silo Users, we
    // should test that doing so causes them to stop appearing in the list.
}

#[nexus_test]
async fn test_silo_groups_jit(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let silo = create_silo(
        &client,
        "test-silo",
        true,
        shared::SiloIdentityMode::SamlJit,
    )
    .await;

    // Create a user in advance
    create_jit_user(datastore, &silo, "external@id.com").await;

    let authn_opctx = nexus.opctx_external_authn();

    let (authz_silo, db_silo) =
        LookupPath::new(&authn_opctx, nexus.datastore())
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
            .silo_group_id(group_membership.silo_group_id.into())
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
    let nexus = &cptestctx.server.server_context().nexus;

    let silo = create_silo(
        &client,
        "test-silo",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    // Create a user in advance
    create_local_user(
        client,
        &silo,
        &"external-id-com".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    let authn_opctx = nexus.opctx_external_authn();

    let (authz_silo, db_silo) =
        LookupPath::new(&authn_opctx, nexus.datastore())
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
                external_id: "external-id-com".into(),
                groups: vec!["a-group".into(), "b-group".into()],
            },
        )
        .await
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
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let silo = create_silo(
        &client,
        "test-silo",
        true,
        shared::SiloIdentityMode::SamlJit,
    )
    .await;

    // Create a user in advance
    create_jit_user(datastore, &silo, "external@id.com").await;

    let authn_opctx = nexus.opctx_external_authn();

    let (authz_silo, db_silo) =
        LookupPath::new(&authn_opctx, nexus.datastore())
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
            .silo_group_id(group_membership.silo_group_id.into())
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
            .silo_group_id(group_membership.silo_group_id.into())
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
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let silo = create_silo(
        &client,
        "test-silo",
        true,
        shared::SiloIdentityMode::SamlJit,
    )
    .await;

    // Create a user in advance
    create_jit_user(datastore, &silo, "external@id.com").await;

    let authn_opctx = nexus.opctx_external_authn();

    let (authz_silo, db_silo) =
        LookupPath::new(&authn_opctx, nexus.datastore())
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
            .silo_group_id(group_membership.silo_group_id.into())
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
            .silo_group_id(group_membership.silo_group_id.into())
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
    let nexus = &cptestctx.server.server_context().nexus;

    // Create a silo
    let silo = create_silo(
        &client,
        "test-silo",
        true,
        shared::SiloIdentityMode::SamlJit,
    )
    .await;

    let opctx_external_authn = nexus.opctx_external_authn();
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    let (authz_silo, db_silo) = LookupPath::new(&opctx, nexus.datastore())
        .silo_name(&silo.identity.name.into())
        .fetch()
        .await
        .unwrap();

    // Add a user with a group membership
    let silo_user = nexus
        .silo_user_from_authenticated_subject(
            &opctx_external_authn,
            &authz_silo,
            &db_silo,
            &AuthenticatedSubject {
                external_id: "user@company.com".into(),
                groups: vec!["sre".into()],
            },
        )
        .await
        .expect("silo_user_from_authenticated_subject");

    // Delete the silo
    NexusRequest::object_delete(&client, &"/v1/system/silos/test-silo")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");

    // Expect the group is gone
    assert!(
        nexus
            .datastore()
            .silo_group_optional_lookup(
                &opctx_external_authn,
                &authz_silo,
                "a-group".into(),
            )
            .await
            .expect("silo_group_optional_lookup")
            .is_none()
    );

    // Expect the group membership is gone
    let memberships = nexus
        .datastore()
        .silo_group_membership_for_user(
            &opctx_external_authn,
            &authz_silo,
            silo_user.id(),
        )
        .await
        .expect("silo_group_membership_for_user");

    assert!(memberships.is_empty());

    // Expect the user is gone
    LookupPath::new(&opctx_external_authn, nexus.datastore())
        .silo_user_id(silo_user.id())
        .fetch()
        .await
        .expect_err("user found");
}

// Test ensuring the same group from different users
#[nexus_test]
async fn test_ensure_same_silo_group(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    // Create a silo
    let silo = create_silo(
        &client,
        "test-silo",
        true,
        shared::SiloIdentityMode::SamlJit,
    )
    .await;

    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        nexus.datastore().clone(),
    );

    let (authz_silo, db_silo) = LookupPath::new(&opctx, nexus.datastore())
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
        .expect("silo_user_from_authenticated_subject 1");

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
        .expect("silo_user_from_authenticated_subject 2");

    // TODO-coverage were we intending to verify something here?
}

/// Tests the behavior of the per-Silo "list users" and "fetch user" endpoints.
///
/// We'll run the tests separately for both kinds of Silo.  The implementation
/// should be the same, but that's why we're verifying it.
#[nexus_test]
async fn test_silo_user_views(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let datastore = cptestctx.server.server_context().nexus.datastore();

    // Create the two Silos.
    let silo1 =
        create_silo(client, "silo1", false, shared::SiloIdentityMode::SamlJit)
            .await;
    let silo2 = create_silo(
        client,
        "silo2",
        false,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    // Create two users in each Silo.  We need two so that we can verify that an
    // ordinary user can see a user other than themselves in each Silo.
    let silo1_user1 = create_jit_user(datastore, &silo1, "silo1-user1").await;
    let silo1_user1_id = silo1_user1.id;
    let silo1_user2 = create_jit_user(datastore, &silo1, "silo1-user2").await;
    let silo1_user2_id = silo1_user2.id;
    let mut silo1_expected_users = [silo1_user1.clone(), silo1_user2.clone()];
    silo1_expected_users.sort_by_key(|u| u.id);

    let silo2_user1 = create_local_user(
        client,
        &silo2,
        &"silo2-user1".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;
    let silo2_user1_id = silo2_user1.id;
    let silo2_user2 = create_local_user(
        client,
        &silo2,
        &"silo2-user2".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;
    let silo2_user2_id = silo2_user2.id;
    let mut silo2_expected_users = [silo2_user1.clone(), silo2_user2.clone()];
    silo2_expected_users.sort_by_key(|u| u.id);

    let users_by_id = {
        let mut users_by_id: BTreeMap<SiloUserUuid, &views::User> =
            BTreeMap::new();
        assert_eq!(users_by_id.insert(silo1_user1_id, &silo1_user1), None);
        assert_eq!(users_by_id.insert(silo1_user2_id, &silo1_user2), None);
        assert_eq!(users_by_id.insert(silo2_user1_id, &silo2_user1), None);
        assert_eq!(users_by_id.insert(silo2_user2_id, &silo2_user2), None);
        users_by_id
    };

    let users_by_name = users_by_id
        .values()
        .map(|user| (user.display_name.to_owned(), *user))
        .collect::<BTreeMap<_, _>>();

    // We'll run through a battery of tests:
    // - for each of our test silos
    //   - for all *five* users ("test-privileged", plus the two users that we
    //     created in each Silo)
    //     - test the "list" endpoint
    //     - for all five user ids
    //       - test the "view user" endpoint for that user id
    //
    // This exercises a lot of different behaviors:
    // - on success, the "list" and "view" endpoints always return the right
    //   contents
    // - on failure, the "list" and "view" endpoints always return the right
    //   status code and message for the failure mode
    // - that users can always list and fetch all users in their own Silo via
    //   /v1/system/silos (/users is tested elsewhere)
    // - that users without privileges cannot list or fetch users in other Silos
    // - that users with privileges on another Silo can list and fetch users in
    //   that Silo
    // - that a user with id "foo" in Silo1 cannot be accessed by that id in
    //   Silo 2.  This case is easy to miss but would be very bad to get wrong!
    let all_callers = {
        std::iter::once(AuthnMode::PrivilegedUser)
            .chain(users_by_name.values().map(|v| AuthnMode::SiloUser(v.id)))
            .collect::<Vec<_>>()
    };

    struct TestSilo<'a> {
        silo: &'a views::Silo,
        expected_users: [views::User; 2],
    }

    let test_silo1 =
        TestSilo { silo: &silo1, expected_users: silo1_expected_users };
    let test_silo2 =
        TestSilo { silo: &silo2, expected_users: silo2_expected_users };

    // Strip the identifier out of error messages because the uuid changes each
    // time.
    let id_re = regex::Regex::new("\".*?\"").unwrap();

    let mut output = String::new();
    for test_silo in [test_silo1, test_silo2] {
        let silo_name = &test_silo.silo.identity().name;

        write!(&mut output, "SILO: {}\n", silo_name).unwrap();

        for calling_user in all_callers.iter() {
            let caller_label = match calling_user {
                AuthnMode::PrivilegedUser => "privileged",
                AuthnMode::SiloUser(silo_user_id) => {
                    let user = users_by_id.get(silo_user_id).unwrap();
                    &user.display_name
                }
                _ => unimplemented!(),
            };
            write!(&mut output, "    test user {}:\n", caller_label).unwrap();

            // Test the "list" endpoint.
            write!(&mut output, "        list = ").unwrap();
            let test_response = NexusRequest::new(RequestBuilder::new(
                client,
                Method::GET,
                &format!("/v1/system/users?silo={}", silo_name),
            ))
            .authn_as(calling_user.clone())
            .execute()
            .await
            .unwrap();
            write!(&mut output, "{}", test_response.status.as_str()).unwrap();

            // If this succeeded, it must have returned the expected users for
            // this Silo.
            if test_response.status == http::StatusCode::OK {
                let found_users = test_response
                    .parsed_body::<dropshot::ResultsPage<views::User>>()
                    .unwrap()
                    .items;
                assert_eq!(found_users, test_silo.expected_users);
            } else {
                let error = test_response
                    .parsed_body::<dropshot::HttpErrorResponseBody>()
                    .unwrap();
                write!(&mut output, " (message = {:?})", error.message)
                    .unwrap();
            }

            write!(&mut output, "\n").unwrap();

            // Test the "view" endpoint for each user in this Silo.
            for (_, user) in &users_by_name {
                let user_id = user.id;
                write!(&mut output, "        view {:?} = ", user.display_name)
                    .unwrap();
                let test_response = NexusRequest::new(RequestBuilder::new(
                    client,
                    Method::GET,
                    &format!("/v1/system/users/{}?silo={}", user_id, silo_name),
                ))
                .authn_as(calling_user.clone())
                .execute()
                .await
                .unwrap();
                write!(&mut output, "{}", test_response.status.as_str())
                    .unwrap();
                // If this succeeded, it must have returned the right user back.
                if test_response.status == http::StatusCode::OK {
                    let found_user =
                        test_response.parsed_body::<views::User>().unwrap();
                    assert_eq!(
                        found_user.silo_id,
                        test_silo.silo.identity().id
                    );
                    assert_eq!(found_user, **user);
                } else {
                    let error = test_response
                        .parsed_body::<dropshot::HttpErrorResponseBody>()
                        .unwrap();
                    let message = id_re.replace_all(&error.message, "...");
                    write!(&mut output, " (message = {:?})", message).unwrap();
                }

                write!(&mut output, "\n").unwrap();
            }

            write!(&mut output, "\n").unwrap();
        }
    }

    expectorate::assert_contents(
        "tests/output/silo-user-views-output.txt",
        &output,
    );
}

/// Create a user in a SamlJit Silo for testing
///
/// For local-only Silos, use the real API (via `create_local_user()`).
async fn create_jit_user(
    datastore: &db::DataStore,
    silo: &views::Silo,
    external_id: &str,
) -> views::User {
    assert_eq!(silo.identity_mode, shared::SiloIdentityMode::SamlJit);
    let silo_id = silo.identity.id;
    let silo_user_id = SiloUserUuid::new_v4();
    let authz_silo =
        authz::Silo::new(authz::FLEET, silo_id, LookupType::ById(silo_id));
    let silo_user =
        db::model::SiloUser::new(silo_id, silo_user_id, external_id.to_owned());
    datastore
        .silo_user_create(&authz_silo, silo_user)
        .await
        .expect("failed to create user in SamlJit Silo")
        .1
        .into()
}

/// Tests that LocalOnly-specific endpoints are not available in SamlJit Silos
#[nexus_test]
async fn test_jit_silo_constraints(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let silo =
        create_silo(&client, "jit", true, shared::SiloIdentityMode::SamlJit)
            .await;

    // We need one initial user that would in principle have privileges to
    // create other users.
    let admin_username = "admin-user";
    let admin_user = create_jit_user(&datastore, &silo, admin_username).await;

    // Grant this user "admin" privileges on that Silo.
    grant_iam(
        client,
        "/v1/system/silos/jit",
        SiloRole::Admin,
        admin_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Neither the "test-privileged" user nor this newly-created admin user
    // ought to be able to create a user via the Silo's local identity provider
    // (because that provider does not exist).
    for caller in
        [AuthnMode::PrivilegedUser, AuthnMode::SiloUser(admin_user.id)]
    {
        verify_local_idp_404(
            NexusRequest::expect_failure_with_body(
                client,
                StatusCode::NOT_FOUND,
                Method::POST,
                "/v1/system/identity-providers/local/users?silo=jit",
                &test_params::UserCreate {
                    external_id: UserId::from_str("dummy").unwrap(),
                    password: test_params::UserPassword::LoginDisallowed,
                },
            )
            .authn_as(caller),
        )
        .await;
    }

    // Now create another user, as might happen via JIT.
    let other_user_id =
        create_jit_user(datastore, &silo, "other-user").await.id;
    let user_url_delete = format!(
        "/v1/system/identity-providers/local/users/{}?silo=jit",
        other_user_id
    );
    let user_url_set_password = format!(
        "/v1/system/identity-providers/local/users/{}/set-password?silo=jit",
        other_user_id
    );

    // Neither the "test-privileged" user nor the Silo Admin ought to be able to
    // remove this user via the local identity provider, nor set the user's
    // password.
    let password = "dummy";
    for caller in
        [AuthnMode::PrivilegedUser, AuthnMode::SiloUser(admin_user.id)]
    {
        verify_local_idp_404(
            NexusRequest::expect_failure(
                client,
                StatusCode::NOT_FOUND,
                Method::DELETE,
                &user_url_delete,
            )
            .authn_as(caller.clone()),
        )
        .await;

        verify_local_idp_404(
            NexusRequest::expect_failure_with_body(
                client,
                StatusCode::NOT_FOUND,
                Method::POST,
                &user_url_set_password,
                &test_params::UserPassword::Password(password.to_string()),
            )
            .authn_as(caller.clone()),
        )
        .await;
    }

    // One should also not be able to log into this kind of Silo with a username
    // and password.
    verify_local_idp_404(NexusRequest::expect_failure_with_body(
        client,
        StatusCode::NOT_FOUND,
        Method::POST,
        "/v1/login/jit/local",
        &test_params::UsernamePasswordCredentials {
            username: UserId::from_str(admin_username).unwrap(),
            password: password.to_string(),
        },
    ))
    .await;

    // They should get the same error for a user that does not exist.
    verify_local_idp_404(NexusRequest::expect_failure_with_body(
        client,
        StatusCode::NOT_FOUND,
        Method::POST,
        "/v1/login/jit/local",
        &test_params::UsernamePasswordCredentials {
            username: UserId::from_str("bogus").unwrap(),
            password: password.to_string(),
        },
    ))
    .await;
}

async fn verify_local_idp_404(request: NexusRequest<'_>) {
    let error = request
        .execute()
        .await
        .unwrap()
        .parsed_body::<dropshot::HttpErrorResponseBody>()
        .unwrap();
    assert_eq!(
        error.message,
        "not found: identity-provider with name \"local\""
    );
}

/// Tests that SamlJit-specific endpoints are not available in LocalOnly Silos
#[nexus_test]
async fn test_local_silo_constraints(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a "LocalOnly" Silo with its own admin user.
    let silo = create_silo(
        &client,
        "fixed",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;
    let new_silo_user_id = create_local_user(
        client,
        &silo,
        &"admin-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;
    grant_iam(
        client,
        "/v1/system/silos/fixed",
        SiloRole::Admin,
        new_silo_user_id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // It's not allowed to create an identity provider in a LocalOnly Silo.
    let error: dropshot::HttpErrorResponseBody =
        NexusRequest::expect_failure_with_body(
            client,
            StatusCode::BAD_REQUEST,
            Method::POST,
            "/v1/system/identity-providers/saml?silo=fixed",
            &params::SamlIdentityProviderCreate {
                identity: IdentityMetadataCreateParams {
                    name: "some-totally-real-saml-provider"
                        .to_string()
                        .parse()
                        .unwrap(),
                    description: "a demo provider".to_string(),
                },

                idp_metadata_source:
                    params::IdpMetadataSource::Base64EncodedXml {
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
        .authn_as(AuthnMode::SiloUser(new_silo_user_id))
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();

    assert_eq!(
        error.message,
        "cannot create identity providers in this kind of Silo"
    );

    // The SAML login endpoints should not work, either.
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        "/login/fixed/saml/foo/redirect",
    )
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: identity-provider with name \"foo\"");
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::POST,
        "/login/fixed/saml/foo",
    )
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: identity-provider with name \"foo\"");
}

#[nexus_test]
async fn test_local_silo_users(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a "LocalOnly" Silo for testing.
    let silo1 = create_silo(
        &client,
        "silo1",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    // We'll run through a battery of tests as each of two different users: the
    // usual "test-privileged" user (which should have full access because
    // they're a Fleet Administrator) as well as a newly-created Silo Admin
    // user.
    run_user_tests(client, &silo1, &AuthnMode::PrivilegedUser, &[]).await;

    // Create a Silo Admin in our test Silo and run through the same tests.
    let admin_user = create_local_user(
        client,
        &silo1,
        &"admin-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;
    grant_iam(
        client,
        "/v1/system/silos/silo1",
        SiloRole::Admin,
        admin_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;
    run_user_tests(
        client,
        &silo1,
        &AuthnMode::SiloUser(admin_user.id),
        std::slice::from_ref(&admin_user),
    )
    .await;
}

/// Runs a sequence of tests for create, read, and delete of API-managed users
async fn run_user_tests(
    client: &dropshot::test_util::ClientTestContext,
    silo: &views::Silo,
    authn_mode: &AuthnMode,
    existing_users: &[views::User],
) {
    let url_all_users = format!("/v1/system/users?silo={}", silo.identity.name);
    let url_local_idp_users = format!(
        "/v1/system/identity-providers/local/users?silo={}",
        silo.identity.name
    );
    let url_user_create = url_local_idp_users.to_string();

    // Fetch users and verify it matches what the caller expects.
    println!("run_user_tests: as {:?}: fetch all users", authn_mode);
    let users = NexusRequest::object_get(client, &url_all_users)
        .authn_as(authn_mode.clone())
        .execute()
        .await
        .expect("failed to list users")
        .parsed_body::<dropshot::ResultsPage<views::User>>()
        .unwrap()
        .items;
    println!("users: {:?}", users);
    assert_eq!(users, existing_users);

    // Create a user.
    let user_created = NexusRequest::objects_post(
        client,
        &url_user_create,
        &test_params::UserCreate {
            external_id: UserId::from_str("a-test-user").unwrap(),
            password: test_params::UserPassword::LoginDisallowed,
        },
    )
    .authn_as(authn_mode.clone())
    .execute()
    .await
    .expect("failed to create user")
    .parsed_body::<views::User>()
    .unwrap();
    assert_eq!(user_created.display_name, "a-test-user");
    println!("created user: {:?}", user_created);

    // Fetch the user we just created.
    let user_url_get = format!(
        "/v1/system/users/{}?silo={}",
        user_created.id, silo.identity.name,
    );
    let user_found = NexusRequest::object_get(client, &user_url_get)
        .authn_as(authn_mode.clone())
        .execute()
        .await
        .expect("failed to fetch user we just created")
        .parsed_body::<views::User>()
        .unwrap();
    assert_eq!(user_created, user_found);

    // List users.  We should find whatever was there before, plus our new one.
    let new_users = NexusRequest::object_get(client, &url_all_users)
        .authn_as(authn_mode.clone())
        .execute()
        .await
        .expect("failed to list users")
        .parsed_body::<dropshot::ResultsPage<views::User>>()
        .unwrap()
        .items;
    println!("new_users: {:?}", new_users);
    let new_users = new_users
        .iter()
        .filter(|new_user| !users.iter().any(|old_user| *new_user == old_user))
        .collect::<Vec<_>>();
    assert_eq!(new_users, &[&user_created]);

    // Delete the user that we created.
    let user_url_delete = format!(
        "/v1/system/identity-providers/local/users/{}?silo={}",
        user_created.id, silo.identity.name,
    );
    NexusRequest::object_delete(client, &user_url_delete)
        .authn_as(authn_mode.clone())
        .execute()
        .await
        .expect("failed to delete the user we just created");

    // We should not be able to fetch or delete the user again.
    for method in [Method::GET, Method::DELETE] {
        let url = if method == Method::GET {
            &user_url_get
        } else {
            &user_url_delete
        };
        let error = NexusRequest::expect_failure(
            client,
            StatusCode::NOT_FOUND,
            method,
            url,
        )
        .authn_as(authn_mode.clone())
        .execute()
        .await
        .expect("unexpectedly succeeded in fetching deleted user")
        .parsed_body::<dropshot::HttpErrorResponseBody>()
        .unwrap();
        let not_found_message =
            format!("not found: silo-user with id \"{}\"", user_created.id);
        assert_eq!(error.message, not_found_message);
    }

    // List users again.  We should just find whatever we started with.
    let last_users = NexusRequest::object_get(client, &url_all_users)
        .authn_as(authn_mode.clone())
        .execute()
        .await
        .expect("failed to list users")
        .parsed_body::<dropshot::ResultsPage<views::User>>()
        .unwrap()
        .items;
    println!("last_users: {:?}", last_users);
    assert_eq!(last_users, existing_users);
}

pub async fn verify_silo_dns_name(
    cptestctx: &ControlPlaneTestContext,
    silo_name: &str,
    should_exist: bool,
) {
    // The DNS naming scheme for Silo DNS names is just:
    //     $silo_name.sys.$delegated_name
    // This is determined by RFD 357 and also implemented in Nexus.
    let dns_name =
        format!("{}.sys.{}", silo_name, cptestctx.external_dns_zone_name);

    // We assume that in the test suite, Nexus's "external" address is
    // localhost.
    let nexus_ip = Ipv4Addr::LOCALHOST;

    wait_for_condition(
        || async {
            let found = match cptestctx
                .external_dns
                .resolver()
                .await
                .expect("Failed to create external DNS resolver")
                .ipv4_lookup(&dns_name)
                .await
            {
                Ok(result) => {
                    let addrs: Vec<_> = result.iter().map(|a| &a.0).collect();
                    if addrs.is_empty() {
                        false
                    } else {
                        assert_eq!(addrs, [&nexus_ip]);
                        true
                    }
                }
                Err(error) => match resolve_error_proto_kind(&error) {
                    Some(ProtoErrorKind::NoRecordsFound { .. }) => false,
                    _ => panic!(
                        "unexpected error querying external \
                            DNS server for Silo DNS name {:?}: {:#}",
                        dns_name, error
                    ),
                },
            };

            if should_exist == found {
                Ok(())
            } else {
                Err::<_, CondCheckError<Infallible>>(CondCheckError::NotYet)
            }
        },
        &Duration::from_millis(50),
        &Duration::from_secs(15),
    )
    .await
    .expect("failed to verify external DNS configuration");
}

fn resolve_error_proto_kind(
    e: &hickory_resolver::ResolveError,
) -> Option<&ProtoErrorKind> {
    let ResolveErrorKind::Proto(proto_error) = e.kind() else { return None };
    Some(proto_error.kind())
}

// Test the basic behavior of the Silo-level IAM policy that supports
// configuring Silo roles to confer Fleet-level roles.  Because we don't support
// modifying Silos at all, we have to use separate Silos to test this behavior.
//
// We'll create a few Silos for testing:
//
// - default-policy: uses the default conferred-roles policy
// - viewer-policy: silo viewers get fleet viewer role
// - admin-policy: silo admins get fleet admin role
//
// For each of these Silos, we'll create an admin user in that Silo and test
// what privileges they have.
//
// This is not an exhaustive test of the policy choices here.  That's done
// in the "policy_test" unit test in Nexus.  This is an end-to-end test
// exercising _that_ this policy seems to be used when it should be.
#[nexus_test]
async fn test_silo_authn_policy(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let test_cases = [
        ("default-policy", ExpectedFleetPrivileges::None, BTreeMap::new()),
        (
            "viewer-policy",
            ExpectedFleetPrivileges::ReadOnly,
            BTreeMap::from([(
                SiloRole::Viewer,
                BTreeSet::from([FleetRole::Viewer]),
            )]),
        ),
        // It's important to test the case of someone with "Fleet Collaborator"
        // because that's the only role that would allow someone to create
        // ordinary Silos but _not_ Silos that confer additional privileges.
        // Thus, this is the only case that tests that we don't allow this
        // potentially dangerous privilege escalation!
        (
            "collaborator-policy",
            ExpectedFleetPrivileges::CreateSilo,
            BTreeMap::from([(
                SiloRole::Admin,
                BTreeSet::from([FleetRole::Collaborator]),
            )]),
        ),
        (
            "admin-policy",
            ExpectedFleetPrivileges::CreatePrivilegedSilo,
            BTreeMap::from([(
                SiloRole::Admin,
                BTreeSet::from([FleetRole::Admin]),
            )]),
        ),
    ];

    for (label, expected_privileges, policy) in test_cases {
        println!("test case: {:?}", label);

        // Create a Silo with the expected policy.
        let silo_name = label.parse().unwrap();
        let silo = NexusRequest::objects_post(
            client,
            "/v1/system/silos",
            &params::SiloCreate {
                identity: IdentityMetadataCreateParams {
                    name: silo_name,
                    description: String::new(),
                },
                quotas: params::SiloQuotasCreate::empty(),
                discoverable: false,
                identity_mode: shared::SiloIdentityMode::LocalOnly,
                admin_group_name: None,
                tls_certificates: vec![],
                mapped_fleet_roles: policy,
                restrict_network_actions: None,
            },
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body::<views::Silo>()
        .unwrap();

        // Create an administrator in this Silo.
        let admin_user = create_local_user(
            client,
            &silo,
            &(format!("{}-user", label).parse().unwrap()),
            test_params::UserPassword::LoginDisallowed,
        )
        .await;
        grant_iam(
            client,
            &format!("/v1/system/silos/{}", label),
            SiloRole::Admin,
            admin_user.id,
            AuthnMode::PrivilegedUser,
        )
        .await;

        // See what Fleet-level privileges they have.
        check_fleet_privileges(
            client,
            &AuthnMode::SiloUser(admin_user.id),
            expected_privileges,
        )
        .await;
    }
}

enum ExpectedFleetPrivileges {
    None,
    ReadOnly,
    CreateSilo,
    CreatePrivilegedSilo,
}

async fn check_fleet_privileges(
    client: &dropshot::test_util::ClientTestContext,
    authn_mode: &AuthnMode,
    expected: ExpectedFleetPrivileges,
) {
    // To test reading the fleet, we try listing racks.
    const URL_RO: &'static str = "/v1/system/hardware/racks";
    let nexus_request = if let ExpectedFleetPrivileges::None = expected {
        NexusRequest::expect_failure(
            client,
            http::StatusCode::FORBIDDEN,
            http::Method::GET,
            URL_RO,
        )
    } else {
        NexusRequest::object_get(client, URL_RO)
    };
    nexus_request.authn_as(authn_mode.clone()).execute().await.unwrap();

    // Next, see if the user can create an unprivileged Silo (i.e., one that
    // confers no Fleet-level roles).
    const URL_SILOS: &'static str = "/v1/system/silos";
    const SILO_NAME: &'static str = "probe-silo";
    let body = params::SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: SILO_NAME.parse().unwrap(),
            description: String::new(),
        },
        quotas: params::SiloQuotasCreate::empty(),
        discoverable: false,
        identity_mode: shared::SiloIdentityMode::LocalOnly,
        admin_group_name: None,
        tls_certificates: vec![],
        mapped_fleet_roles: BTreeMap::new(),
        restrict_network_actions: None,
    };
    let (do_delete, nexus_request) = match expected {
        ExpectedFleetPrivileges::None | ExpectedFleetPrivileges::ReadOnly => (
            false,
            NexusRequest::expect_failure_with_body(
                client,
                http::StatusCode::FORBIDDEN,
                http::Method::POST,
                URL_SILOS,
                &body,
            ),
        ),
        ExpectedFleetPrivileges::CreateSilo
        | ExpectedFleetPrivileges::CreatePrivilegedSilo => (
            true,
            NexusRequest::objects_post(
                client,
                URL_SILOS,
                &params::SiloCreate {
                    identity: IdentityMetadataCreateParams {
                        name: SILO_NAME.parse().unwrap(),
                        description: String::new(),
                    },
                    quotas: params::SiloQuotasCreate::empty(),
                    discoverable: false,
                    identity_mode: shared::SiloIdentityMode::LocalOnly,
                    admin_group_name: None,
                    tls_certificates: vec![],
                    mapped_fleet_roles: BTreeMap::new(),
                    restrict_network_actions: None,
                },
            ),
        ),
    };
    nexus_request.authn_as(authn_mode.clone()).execute().await.unwrap();

    if do_delete {
        // Try to delete what we created.
        let url = format!("{}/{}", URL_SILOS, SILO_NAME);
        NexusRequest::object_delete(client, &url)
            .authn_as(authn_mode.clone())
            .execute()
            .await
            .unwrap();
    }

    // Last, see if the user can create a privileged Silo.
    let body = params::SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: SILO_NAME.parse().unwrap(),
            description: String::new(),
        },
        quotas: params::SiloQuotasCreate::empty(),
        discoverable: false,
        identity_mode: shared::SiloIdentityMode::LocalOnly,
        admin_group_name: None,
        tls_certificates: vec![],
        mapped_fleet_roles: BTreeMap::from([(
            SiloRole::Admin,
            BTreeSet::from([FleetRole::Viewer]),
        )]),
        restrict_network_actions: None,
    };
    let (do_delete, nexus_request) = match expected {
        ExpectedFleetPrivileges::None
        | ExpectedFleetPrivileges::ReadOnly
        | ExpectedFleetPrivileges::CreateSilo => (
            false,
            NexusRequest::expect_failure_with_body(
                client,
                http::StatusCode::FORBIDDEN,
                http::Method::POST,
                URL_SILOS,
                &body,
            ),
        ),
        ExpectedFleetPrivileges::CreatePrivilegedSilo => (
            true,
            NexusRequest::objects_post(
                client,
                URL_SILOS,
                &params::SiloCreate {
                    identity: IdentityMetadataCreateParams {
                        name: SILO_NAME.parse().unwrap(),
                        description: String::new(),
                    },
                    quotas: params::SiloQuotasCreate::empty(),
                    discoverable: false,
                    identity_mode: shared::SiloIdentityMode::LocalOnly,
                    admin_group_name: None,
                    tls_certificates: vec![],
                    mapped_fleet_roles: BTreeMap::new(),
                    restrict_network_actions: None,
                },
            ),
        ),
    };
    nexus_request.authn_as(authn_mode.clone()).execute().await.unwrap();

    if do_delete {
        // Try to delete what we created.
        let url = format!("{}/{}", URL_SILOS, SILO_NAME);
        NexusRequest::object_delete(client, &url)
            .authn_as(authn_mode.clone())
            .execute()
            .await
            .unwrap();
    }
}

// Test that a silo admin can create new certificates for their silo
//
// Internally, the certificate validation check requires the `authz::DNS_CONFIG`
// resource (to check that the certificate is valid for
// `{silo_name}.{external_dns_zone_name}`), which silo admins may not have. We
// have to use an alternate, elevated context to perform that check, and this
// test confirms we do so.
#[nexus_test]
async fn test_silo_admin_can_create_certs(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let certs_url = "/v1/certificates";

    // Create a silo with an admin user
    let silo = create_silo(
        client,
        "silo-name",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    let new_silo_user_id = create_local_user(
        client,
        &silo,
        &"admin".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;

    grant_iam(
        client,
        "/v1/system/silos/silo-name",
        SiloRole::Admin,
        new_silo_user_id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // The user should be able to create certs for this silo
    let chain = CertificateChain::new(cptestctx.wildcard_silo_dns_name());
    let (cert, key) =
        (chain.cert_chain_as_pem(), chain.end_cert_private_key_as_pem());

    let cert: Certificate = NexusRequest::objects_post(
        client,
        certs_url,
        &params::CertificateCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-cert".parse().unwrap(),
                description: "the test cert".to_string(),
            },
            cert,
            key,
            service: shared::ServiceUsingCertificate::ExternalApi,
        },
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to create certificate")
    .parsed_body()
    .unwrap();

    // The cert should exist when listing the silo's certs as the silo admin
    let silo_certs =
        NexusRequest::object_get(client, &format!("{certs_url}?limit=10"))
            .authn_as(AuthnMode::SiloUser(new_silo_user_id))
            .execute()
            .await
            .expect("failed to list certificates")
            .parsed_body::<ResultsPage<Certificate>>()
            .expect("failed to parse body as ResultsPage<Certificate>")
            .items;

    assert_eq!(silo_certs.len(), 1);
    assert_eq!(silo_certs[0].identity.id, cert.identity.id);
}

// Test that silo delete cleans up associated groups
#[nexus_test]
async fn test_silo_delete_cleans_up_ip_pool_links(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a silo
    let silo1 =
        create_silo(&client, "silo1", true, shared::SiloIdentityMode::SamlJit)
            .await;
    let silo2 =
        create_silo(&client, "silo2", true, shared::SiloIdentityMode::SamlJit)
            .await;

    // link pool1 to both, link pool2 to silo1 only
    let range1 = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 51),
            std::net::Ipv4Addr::new(10, 0, 0, 52),
        )
        .unwrap(),
    );
    create_ip_pool(client, "pool1", Some(range1)).await;
    link_ip_pool(client, "pool1", &silo1.identity.id, true).await;
    link_ip_pool(client, "pool1", &silo2.identity.id, true).await;

    let range2 = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 53),
            std::net::Ipv4Addr::new(10, 0, 0, 54),
        )
        .unwrap(),
    );
    create_ip_pool(client, "pool2", Some(range2)).await;
    link_ip_pool(client, "pool2", &silo1.identity.id, false).await;

    // we want to make sure the links are there before we make sure they're gone
    let url = "/v1/system/ip-pools/pool1/silos";
    let links =
        objects_list_page_authz::<views::IpPoolSiloLink>(client, &url).await;
    assert_eq!(links.items.len(), 2);

    let url = "/v1/system/ip-pools/pool2/silos";
    let links =
        objects_list_page_authz::<views::IpPoolSiloLink>(client, &url).await;
    assert_eq!(links.items.len(), 1);

    // Delete the silo
    let url = format!("/v1/system/silos/{}", silo1.identity.id);
    object_delete(client, &url).await;

    // Now make sure the links are gone
    let url = "/v1/system/ip-pools/pool1/silos";
    let links =
        objects_list_page_authz::<views::IpPoolSiloLink>(client, &url).await;
    assert_eq!(links.items.len(), 1);

    let url = "/v1/system/ip-pools/pool2/silos";
    let links =
        objects_list_page_authz::<views::IpPoolSiloLink>(client, &url).await;
    assert_eq!(links.items.len(), 0);

    // but the pools are of course still there
    let url = "/v1/system/ip-pools";
    let pools = objects_list_page_authz::<views::IpPool>(client, &url).await;
    assert_eq!(pools.items.len(), 2);
    assert_eq!(pools.items[0].identity.name, "pool1");
    assert_eq!(pools.items[1].identity.name, "pool2");

    // nothing prevents us from deleting the pools (except the child ranges --
    // we do have to remove those)

    let url = "/v1/system/ip-pools/pool1/ranges/remove";
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, url)
            .body(Some(&range1))
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to delete IP range from a pool");

    let url = "/v1/system/ip-pools/pool2/ranges/remove";
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, url)
            .body(Some(&range2))
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to delete IP range from a pool");

    object_delete(client, "/v1/system/ip-pools/pool1").await;
    object_delete(client, "/v1/system/ip-pools/pool2").await;
}

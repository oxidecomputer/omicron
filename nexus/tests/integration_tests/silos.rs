// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use omicron_common::api::external::{IdentityMetadataCreateParams, Name};
use omicron_nexus::authn::silos::IdentityProviderType;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::views::{
    self, IdentityProvider, Organization, SamlIdentityProvider, Silo,
};
use omicron_nexus::TestInterfaces as _;
use std::collections::HashSet;

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::resource_helpers::{
    create_organization, create_silo, grant_iam, object_create,
    objects_list_page_authz,
};

use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_nexus::authz::SiloRole;

use httptest::{matchers::*, responders::*, Expectation, Server};
use omicron_nexus::authn::{USER_TEST_PRIVILEGED, USER_TEST_UNPRIVILEGED};
use omicron_nexus::db::fixed_data::silo::SILO_ID;

#[nexus_test]
async fn test_silos(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create two silos: one discoverable, one not
    create_silo(&client, "discoverable", true).await;
    create_silo(&client, "hidden", false).await;

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

// Test listing providers
#[nexus_test]
async fn test_listing_identity_providers(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // List providers - should be none
    let providers = objects_list_page_authz::<IdentityProvider>(
        client,
        "/silos/default-silo/identity_providers",
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
        &"/silos/default-silo/saml_identity_providers",
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

    let silo_saml_idp_2: SamlIdentityProvider = object_create(
        client,
        &"/silos/default-silo/saml_identity_providers",
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
        },
    )
    .await;

    // List providers again - expect 2
    let providers = objects_list_page_authz::<IdentityProvider>(
        client,
        "/silos/default-silo/identity_providers",
    )
    .await
    .items;

    assert_eq!(providers.len(), 2);

    let provider_name_set =
        providers.into_iter().map(|x| x.identity.name).collect::<HashSet<_>>();
    assert!(provider_name_set.contains(&silo_saml_idp_1.identity.name));
    assert!(provider_name_set.contains(&silo_saml_idp_2.identity.name));
}

// Valid SAML IdP entity descriptor from https://en.wikipedia.org/wiki/SAML_metadata#Identity_provider_metadata
// note: no signing keys
pub const SAML_IDP_DESCRIPTOR: &str = r#"
<md:EntityDescriptor entityID="https://sso.example.org/idp" validUntil="3017-08-30T19:10:29Z"
    xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
    xmlns:mdrpi="urn:oasis:names:tc:SAML:metadata:rpi"
    xmlns:mdattr="urn:oasis:names:tc:SAML:metadata:attribute"
    xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
    xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
    <md:Extensions>
      <mdrpi:RegistrationInfo registrationAuthority="https://registrar.example.net"/>
      <mdrpi:PublicationInfo creationInstant="2017-08-16T19:10:29Z" publisher="https://registrar.example.net"/>
      <mdattr:EntityAttributes>
        <saml:Attribute Name="http://registrar.example.net/entity-category" NameFormat="urn:oasis:names:tc:SAML:2.0:attrname-format:uri">
          <saml:AttributeValue>https://registrar.example.net/category/self-certified</saml:AttributeValue>
        </saml:Attribute>
      </mdattr:EntityAttributes>
    </md:Extensions>
    <md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
      <md:Extensions>
        <mdui:UIInfo>
          <mdui:DisplayName xml:lang="en">Example.org</mdui:DisplayName>
          <mdui:Description xml:lang="en">The identity provider at Example.org</mdui:Description>
          <mdui:Logo height="32" width="32" xml:lang="en">https://idp.example.org/myicon.png</mdui:Logo>
        </mdui:UIInfo>
      </md:Extensions>
      <md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.org/SAML2/SSO/Redirect"/>
      <md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://idp.example.org/SAML2/SSO/POST"/>
    </md:IDPSSODescriptor>
    <md:Organization>
      <md:OrganizationName xml:lang="en">Example.org Non-Profit Org</md:OrganizationName>
      <md:OrganizationDisplayName xml:lang="en">Example.org</md:OrganizationDisplayName>
      <md:OrganizationURL xml:lang="en">https://www.example.org/</md:OrganizationURL>
    </md:Organization>
    <md:ContactPerson contactType="technical">
      <md:SurName>SAML Technical Support</md:SurName>
      <md:EmailAddress>mailto:technical-support@example.org</md:EmailAddress>
    </md:ContactPerson>
  </md:EntityDescriptor>"#;

// Create a SAML IdP
#[nexus_test]
async fn test_create_a_saml_idp(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true).await;

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

    let silo_saml_idp: SamlIdentityProvider = object_create(
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

    let retrieved_silo_idp_from_nexus = IdentityProviderType::lookup(
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

// Fail to create a SAML IdP out of an invalid descriptor
#[nexus_test]
async fn test_create_a_saml_idp_invalid_descriptor_truncated(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    const SILO_NAME: &str = "saml-silo";
    create_silo(&client, SILO_NAME, true).await;

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
    create_silo(&client, SILO_NAME, true).await;

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

    create_silo(&client, "hidden", false).await;

    // Valid IdP descriptor
    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR.to_string();

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    let silo_saml_idp: SamlIdentityProvider = object_create(
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
    create_silo(&client, SILO_NAME, true).await;

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
    create_silo(&client, SILO_NAME, true).await;

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

// Create a Silo with a SAML IdP document string
#[nexus_test]
async fn test_saml_idp_metadata_data_valid(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_silo(&client, "blahblah", true).await;

    let silo_saml_idp: SamlIdentityProvider = object_create(
        client,
        "/silos/blahblah/saml_identity_providers",
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

    create_silo(&client, "blahblah", true).await;

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &"/silos/blahblah/saml_identity_providers",
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
    create_silo(&client, SILO_NAME, true).await;

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

            idp_metadata_source: params::IdpMetadataSource::Base64EncodedXml {
                data: "bad data".to_string(),
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

// TODO samael does not support ECDSA yet, add tests when it does
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
    create_silo(&client, SILO_NAME, true).await;

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
    create_silo(&client, SILO_NAME, true).await;

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
            views::User { id: USER_TEST_PRIVILEGED.id() },
            views::User { id: USER_TEST_UNPRIVILEGED.id() },
        ]
    );

    // Now create another user and make sure we can see them.  While we're at
    // it, use a small limit to check that pagination is really working.
    let new_silo_user_id =
        "bd75d207-37f3-4769-b808-677ae04eaf23".parse().unwrap();
    nexus.silo_user_create(SILO_ID, new_silo_user_id).await.unwrap();

    let silo_users: Vec<views::User> =
        NexusRequest::iter_collection_authn(client, "/users", "", Some(1))
            .await
            .expect("failed to list silo users (2)")
            .all_items;
    assert_eq!(
        silo_users,
        vec![
            views::User { id: USER_TEST_PRIVILEGED.id() },
            views::User { id: USER_TEST_UNPRIVILEGED.id() },
            views::User { id: new_silo_user_id() },
        ]
    );

    // TODO-coverage When we have a way to remove or invalidate Silo Users, we
    // should test that doing so causes them to stop appearing in the list.
}

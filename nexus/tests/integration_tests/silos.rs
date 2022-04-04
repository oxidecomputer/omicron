// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use uuid::Uuid;

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::SiloSamlIdentityProvider;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::views::{
    Organization, Silo, SiloIdentityProvider,
};
use omicron_nexus::TestInterfaces as _;

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::resource_helpers::{
    create_organization, create_silo, object_create, objects_list_page_authz,
};

use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

use httptest::{matchers::*, responders::*, Expectation, Server};

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

    // Create a new user in the discoverable silo, then create a console session
    let new_silo_user = nexus
        .silo_user_create(
            silos[0].identity.id, /* silo id */
            Uuid::new_v4(),       /* silo user id */
        )
        .await
        .unwrap();

    let session = nexus.session_create(new_silo_user.id).await.unwrap();

    // Create organization with built-in user auth
    // Note: this currently goes to the built-in silo!
    create_organization(&client, "someorg").await;

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
        &"/silos/fakesilo",
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
        .silo_user_fetch(new_silo_user.id)
        .await
        .expect_err("unexpected success");

    // Verify new user's console session isn't valid anymore.
    nexus
        .session_fetch(session.token.clone())
        .await
        .expect_err("unexpected success");
}

// Valid SAML IdP entity descriptor from https://en.wikipedia.org/wiki/SAML_metadata#Identity_provider_metadata
// note: no signing keys
const SAML_IDP_DESCRIPTOR: &str = r#"
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

    let silo: Silo = create_silo(&client, "discoverable", true).await;

    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR;

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    let silo_saml_idp: SiloSamlIdentityProvider = object_create(
        client,
        "/silos/discoverable/saml_identity_provider",
        &params::SiloSamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "an org".to_string(),
            },

            silo_id: silo.identity.id,
            idp_metadata_url: server.url("/descriptor").to_string(),

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            public_cert: None,
            private_key: None,
        },
    )
    .await;

    // Expect the SSO redirect when trying to log in
    let result = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!("/login/{}", silo_saml_idp.identity.id),
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

    // Expect that the list of providers contains this
    let identity_providers: Vec<SiloIdentityProvider> =
        NexusRequest::object_get(client, "/identity_provider")
            .execute()
            .await
            .expect("success")
            .parsed_body()
            .unwrap();

    assert_eq!(identity_providers.len(), 1);
    assert_eq!(identity_providers[0].provider_id, silo_saml_idp.identity.id);
}

// Test that deleting the silo deletes the idp
#[nexus_test]
async fn test_deleting_a_silo_deletes_the_idp(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let silo: Silo = create_silo(&client, "discoverable", true).await;

    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR;

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    let silo_saml_idp: SiloSamlIdentityProvider = object_create(
        client,
        "/silos/discoverable/saml_identity_provider",
        &params::SiloSamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "an org".to_string(),
            },

            silo_id: silo.identity.id,
            idp_metadata_url: server.url("/descriptor").to_string(),

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            public_cert: None,
            private_key: None,
        },
    )
    .await;

    // Expect that the list of providers contains this
    let identity_providers: Vec<SiloIdentityProvider> =
        NexusRequest::object_get(client, "/identity_provider")
            .execute()
            .await
            .expect("success")
            .parsed_body()
            .unwrap();

    assert_eq!(identity_providers.len(), 1);
    assert_eq!(identity_providers[0].provider_id, silo_saml_idp.identity.id);

    // Delete the silo
    NexusRequest::object_delete(&client, &"/silos/discoverable")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");

    // Expect that the provider is gone
    let identity_providers: Vec<SiloIdentityProvider> =
        NexusRequest::object_get(client, "/identity_provider")
            .execute()
            .await
            .expect("success")
            .parsed_body()
            .unwrap();

    assert_eq!(identity_providers.len(), 0);
}

// Fail to create a SAML IdP out of an invalid descriptor
#[nexus_test]
async fn test_create_a_saml_idp_invalid_descriptor_truncated(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let silo: Silo = create_silo(&client, "discoverable", true).await;

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
            "/silos/discoverable/saml_identity_provider",
        )
        .body(Some(&params::SiloSamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "an org".to_string(),
            },

            silo_id: silo.identity.id,
            idp_metadata_url: server.url("/descriptor").to_string(),

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            public_cert: None,
            private_key: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
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

    let silo: Silo = create_silo(&client, "discoverable", true).await;

    let saml_idp_descriptor = {
        let mut saml_idp_descriptor = SAML_IDP_DESCRIPTOR.to_string();
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
            "/silos/discoverable/saml_identity_provider",
        )
        .body(Some(&params::SiloSamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "an org".to_string(),
            },

            silo_id: silo.identity.id,
            idp_metadata_url: server.url("/descriptor").to_string(),

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            public_cert: None,
            private_key: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
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

    let silo: Silo = create_silo(&client, "hidden", false).await;

    // Valid IdP descriptor
    let saml_idp_descriptor = SAML_IDP_DESCRIPTOR.to_string();

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(200).body(saml_idp_descriptor)),
    );

    let silo_saml_idp: SiloSamlIdentityProvider = object_create(
        client,
        "/silos/hidden/saml_identity_provider",
        &params::SiloSamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "an org".to_string(),
            },

            silo_id: silo.identity.id,
            idp_metadata_url: server.url("/descriptor").to_string(),

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            public_cert: None,
            private_key: None,
        },
    )
    .await;

    // Expect the SSO redirect when trying to log in
    let result = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!("/login/{}", silo_saml_idp.identity.id),
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

    // Expect that the list of providers is empty
    let identity_providers: Vec<SiloIdentityProvider> =
        NexusRequest::object_get(client, "/identity_provider")
            .execute()
            .await
            .expect("success")
            .parsed_body()
            .unwrap();

    assert_eq!(identity_providers.len(), 0);
}

// Can't create a SAML IdP if the metadata URL returns something that's not 200
#[nexus_test]
async fn test_saml_idp_metadata_url_404(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let silo: Silo = create_silo(&client, "discoverable", true).await;

    let server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/descriptor"))
            .respond_with(status_code(404).body("no descriptor found")),
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            "/silos/discoverable/saml_identity_provider",
        )
        .body(Some(&params::SiloSamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "an org".to_string(),
            },

            silo_id: silo.identity.id,
            idp_metadata_url: server.url("/descriptor").to_string(),

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            public_cert: None,
            private_key: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
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

    let silo: Silo = create_silo(&client, "discoverable", true).await;

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            "/silos/discoverable/saml_identity_provider",
        )
        .body(Some(&params::SiloSamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: "some-totally-real-saml-provider"
                    .to_string()
                    .parse()
                    .unwrap(),
                description: "an org".to_string(),
            },

            silo_id: silo.identity.id,
            idp_metadata_url: "htttps://fake.url".to_string(),

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            public_cert: None,
            private_key: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .execute()
    .await
    .expect("unexpected success");
}

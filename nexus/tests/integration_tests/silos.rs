// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use uuid::Uuid;

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use omicron_nexus::external_api::views::{Organization, Silo};

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::resource_helpers::{
    create_organization, create_silo, objects_list_page_authz,
    objects_list_page_authz_with_session,
};

use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

#[nexus_test]
async fn test_silos(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create two silos: one discoverable, one not
    create_silo(&client, &"discoverable", true).await;
    create_silo(&client, &"hidden", false).await;

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
            silos[0].identity.id,        /* silo id */
            Uuid::new_v4(),              /* silo user id */
            "new silo user".to_string(), /* name */
            Uuid::new_v4(),              /* internal user id */
        )
        .await
        .unwrap();

    let session =
        nexus.session_create(new_silo_user.internal_user_id).await.unwrap();

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
        .get_silo_id_from_internal_user_id(new_silo_user.internal_user_id)
        .await
        .expect_err("unexpected success");
    nexus
        .get_silo_id_from_silo_user_id(new_silo_user.id)
        .await
        .expect_err("unexpected success");

    // Verify new user's console session isn't valid anymore.
    nexus
        .session_fetch(session.token.clone())
        .await
        .expect_err("unexpected success");
}

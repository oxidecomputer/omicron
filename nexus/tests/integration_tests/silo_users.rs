use dropshot::ResultsPage;
use http::{method::Method, StatusCode};
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils_macros::nexus_test;
use nexus_types::identity::Asset;
use omicron_common::api::external::LookupType;
use omicron_nexus::{
    authn::USER_TEST_UNPRIVILEGED, authz, context::OpContext,
    db::fixed_data::silo::SILO_ID, external_api::views,
};
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

/// Create two users, one in group A and the other not, with direct calls to
/// silo_user_from_authenticated_subject, then test that the `/v1/users` gives
/// back both users and `/v1/users?group=<a_id>` only gives back one
#[nexus_test]
async fn test_silo_group_users(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        cptestctx.server.apictx().nexus.datastore().clone(),
    );

    let users = NexusRequest::object_get(&client, &"/v1/users")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::User>>()
        .await;

    // we start out with the two default users
    let user_names: Vec<String> =
        users.items.iter().map(|u| u.display_name.clone()).collect();
    assert_eq!(users.items.len(), 2);
    assert!(user_names.contains(&"privileged".to_string()));
    assert!(user_names.contains(&"unprivileged".to_string()));

    // no groups to start with
    let groups = NexusRequest::object_get(&client, &"/v1/groups")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Group>>()
        .await;
    assert_eq!(groups.items.len(), 0);

    let authz_silo =
        authz::Silo::new(authz::FLEET, *SILO_ID, LookupType::ById(*SILO_ID));

    // create a group
    let group = nexus
        .silo_group_lookup_or_create_by_name(
            &opctx,
            &authz_silo,
            &"group1".to_string(),
        )
        .await
        .unwrap();

    // now we have a group
    let groups = NexusRequest::object_get(&client, &"/v1/groups")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Group>>()
        .await;
    assert_eq!(groups.items.len(), 1);

    let group_users_url = format!("/v1/users?group={}", group.id());

    // we can now fetch the group by ID and get an empty list of users
    let group_users = NexusRequest::object_get(&client, &group_users_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::User>>()
        .await;

    assert_eq!(group_users.items.len(), 0);

    let authz_silo_user = authz::SiloUser::new(
        authz_silo,
        USER_TEST_UNPRIVILEGED.id(),
        LookupType::ById(USER_TEST_UNPRIVILEGED.id()),
    );

    // Now add unprivileged user to the group, and we should see only that user
    // in the response when we filter for the group
    nexus
        .datastore()
        .silo_group_membership_replace_for_user(
            &opctx,
            &authz_silo_user,
            vec![group.id()],
        )
        .await
        .expect("Failed to set user group memberships");

    let group_users = NexusRequest::object_get(&client, &group_users_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::User>>()
        .await;

    assert_eq!(group_users.items.len(), 1);
    assert_eq!(
        group_users.items.get(0).unwrap().id,
        USER_TEST_UNPRIVILEGED.id()
    );
}

#[nexus_test]
async fn test_silo_group_users_bad_group_id(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // expect 404 on valid UUID that doesn't exist
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &format!("/v1/users?group={}", Uuid::new_v4()),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Expected 404");

    // supposed to 400 (or I guess we could 404) on non-UUID group identifier,
    // but it just ignores it if it fails to parse

    NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::GET,
        &"/v1/users?group=abc",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Expected 404");

    NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::GET,
        &"/v1/users?group=",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Expected 404");
}

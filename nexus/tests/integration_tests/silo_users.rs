// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use http::{StatusCode, method::Method};
use nexus_db_queries::authn::USER_TEST_UNPRIVILEGED;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::SiloGroupApiOnly;
use nexus_test_utils::assert_same_items;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use nexus_types::silo::DEFAULT_SILO_ID;
use omicron_common::api::external::LookupType;
use omicron_uuid_kinds::SiloGroupUuid;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

/// Create two users, one in group A and the other not, with direct calls to
/// silo_user_from_authenticated_subject, then test that the `/v1/users` gives
/// back both users and `/v1/users?group=<a_id>` only gives back one
#[nexus_test]
async fn test_silo_group_users(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        cptestctx.server.server_context().nexus.datastore().clone(),
    );

    // we start out with the two default users
    let users =
        objects_list_page_authz::<views::User>(client, &"/v1/users").await;
    let user_names: Vec<&str> =
        users.items.iter().map(|u| u.display_name.as_str()).collect();
    assert_same_items(user_names, vec!["privileged", "unprivileged"]);

    // no groups to start with
    let groups =
        objects_list_page_authz::<views::User>(client, &"/v1/groups").await;
    assert_eq!(groups.items.len(), 0);

    let authz_silo = authz::Silo::new(
        authz::FLEET,
        DEFAULT_SILO_ID,
        LookupType::ById(DEFAULT_SILO_ID),
    );

    // create a group
    let group_name = "group1".to_string();
    nexus
        .datastore()
        .silo_group_ensure(
            &opctx,
            &authz_silo,
            SiloGroupApiOnly::new(
                authz_silo.id(),
                SiloGroupUuid::new_v4(),
                group_name.clone(),
            )
            .into(),
        )
        .await
        .expect("Group created");

    // now we have a group
    let groups =
        objects_list_page_authz::<views::User>(client, &"/v1/groups").await;
    assert_eq!(groups.items.len(), 1);

    let group = groups.items.get(0).unwrap();
    assert_eq!(group.display_name, group_name);

    // we can fetch that group by ID
    let group_url = format!("/v1/groups/{}", group.id);
    let group = NexusRequest::object_get(&client, &group_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<views::Group>()
        .await;
    assert_eq!(group.display_name, group_name);

    let group_users_url = format!("/v1/users?group={}", group.id);

    // we can now fetch the group's user list and get an empty list of users
    let group_users =
        objects_list_page_authz::<views::User>(client, &group_users_url).await;

    assert_eq!(group_users.items.len(), 0);

    let authz_silo_user = authz::SiloUser::new(
        authz_silo,
        USER_TEST_UNPRIVILEGED.id(),
        LookupType::by_id(USER_TEST_UNPRIVILEGED.id()),
    );

    // Now add unprivileged user to the group, and we should see only that user
    // in the response when we filter for the group
    nexus
        .datastore()
        .silo_group_membership_replace_for_user(
            &opctx,
            &authz_silo_user,
            vec![group.id],
        )
        .await
        .expect("Failed to set user group memberships");

    let group_users =
        objects_list_page_authz::<views::User>(client, &group_users_url).await;
    let user_ids = group_users.items.iter().map(|g| g.id).collect();

    assert_same_items(user_ids, vec![USER_TEST_UNPRIVILEGED.id()]);
}

#[nexus_test]
async fn test_silo_group_users_bad_group_id(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // 404 on UUID that doesn't exist
    let nonexistent_group = format!("/v1/users?group={}", Uuid::new_v4());
    expect_failure(&client, &nonexistent_group, StatusCode::NOT_FOUND).await;

    // 400 on non-UUID identifier
    expect_failure(&client, &"/v1/users?group=abc", StatusCode::BAD_REQUEST)
        .await;

    // 400 on empty identifier
    expect_failure(&client, &"/v1/users?group=", StatusCode::BAD_REQUEST).await;
}

#[nexus_test]
async fn test_silo_group_detail_bad_group_id(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // 404 on UUID that doesn't exist
    let nonexistent_group = format!("/v1/groups/{}", Uuid::new_v4());
    expect_failure(&client, &nonexistent_group, StatusCode::NOT_FOUND).await;

    // 400 on non-UUID identifier
    expect_failure(&client, &"/v1/groups/abc", StatusCode::BAD_REQUEST).await;
}

async fn expect_failure(
    client: &ClientTestContext,
    url: &str,
    status_code: StatusCode,
) {
    NexusRequest::expect_failure(client, status_code, Method::GET, url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected failure");
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Basic test for built-in roles

use dropshot::ResultsPage;
use http::Method;
use http::StatusCode;
use std::collections::BTreeMap;

use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::test_setup;
use omicron_nexus::external_api::views::Role;

#[tokio::test]
async fn test_roles_builtin() {
    let cptestctx = test_setup("test_roles_builtin").await;
    let testctx = &cptestctx.external_client;

    RequestBuilder::new(testctx, Method::GET, "/roles")
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .unwrap();

    NexusRequest::new(
        RequestBuilder::new(testctx, Method::GET, "/roles")
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .unwrap();

    let mut roles = NexusRequest::object_get(&testctx, "/roles")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body::<ResultsPage<Role>>()
        .unwrap()
        .items
        .into_iter()
        .map(|r| (r.name.clone(), r))
        .collect::<BTreeMap<String, Role>>();

    // let u = users.remove(&authn::USER_DB_INIT.name.to_string()).unwrap();
    // assert_eq!(u.identity.id, authn::USER_DB_INIT.id);
    // let u = users.remove(&authn::USER_SAGA_RECOVERY.name.to_string()).unwrap();
    // assert_eq!(u.identity.id, authn::USER_SAGA_RECOVERY.id);
    // let u =
    //     users.remove(&authn::USER_TEST_PRIVILEGED.name.to_string()).unwrap();
    // assert_eq!(u.identity.id, authn::USER_TEST_PRIVILEGED.id);
    // let u =
    //     users.remove(&authn::USER_TEST_UNPRIVILEGED.name.to_string()).unwrap();
    // assert_eq!(u.identity.id, authn::USER_TEST_UNPRIVILEGED.id);
    // assert!(users.is_empty(), "found unexpected built-in users");
    cptestctx.teardown().await;
}

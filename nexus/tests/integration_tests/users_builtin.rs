//! Sanity-tests for built-in users

use dropshot::ResultsPage;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_nexus::authn;
use omicron_nexus::external_api::views::User;
use std::collections::BTreeMap;

#[nexus_test]
async fn test_users_builtin(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    let mut users = NexusRequest::object_get(&testctx, "/users")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body::<ResultsPage<User>>()
        .unwrap()
        .items
        .into_iter()
        .map(|u| (u.identity.name.to_string(), u))
        .collect::<BTreeMap<String, User>>();

    let u = users.remove(&authn::USER_DB_INIT.name.to_string()).unwrap();
    assert_eq!(u.identity.id, authn::USER_DB_INIT.id);
    let u = users.remove(&authn::USER_SAGA_RECOVERY.name.to_string()).unwrap();
    assert_eq!(u.identity.id, authn::USER_SAGA_RECOVERY.id);
    let u =
        users.remove(&authn::USER_TEST_PRIVILEGED.name.to_string()).unwrap();
    assert_eq!(u.identity.id, authn::USER_TEST_PRIVILEGED.id);
    let u =
        users.remove(&authn::USER_TEST_UNPRIVILEGED.name.to_string()).unwrap();
    assert_eq!(u.identity.id, authn::USER_TEST_UNPRIVILEGED.id);
    assert!(users.is_empty(), "found unexpected built-in users");

    // TODO-coverage add test for fetching individual users, including invalid
    // names?  See roles_builtin.rs.
}

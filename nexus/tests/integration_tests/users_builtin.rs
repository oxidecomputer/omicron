//! Sanity-tests for built-in users

use dropshot::ResultsPage;
use nexus_db_queries::authn;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views::UserBuiltin;
use std::collections::BTreeMap;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_users_builtin(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    let mut users =
        NexusRequest::object_get(testctx, "/v1/system/users-builtin")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body::<ResultsPage<UserBuiltin>>()
            .unwrap()
            .items
            .into_iter()
            .map(|u| (u.identity.name.to_string(), u))
            .collect::<BTreeMap<String, UserBuiltin>>();

    let u = users.remove(&authn::USER_DB_INIT.name.to_string()).unwrap();
    assert_eq!(u.identity.id, authn::USER_DB_INIT.id);
    let u =
        users.remove(&authn::USER_SERVICE_BALANCER.name.to_string()).unwrap();
    assert_eq!(u.identity.id, authn::USER_SERVICE_BALANCER.id);
    let u = users.remove(&authn::USER_INTERNAL_API.name.to_string()).unwrap();
    assert_eq!(u.identity.id, authn::USER_INTERNAL_API.id);
    let u = users.remove(&authn::USER_INTERNAL_READ.name.to_string()).unwrap();
    assert_eq!(u.identity.id, authn::USER_INTERNAL_READ.id);
    let u = users.remove(&authn::USER_EXTERNAL_AUTHN.name.to_string()).unwrap();
    assert_eq!(u.identity.id, authn::USER_EXTERNAL_AUTHN.id);
    let u = users.remove(&authn::USER_SAGA_RECOVERY.name.to_string()).unwrap();
    assert_eq!(u.identity.id, authn::USER_SAGA_RECOVERY.id);
    assert!(users.is_empty(), "found unexpected built-in users");

    // TODO-coverage add test for fetching individual users, including invalid
    // names?  See roles_builtin.rs.
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::endpoint_coverage::ApiOperations;
use super::endpoints::VERIFY_ENDPOINTS;
use expectorate::assert_contents;

/// Checks for uncovered API endpoints
///
/// This test compares the endpoints covered by the "unauthorized" test with
/// all endpoints from the API description (including unpublished ones) to make
/// sure all endpoints are accounted-for.
#[test]
fn test_unauthorized_coverage() {
    let mut api_operations = ApiOperations::new();

    // Go through each of the authz test cases and match each one against an
    // API operation.
    let mut unexpected_endpoints = String::from(
        "API endpoints tested by unauthorized.rs but not found \
        in the OpenAPI spec:\n",
    );
    for v in &*VERIFY_ENDPOINTS {
        for m in &v.allowed_methods {
            let method_string = m.http_method().to_string();
            if let Some(op) = api_operations.find(&method_string, v.url) {
                println!(
                    "covered: {:40} ({:6} {:?}) (by {:?})",
                    op.operation_id, op.method, op.path, v.url
                );
                let op = op.clone();
                api_operations.remove(&op);
            } else {
                unexpected_endpoints.push_str(&format!(
                    "{:6} {:?}\n",
                    method_string.to_uppercase(),
                    v.url
                ));
            }
        }
    }

    println!("-----");

    // If you're here because this assertion failed, we found an endpoint tested
    // by "unauthorized.rs" that's not in the OpenAPI spec.  This could happen
    // if you're adding a test for an endpoint that's marked "unpublished".  In
    // that case, you might just allow expectorate to add this endpoint to the
    // allowlist here.
    assert_contents(
        "tests/output/unexpected-authz-endpoints.txt",
        &unexpected_endpoints,
    );

    // Check for uncovered endpoints (endpoints that are in the API description
    // but not tested by the authz tests).
    let mut uncovered_endpoints =
        "API endpoints with no coverage in authz tests:\n".to_string();
    for op in api_operations.iter() {
        uncovered_endpoints.push_str(&format!(
            "{:40} ({:6} {:?})\n",
            op.operation_id,
            op.method.to_lowercase(),
            op.path
        ));
    }

    // If you're here because this assertion failed, you've added an API
    // operation to Nexus without adding a corresponding test in
    // "unauthorized.rs" to check its behavior for unauthenticated and
    // unauthorized users. DO NOT SKIP THIS. Even if you're just adding a stub,
    // see [`Nexus::unimplemented_todo()`].
    //
    // To fix this:
    // 1. Add a VerifyEndpoint entry in endpoints.rs for your new endpoint
    // 2. Run the test_unauthorized test to verify it works
    //
    // The allowed uncovered endpoints file should only ever SHRINK (when you
    // add coverage for an endpoint). It should never grow. If you've added
    // coverage for an endpoint, you can remove it from the allowlist file.
    //
    // NOTE: We intentionally do NOT use expectorate's assert_contents here
    // because we don't want EXPECTORATE=overwrite to allow people to
    // accidentally add uncovered endpoints to the allowlist.
    let expected_uncovered_endpoints =
        std::fs::read_to_string("tests/output/uncovered-authz-endpoints.txt")
            .expect("failed to read uncovered-authz-endpoints.txt");
    assert!(
        uncovered_endpoints == expected_uncovered_endpoints,
        "Uncovered endpoints list doesn't match expected.\n\n\
         If you ADDED a new endpoint, add authz coverage in endpoints.rs.\n\n\
         If you ADDED coverage for an existing endpoint, remove it from \
         tests/output/uncovered-authz-endpoints.txt.\n\n\
         Expected:\n{}\n\nActual:\n{}",
        expected_uncovered_endpoints,
        uncovered_endpoints
    );
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::endpoints::VERIFY_ENDPOINTS;
use expectorate::assert_contents;
use nexus_external_api::nexus_external_api_mod;
use std::collections::BTreeMap;

/// Checks for uncovered API endpoints
///
/// This test compares the endpoints covered by the "unauthorized" test with
/// all endpoints from the API description (including unpublished ones) to make
/// sure all endpoints are accounted-for.
#[test]
fn test_unauthorized_coverage() {
    // Get all endpoints from the API description, including unpublished ones.
    // The OpenAPI spec only includes published endpoints, but we need to track
    // coverage for unpublished endpoints (SCIM, login, console routes) as well.
    let api = nexus_external_api_mod::stub_api_description().unwrap();

    // Take each operation that we find in the API description, make a regular
    // expression that we can use to match against it (more on this below), and
    // throw them into a BTreeMap.
    //
    // We filter out versioned endpoints (operation IDs starting with "v20")
    // because those are older API versions that share coverage with their
    // current counterparts. We also deduplicate by (method, path) since
    // multiple API versions can share the same path with different operation
    // IDs, and we only care about coverage per path/method combination.
    let mut seen_paths: std::collections::BTreeSet<(String, String)> =
        std::collections::BTreeSet::new();
    let mut api_operations: BTreeMap<Operation, regex::Regex> = api
        .into_router()
        .endpoints(None)
        .filter(|(_, _, endpoint)| !endpoint.operation_id.starts_with("v20"))
        .filter(|(path, method, _)| {
            seen_paths.insert((method.to_string(), path.clone()))
        })
        .map(|(path, method, endpoint)| {
            // We're going to take URLs from our test cases and match them
            // against operations in the API description.  The URLs from the API
            // contain variables (e.g., "/instances/{instance_name}").  Our test
            // cases have those variables filled in already (e.g.,
            // "/instances/my-instance").
            //
            // To match a URL from the test case against one from the API
            // description, we're going to:
            //
            // - use a regular expression to replace `{varname}` in the API's
            //   URL with `[^/]+` (one or more non-slash characters)
            //
            // - use that string as the basis for a second regex that we'll
            //   store with the Operation.  We'll use this second regex to match
            //   URLs to their operation.
            //
            // This is slow (lookups will take time linear in the total number
            // of API endpoints) and a little cheesy, but it's expedient and
            // robust enough for our purposes.
            //
            // This will fail badly if it turns out that the URL contains any
            // characters that would be interpreted specially by the regular
            // expression engine.  So let's check up front that those aren't
            // present.
            assert!(
                path.chars().all(|c| c.is_ascii_alphanumeric()
                    || c == '_'
                    || c == '-'
                    || c == '{'
                    || c == '}'
                    || c == '/'),
                "unexpected character in URL: {:?}",
                path
            );
            let re = regex::Regex::new("/\\{[^}]+\\}").unwrap();
            let regex_path = re.replace_all(&path, "/[^/]+");
            let regex = regex::Regex::new(&format!("^{}$", regex_path))
                .expect("modified URL string was not a valid regex");
            let label = endpoint.operation_id.clone();
            (
                Operation {
                    method: method.to_string(),
                    path: path.clone(),
                    label,
                },
                regex,
            )
        })
        .collect();

    // Go through each of the authz test cases and match each one against an
    // API operation.
    let mut unexpected_endpoints = String::from(
        "API endpoints tested by unauthorized.rs but not found \
        in the OpenAPI spec:\n",
    );
    for v in &*VERIFY_ENDPOINTS {
        for m in &v.allowed_methods {
            // Remove the method and path from the list of operations if there's
            // a VerifyEndpoint for it.
            let method_string = m.http_method().to_string().to_uppercase();
            let found = api_operations.iter().find(|(op, regex)| {
                // Strip query parameters, if they exist.
                let url = v.url.split('?').next().unwrap();
                op.method == method_string && regex.is_match(url)
            });
            if let Some((op, _)) = found {
                println!(
                    "covered: {:40} ({:6} {:?}) (by {:?})",
                    op.label, op.method, op.path, v.url
                );
                let op = op.clone();
                api_operations.remove(&op);
            } else {
                unexpected_endpoints
                    .push_str(&format!("{:6} {:?}\n", method_string, v.url));
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
    for op in api_operations.keys() {
        uncovered_endpoints.push_str(&format!(
            "{:40} ({:6} {:?})\n",
            op.label,
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Operation {
    method: String,
    path: String,
    label: String,
}

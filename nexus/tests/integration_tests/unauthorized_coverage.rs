// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::endpoints::VERIFY_ENDPOINTS;
use expectorate::assert_contents;
use openapiv3::OpenAPI;
use std::collections::BTreeMap;

/// Checks for uncovered public API endpoints
///
/// This test compares the endpoints covered by the "unauthorized" test with
/// what's in the OpenAPI spec for the public API to make sure all endpoints are
/// accounted-for.
#[test]
fn test_unauthorized_coverage() {
    // Load the OpenAPI schema for Nexus's public API.
    let schema_path = "../openapi/nexus/nexus-latest.json";
    let schema_contents = std::fs::read_to_string(&schema_path)
        .expect("failed to read Nexus OpenAPI spec");
    let spec: OpenAPI = serde_json::from_str(&schema_contents)
        .expect("Nexus OpenAPI spec was not valid OpenAPI");

    // Take each operation that we find in the OpenAPI spec, make a regular
    // expression that we can use to match against it (more on this below), and
    // throw them into a BTreeMap.
    let mut spec_operations: BTreeMap<Operation, regex::Regex> = spec
        .operations()
        .map(|(path, method, op)| {
            // We're going to take URLs from our test cases and match them
            // against operations in the API spec.  The URLs from the API spec
            // contain variables (e.g., "/instances/{instance_name}").  Our test
            // cases have those variables filled in already (e.g.,
            // "/instances/my-instance").
            //
            // To match a URL from the test case against one from the OpenAPI
            // spec, we're going to:
            //
            // - use a regular expression to replace `{varname}` in the API
            //   spec's URL with `[^/]+` (one or more non-slash characters)
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
            let regex_path = re.replace_all(path, "/[^/]+");
            let regex = regex::Regex::new(&format!("^{}$", regex_path))
                .expect("modified URL string was not a valid regex");
            let label = op
                .operation_id
                .clone()
                .unwrap_or_else(|| String::from("unknown operation-id"));
            (Operation { method, path, label }, regex)
        })
        .collect();

    // Go through each of the authz test cases and match each one against an
    // OpenAPI operation.
    let mut unexpected_endpoints = String::from(
        "API endpoints tested by unauthorized.rs but not found \
        in the OpenAPI spec:\n",
    );
    for v in &*VERIFY_ENDPOINTS {
        for m in &v.allowed_methods {
            // Remove the method and path from the list of operations if there's
            // a VerifyEndpoint for it.
            let method_string = m.http_method().to_string().to_uppercase();
            let found = spec_operations.iter().find(|(op, regex)| {
                // Strip query parameters, if they exist.
                let url = v.url.split('?').next().unwrap();
                op.method.to_uppercase() == method_string && regex.is_match(url)
            });
            if let Some((op, _)) = found {
                println!(
                    "covered: {:40} ({:6?} {:?}) (by {:?})",
                    op.label, op.method, op.path, v.url
                );
                let op = op.clone();
                spec_operations.remove(&op);
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

    // Check for uncovered endpoints (endpoints that are in the OpenAPI spec but
    // not tested by the authz tests).
    let mut uncovered_endpoints =
        "API endpoints with no coverage in authz tests:\n".to_string();
    for op in spec_operations.keys() {
        uncovered_endpoints.push_str(&format!(
            "{:40} ({:6} {:?})\n",
            op.label, op.method, op.path
        ));
    }

    // If you're here because this assertion failed, check that if you've added
    // any API operations to Nexus, you've also added a corresponding test in
    // "unauthorized.rs" so that it will automatically be checked for its
    // behavior for unauthenticated and unauthorized users.  DO NOT SKIP THIS.
    // Even if you're just adding a stub, see [`Nexus::unimplemented_todo()`].
    // If you _added_ a test that covered an endpoint from the allowlist --
    // hooray!  Just delete the corresponding line from this file.  (Why is this
    // not `expectorate::assert_contents`?  Because we only expect this file to
    // ever shrink, which is easy enough to fix by hand, and we don't want to
    // make it easy to accidentally add things to the allowlist.)
    // let expected_uncovered_endpoints =
    //     std::fs::read_to_string("tests/output/uncovered-authz-endpoints.txt")
    //         .expect("failed to load file of allowed uncovered endpoints");

    // TODO: Update this to remove overwrite capabilities
    // See https://github.com/oxidecomputer/expectorate/pull/12
    assert_contents(
        "tests/output/uncovered-authz-endpoints.txt",
        uncovered_endpoints.as_str(),
    );
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Operation<'a> {
    method: &'a str,
    path: &'a str,
    label: String,
}

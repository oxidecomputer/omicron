// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared utilities for endpoint coverage tests.
//!
//! This module provides common logic for tests that verify coverage of API
//! endpoints, such as the unauthorized coverage test and audit log coverage
//! test.

use dropshot::ApiEndpointVersions;
use nexus_external_api::nexus_external_api_mod;
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet};

/// Returns true for "current" endpoints, false for backwards-compat shims.
fn is_current_endpoint(versions: &ApiEndpointVersions) -> bool {
    match versions {
        ApiEndpointVersions::All | ApiEndpointVersions::From(_) => true,
        ApiEndpointVersions::Until(_) | ApiEndpointVersions::FromUntil(_) => {
            false
        }
    }
}

/// An API operation (one HTTP method on one path).
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Operation {
    pub method: String,
    pub path: String,
    pub operation_id: String,
}

/// All API operations with precompiled regexes for URL matching.
pub struct ApiOperations {
    /// Operations keyed for ordered iteration, with regex for URL matching.
    operations: BTreeMap<Operation, Regex>,
}

impl ApiOperations {
    /// Create an ApiOperations containing all operations from the API description.
    ///
    /// Filters to only "current" endpoints (not backwards-compat shims) and
    /// deduplicates by (method, path) since multiple API versions can share
    /// the same path.
    pub fn new() -> Self {
        let api = nexus_external_api_mod::stub_api_description().unwrap();

        let mut seen_paths: BTreeSet<(String, String)> = BTreeSet::new();

        let operations = api
            .into_router()
            .endpoints(None)
            .filter(|(_, _, endpoint)| is_current_endpoint(&endpoint.versions))
            .filter(|(path, method, _)| {
                seen_paths.insert((method.to_string(), path.clone()))
            })
            .map(|(path, method, endpoint)| {
                let regex = path_to_regex(&path);
                (
                    Operation {
                        method: method.to_uppercase(),
                        path,
                        operation_id: endpoint.operation_id.clone(),
                    },
                    regex,
                )
            })
            .collect();

        Self { operations }
    }

    /// Find the operation matching a URL and HTTP method.
    ///
    /// The URL should have path parameters filled in (e.g., "/v1/instances/my-instance").
    /// Query parameters are stripped before matching.
    pub fn find(&self, method: &str, url: &str) -> Option<&Operation> {
        let url_path = url.split('?').next().unwrap();
        let method_upper = method.to_uppercase();

        self.operations
            .iter()
            .find(|(op, regex)| {
                op.method == method_upper && regex.is_match(url_path)
            })
            .map(|(op, _)| op)
    }

    /// Iterate over all operations.
    pub fn iter(&self) -> impl Iterator<Item = &Operation> {
        self.operations.keys()
    }

    /// Remove an operation from the set (for tracking coverage).
    pub fn remove(&mut self, op: &Operation) {
        self.operations.remove(op);
    }
}

/// Convert a path template to a regex for matching URLs.
///
/// E.g., "/v1/instances/{instance}" becomes "^/v1/instances/[^/]+$"
///
/// We're going to take URLs from our test cases and match them against
/// operations in the API description. The URLs from the API description contain
/// path parameters (e.g., "/instances/{instance_name}"). Our test cases have
/// those parameters filled in already (e.g., "/instances/my-instance").
///
/// To match a URL from a test case against a path template, we replace
/// `{varname}` with `[^/]+` (one or more non-slash characters) and use the
/// result as a regex. Lookups are linear in the number of endpoints — a little
/// cheesy, but expedient and robust enough for our purposes.
///
/// This will fail if the URL contains characters that would be interpreted
/// specially by the regex engine, so we check up front that those aren't
/// present.
fn path_to_regex(path: &str) -> Regex {
    // Verify path contains only safe characters for regex
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

    let re = Regex::new("/\\{[^}]+\\}").unwrap();
    let regex_path = re.replace_all(path, "/[^/]+");
    Regex::new(&format!("^{}$", regex_path))
        .expect("modified URL string was not a valid regex")
}

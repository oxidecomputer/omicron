// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_auth::authz;
use nexus_auth::authz::AuthorizedResource;
use slog::{debug, error, o, warn};
use std::collections::BTreeSet;

/// Helper for identifying authz resources not covered by the IAM role policy
/// test
pub struct Coverage {
    log: slog::Logger,
    /// names of all authz classes
    class_names: BTreeSet<String>,
    /// names of authz classes for which we expect to find no tests
    exempted: BTreeSet<String>,
    /// names of authz classes for which we have found a test
    covered: BTreeSet<String>,
}

impl Coverage {
    pub fn new(log: &slog::Logger, exempted: BTreeSet<String>) -> Coverage {
        let log = log.new(o!("component" => "IamTestCoverage"));
        let class_names = authz::Authz::new(&log).into_class_names();
        Coverage { log, class_names, exempted, covered: BTreeSet::new() }
    }

    /// Record that the Polar class associated with `covered` is covered by the
    /// test
    pub fn covered(&mut self, covered: &dyn AuthorizedResource) {
        self.covered_class(covered.polar_class())
    }

    /// Record that type `class` is covered by the test
    pub fn covered_class(&mut self, class: oso::Class) {
        let class_name = class.name;
        debug!(&self.log, "covering"; "class_name" => &class_name);
        self.covered.insert(class_name);
    }

    /// Checks coverage and panics if any non-exempt types were _not_ covered or
    /// if any exempt types _were_ covered
    pub fn verify(&self) {
        let mut uncovered = Vec::new();
        let mut bad_exemptions = Vec::new();

        for class_name in &self.class_names {
            let class_name = class_name.as_str();
            let exempted = self.exempted.contains(class_name);
            let covered = self.covered.contains(class_name);

            match (exempted, covered) {
                (true, false) => {
                    warn!(&self.log, "exempt"; "class_name" => class_name);
                }
                (false, true) => {
                    debug!(&self.log, "covered"; "class_name" => class_name);
                }
                (true, true) => {
                    error!(
                        &self.log,
                        "bad exemption (class was covered)";
                        "class_name" => class_name
                    );
                    bad_exemptions.push(class_name);
                }
                (false, false) => {
                    error!(
                        &self.log,
                        "uncovered class";
                        "class_name" => class_name
                    );
                    uncovered.push(class_name);
                }
            };
        }

        if !bad_exemptions.is_empty() {
            panic!(
                "these classes were covered by the tests and should \
                    not have been part of the exemption list: {}",
                bad_exemptions.join(", ")
            );
        }

        if !uncovered.is_empty() {
            // See exempted_authz_classes().
            panic!(
                "these classes were not covered by the IAM role \
                    policy test: {}",
                uncovered.join(", ")
            );
        }
    }
}

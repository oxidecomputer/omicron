// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use oso::PolarClass;
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
    pub fn new(log: &slog::Logger) -> Coverage {
        let log = log.new(o!("component" => "IamTestCoverage"));
        let authz = authz::Authz::new(&log);
        let class_names = authz.into_class_names();

        // Class names should be added to this exemption list when their Polar
        // code snippets and authz behavior is identical to another class.  This
        // is primarily for performance reasons because this test takes a long
        // time.  But with every exemption comes the risk of a security issue!
        //
        // PLEASE: instead of adding a class to this list, consider updating
        // this test to create an instance of the class and then test it.
        let exempted = [
            // Non-resources
            authz::Action::get_polar_class(),
            authz::actor::AnyActor::get_polar_class(),
            authz::actor::AuthenticatedActor::get_polar_class(),
            // XXX-dap TODO-coverage Not yet implemented, but not exempted for a
            // good reason.
            authz::IpPoolList::get_polar_class(),
            authz::GlobalImageList::get_polar_class(),
            authz::ConsoleSessionList::get_polar_class(),
            authz::DeviceAuthRequestList::get_polar_class(),
            authz::IpPool::get_polar_class(),
            authz::NetworkInterface::get_polar_class(),
            authz::VpcRouter::get_polar_class(),
            authz::RouterRoute::get_polar_class(),
            authz::ConsoleSession::get_polar_class(),
            authz::DeviceAuthRequest::get_polar_class(),
            authz::DeviceAccessToken::get_polar_class(),
            authz::Rack::get_polar_class(),
            authz::RoleBuiltin::get_polar_class(),
            authz::SshKey::get_polar_class(),
            authz::SiloUser::get_polar_class(),
            authz::SiloGroup::get_polar_class(),
            authz::IdentityProvider::get_polar_class(),
            authz::SamlIdentityProvider::get_polar_class(),
            authz::Sled::get_polar_class(),
            authz::UpdateAvailableArtifact::get_polar_class(),
            authz::UserBuiltin::get_polar_class(),
            authz::GlobalImage::get_polar_class(),
        ]
        .into_iter()
        .map(|c| c.name.clone())
        .collect();

        Coverage { log, class_names, exempted, covered: BTreeSet::new() }
    }

    pub fn covered<T: oso::PolarClass>(&mut self, _covered: &T) {
        self.covered_class(T::get_polar_class())
    }

    pub fn covered_class(&mut self, class: oso::Class) {
        let class_name = class.name.clone();
        debug!(&self.log, "covering"; "class_name" => &class_name);
        self.covered.insert(class_name);
    }

    pub fn verify(&self) {
        let mut uncovered = Vec::new();
        let mut bad_exemptions = Vec::new();

        for class_name in &self.class_names {
            let class_name = class_name.as_str();
            let exempted = self.exempted.contains(class_name);
            let covered = self.covered.contains(class_name);

            match (exempted, covered) {
                (true, false) => {
                    // XXX-dap consider checking whether the Polar snippet
                    // exactly matches that of another class?
                    debug!(&self.log, "exempt"; "class_name" => class_name);
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
            panic!(
                "these classes were not covered by the IAM role \
                    policy test: {}",
                uncovered.join(", ")
            );
        }
    }
}

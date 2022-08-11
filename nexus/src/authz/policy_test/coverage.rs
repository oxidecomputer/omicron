// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::authz::AuthorizedResource;
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

        // Exemption list for this coverage test
        //
        // There are two possible reasons for a resource to appear on this list:
        //
        // (1) because its behavior is identical to that of some other resource
        //     that we are testing (i.e., same Polar snippet and identical
        //     configuration for the authz type).  There aren't any examples of
        //     this today, but it might be reasonable to do this for resources
        //     that are indistinguishable to the authz subsystem (e.g., Disks,
        //     Instances, Vpcs, and other things nested directly below Project)
        //
        // (2) because we have not yet gotten around to adding the type to this
        //     test.  We don't want to expand this list if we can avoid it!
        let exempted = [
            // Non-resources:
            authz::Action::get_polar_class(),
            authz::actor::AnyActor::get_polar_class(),
            authz::actor::AuthenticatedActor::get_polar_class(),
            // Resources whose behavior should be identical to an existing type
            // and we don't want to do the test twice for performance reasons:
            // none yet.
            //
            // TODO-coverage Resources that we should test, but for which we
            // have not yet added a test.  PLEASE: instead of adding something
            // to this list, modify `make_resources()` to test it instead.  This
            // should be pretty straightforward in most cases.  Adding a new
            // class to this list makes it harder to catch security flaws!
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

    pub fn covered(&mut self, covered: &dyn AuthorizedResource) {
        self.covered_class(covered.polar_class())
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
                    // TODO-coverage It would be nice if we could verify that
                    // the Polar snippet and authz_resource! configuration were
                    // identical to that of an existing class.  Then it would be
                    // safer to exclude types that are truly duplicative of
                    // some other type.
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
            panic!(
                "these classes were not covered by the IAM role \
                    policy test: {}",
                uncovered.join(", ")
            );
        }
    }
}

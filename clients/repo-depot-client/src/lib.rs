// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for Sled Agent's Repo Depot to make API requests.

progenitor::generate_api!(
    // We are rolling out v2 over multiple releases.
    // spec = "../../openapi/repo-depot/repo-depot-2.0.0-68b85f.json",
    spec = {
        path = "git-stub-vcs/openapi/repo-depot/repo-depot-1.0.0-65083f.json",
        relative_to = OutDir,
    },
    interface = Positional,
    inner_type = slog::Logger,
    hooks = Expected,
    derives = [schemars::JsonSchema],
);

progenitor_extras::slog_hooks::impl_slog_client_hooks!(Client);

/// A type alias for errors returned by this crate.
pub type ClientError = crate::Error<crate::types::Error>;

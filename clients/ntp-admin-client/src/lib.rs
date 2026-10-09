// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to an Omicron NTP admin server

progenitor::generate_api!(
    spec = "../../openapi/ntp-admin/ntp-admin-latest.json",
    interface = Positional,
    inner_type = slog::Logger,
    hooks = Expected,
    derives = [schemars::JsonSchema],
);

progenitor_extras::slog_hooks::impl_slog_client_hooks!(Client);

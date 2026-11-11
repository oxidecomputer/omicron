// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to a clickhouse-admin-server server
//! running in an omicron zone.

progenitor::generate_api!(
    spec = "../../openapi/clickhouse-admin-server/clickhouse-admin-server-latest.json",
    interface = Positional,
    inner_type = slog::Logger,
    hooks = Expected,
    crates = {
        "iddqd" = "*",
        "omicron-uuid-kinds" = "*",
    },
    derives = [schemars::JsonSchema],
    replace = {
        DatabaseRetentionPolicy = clickhouse_admin_types_versions::latest::retention::DatabaseRetentionPolicy,
        DatabaseUsage = clickhouse_admin_types_versions::latest::usage::DatabaseUsage,
        Days = clickhouse_admin_types_versions::latest::retention::Days,
        RetentionPolicy = clickhouse_admin_types_versions::latest::retention::RetentionPolicy,
        ServerConfigurableSettings = clickhouse_admin_types_versions::latest::server::ServerConfigurableSettings,
        TableUsage = clickhouse_admin_types_versions::latest::usage::TableUsage,
    }
);

progenitor_extras::slog_hooks::impl_slog_client_hooks!(Client);

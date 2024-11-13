// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for clickhouse-admin

use camino::Utf8PathBuf;
use clickward::{BasePorts, Deployment, DeploymentConfig};
use dropshot::test_util::{log_prefix_for_test, LogContext};
use dropshot::{ConfigLogging, ConfigLoggingLevel};

pub const DEFAULT_CLICKHOUSE_ADMIN_BASE_PORTS: BasePorts = BasePorts {
    keeper: 29000,
    raft: 29100,
    clickhouse_tcp: 29200,
    clickhouse_http: 29300,
    clickhouse_interserver_http: 29400,
};

pub fn default_clickhouse_cluster_test_deployment(
    path: Utf8PathBuf,
) -> Deployment {
    let base_ports = DEFAULT_CLICKHOUSE_ADMIN_BASE_PORTS;

    let config = DeploymentConfig {
        path,
        base_ports,
        cluster_name: "oximeter_cluster".to_string(),
    };

    Deployment::new(config)
}

pub fn default_clickhouse_log_ctx_and_path() -> (LogContext, Utf8PathBuf) {
    let logctx = LogContext::new(
        "clickhouse_cluster",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    let (parent_dir, _prefix) = log_prefix_for_test("clickhouse_cluster");
    let path = parent_dir.join("clickward_test");

    (logctx, path)
}

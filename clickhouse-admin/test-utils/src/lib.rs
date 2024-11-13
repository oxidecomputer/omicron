// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for clickhouse-admin

use clickward::{BasePorts, Deployment};

pub const DEFAULT_CLICKHOUSE_ADMIN_BASE_PORTS: BasePorts = BasePorts {
    keeper: 29000,
    raft: 29100,
    clickhouse_tcp: 29200,
    clickhouse_http: 29300,
    clickhouse_interserver_http: 29400,
};

//pub fn default_clickhouse_cluster_test_deployment() -> Deployment {
//
//}

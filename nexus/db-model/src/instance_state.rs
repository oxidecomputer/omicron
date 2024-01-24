// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_wrapper;
use omicron_common::api::external;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::io::Write;

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "instance_state", schema = "public"))]
    pub struct InstanceStateEnum;

    #[derive(Clone, Debug, PartialEq, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = InstanceStateEnum)]
    pub struct InstanceState(pub external::InstanceState);

    // Enum values
    Creating => b"creating"
    Starting => b"starting"
    Running => b"running"
    Stopping => b"stopping"
    Stopped => b"stopped"
    Rebooting => b"rebooting"
    Migrating => b"migrating"
    Repairing => b"repairing"
    Failed => b"failed"
    Destroyed => b"destroyed"
);

impl InstanceState {
    pub fn new(state: external::InstanceState) -> Self {
        Self(state)
    }

    pub fn state(&self) -> &external::InstanceState {
        &self.0
    }
}

impl fmt::Display for InstanceState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<InstanceState> for sled_agent_client::types::InstanceState {
    fn from(s: InstanceState) -> Self {
        use external::InstanceState::*;
        use sled_agent_client::types::InstanceState as Output;
        match s.0 {
            Creating => Output::Creating,
            Starting => Output::Starting,
            Running => Output::Running,
            Stopping => Output::Stopping,
            Stopped => Output::Stopped,
            Rebooting => Output::Rebooting,
            Migrating => Output::Migrating,
            Repairing => Output::Repairing,
            Failed => Output::Failed,
            Destroyed => Output::Destroyed,
        }
    }
}

impl From<external::InstanceState> for InstanceState {
    fn from(state: external::InstanceState) -> Self {
        Self::new(state)
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! View status of background tasks (for support and debugging)

use crate::Nexus;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::internal_api::views::BackgroundTask;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use std::collections::BTreeMap;

impl Nexus {
    pub(crate) async fn bgtasks_list(
        &self,
        opctx: &OpContext,
    ) -> Result<BTreeMap<String, BackgroundTask>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let driver = &self.background_tasks.driver;
        Ok(driver
            .tasks()
            .map(|t| {
                let name = t.name();
                let description = driver.task_description(t);
                let period = driver.task_period(t);
                let status = driver.task_status(t);
                (
                    name.to_owned(),
                    BackgroundTask::new(name, description, period, status),
                )
            })
            .collect())
    }

    pub(crate) async fn bgtask_status(
        &self,
        opctx: &OpContext,
        name: &str,
    ) -> LookupResult<BackgroundTask> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let driver = &self.background_tasks.driver;
        let task =
            driver.tasks().find(|t| t.name() == name).ok_or_else(|| {
                LookupType::ByName(name.to_owned())
                    .into_not_found(ResourceType::BackgroundTask)
            })?;
        let description = driver.task_description(task);
        let status = driver.task_status(task);
        let period = driver.task_period(task);
        Ok(BackgroundTask::new(task.name(), description, period, status))
    }
}

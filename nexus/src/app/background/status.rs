// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! View status of background tasks (for support and debugging)

use super::Driver;
use crate::Nexus;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::internal_api::views::BackgroundTask;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::watch;

impl Nexus {
    pub(crate) async fn bgtasks_list(
        &self,
        opctx: &OpContext,
    ) -> Result<BTreeMap<String, BackgroundTask>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let driver = self.driver()?;
        Ok(driver
            .tasks()
            .map(|t| {
                let name = t.as_str();
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
        let driver = self.driver()?;
        let task =
            driver.tasks().find(|t| t.as_str() == name).ok_or_else(|| {
                LookupType::ByName(name.to_owned())
                    .into_not_found(ResourceType::BackgroundTask)
            })?;
        let description = driver.task_description(task);
        let status = driver.task_status(task);
        let period = driver.task_period(task);
        Ok(BackgroundTask::new(task.as_str(), description, period, status))
    }

    pub(crate) async fn bgtask_activate(
        &self,
        opctx: &OpContext,
        mut names: BTreeSet<String>,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let driver = self.driver()?;

        // Ensure all task names are valid by removing them from the set of
        // names as we find them.
        let tasks_to_activate: Vec<_> =
            driver.tasks().filter(|t| names.remove(t.as_str())).collect();

        // If any names weren't recognized, return an error.
        if !names.is_empty() {
            let mut names_str = "background tasks: ".to_owned();
            for (i, name) in names.iter().enumerate() {
                names_str.push_str(name);
                if i < names.len() - 1 {
                    names_str.push_str(", ");
                }
            }

            return Err(LookupType::ByOther(names_str)
                .into_not_found(ResourceType::BackgroundTask));
        }

        for task in tasks_to_activate {
            driver.activate(task);
        }

        Ok(())
    }

    pub(crate) fn inventory_load_rx(
        &self,
    ) -> watch::Receiver<Option<Arc<Collection>>> {
        self.background_tasks_internal.inventory_load_rx()
    }

    fn driver(&self) -> Result<&Driver, Error> {
        self.background_tasks_driver.get().ok_or_else(|| {
            Error::unavail("background tasks not yet initialized")
        })
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generic deployment execution types and implementation
//!
//! See crate-level docs for details.

use crate::planner::Plan;
use futures::future::BoxFuture;
use nexus_types::inventory::Collection;
use omicron_common::backoff::BackoffError;
use std::fmt::Debug;

/// Pluggable execution implementation
///
/// Like the planner, this is a trait so that we can provide various impls that
/// cover various parts of the system.  The thing that knows how to provision
/// new zones doesn't also need to know how to update SP software.
pub trait Executor {
    type Action: Action;

    fn plan_actions(
        &self,
        latest_collection: Option<&Collection>,
        plan: &Plan,
        actions: &mut ActionBuilder,
    ) -> anyhow::Result<()>;
}

/// Action carried out by an executor
pub trait Action: Debug + slog::Value {
    fn execute(&self) -> BoxFuture<'_, BackoffError<anyhow::Error>>;
}

/// Glorified list of actions
pub struct ActionBuilder {
    actions: Vec<Box<dyn Action>>,
}

impl ActionBuilder {
    pub fn new() -> ActionBuilder {
        ActionBuilder { actions: Vec::new() }
    }

    pub fn action(&mut self, action: Box<dyn Action>) {
        self.actions.push(action);
    }
}

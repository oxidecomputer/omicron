// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle collection step execution framework

use crate::app::background::tasks::support_bundle::collection::BundleCollection;

use camino::Utf8Path;
use chrono::DateTime;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_types::internal_api::background::SupportBundleCollectionReport;
use nexus_types::internal_api::background::SupportBundleCollectionStep;
use nexus_types::internal_api::background::SupportBundleCollectionStepStatus;
use nexus_types::internal_api::background::SupportBundleEreportStatus;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;

// This type describes a single step in the Support Bundle collection.
//
// - All steps have access to the "BundleCollection", which includes
// tools for actually acquiring data.
// - All steps have access to an output directory where they can store
// serialized data to a file.
// - Finally, all steps can emit a "CollectionStepOutput", which can either
// update the collection report, or generate more steps.
pub type CollectionStepFn = Box<
    dyn for<'b> FnOnce(
            &'b Arc<BundleCollection>,
            &'b Utf8Path,
        )
            -> BoxFuture<'b, anyhow::Result<CollectionStepOutput>>
        + Send,
>;

pub struct CollectionStep {
    pub name: String,
    pub step_fn: CollectionStepFn,
}

impl CollectionStep {
    pub fn new(name: impl Into<String>, step_fn: CollectionStepFn) -> Self {
        Self { name: name.into(), step_fn }
    }

    pub async fn run(
        self,
        collection: &Arc<BundleCollection>,
        output: &Utf8Path,
        log: &slog::Logger,
    ) -> CompletedCollectionStep {
        let start = Utc::now();

        let output = (self.step_fn)(collection, output)
            .await
            .inspect_err(|err| {
                warn!(
                    log,
                    "Step failed";
                    "step" => &self.name,
                    InlineErrorChain::new(err.as_ref()),
                );
            })
            .unwrap_or_else(|err| CollectionStepOutput::Failed(err));

        let end = Utc::now();

        CompletedCollectionStep { name: self.name, start, end, output }
    }
}

pub struct CompletedCollectionStep {
    pub name: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub output: CollectionStepOutput,
}

impl CompletedCollectionStep {
    // Updates the collection report based on the output of a collection step,
    // and possibly extends the set of all steps to be executed.
    pub fn process(
        self,
        report: &mut SupportBundleCollectionReport,
        steps: &mut Vec<CollectionStep>,
    ) {
        use SupportBundleCollectionStepStatus as Status;

        let status = match self.output {
            CollectionStepOutput::Skipped => Status::Skipped,
            CollectionStepOutput::Failed(err) => {
                Status::Failed(err.to_string())
            }
            CollectionStepOutput::Ereports(status) => {
                report.ereports = Some(status);
                Status::Ok
            }
            CollectionStepOutput::Spawn { extra_steps } => {
                steps.extend(extra_steps);
                Status::Ok
            }
            CollectionStepOutput::None => Status::Ok,
        };

        // Add information about this completed step the bundle report.
        let step = SupportBundleCollectionStep {
            name: self.name,
            start: self.start,
            end: self.end,
            status,
        };
        report.steps.push(step);
    }
}

pub enum CollectionStepOutput {
    // The step was not executed intentionally
    Skipped,
    // The step encountered a fatal error and could not complete.
    //
    // It may have still saved a partial set of data to the bundle.
    Failed(anyhow::Error),
    Ereports(SupportBundleEreportStatus),
    // The step spawned additional steps to execute
    Spawn { extra_steps: Vec<CollectionStep> },
    // The step completed with nothing to report, and no follow-up steps
    None,
}

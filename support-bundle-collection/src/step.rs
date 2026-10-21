// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle collection step execution framework

use crate::collection::BundleCollection;
use crate::output::BundleOutput;
use crate::output::WriteRefused;

use chrono::DateTime;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_types::internal_api::background::SupportBundleCollectionReport;
use nexus_types::internal_api::background::SupportBundleCollectionStep;
use nexus_types::internal_api::background::SupportBundleCollectionStepStatus;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::fmt::Display;
use std::sync::Arc;

/// The error returned by a collection step, and by writes to the bundle.
///
/// The size limit is kept apart from other failures, so a step that hits it
/// is reported as cut short, not failed.
///
/// Deliberately not `std::error::Error`: otherwise `?` or anyhow's `Context`
/// could turn it into an `anyhow::Error`, losing the size limit variant. Use
/// [`StepResultExt`] to add context.
#[derive(Debug)]
pub enum StepError {
    /// A write was refused because the bundle reached its size limit.
    WriteRefused(WriteRefused),
    /// Any other failure.
    Other(anyhow::Error),
}

impl StepError {
    /// Returns this error as a `std::error::Error`, for logging.
    pub fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        match self {
            StepError::WriteRefused(err) => err,
            StepError::Other(err) => err.as_ref(),
        }
    }
}

impl From<WriteRefused> for StepError {
    fn from(err: WriteRefused) -> Self {
        StepError::WriteRefused(err)
    }
}

impl From<anyhow::Error> for StepError {
    fn from(err: anyhow::Error) -> Self {
        StepError::Other(err)
    }
}

/// Like anyhow's `Context`, but only adds context to [`StepError::Other`],
/// keeping the size limit variant as-is.
pub trait StepResultExt<T> {
    fn context<C>(self, context: C) -> Result<T, StepError>
    where
        C: Display + Send + Sync + 'static;

    fn with_context<C, F>(self, f: F) -> Result<T, StepError>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C;
}

impl<T> StepResultExt<T> for Result<T, StepError> {
    fn context<C>(self, context: C) -> Result<T, StepError>
    where
        C: Display + Send + Sync + 'static,
    {
        self.with_context(|| context)
    }

    fn with_context<C, F>(self, f: F) -> Result<T, StepError>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        self.map_err(|err| match err {
            StepError::Other(err) => StepError::Other(err.context(f())),
            err @ StepError::WriteRefused(_) => err,
        })
    }
}

// This type describes a single step in the Support Bundle collection.
//
// - All steps have access to the "BundleCollection", which includes
// tools for actually acquiring data.
// - All steps have access to the bundle's output directory, where they can
// store serialized data to files. Everything written there is charged against
// the bundle's size limit.
// - Finally, all steps can emit a "CollectionStepOutput", which can either
// update the collection report, or generate more steps.
pub type CollectionStepFn = Box<
    dyn for<'b> FnOnce(
            &'b Arc<BundleCollection>,
            &'b BundleOutput,
        )
            -> BoxFuture<'b, Result<CollectionStepOutput, StepError>>
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
        output: &BundleOutput,
        log: &slog::Logger,
    ) -> CompletedCollectionStep {
        let start = Utc::now();

        let output = match (self.step_fn)(collection, output).await {
            Ok(output) => output,
            // Not a step failure; the scheduler reports the size limit.
            Err(StepError::WriteRefused(_)) => CollectionStepOutput::None,
            Err(StepError::Other(err)) => {
                warn!(
                    log,
                    "Step failed";
                    "step" => &self.name,
                    InlineErrorChain::new(err.as_ref()),
                );
                CollectionStepOutput::Failed(err)
            }
        };

        let end = Utc::now();

        // A step still running once the size limit was exceeded may have been
        // stopped before collecting everything it would have otherwise.
        let stopped_by_size_limit = collection.size_budget().is_exceeded()
            && !matches!(output, CollectionStepOutput::Skipped);

        CompletedCollectionStep {
            name: self.name,
            start,
            end,
            output,
            stopped_by_size_limit,
        }
    }
}

pub struct CompletedCollectionStep {
    pub name: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub output: CollectionStepOutput,
    pub stopped_by_size_limit: bool,
}

impl CompletedCollectionStep {
    // Updates the collection report based on the output of a collection step,
    // and possibly extends the set of all steps to be executed.
    //
    // If the step was stopped by the bundle's size limit, its name is added to
    // `incomplete_steps`.
    pub fn process(
        self,
        report: &mut SupportBundleCollectionReport,
        steps: &mut Vec<CollectionStep>,
        incomplete_steps: &mut Vec<String>,
    ) {
        use SupportBundleCollectionStepStatus as Status;

        if self.stopped_by_size_limit {
            incomplete_steps.push(self.name.clone());
        }

        let mut details = None;
        let status = match self.output {
            CollectionStepOutput::Skipped => Status::Skipped,
            CollectionStepOutput::Failed(err) => {
                Status::Failed(err.to_string())
            }
            CollectionStepOutput::Details(value) => {
                details = Some(value);
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
            details,
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
    // The step completed successfully and wants to surface a structured
    // summary in its report entry (counts, partial-success info, errors,
    // etc.). Use any `Serialize` type that fits the step.
    Details(serde_json::Value),
    // The step spawned additional steps to execute
    Spawn { extra_steps: Vec<CollectionStep> },
    // The step completed with nothing to report, and no follow-up steps
    None,
}

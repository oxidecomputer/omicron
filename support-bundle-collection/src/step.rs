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
use std::collections::BTreeSet;
use std::fmt::Display;
use std::sync::Arc;

/// The error returned by a collection step, and by writes to the bundle.
///
/// Stopping early is kept apart from other failures, so a step that stops
/// early is reported as stopped, not failed.
///
/// Deliberately not `std::error::Error`: otherwise `?` or anyhow's `Context`
/// could turn it into an `anyhow::Error`, losing the other variants. Use
/// [`StepResultExt`] to add context.
#[derive(Debug)]
pub enum StepError {
    /// A write was refused because the bundle reached its size limit.
    WriteRefused(WriteRefused),
    /// The step stopped early, because the bundle reached its size limit
    /// or was cancelled.
    Stopped,
    /// Any other failure.
    Other(anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("collection was stopped")]
struct CollectionStopped;

impl StepError {
    /// Returns this error as a `std::error::Error`, for logging.
    pub fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        match self {
            StepError::WriteRefused(err) => err,
            StepError::Stopped => &CollectionStopped,
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
/// keeping the other variants as-is.
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
            err @ (StepError::WriteRefused(_) | StepError::Stopped) => err,
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
            Err(StepError::WriteRefused(_) | StepError::Stopped) => {
                CollectionStepOutput::Stopped
            }
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
    //
    // If the step stopped before finishing, its name is added to
    // `incomplete_steps`.
    pub fn process(
        self,
        report: &mut SupportBundleCollectionReport,
        steps: &mut Vec<CollectionStep>,
        incomplete_steps: &mut BTreeSet<String>,
    ) {
        use SupportBundleCollectionStepStatus as Status;

        let mut details = None;
        let status = match self.output {
            CollectionStepOutput::Skipped => Status::Skipped,
            CollectionStepOutput::Stopped => {
                incomplete_steps.insert(self.name.clone());
                Status::Stopped
            }
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
    // The step stopped before finishing, because collection was stopped.
    //
    // It may have still saved a partial set of data to the bundle.
    Stopped,
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

#[cfg(test)]
mod test {
    use super::*;
    use omicron_uuid_kinds::SupportBundleUuid;

    fn completed(
        name: &str,
        output: CollectionStepOutput,
    ) -> CompletedCollectionStep {
        let now = Utc::now();
        CompletedCollectionStep {
            name: name.to_string(),
            start: now,
            end: now,
            output,
        }
    }

    // Only steps which stopped are listed as incomplete, whatever else
    // happened to the bundle.
    #[test]
    fn test_process_lists_only_stopped_steps() {
        use SupportBundleCollectionStepStatus as Status;

        let mut report =
            SupportBundleCollectionReport::new(SupportBundleUuid::new_v4());
        let mut steps = vec![];
        let mut incomplete_steps = BTreeSet::new();
        for step in [
            completed("stopped", CollectionStepOutput::Stopped),
            completed(
                "failed",
                CollectionStepOutput::Failed(anyhow::anyhow!("MGS is down")),
            ),
            completed("finished", CollectionStepOutput::None),
            completed(
                "details",
                CollectionStepOutput::Details(serde_json::json!({})),
            ),
            completed("skipped", CollectionStepOutput::Skipped),
        ] {
            step.process(&mut report, &mut steps, &mut incomplete_steps);
        }

        assert_eq!(incomplete_steps, BTreeSet::from(["stopped".to_string()]));
        let statuses: Vec<_> = report
            .steps
            .iter()
            .map(|step| (step.name.as_str(), step.status.clone()))
            .collect();
        assert_eq!(
            statuses,
            [
                ("stopped", Status::Stopped),
                ("failed", Status::Failed("MGS is down".to_string())),
                ("finished", Status::Ok),
                ("details", Status::Ok),
                ("skipped", Status::Skipped),
            ]
        );
    }
}

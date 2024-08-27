// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fmt;

use crate::{EventBuffer, ExecutionStatus, StepSpec, TerminalKind};

/// Display the root execution status of an event buffer.
#[derive(Debug)]
pub struct RootExecutionDisplay<'a, S: StepSpec> {
    buffer: &'a EventBuffer<S>,
    // TODO: implement color
}

impl<'a, S: StepSpec> fmt::Display for RootExecutionDisplay<'a, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(summary) = self.buffer.root_execution_summary() else {
            return write!(f, "(event buffer is empty)");
        };
        let steps_width = summary.total_steps.to_string().len();

        match summary.execution_status {
            ExecutionStatus::NotStarted => write!(f, "not started"),
            ExecutionStatus::Running { step_key, .. } => {
                let step_data = self.buffer.get(&step_key).unwrap();
                write!(
                    f,
                    "running: {} (step {:>steps_width$}/{})",
                    step_data.step_info().description,
                    step_key.index + 1,
                    summary.total_steps
                )
            }
            ExecutionStatus::Terminal(info) => {
                let step_data =
                    self.buffer.get(&info.step_key).expect("step exists");
                match info.kind {
                    TerminalKind::Completed => write!(f, "completed")?,
                    TerminalKind::Failed => {
                        write!(
                            f,
                            "failed at: {} (step {:>steps_width$}/{})",
                            step_data.step_info().description,
                            info.step_key.index + 1,
                            summary.total_steps
                        )?;
                    }
                    TerminalKind::Aborted => write!(
                        f,
                        "aborted at: {} (step {:>steps_width$}/{})",
                        step_data.step_info().description,
                        info.step_key.index + 1,
                        summary.total_steps
                    )?,
                }

                Ok(())
            }
        }
    }
}

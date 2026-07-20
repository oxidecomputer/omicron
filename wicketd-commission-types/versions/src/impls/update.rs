// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::update::{UpdateProgress, UpdateStep, UpdateStepStatus};

impl UpdateProgress {
    pub fn iter_steps(&self) -> impl Iterator<Item = &UpdateStep> + '_ {
        self.steps.iter().flat_map(UpdateStep::iter)
    }

    pub fn innermost_running_steps(
        &self,
    ) -> impl Iterator<Item = &UpdateStep> + '_ {
        self.iter_steps().filter(|step| {
            step.is_running()
                && !step
                    .children
                    .iter()
                    .flat_map(|progress| progress.steps.iter())
                    .any(UpdateStep::is_running)
        })
    }
}

impl UpdateStep {
    pub fn is_running(&self) -> bool {
        matches!(self.status, UpdateStepStatus::Running { .. })
    }

    pub fn iter(&self) -> impl Iterator<Item = &UpdateStep> + '_ {
        let mut stack = vec![self];
        std::iter::from_fn(move || {
            let step = stack.pop()?;
            for progress in step.children.iter().rev() {
                stack.extend(progress.steps.iter().rev());
            }
            Some(step)
        })
    }
}

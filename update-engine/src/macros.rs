// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

/// Defines type aliases for a particular specification of the update engine.
///
/// This macro defines a number of type aliases. For example:
///
/// ```ignore
/// update_engine::define_update_engine!(pub(crate) MySpec);
/// ```
///
/// defines a number of type aliases, each of which are of the form:
///
/// ```ignore
/// pub(crate) type UpdateEngine<'a, S = MySpec> =
///     ::update_engine::UpdateEngine<'a, S>;
/// pub(crate) type Event<S = MySpec> = ::update_engine::events::Event<S>;
/// // ... and so on.
/// ```
///
/// These aliases make it easy to use a type without having to repeat the name
/// of the specification over and over, while still providing a type parameter
/// as an escape hatch if required.
#[macro_export]
macro_rules! define_update_engine {
    ($v:vis $spec_type:ty) => {
        $v type UpdateEngine<'a, S = $spec_type> =
            ::update_engine::UpdateEngine<'a, S>;
        $v type ComponentRegistrar<'engine, 'a, S = $spec_type> =
            ::update_engine::ComponentRegistrar<'engine, 'a, S>;
        $v type Event<S = $spec_type> = ::update_engine::events::Event<S>;
        $v type StepEvent<S = $spec_type> =
            ::update_engine::events::StepEvent<S>;
        $v type StepEventKind<S = $spec_type> =
            ::update_engine::events::StepEventKind<S>;
        $v type ProgressEvent<S = $spec_type> =
            ::update_engine::events::ProgressEvent<S>;
        $v type ProgressEventKind<S = $spec_type> =
            ::update_engine::events::ProgressEventKind<S>;
        $v type StepInfo<S = $spec_type> =
            ::update_engine::events::StepInfo<S>;
        $v type StepComponentSummary<S = $spec_type> =
            ::update_engine::events::StepComponentSummary<S>;
        $v type StepInfoWithMetadata<S = $spec_type> =
            ::update_engine::events::StepInfoWithMetadata<S>;
        $v type StepContext<S = $spec_type> =
            ::update_engine::StepContext<S>;
        $v type StepProgress<S = $spec_type> =
            ::update_engine::events::StepProgress<S>;
        $v type StepResult<T, S = $spec_type> =
            ::update_engine::StepResult<T, S>;
        $v type StepOutcome<S = $spec_type> =
            ::update_engine::events::StepOutcome<S>;
        $v type StepSuccess<T, S = $spec_type> =
            ::update_engine::StepSuccess<T, S>;
        $v type StepWarning<T, S = $spec_type> =
            ::update_engine::StepWarning<T, S>;
        $v type StepSkipped<T, S = $spec_type> =
            ::update_engine::StepSkipped<T, S>;
        $v type EventBuffer<S = $spec_type> =
            ::update_engine::EventBuffer<S>;
        $v type StepStatus<S = $spec_type> =
            ::update_engine::StepStatus<S>;
        $v type EventReport<S = $spec_type> =
            ::update_engine::events::EventReport<S>;
        $v type StepHandle<T, S = $spec_type> =
            ::update_engine::StepHandle<T, S>;
        $v type SharedStepHandle<T, S = $spec_type> =
            ::update_engine::SharedStepHandle<T, S>;
    };
}

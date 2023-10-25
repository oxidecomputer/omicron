// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::collections::BTreeMap;

use crate::{EventBuffer, ExecutionTerminalStatus, StepSpec};

use super::{LineDisplay, LineDisplayStyles};

/// A displayer that simultaneously shows line displays for several event buffers.
#[derive(Debug)]
pub struct GroupLineDisplay<K, W, S: StepSpec> {
    states: BTreeMap<K, LineDisplayState<W, S>>,
    // Styles for new line displays.
    styles: LineDisplayStyles,
}

impl<K: Eq + Ord, W: std::io::Write, S: StepSpec> GroupLineDisplay<K, W, S> {
    /// Creates a new `GroupLineDisplay` with the provided states.
    ///
    /// The function passed in is expected to create a writer.
    pub fn new(
        inputs: impl IntoIterator<Item = K>,
        styles: LineDisplayStyles,
    ) -> Self {
        Self { states, styles }
    }
}

#[derive(Debug)]
struct LineDisplayState<W, S: StepSpec> {
    kind: LineDisplayKind<S>,
    line_display: LineDisplay<W>,
}

#[derive(Debug)]
enum LineDisplayKind<S: StepSpec> {
    NotStarted { displayed: bool },
    Running { event_buffer: EventBuffer<S> },
    Terminal { status: ExecutionTerminalStatus },
    Overwritten { displayed: bool },
}

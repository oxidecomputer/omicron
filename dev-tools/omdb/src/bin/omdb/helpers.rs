// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility helpers for the omdb CLI.

use anyhow::bail;
use clap::ColorChoice;
use reedline::DefaultPrompt;
use reedline::DefaultPromptSegment;
use reedline::Reedline;
use supports_color::Stream;

pub(crate) const CONNECTION_OPTIONS_HEADING: &str = "Connection Options";
pub(crate) const DATABASE_OPTIONS_HEADING: &str = "Database Options";
pub(crate) const SAFETY_OPTIONS_HEADING: &str = "Safety Options";

pub(crate) fn should_colorize(color: ColorChoice, stream: Stream) -> bool {
    match color {
        ColorChoice::Always => true,
        ColorChoice::Auto => supports_color::on(stream).is_some(),
        ColorChoice::Never => false,
    }
}

pub(crate) const fn const_max_len(strs: &[&str]) -> usize {
    let mut max = 0;
    let mut i = 0;
    while i < strs.len() {
        let len = strs[i].len();
        if len > max {
            max = len;
        }
        i += 1;
    }
    max
}

pub(crate) struct ConfirmationPrompt(Reedline);

impl ConfirmationPrompt {
    pub(crate) fn new() -> Self {
        Self(Reedline::create())
    }

    fn read(&mut self, message: &str) -> Result<String, anyhow::Error> {
        let prompt = DefaultPrompt::new(
            DefaultPromptSegment::Basic(message.to_string()),
            DefaultPromptSegment::Empty,
        );
        if let Ok(reedline::Signal::Success(input)) = self.0.read_line(&prompt)
        {
            Ok(input)
        } else {
            bail!("operation aborted")
        }
    }

    pub(crate) fn read_and_validate(
        &mut self,
        message: &str,
        expected: &str,
    ) -> Result<(), anyhow::Error> {
        let input = self.read(message)?;
        if input != expected {
            bail!("Aborting, input did not match expected value");
        }
        Ok(())
    }
}

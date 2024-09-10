// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility helpers for the omdb CLI.

use clap::ColorChoice;
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

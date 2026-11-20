// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault Management simulator command line library.
//!
//! This crate builds on top of [`nexus_fm_simulation`] to provide a command
//! line interface for running fault management in a simulated environment,
//! including session history. It's designed to be used both as a library in
//! scripted integration tests and interactively via the `fm-sim` binary.

mod command;
mod error;
mod simulator;
#[cfg(test)]
pub(crate) mod test_utils;

pub use error::Error;
pub use simulator::Simulator;

use clap::Parser;

use command::SimCommand;

/// fm-sim: Fault Management Simulator
#[derive(Debug, PartialEq, Parser)]
#[command(multicall = true)]
pub(crate) enum CliCommand {
    #[command(flatten)]
    Sim(SimCommand),
    // TODO(#10209): Add support for session commands: `state`, `op`, `script`.
}

/// Parses one line of input into a command.
///
/// The line is split with shell-style quoting, so for example
/// `time advance '1h 30m'` is three tokens input to clap.
pub(crate) fn parse(line: &str) -> Result<CliCommand, Error> {
    let words = shell_words::split(line)?;
    Ok(CliCommand::try_parse_from(words)?)
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    #[test]
    fn empty_line_is_parse_error() {
        assert_matches!(parse(""), Err(Error::Parse(_)));
    }

    #[test]
    fn unclosed_quote_is_split_error() {
        assert_matches!(parse("time advance '1h"), Err(Error::Split(_)));
    }

    #[test]
    fn about() {
        use clap::CommandFactory;
        use clap::builder::StyledStr;

        assert_eq!(
            CliCommand::command().get_about(),
            Some(&StyledStr::from("fm-sim: Fault Management Simulator"))
        );
    }
}

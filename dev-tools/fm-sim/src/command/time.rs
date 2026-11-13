// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The `time` noun's clap commands, and how their results print. See
//! [`nexus_fm_simulation::time`] for the core implementation.

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use clap::Subcommand;
use nexus_fm_simulation::{Error, SimState, SimStateBuilder, time};

use super::{Mutate, Query};

/// `time` verbs that read state.
#[derive(Debug, PartialEq, Subcommand)]
pub(crate) enum TimeQuery {
    /// Show the simulated clock.
    Show,
}

/// `time` verbs that change state.
#[derive(Debug, PartialEq, Subcommand)]
pub(crate) enum TimeMutate {
    /// Move the simulated clock forward.
    Advance {
        /// How far, for example `30s`, `5m`, or `1h30m`.
        duration: humantime::Duration,
    },
}

impl Query for TimeQuery {
    fn run(&self, state: &SimState) -> Result<String, Error> {
        match self {
            TimeQuery::Show => Ok(render_show(state.now())),
        }
    }
}

impl Mutate for TimeMutate {
    fn apply(&self, builder: &mut SimStateBuilder) -> Result<String, Error> {
        match self {
            TimeMutate::Advance { duration } => {
                let now = time::advance(builder, Duration::from(*duration))?;
                Ok(render_advance(*duration, now))
            }
        }
    }
}

fn render_show(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn render_advance(duration: humantime::Duration, now: DateTime<Utc>) -> String {
    format!("advanced time by {duration} to {}", render_show(now))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::CliCommand;
    use crate::command::{SimCommand, Verb};
    use crate::test_utils::{parse_err, parse_ok};

    #[test]
    fn time_commands_parse() {
        let show =
            CliCommand::Sim(SimCommand::Time(Verb::Query(TimeQuery::Show)));
        let advance = |secs: u64| {
            let duration = humantime::Duration::from(Duration::from_secs(secs));
            CliCommand::Sim(SimCommand::Time(Verb::Mutate(
                TimeMutate::Advance { duration },
            )))
        };

        assert_eq!(parse_ok("time show"), show);
        assert_eq!(parse_ok("time advance 30s"), advance(30));
        assert_eq!(parse_ok("time advance 5m"), advance(300));
        assert_eq!(parse_ok("time advance '1h 30m'"), advance(5400));

        parse_err(&[
            "time",
            "time show extra",
            "time advance",
            "time advance soon",
            "time advance 5",
            "time advance 1h 30m",
            "time advance -5m",
            "time rewind 5m",
        ]);
    }
}

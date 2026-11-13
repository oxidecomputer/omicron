// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The `time` noun: the simulated clock.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};

use crate::{Error, SimStateBuilder};

/// Moves the simulated clock forward by `duration`, and returns the new time.
///
/// If the result is later than the clock can represent, this returns
/// `Error::TimeOutOfRange` and leaves the clock unchanged.
pub fn advance(
    builder: &mut SimStateBuilder,
    duration: Duration,
) -> Result<DateTime<Utc>, Error> {
    let delta =
        TimeDelta::from_std(duration).map_err(|_| Error::TimeOutOfRange)?;
    let now =
        builder.now().checked_add_signed(delta).ok_or(Error::TimeOutOfRange)?;
    builder.set_now(now);
    Ok(now)
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;
    use crate::SimState;

    #[test]
    fn advance_moves_the_clock_forward() {
        let mut builder = SimStateBuilder::new();
        let now = advance(&mut builder, Duration::from_millis(123))
            .expect("123ms is within the clock's range");
        assert_eq!(now, SimState::EPOCH + TimeDelta::milliseconds(123));
        assert_eq!(builder.build().now(), now);
    }

    #[test]
    fn advance_rejects_a_bad_duration() {
        let mut builder = SimStateBuilder::new();
        let error = advance(&mut builder, Duration::MAX)
            .expect_err("Duration::MAX is longer than chrono can hold");
        assert_matches!(error, Error::TimeOutOfRange);
        assert_eq!(builder.now(), SimState::EPOCH);
    }
}

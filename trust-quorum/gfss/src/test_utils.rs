// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use proptest::{
    prelude::{Just, Strategy},
    test_runner::TestRng,
};

/// Returns a strategy that generates a test RNG.
///
/// Several parts of this library require an RNG instance to do work. We don't
/// want to call these parts as part of generating values in the first place,
/// since they're part of the system under test. So we use `prop_perturb` to
/// squirrel out a `TestRng` instance.
pub(crate) fn test_rng_strategy() -> impl Strategy<Value = TestRng> {
    Just(()).prop_perturb(|(), rng| rng)
}

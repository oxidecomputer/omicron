// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers and utility functions for wicketd.

use std::fmt;

use itertools::Itertools;
use wicket_common::inventory::{SpIdentifier, SpType};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub(crate) struct SpIdentifierDisplay(pub(crate) SpIdentifier);

impl From<SpIdentifier> for SpIdentifierDisplay {
    fn from(id: SpIdentifier) -> Self {
        SpIdentifierDisplay(id)
    }
}

impl<'a> From<&'a SpIdentifier> for SpIdentifierDisplay {
    fn from(id: &'a SpIdentifier) -> Self {
        SpIdentifierDisplay(*id)
    }
}

impl fmt::Display for SpIdentifierDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.type_ {
            SpType::Sled => write!(f, "sled {}", self.0.slot),
            SpType::Switch => write!(f, "switch {}", self.0.slot),
            SpType::Power => write!(f, "PSC {}", self.0.slot),
        }
    }
}

pub(crate) fn sps_to_string<S: Into<SpIdentifierDisplay>>(
    sps: impl IntoIterator<Item = S>,
) -> String {
    sps.into_iter().map_into().join(", ")
}

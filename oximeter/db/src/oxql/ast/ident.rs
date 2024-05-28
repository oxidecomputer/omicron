// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OxQL identifiers, such as column names.

// Copyright 2024 Oxide Computer Company

use std::fmt;

/// An identifier, such as a column or function name.
#[derive(Clone, Debug, PartialEq)]
pub struct Ident(pub(in crate::oxql) String);

impl Ident {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

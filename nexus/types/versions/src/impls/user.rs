// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for user types.

use crate::latest::user::Password;
use std::str::FromStr;

impl FromStr for Password {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Password::try_from(String::from(value))
    }
}

impl TryFrom<String> for Password {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let inner = omicron_passwords::Password::new(&value)
            .map_err(|e| format!("unsupported password: {:#}", e))?;
        Ok(Password(inner))
    }
}

impl AsRef<omicron_passwords::Password> for Password {
    fn as_ref(&self) -> &omicron_passwords::Password {
        &self.0
    }
}

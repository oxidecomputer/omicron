// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for alert types.

use crate::latest::alert::{
    AlertDeliveryState, AlertDeliveryStateFilter, AlertDeliveryTrigger,
    AlertSubscription, WebhookDeliveryAttemptResult,
};
use omicron_common::api::external::Error;
use std::fmt;
use std::sync::LazyLock;

impl AlertSubscription {
    pub(crate) fn is_valid(s: &str) -> Result<(), anyhow::Error> {
        static REGEX: std::sync::LazyLock<regex::Regex> =
            std::sync::LazyLock::new(|| {
                regex::Regex::new(AlertSubscription::PATTERN).expect(
                    "AlertSubscription validation regex should be valid",
                )
            });
        if REGEX.is_match(s) {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "alert subscription {s:?} does not match the pattern {}",
                AlertSubscription::PATTERN
            ))
        }
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl TryFrom<String> for AlertSubscription {
    type Error = anyhow::Error;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::is_valid(&s)?;
        Ok(Self(s))
    }
}

impl std::str::FromStr for AlertSubscription {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::is_valid(s)?;
        Ok(Self(s.to_string()))
    }
}

impl From<AlertSubscription> for String {
    fn from(AlertSubscription(s): AlertSubscription) -> Self {
        s
    }
}

impl AsRef<str> for AlertSubscription {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for AlertSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AlertDeliveryState {
    pub const ALL: &[Self] = <Self as strum::VariantArray>::VARIANTS;

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for AlertDeliveryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AlertDeliveryState {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        static EXPECTED_ONE_OF: LazyLock<String> =
            LazyLock::new(expected_one_of::<AlertDeliveryState>);

        for &v in Self::ALL {
            if s.trim().eq_ignore_ascii_case(v.as_str()) {
                return Ok(v);
            }
        }
        Err(Error::invalid_value("AlertDeliveryState", &*EXPECTED_ONE_OF))
    }
}

impl AlertDeliveryTrigger {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Alert => "alert",
            Self::Resend => "resend",
            Self::Probe => "probe",
        }
    }
}

impl fmt::Display for AlertDeliveryTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AlertDeliveryTrigger {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        static EXPECTED_ONE_OF: LazyLock<String> =
            LazyLock::new(expected_one_of::<AlertDeliveryTrigger>);

        for &v in <Self as strum::VariantArray>::VARIANTS {
            if s.trim().eq_ignore_ascii_case(v.as_str()) {
                return Ok(v);
            }
        }
        Err(Error::invalid_value("AlertDeliveryTrigger", &*EXPECTED_ONE_OF))
    }
}

impl WebhookDeliveryAttemptResult {
    pub const ALL: &[Self] = <Self as strum::VariantArray>::VARIANTS;
    pub const ALL_FAILED: &[Self] =
        &[Self::FailedHttpError, Self::FailedUnreachable, Self::FailedTimeout];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::FailedHttpError => "failed_http_error",
            Self::FailedTimeout => "failed_timeout",
            Self::FailedUnreachable => "failed_unreachable",
        }
    }

    /// Returns `true` if this `WebhookDeliveryAttemptResult` represents a failure
    pub fn is_failed(&self) -> bool {
        *self != Self::Succeeded
    }
}

impl fmt::Display for WebhookDeliveryAttemptResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Default for AlertDeliveryStateFilter {
    fn default() -> Self {
        Self::ALL
    }
}

impl AlertDeliveryStateFilter {
    pub const ALL: Self =
        Self { pending: Some(true), failed: Some(true), delivered: Some(true) };

    pub fn include_pending(&self) -> bool {
        self.pending == Some(true) || self.is_all_none()
    }

    pub fn include_failed(&self) -> bool {
        self.failed == Some(true) || self.is_all_none()
    }

    pub fn include_delivered(&self) -> bool {
        self.delivered == Some(true) || self.is_all_none()
    }

    pub fn include_all(&self) -> bool {
        self.is_all_none()
            || (self.pending == Some(true)
                && self.failed == Some(true)
                && self.delivered == Some(true))
    }

    fn is_all_none(&self) -> bool {
        self.pending.is_none()
            && self.failed.is_none()
            && self.delivered.is_none()
    }
}

fn expected_one_of<T: strum::VariantArray + fmt::Display>() -> String {
    use std::fmt::Write;
    let mut msg = "expected one of:".to_string();
    let mut variants = T::VARIANTS.iter().peekable();
    while let Some(variant) = variants.next() {
        if variants.peek().is_some() {
            write!(&mut msg, " '{variant}',").unwrap();
        } else {
            write!(&mut msg, " or '{variant}'").unwrap();
        }
    }
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_webhook_subscription_validation() {
        let successes = [
            "foo.bar",
            "foo.bar.baz",
            "foo_bar.baz",
            "foo_1.bar_200.**",
            "1.2.3",
            "foo.**",
            "foo.*.baz",
            "foo.**.baz",
            "foo.bar.**",
            "foo.*.baz.**",
            "**.foo",
            "**.foo.bar.*",
            "**.foo.bar.*.baz",
            "*.foo.bar.*",
            "*.foo",
            "*",
            "*.*",
        ];
        let failures = [
            "",
            "f*o.bar",
            "**foo.bar",
            "foo.**bar",
            "foo.*bar*",
            "*foo*",
            "f**.bar",
            "foo.***",
            "***",
            "$.foo.bar",
            "foo.%bar",
            "foo.[barbaz]",
        ];
        for s in successes {
            match s.parse::<AlertSubscription>() {
                Ok(_) => {}
                Err(e) => panic!(
                    "expected string {s:?} to be a valid webhook subscription: {e}"
                ),
            }
        }

        for s in failures {
            match s.parse::<AlertSubscription>() {
                Ok(_) => panic!(
                    "expected string {s:?} to NOT be a valid webhook subscription"
                ),
                Err(_) => {}
            }
        }
    }
}

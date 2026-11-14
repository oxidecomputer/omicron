// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for accessing services.

use omicron_common::backoff;
use slog::{Logger, warn};
use std::time::Duration;

/// Errors returned by [`wait_for_service`].
#[derive(thiserror::Error, Debug)]
#[error(
    "service {fmri} in zone {zone:?} did not reach 'online' in {timeout:?}'"
)]
pub struct WaitForServiceTimeout {
    zone: Option<String>,
    fmri: String,
    timeout: Duration,
}

// TODO(https://www.illumos.org/issues/13837): This is a hack;
// remove me when when fixed. Ideally, the ".synchronous()" argument
// to "svcadm enable" would wait for the service to be online, which
// would simplify all this stuff.
//
// Ideally, when "svccfg add" returns, these properties would be set,
// but unfortunately, they are not. This means that when we invoke
// "svcadm enable -s", it's possible for critical restarter
// properties to not exist when the command returns.
//
// We workaround this by querying for these properties in a loop.
//
// TODO: remove this when clippy no longer flags
// https://github.com/rust-lang/rust-clippy/issues/13923 (ideally we'd have
// used `expect` but on 1.84, it says that it's unfulfilled even though it
// is fulfilled?)
#[allow(clippy::needless_lifetimes)]
pub async fn wait_for_service<'a>(
    zone: Option<&'a str>,
    fmri: &str,
    log: Logger,
) -> Result<(), WaitForServiceTimeout> {
    // Okay, so...all timeouts are arbitrary, right? The important thing here is
    // that if the service isn't coming up, we should eventually give up and
    // return an error, instead of waiting forever. Assuming that a service that
    // can't start within ten minutes isn't going to start seems fairly
    // reasonable?
    const TIMEOUT: Duration = Duration::from_mins(10);

    let name = smf::PropertyName::new("restarter", "state").unwrap();

    let log_notification_failure = |error, delay| {
        warn!(
            log,
            "wait for service {} in zone {:?} failed: {}. retry in {:?}",
            fmri,
            zone,
            error,
            delay
        );
    };
    let online = smf::PropertyValue::Astring("online".to_string());
    backoff::retry_notify(
        backoff::retry_policy_local_timeout(TIMEOUT),
        || async {
            let mut p = smf::Properties::new();
            let properties =
                { if let Some(zone) = zone { p.zone(zone) } else { &mut p } };
            if let Ok(value) = properties.lookup().run(&name, &fmri) {
                if value.value() == &online {
                    return Ok(());
                } else {
                    // This is helpful in virtual environments where
                    // services take a few tries to come up. To enable,
                    // compile with RUSTFLAGS="--cfg svcadm_autoclear"
                    #[cfg(svcadm_autoclear)]
                    if let Some(zname) = zone {
                        if let Err(out) =
                            tokio::process::Command::new(crate::PFEXEC)
                                .env_clear()
                                .arg("svcadm")
                                .arg("-z")
                                .arg(zname)
                                .arg("clear")
                                .arg("*")
                                .output()
                                .await
                        {
                            warn!(
                                log,
                                "clearing service maintenance failed: {out}"
                            );
                        };
                    }
                }
            }
            return Err(backoff::BackoffError::transient("Property not found"));
        },
        log_notification_failure,
    )
    .await
    .map_err(|_| {
        // N.B. that we throw away the underlying error here because it's always
        // the string "Property not found", which is not that interesting
        // here...
        WaitForServiceTimeout {
            zone: zone.map(ToString::to_string),
            fmri: fmri.to_string(),
            timeout: TIMEOUT,
        }
    })
}

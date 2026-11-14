// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for accessing services.

use omicron_common::backoff::{self, BackoffError};
use slog::{Logger, debug, warn};
use std::time::Duration;

/// Errors returned by [`wait_for_service`].
#[derive(thiserror::Error, Debug)]
pub enum WaitForServiceError {
    /// The service did not reach the 'online' state within the provided timeout
    /// duration.
    #[error(
        "service {fmri} in zone {zone:?} did not reach '{ONLINE_STATE}' \
         within {timeout:?}'"
    )]
    TimedOut { zone: Option<String>, fmri: String, timeout: Duration },
    /// The service is in the 'maintenance' state and will not come online
    /// without human intervention.
    #[error(
        "service {fmri} in zone {zone:?} is in the '{MAINTENANCE_STATE}' \
        state and will not reach '{ONLINE_STATE}' without manual intervention"
    )]
    ServiceInMaintenance { zone: Option<String>, fmri: String },
}

const ONLINE_STATE: &str = "online";
const MAINTENANCE_STATE: &str = "maintenance";

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
) -> Result<(), WaitForServiceError> {
    // Okay, so...all timeouts are arbitrary, right? The important thing here is
    // that if the service isn't coming up, we should eventually give up and
    // return an error, instead of waiting forever. Assuming that a service that
    // can't start within ten minutes isn't going to start seems fairly
    // reasonable?
    const TIMEOUT: Duration = Duration::from_mins(10);

    // This is an internal the error enum which is necessary to disambiguate
    // between cases where we returned a `backoff::RetryError::permanent`
    // explicitly because the service is in maintenance and cases where we just
    // hit the retry limit timeout.
    #[derive(thiserror::Error, Debug)]
    enum InternalError {
        #[error("restarter/name property not found")]
        PropertyNotFound,
        #[error("restarter/state property is not '{ONLINE_STATE}'")]
        ServiceNotOnline,
        #[error("restarter/state property is '{MAINTENANCE_STATE}'")]
        ServiceInMaintenance,
        #[error("restarter/state property had an unexpected value {0}")]
        UnexpectedPropertyValue(String),
    }

    let name = smf::PropertyName::new("restarter", "state").unwrap();

    let log_notification_failure = |error, _, elapsed| {
        // Don't start spamming the log until we've waited a reasonable amount
        // of time first.
        const DONT_FREAK_OUT_FOR: Duration = Duration::from_mins(1);
        if elapsed <= DONT_FREAK_OUT_FOR {
            debug!(
                log,
                "wait for service {fmri} in zone {zone:?}: service has not \
                 reached '{ONLINE_STATE}' after {elapsed:?}";
                "error" => %error,
            );
        } else {
            warn!(
                log,
                "wait for service {fmri} in zone {zone:?}: service has not \
                 reached ''{ONLINE_STATE}' after {elapsed:?}";
                "error" => %error,
            );
        }
    };
    backoff::retry_notify_ext(
        backoff::retry_policy_local_timeout(TIMEOUT),
        || async {
            let mut p = smf::Properties::new();
            let properties =
                { if let Some(zone) = zone { p.zone(zone) } else { &mut p } };
            let Ok(property) = properties.lookup().run(&name, &fmri) else {
                return Err(BackoffError::transient(
                    InternalError::PropertyNotFound,
                ));
            };
            let error = match property.value() {
                // Service has reached the 'online' state, we can stop waiting!
                smf::PropertyValue::Astring(state) if state == ONLINE_STATE => {
                    return Ok(());
                }
                // Service is in the 'maintenance' state, and will never become
                // 'online'. Give up trying, unless we are configured to
                // automatically clear services in maintenance (in which case,
                // this is also considered retryable).
                smf::PropertyValue::Astring(state)
                    if state == MAINTENANCE_STATE =>
                {
                    let err = InternalError::ServiceInMaintenance;
                    if cfg!(svcadm_autoclear) {
                        BackoffError::transient(err)
                    } else {
                        BackoffError::permanent(err)
                    }
                }
                // Some other state, keep waiting.
                smf::PropertyValue::Astring(_) => {
                    BackoffError::transient(InternalError::ServiceNotOnline)
                }
                // Well, this is weird, it should be a string, but it isn't!
                // Return a distinct error so that this gets logged when we
                // retry.
                prop => BackoffError::transient(
                    InternalError::UnexpectedPropertyValue(prop.to_string()),
                ),
            };

            // This is helpful in virtual environments where services
            // take a few tries to come up. To enable, compile with
            // RUSTFLAGS="--cfg svcadm_autoclear".
            //
            // Note that we do this *any* time the service has not reached
            // online, not just if it is in maintenance, because the service may
            // have a *dependency* that is in maintenance, and clearing that may
            // allow the service to come up okay.
            #[cfg(svcadm_autoclear)]
            if let Some(zname) = zone {
                if let Err(out) = tokio::process::Command::new(crate::PFEXEC)
                    .env_clear()
                    .arg("svcadm")
                    .arg("-z")
                    .arg(zname)
                    .arg("clear")
                    .arg("*")
                    .output()
                    .await
                {
                    warn!(log, "clearing service maintenance failed: {out}");
                }
            }

            Err(error)
        },
        log_notification_failure,
    )
    .await
    .map_err(|error| {
        let zone = zone.map(ToString::to_string);
        let fmri = fmri.to_string();
        match error {
            // We fail fast when the service is in the 'maintenance' state
            // unless the "svcadm_autoclear" cfg is enabled, so return an error
            // indicating that the service is in maintenance.
            InternalError::ServiceInMaintenance => {
                WaitForServiceError::ServiceInMaintenance { zone, fmri }
            }
            // All other errors are retried indefinitely, so if we got here, we
            // must have hit the timeout.
            InternalError::PropertyNotFound
            | InternalError::ServiceNotOnline
            | InternalError::UnexpectedPropertyValue(_) => {
                WaitForServiceError::TimedOut { zone, fmri, timeout: TIMEOUT }
            }
        }
    })
}

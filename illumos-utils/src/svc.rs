// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for accessing services.

use crate::host::BoxedExecutor;
use omicron_common::api::external::Error;
use omicron_common::backoff;

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
pub async fn wait_for_service<'a, 'b>(
    executor: &BoxedExecutor,
    zone: Option<&'a str>,
    fmri: &'b str,
) -> Result<(), Error> {
    let name = smf::PropertyName::new("restarter", "state").unwrap();

    let log_notification_failure = |_error, _delay| {};
    backoff::retry_notify(
        backoff::retry_policy_local(),
        || async {
            let mut p = smf::Properties::new();
            let properties = {
                if let Some(zone) = zone {
                    p.zone(zone)
                } else {
                    &mut p
                }
            };
            let mut cmd = properties.lookup().as_command(&name, &fmri);

            let Ok(output) = executor.execute(&mut cmd) else {
                return Err(backoff::BackoffError::transient(
                    "Failed to execute command",
                ));
            };

            if let Ok(value) = smf::PropertyLookup::parse_output(&output) {
                if value.value()
                    == &smf::PropertyValue::Astring("online".to_string())
                {
                    return Ok(());
                }
            }
            return Err(backoff::BackoffError::transient("Property not found"));
        },
        log_notification_failure,
    )
    .await
    .map_err(|e| Error::InternalError {
        internal_message: format!("Failed to wait for service: {}", e),
    })
}

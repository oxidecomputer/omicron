//! Utilities for accessing services.

use cfg_if::cfg_if;

use omicron_common::api::external::Error;
use omicron_common::dev::poll;
use std::time::Duration;

#[cfg_attr(test, mockall::automock, allow(dead_code))]
mod inner {
    use super::*;

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
        zone: Option<&'a str>,
        fmri: &'b str,
    ) -> Result<(), Error> {
        let name = smf::PropertyName::new("restarter", "state").unwrap();

        poll::wait_for_condition::<(), std::convert::Infallible, _, _>(
            || async {
                let mut p = smf::Properties::new();
                let properties = {
                    if let Some(zone) = zone {
                        p.zone(zone)
                    } else {
                        &mut p
                    }
                };
                if let Ok(value) = properties.lookup().run(&name, &fmri) {
                    if value.value()
                        == &smf::PropertyValue::Astring("online".to_string())
                    {
                        return Ok(());
                    }
                }
                return Err(poll::CondCheckError::NotYet);
            },
            &Duration::from_millis(500),
            &Duration::from_secs(60),
        )
        .await
        .map_err(|e| Error::InternalError {
            message: format!("Failed to wait for service: {}", e),
        })
    }
}

cfg_if! {
    if #[cfg(test)] {
        pub use mock_inner::*;
    } else {
        pub use inner::*;
    }
}

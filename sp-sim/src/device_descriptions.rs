// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::SpComponentConfig;
use gateway_messages::SpComponent;
use gateway_messages::sp_impl::BoundsChecked;
use gateway_messages::sp_impl::DeviceDescription;

/// The descriptions of a simulated SP's components, as reported in its
/// inventory.
pub(crate) struct DeviceDescriptions(Vec<DeviceDescription<'static>>);

impl DeviceDescriptions {
    /// # Panics
    ///
    /// Panics if any component's ID is too long to be an [`SpComponent`].
    pub(crate) fn from_component_configs(
        configs: &[SpComponentConfig],
    ) -> Self {
        let descriptions = configs
            .iter()
            .map(|c| DeviceDescription {
                component: SpComponent::try_from(c.id.as_str()).unwrap_or_else(
                    |_| panic!("component ID {:?} is too long", c.id),
                ),
                // `SpHandler` wants `&'static str` references when describing
                // components; this is fine on the real SP where the strings
                // are baked in at build time, but awkward here where we read
                // them in at runtime. We'll leak the strings to conform to
                // `SpHandler` rather than making it more complicated to ease
                // our life as a simulator.
                device: Box::leak(c.device.clone().into_boxed_str()),
                description: Box::leak(c.description.clone().into_boxed_str()),
                capabilities: c.capabilities(),
                presence: c.presence,
            })
            .collect();
        Self(descriptions)
    }

    pub(crate) fn num_devices(&self) -> u32 {
        self.0.len().try_into().unwrap()
    }

    pub(crate) fn device_description(
        &self,
        index: BoundsChecked,
    ) -> DeviceDescription<'static> {
        self.0[index.0 as usize]
    }
}

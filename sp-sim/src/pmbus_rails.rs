// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::config::PmbusStatusConfig;
use crate::config::SpComponentConfig;
use gateway_messages::PmbusStatusError;
use gateway_messages::PmbusStatusReadError;
use gateway_messages::PowerRailName;
use gateway_messages::SpError;
use iddqd::IdHashItem;
use iddqd::IdHashMap;

pub(crate) struct PmbusRails {
    rails: IdHashMap<PmbusRail>,
}

struct PmbusRail {
    name: PowerRailName,
    component_id: String,
    status: gateway_messages::PmbusStatus,
}

impl IdHashItem for PmbusRail {
    type Key<'a> = &'a PowerRailName;

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }

    iddqd::id_upcast!();
}

impl PmbusRails {
    pub(crate) fn from_component_configs(
        configs: &[SpComponentConfig],
    ) -> anyhow::Result<Self> {
        let mut rails = IdHashMap::default();

        for config in configs {
            if config.pmbus_rails.is_empty() {
                continue;
            }

            let component_id = config.id.as_str();
            for (name, rail) in &config.pmbus_rails {
                let name = PowerRailName::try_from(name.as_str())
                    .map_err(|_| anyhow::anyhow!(
                        "component '{component_id}' defines a PMBus rail whose \
                         name is too long: {name}",
                    ))?;
                let &PmbusStatusConfig {
                    status_word,
                    status_vout,
                    status_iout,
                    status_cml,
                    status_temperature,
                    status_input,
                    status_mfr_specific,
                    status_fans_1_2,
                    status_fans_3_4,
                    status_other,
                } = rail;
                let status = gateway_messages::PmbusStatus {
                    status_word,
                    status_vout: status_vout
                        .ok_or(PmbusStatusReadError::Unsupported),
                    status_iout: status_iout
                        .ok_or(PmbusStatusReadError::Unsupported),
                    status_cml: status_cml
                        .ok_or(PmbusStatusReadError::Unsupported),
                    status_temperature: status_temperature
                        .ok_or(PmbusStatusReadError::Unsupported),
                    status_input: status_input
                        .ok_or(PmbusStatusReadError::Unsupported),
                    status_mfr_specific: status_mfr_specific
                        .ok_or(PmbusStatusReadError::Unsupported),
                    status_fans_1_2: status_fans_1_2
                        .ok_or(PmbusStatusReadError::Unsupported),
                    status_fans_3_4: status_fans_3_4
                        .ok_or(PmbusStatusReadError::Unsupported),
                    status_other: status_other
                        .ok_or(PmbusStatusReadError::Unsupported),
                };
                rails.insert_unique(PmbusRail {
                    name,
                    status,
                    component_id: component_id.to_string(),
                }).map_err(|err| {
                    anyhow::anyhow!(
                        "invalid config: component '{component_id}' defines a \
                         PMBus power rail '{name}' that was already defined by \
                         component '{}'",
                        err.duplicates()[0].component_id)
                })?;
            }
        }

        Ok(Self { rails })
    }

    pub(crate) fn pmbus_status(
        &self,
        name: &PowerRailName,
    ) -> Result<gateway_messages::PmbusStatus, SpError> {
        Ok(self
            .rails
            .get(&name)
            .ok_or(SpError::PmbusStatus(PmbusStatusError::UnknownRail))?
            .status)
    }
}

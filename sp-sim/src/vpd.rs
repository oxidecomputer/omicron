// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Runtime component VPD responses constructed from simulator configuration.

use crate::config::ComponentVpdConfig;
use crate::config::PmbusBlockConfig;
use crate::config::SpComponentConfig;

use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::vpd as gw;

use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use std::collections::HashMap;

pub(crate) struct ComponentVpds {
    by_component: HashMap<SpComponent, ComponentVpd>,
}

#[derive(Debug)]
struct ComponentVpd {
    vpd: gw::Vpd,
}

impl ComponentVpds {
    pub(crate) fn from_component_configs<'a>(
        configs: impl IntoIterator<Item = &'a SpComponentConfig>,
    ) -> anyhow::Result<Self> {
        let mut by_component = HashMap::new();
        for config in configs {
            let Some(vpd_config) = &config.vpd else {
                continue;
            };
            let component = SpComponent::try_from(config.id.as_str())
                .map_err(|_| anyhow!("invalid component ID {:?}", config.id))?;
            let vpd =
                ComponentVpd::from_config(vpd_config).with_context(|| {
                    format!("invalid VPD config for component {component}")
                })?;
            if by_component.insert(component, vpd).is_some() {
                bail!(
                    "invalid config file: duplicate VPD configs for component
                     ID {component}"
                );
            }
        }
        Ok(Self { by_component })
    }

    pub(crate) fn component_get_vpd(
        &self,
        component: &SpComponent,
        buf: &mut [u8],
    ) -> Result<usize, SpError> {
        let vpd = self
            .by_component
            .get(component)
            .ok_or(SpError::RequestUnsupportedForComponent)?;
        // TODO(eliza): allow simulating errors as well here?
        match gateway_messages::serialize(buf, &vpd.vpd) {
            Ok(len) => Ok(len),
            Err(e) => {
                panic!(
                    "failed to serialize component_get_vpd response for \
                     {component}: {e}"
                )
            }
        }
    }
}

impl ComponentVpd {
    fn from_config(config: &ComponentVpdConfig) -> anyhow::Result<Self> {
        let vpd = match config {
            ComponentVpdConfig::Pmbus(config) => {
                let vpd = gw::PmbusVpd {
                    mfr_id: pmbus_block(config.mfr_id.as_ref())
                        .context("invalid PMBus mfr_id")?,
                    mfr_model: pmbus_block(config.mfr_model.as_ref())
                        .context("invalid PMBus mfr_model")?,
                    mfr_revision: pmbus_block(config.mfr_revision.as_ref())
                        .context("invalid PMBus mfr_revision")?,
                    mfr_location: pmbus_block(config.mfr_location.as_ref())
                        .context("invalid PMBus mfr_location")?,
                    mfr_date: pmbus_block(config.mfr_date.as_ref())
                        .context("invalid PMBus mfr_date")?,
                    mfr_serial: pmbus_block(config.mfr_serial.as_ref())
                        .context("invalid PMBus mfr_serial")?,
                    ic_device_id: pmbus_block(config.ic_device_id.as_ref())
                        .context("invalid PMBus ic_device_id")?,
                    ic_device_rev: pmbus_block(config.ic_device_rev.as_ref())
                        .context("invalid PMBus ic_device_rev")?,
                };
                gw::Vpd::Pmbus(vpd)
            }
            ComponentVpdConfig::Barcode(vpd) => gw::Vpd::Barcode(*vpd),
            ComponentVpdConfig::SledFanTray(vpd) => {
                gw::Vpd::SledFanTray((**vpd).clone())
            }
            ComponentVpdConfig::Tmp11x(vpd) => gw::Vpd::Tmp11x(vpd.clone()),
        };
        Ok(Self { vpd })
    }
}

fn pmbus_block(
    value: Option<&PmbusBlockConfig>,
) -> anyhow::Result<gw::SmbusBlock> {
    let mut block = gw::SmbusBlock::UNSUPPORTED;
    let Some(value) = value else {
        return Ok(block);
    };
    let value = value.as_bytes();
    if value.len() > gw::SmbusBlock::MAX_LEN {
        bail!(
            "the simulated response value for this PMBus VPD command is {}B, \
             which exceeds the SP's maximum supported length of {}B",
            value.len(),
            gw::SmbusBlock::MAX_LEN,
        );
    }
    block
        .read_into(|buf| {
            buf[..value.len()].copy_from_slice(value);
            Ok::<_, std::convert::Infallible>(Some(value.len()))
        })
        .expect(
            "we just checked that the configured response is within the \
            max length",
        );
    Ok(block)
}

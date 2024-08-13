// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::SpComponentConfig;
use gateway_messages::measurement::MeasurementError;
use gateway_messages::measurement::MeasurementKind;
use gateway_messages::sp_impl::BoundsChecked;
use gateway_messages::ComponentDetails;
use gateway_messages::DeviceCapabilities;
use gateway_messages::Measurement;
use gateway_messages::SensorDataMissing;
use gateway_messages::SensorError;
use gateway_messages::SensorReading;
use gateway_messages::SensorRequest;
use gateway_messages::SensorRequestKind;
use gateway_messages::SensorResponse;
use gateway_messages::SpComponent;

use std::collections::HashMap;

pub(crate) struct Sensors {
    by_component: HashMap<SpComponent, Vec<u32>>,

    sensors: HashMap<u32, Sensor>,
}

#[derive(Debug)]
struct Sensor {
    def: SensorDef,
    state: SensorState,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq)]
pub struct SensorDef {
    pub name: String,
    pub kind: MeasurementKind,
    pub sensor_id: u32,
}

// TODO(eliza): note that currently, we just hardcode these in
// `MeasurementConfig`. Eventually, it would be neat to allow the sensor to be
// changed dynamically as part of a simulation.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct SensorState {
    #[serde(default)]
    pub last_error: Option<LastError>,

    #[serde(default)]
    pub last_data: Option<LastData>,
}

#[derive(
    Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq,
)]
pub struct LastError {
    pub timestamp: u64,
    pub value: SensorDataMissing,
}

#[derive(
    Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq,
)]
pub struct LastData {
    pub timestamp: u64,
    pub value: f32,
}

impl SensorState {
    fn last_reading(&self) -> SensorReading {
        match self {
            Self { last_data: Some(data), last_error: Some(error) } => {
                if data.timestamp >= error.timestamp {
                    SensorReading {
                        value: Ok(data.value),
                        timestamp: data.timestamp,
                    }
                } else {
                    SensorReading {
                        value: Err(error.value),
                        timestamp: error.timestamp,
                    }
                }
            }
            Self { last_data: Some(data), last_error: None } => SensorReading {
                value: Ok(data.value),
                timestamp: data.timestamp,
            },
            Self { last_data: None, last_error: Some(error) } => {
                SensorReading {
                    value: Err(error.value),
                    timestamp: error.timestamp,
                }
            }
            Self { last_data: None, last_error: None } => SensorReading {
                value: Err(SensorDataMissing::DeviceNotPresent),
                timestamp: 0, // TODO(eliza): what do?
            },
        }
    }
}

impl Sensors {
    pub(crate) fn from_component_configs<'a>(
        cfgs: impl IntoIterator<Item = &'a SpComponentConfig>,
    ) -> Self {
        let mut sensors = HashMap::new();
        let mut by_component = HashMap::new();
        for cfg in cfgs {
            if cfg.sensors.is_empty() {
                continue;
            }
            if !cfg
                .capabilities
                .contains(DeviceCapabilities::HAS_MEASUREMENT_CHANNELS)
            {
                panic!(
                    "invalid component config: a device with sensors should \
                     have the `HAS_MEASUREMENT_CHANNELS` capability:{cfg:#?}"
                );
            }

            let mut ids = Vec::with_capacity(cfg.sensors.len());
            for sensor in &cfg.sensors {
                let sensor_id = sensor.def.sensor_id;
                let prev = sensors.insert(
                    sensor_id,
                    Sensor {
                        def: sensor.def.clone(),
                        state: sensor.state.clone(),
                    },
                );
                assert!(
                    prev.is_none(),
                    "invalid config: sensor ID {sensor_id} already exists!\n
                     previous sensor: {prev:#?}\nnew sensor: {sensor:#?}"
                );
                ids.push(sensor_id)
            }

            let component = SpComponent::try_from(cfg.id.as_str()).unwrap();
            let prev = by_component.insert(component, ids);
            assert!(prev.is_none(), "component ID collision!");
        }
        Self { sensors, by_component }
    }

    fn sensor_for_component<'sensors>(
        &'sensors self,
        component: &SpComponent,
        index: BoundsChecked,
    ) -> Option<&'sensors Sensor> {
        let id = self.by_component.get(component)?.get(index.0 as usize)?;
        self.sensors.get(id)
    }

    pub(crate) fn num_component_details(
        &self,
        component: &SpComponent,
    ) -> Option<u32> {
        let len = self
            .by_component
            .get(component)?
            .len()
            .try_into()
            .expect("why would you have more than `u32::MAX` sensors?");
        Some(len)
    }

    /// This method returns an `Option` because the component's details might
    /// be a port status rather than a measurement, if we eventually decide to
    /// implement port statuses in the simulated sidecar...
    pub(crate) fn component_details(
        &self,
        component: &SpComponent,
        index: BoundsChecked,
    ) -> Option<ComponentDetails> {
        let sensor = self.sensor_for_component(component, index)?;
        let value =
            sensor.state.last_reading().value.map_err(|err| match err {
                SensorDataMissing::DeviceError => MeasurementError::DeviceError,
                SensorDataMissing::DeviceNotPresent => {
                    MeasurementError::NotPresent
                }
                SensorDataMissing::DeviceOff => MeasurementError::DeviceOff,
                SensorDataMissing::DeviceTimeout => {
                    MeasurementError::DeviceTimeout
                }
                SensorDataMissing::DeviceUnavailable => {
                    MeasurementError::DeviceUnavailable
                }
            });
        Some(ComponentDetails::Measurement(Measurement {
            name: sensor.def.name.clone(),
            kind: sensor.def.kind,
            value,
        }))
    }

    pub(crate) fn read_sensor(
        &self,
        SensorRequest { id, kind }: SensorRequest,
    ) -> Result<SensorResponse, SensorError> {
        let sensor = self.sensors.get(&id).ok_or(SensorError::InvalidSensor)?;
        match kind {
            SensorRequestKind::LastReading => {
                Ok(SensorResponse::LastReading(sensor.state.last_reading()))
            }
            SensorRequestKind::ErrorCount => {
                let count =
                    // TODO(eliza): simulate more than one error...
                    if sensor.state.last_error.is_some() { 1 } else { 0 };
                Ok(SensorResponse::ErrorCount(count))
            }
            SensorRequestKind::LastData => {
                let LastData { timestamp, value } =
                    sensor.state.last_data.ok_or(SensorError::NoReading)?;
                Ok(SensorResponse::LastData { value, timestamp })
            }
            SensorRequestKind::LastError => {
                let LastError { timestamp, value } =
                    sensor.state.last_error.ok_or(SensorError::NoReading)?;
                Ok(SensorResponse::LastError { value, timestamp })
            }
        }
    }
}

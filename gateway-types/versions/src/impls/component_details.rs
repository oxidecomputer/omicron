// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::HttpError;

use crate::latest::component_details::{
    LinkStatus, Measurement, MeasurementError, MeasurementErrorCode,
    MeasurementKind, PacketCount, PhyStatus, PhyType, PortConfig, PortCounters,
    PortDev, PortMode, PortSerdes, PortStatus, PortStatusError,
    PortStatusErrorCode, SpComponentDetails, Speed,
    UnsupportedComponentDetails,
};

impl From<UnsupportedComponentDetails> for HttpError {
    fn from(value: UnsupportedComponentDetails) -> Self {
        HttpError::for_bad_request(
            None,
            format!(
                "requested component details are not yet supported: {value}"
            ),
        )
    }
}

impl TryFrom<gateway_messages::ComponentDetails> for SpComponentDetails {
    type Error = UnsupportedComponentDetails;

    fn try_from(
        details: gateway_messages::ComponentDetails,
    ) -> Result<Self, Self::Error> {
        use gateway_messages::ComponentDetails;
        match details {
            ComponentDetails::PortStatus(Ok(status)) => {
                Ok(Self::PortStatus(status.into()))
            }
            ComponentDetails::PortStatus(Err(err)) => {
                Ok(Self::PortStatusError(err.into()))
            }
            ComponentDetails::Measurement(m) => Ok(match m.value {
                Ok(value) => Self::Measurement(Measurement {
                    name: m.name,
                    kind: m.kind.into(),
                    value,
                }),
                Err(err) => Self::MeasurementError(MeasurementError {
                    name: m.name,
                    kind: m.kind.into(),
                    error: err.into(),
                }),
            }),
            ComponentDetails::LastPostCode(inner) => {
                Err(UnsupportedComponentDetails {
                    description: format!("last post code: {inner:?}"),
                })
            }
            ComponentDetails::GpioToggleCount(inner) => {
                Err(UnsupportedComponentDetails {
                    description: format!("GPIO toggle count: {inner:?}"),
                })
            }
        }
    }
}

impl From<gateway_messages::monorail_port_status::PortStatus> for PortStatus {
    fn from(
        status: gateway_messages::monorail_port_status::PortStatus,
    ) -> Self {
        Self {
            port: status.port,
            cfg: status.cfg.into(),
            link_status: status.link_status.into(),
            phy_status: status.phy_status.map(Into::into),
            counters: status.counters.into(),
        }
    }
}

impl From<gateway_messages::monorail_port_status::PortConfig> for PortConfig {
    fn from(cfg: gateway_messages::monorail_port_status::PortConfig) -> Self {
        Self {
            mode: cfg.mode.into(),
            dev_type: cfg.dev.0.into(),
            dev_num: cfg.dev.1,
            serdes_type: cfg.serdes.0.into(),
            serdes_num: cfg.serdes.1,
        }
    }
}

impl From<gateway_messages::monorail_port_status::PortMode> for PortMode {
    fn from(mode: gateway_messages::monorail_port_status::PortMode) -> Self {
        use gateway_messages::monorail_port_status::PortMode;
        match mode {
            PortMode::Sfi => Self::Sfi,
            PortMode::BaseKr => Self::BaseKr,
            PortMode::Sgmii(s) => Self::Sgmii { speed: s.into() },
            PortMode::Qsgmii(s) => Self::Qsgmii { speed: s.into() },
        }
    }
}

impl From<gateway_messages::monorail_port_status::Speed> for Speed {
    fn from(speed: gateway_messages::monorail_port_status::Speed) -> Self {
        use gateway_messages::monorail_port_status::Speed;
        match speed {
            Speed::Speed100M => Self::Speed100M,
            Speed::Speed1G => Self::Speed1G,
            Speed::Speed10G => Self::Speed10G,
        }
    }
}

impl From<gateway_messages::monorail_port_status::PortDev> for PortDev {
    fn from(dev: gateway_messages::monorail_port_status::PortDev) -> Self {
        use gateway_messages::monorail_port_status::PortDev;
        match dev {
            PortDev::Dev1g => Self::Dev1g,
            PortDev::Dev2g5 => Self::Dev2g5,
            PortDev::Dev10g => Self::Dev10g,
        }
    }
}

impl From<gateway_messages::monorail_port_status::PortSerdes> for PortSerdes {
    fn from(
        serdes: gateway_messages::monorail_port_status::PortSerdes,
    ) -> Self {
        use gateway_messages::monorail_port_status::PortSerdes;
        match serdes {
            PortSerdes::Serdes1g => Self::Serdes1g,
            PortSerdes::Serdes6g => Self::Serdes6g,
            PortSerdes::Serdes10g => Self::Serdes10g,
        }
    }
}

impl From<gateway_messages::monorail_port_status::LinkStatus> for LinkStatus {
    fn from(
        status: gateway_messages::monorail_port_status::LinkStatus,
    ) -> Self {
        use gateway_messages::monorail_port_status::LinkStatus;
        match status {
            LinkStatus::Error => Self::Error,
            LinkStatus::Down => Self::Down,
            LinkStatus::Up => Self::Up,
        }
    }
}

impl From<gateway_messages::monorail_port_status::PhyStatus> for PhyStatus {
    fn from(status: gateway_messages::monorail_port_status::PhyStatus) -> Self {
        Self {
            ty: status.ty.into(),
            mac_link_up: status.mac_link_up.into(),
            media_link_up: status.media_link_up.into(),
        }
    }
}

impl From<gateway_messages::monorail_port_status::PhyType> for PhyType {
    fn from(ty: gateway_messages::monorail_port_status::PhyType) -> Self {
        use gateway_messages::monorail_port_status::PhyType;
        match ty {
            PhyType::Vsc8504 => Self::Vsc8504,
            PhyType::Vsc8522 => Self::Vsc8522,
            PhyType::Vsc8552 => Self::Vsc8552,
            PhyType::Vsc8562 => Self::Vsc8562,
        }
    }
}

impl From<gateway_messages::monorail_port_status::PortCounters>
    for PortCounters
{
    fn from(c: gateway_messages::monorail_port_status::PortCounters) -> Self {
        Self {
            rx: c.rx.into(),
            tx: c.tx.into(),
            link_down_sticky: c.link_down_sticky,
            phy_link_down_sticky: c.phy_link_down_sticky,
        }
    }
}

impl From<gateway_messages::monorail_port_status::PacketCount> for PacketCount {
    fn from(c: gateway_messages::monorail_port_status::PacketCount) -> Self {
        Self {
            multicast: c.multicast,
            unicast: c.unicast,
            broadcast: c.broadcast,
        }
    }
}

impl From<gateway_messages::monorail_port_status::PortStatusError>
    for PortStatusError
{
    fn from(
        err: gateway_messages::monorail_port_status::PortStatusError,
    ) -> Self {
        Self { port: err.port, code: err.code.into() }
    }
}

impl From<gateway_messages::monorail_port_status::PortStatusErrorCode>
    for PortStatusErrorCode
{
    fn from(
        err: gateway_messages::monorail_port_status::PortStatusErrorCode,
    ) -> Self {
        use gateway_messages::monorail_port_status::PortStatusErrorCode;
        match err {
            PortStatusErrorCode::Unconfigured => Self::Unconfigured,
            PortStatusErrorCode::Other(raw) => Self::Other { raw },
        }
    }
}

impl From<gateway_messages::measurement::MeasurementKind> for MeasurementKind {
    fn from(kind: gateway_messages::measurement::MeasurementKind) -> Self {
        use gateway_messages::measurement::MeasurementKind;
        match kind {
            MeasurementKind::Temperature => Self::Temperature,
            MeasurementKind::Power => Self::Power,
            MeasurementKind::Current => Self::Current,
            MeasurementKind::Voltage => Self::Voltage,
            MeasurementKind::InputCurrent => Self::InputCurrent,
            MeasurementKind::InputVoltage => Self::InputVoltage,
            MeasurementKind::Speed => Self::Speed,
            MeasurementKind::CpuTctl => Self::CpuTctl,
        }
    }
}

impl From<gateway_messages::measurement::MeasurementError>
    for MeasurementErrorCode
{
    fn from(err: gateway_messages::measurement::MeasurementError) -> Self {
        use gateway_messages::measurement::MeasurementError;
        match err {
            MeasurementError::InvalidSensor => Self::InvalidSensor,
            MeasurementError::NoReading => Self::NoReading,
            MeasurementError::NotPresent => Self::NotPresent,
            MeasurementError::DeviceError => Self::DeviceError,
            MeasurementError::DeviceUnavailable => Self::DeviceUnavailable,
            MeasurementError::DeviceTimeout => Self::DeviceTimeout,
            MeasurementError::DeviceOff => Self::DeviceOff,
        }
    }
}

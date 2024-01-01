// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the "mgs sensors" subcommand

use anyhow::{bail, Context};
use clap::Args;
use gateway_client::types::MeasurementErrorCode;
use gateway_client::types::MeasurementKind;
use gateway_client::types::SpComponentDetails;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpIgnition;
use gateway_client::types::SpType;
use multimap::MultiMap;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};

#[derive(Debug, Args)]
pub(crate) struct SensorsArgs {
    /// verbose messages
    #[clap(long, short)]
    verbose: bool,

    /// restrict to specified sled(s)
    #[clap(long, use_value_delimiter = true)]
    sleds: Vec<u32>,

    /// exclude sleds rather than include them
    #[clap(long, short)]
    exclude: bool,

    /// include switches
    #[clap(long)]
    switches: bool,

    /// include PSC
    #[clap(long)]
    psc: bool,

    /// print sensors every second
    #[clap(long, short)]
    sleep: bool,

    /// parseable output
    #[clap(long, short)]
    parseable: bool,

    /// restrict sensors by type of sensor
    #[clap(
        long,
        short,
        value_name = "sensor type",
        use_value_delimiter = true
    )]
    types: Option<Vec<String>>,

    /// restrict sensors by name
    #[clap(
        long,
        short,
        value_name = "sensor name",
        use_value_delimiter = true
    )]
    named: Option<Vec<String>>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Sensor {
    name: String,
    kind: MeasurementKind,
}

impl Sensor {
    fn units(&self) -> &str {
        match self.kind {
            MeasurementKind::Temperature => "°C",
            MeasurementKind::Current | MeasurementKind::InputCurrent => "A",
            MeasurementKind::Voltage | MeasurementKind::InputVoltage => "V",
            MeasurementKind::Speed => "RPM",
            MeasurementKind::Power => "W",
        }
    }

    fn format(&self, value: f32, parseable: bool) -> String {
        if parseable {
            format!("{value}")
        } else {
            match self.kind {
                MeasurementKind::Speed => {
                    format!("{value:0} RPM")
                }
                _ => {
                    format!("{value:.2}{}", self.units())
                }
            }
        }
    }

    fn to_kind_string(&self) -> &str {
        match self.kind {
            MeasurementKind::Temperature => "temp",
            MeasurementKind::Power => "power",
            MeasurementKind::Current => "current",
            MeasurementKind::Voltage => "voltage",
            MeasurementKind::InputCurrent => "input-current",
            MeasurementKind::InputVoltage => "input-voltage",
            MeasurementKind::Speed => "speed",
        }
    }

    fn from_string(name: &str, kind: &str) -> Option<Self> {
        let k = match kind {
            "temp" | "temperature" => Some(MeasurementKind::Temperature),
            "power" => Some(MeasurementKind::Power),
            "current" => Some(MeasurementKind::Current),
            "voltage" => Some(MeasurementKind::Voltage),
            "input-current" => Some(MeasurementKind::InputCurrent),
            "input-voltage" => Some(MeasurementKind::InputVoltage),
            "speed" => Some(MeasurementKind::Speed),
            _ => None,
        };

        if let Some(kind) = k {
            Some(Sensor { name: name.to_string(), kind })
        } else {
            None
        }
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
struct SensorId(u32);

#[derive(Debug)]
struct SensorMetadata {
    sensors_by_sensor: MultiMap<Sensor, SensorId>,
    sensors_by_sensor_and_sp: HashMap<Sensor, HashMap<SpIdentifier, SensorId>>,
    sensors_by_id: HashMap<SensorId, (SpIdentifier, Sensor, String)>,
    sensors_by_sp: MultiMap<SpIdentifier, SensorId>,
    work_by_sp: HashMap<SpIdentifier, Vec<(String, Vec<SensorId>)>>,
}

struct SensorValues(HashMap<SensorId, Option<f32>>);

struct SpInfo {
    devices: MultiMap<String, (Sensor, Option<f32>)>,
    timestamps: Vec<std::time::Instant>,
}

async fn sp_info(
    mgs_client: gateway_client::Client,
    type_: SpType,
    slot: u32,
) -> Result<SpInfo, anyhow::Error> {
    let mut devices = MultiMap::new();
    let mut timestamps = vec![];

    timestamps.push(std::time::Instant::now());

    //
    // First, get a component list.
    //
    let components = mgs_client.sp_component_list(type_, slot).await?;
    timestamps.push(std::time::Instant::now());

    //
    // Now, for every component, we're going to get its details:  for those
    // that are sensors (and contain measurements), we will store the name
    // of the sensor as well as the retrieved value.
    //
    for c in &components.components {
        for s in mgs_client
            .sp_component_get(type_, slot, &c.component)
            .await?
            .iter()
            .filter_map(|detail| match detail {
                SpComponentDetails::Measurement { kind, name, value } => Some(
                    (Sensor { name: name.clone(), kind: *kind }, Some(*value)),
                ),
                SpComponentDetails::MeasurementError { kind, name, error } => {
                    match error {
                        MeasurementErrorCode::NoReading
                        | MeasurementErrorCode::NotPresent => None,
                        _ => Some((
                            Sensor { name: name.clone(), kind: *kind },
                            None,
                        )),
                    }
                }
                _ => None,
            })
        {
            devices.insert(c.component.clone(), s);
        }
    }

    timestamps.push(std::time::Instant::now());

    Ok(SpInfo { devices, timestamps })
}

async fn sensor_metadata(
    mgs_client: &gateway_client::Client,
    args: &SensorsArgs,
) -> Result<(SensorMetadata, SensorValues), anyhow::Error> {
    let by_kind = if let Some(types) = &args.types {
        let mut h = HashSet::new();

        for t in types {
            h.insert(match Sensor::from_string("", &t) {
                None => bail!("invalid sensor kind {t}"),
                Some(s) => s.kind,
            });
        }

        Some(h)
    } else {
        None
    };

    let by_name = if let Some(named) = &args.named {
        Some(named.into_iter().collect::<HashSet<_>>())
    } else {
        None
    };

    //
    // First, get all of the SPs that we can see via Ignition
    //
    let all_sp_list =
        mgs_client.ignition_list().await.context("listing ignition")?;

    let mut sp_list = all_sp_list
        .iter()
        .filter_map(|ignition| {
            if matches!(ignition.details, SpIgnition::Yes { .. }) {
                if ignition.id.type_ == SpType::Sled {
                    let matched = if args.sleds.len() > 0 {
                        args.sleds
                            .iter()
                            .find(|&&v| v == ignition.id.slot)
                            .is_some()
                    } else {
                        true
                    };

                    if matched != args.exclude {
                        return Some(ignition.id);
                    }
                }
            }
            None
        })
        .collect::<Vec<_>>();

    if args.switches {
        sp_list.push(SpIdentifier { type_: SpType::Switch, slot: 0 });
        sp_list.push(SpIdentifier { type_: SpType::Switch, slot: 1 });
    }

    if args.psc {
        sp_list.push(SpIdentifier { type_: SpType::Power, slot: 0 });
    }

    sp_list.sort();

    let now = std::time::Instant::now();

    let mut handles = vec![];
    for sp_id in sp_list {
        let mgs_client = mgs_client.clone();
        let type_ = sp_id.type_;
        let slot = sp_id.slot;

        let handle =
            tokio::spawn(async move { sp_info(mgs_client, type_, slot).await });

        handles.push((sp_id, handle));
    }

    let mut sensors_by_sensor = MultiMap::new();
    let mut sensors_by_sensor_and_sp = HashMap::new();
    let mut sensors_by_id = HashMap::new();
    let mut sensors_by_sp = MultiMap::new();
    let mut all_values = HashMap::new();
    let mut work_by_sp = HashMap::new();

    let mut current = 0;

    for (sp_id, handle) in handles {
        let mut sp_work = vec![];

        match handle.await.unwrap() {
            Ok(info) => {
                for (device, sensors) in info.devices {
                    let mut device_work = vec![];

                    for (sensor, value) in sensors {
                        if let Some(ref by_kind) = by_kind {
                            if by_kind.get(&sensor.kind).is_none() {
                                continue;
                            }
                        }

                        if let Some(ref by_name) = by_name {
                            if by_name.get(&sensor.name).is_none() {
                                continue;
                            }
                        }

                        let id = SensorId(current);
                        current += 1;

                        sensors_by_id.insert(
                            id,
                            (sp_id, sensor.clone(), device.clone()),
                        );

                        if value.is_none() && args.verbose {
                            eprintln!(
                                "mgs: error for {sp_id:?} on {sensor:?} ({device})"
                            );
                        }

                        sensors_by_sensor.insert(sensor.clone(), id);

                        let by_sp = sensors_by_sensor_and_sp
                            .entry(sensor)
                            .or_insert_with(|| HashMap::new());
                        by_sp.insert(sp_id, id);
                        sensors_by_sp.insert(sp_id, id);
                        all_values.insert(id, value);

                        device_work.push(id);
                    }

                    sp_work.push((device, device_work));
                }

                if args.verbose {
                    eprintln!(
                        "mgs: latencies for {sp_id:?}: {:.1?} {:.1?}",
                        info.timestamps[2].duration_since(info.timestamps[1]),
                        info.timestamps[1].duration_since(info.timestamps[0])
                    );
                }
            }
            Err(err) => {
                eprintln!("failed to read devices for {:?}: {:?}", sp_id, err);
            }
        }

        work_by_sp.insert(sp_id, sp_work);
    }

    if args.verbose {
        eprintln!("total discovery time {:?}", now.elapsed());
    }

    Ok((
        SensorMetadata {
            sensors_by_sensor,
            sensors_by_sensor_and_sp,
            sensors_by_id,
            sensors_by_sp,
            work_by_sp,
        },
        SensorValues(all_values),
    ))
}

async fn sp_read_sensors(
    mgs_client: &gateway_client::Client,
    id: &SpIdentifier,
    metadata: std::sync::Arc<&SensorMetadata>,
) -> Result<Vec<(SensorId, Option<f32>)>, anyhow::Error> {
    let work = metadata.work_by_sp.get(id).unwrap();
    let mut rval = vec![];

    for (component, ids) in work.iter() {
        for (value, id) in mgs_client
            .sp_component_get(id.type_, id.slot, component)
            .await?
            .iter()
            .filter_map(|detail| match detail {
                SpComponentDetails::Measurement { kind: _, name: _, value } => {
                    Some(Some(*value))
                }
                SpComponentDetails::MeasurementError { error, .. } => {
                    match error {
                        MeasurementErrorCode::NoReading
                        | MeasurementErrorCode::NotPresent => None,
                        _ => Some(None),
                    }
                }
                _ => None,
            })
            .zip(ids.iter())
        {
            rval.push((*id, value));
        }
    }

    Ok(rval)
}

async fn sensor_data(
    mgs_client: &gateway_client::Client,
    metadata: std::sync::Arc<&'static SensorMetadata>,
) -> Result<SensorValues, anyhow::Error> {
    let mut all_values = HashMap::new();
    let mut handles = vec![];

    for sp_id in metadata.sensors_by_sp.keys() {
        let mgs_client = mgs_client.clone();
        let id = *sp_id;
        let metadata = metadata.clone();

        let handle = tokio::spawn(async move {
            sp_read_sensors(&mgs_client, &id, metadata).await
        });

        handles.push(handle);
    }

    for handle in handles {
        let rval = handle.await.unwrap()?;

        for (id, value) in rval {
            all_values.insert(id, value);
        }
    }

    Ok(SensorValues(all_values))
}

///
/// Runs `omdb mgs sensors`
///
pub(crate) async fn cmd_mgs_sensors(
    mgs_client: &gateway_client::Client,
    args: &SensorsArgs,
) -> Result<(), anyhow::Error> {
    let (metadata, mut values) = sensor_metadata(mgs_client, args).await?;

    //
    // A bit of shenangians to force metadata to be 'static -- which allows
    // us to share it with tasks.
    //
    let metadata = Box::leak(Box::new(metadata));
    let metadata: &_ = metadata;
    let metadata = std::sync::Arc::new(metadata);

    let mut sensors = metadata.sensors_by_sensor.keys().collect::<Vec<_>>();
    sensors.sort();

    let mut sps = metadata.sensors_by_sp.keys().collect::<Vec<_>>();
    sps.sort();

    let print_value = |v| {
        if args.parseable {
            print!(",{v}");
        } else {
            print!(" {v:>8}");
        }
    };

    let print_header = || {
        if !args.parseable {
            print!("{:20} ", "NAME");
        } else {
            print!("TIME,SENSOR");
        }

        for sp in &sps {
            print_value(format!(
                "{}-{}",
                crate::mgs::sp_type_to_str(&sp.type_).to_uppercase(),
                sp.slot
            ));
        }

        println!();
    };

    let print_name = |sensor: &Sensor, now: Duration| {
        if !args.parseable {
            print!("{:20} ", sensor.name);
        } else {
            print!(
                "{},{},{}",
                now.as_secs(),
                sensor.name,
                sensor.to_kind_string()
            );
        }
    };

    let mut wakeup =
        tokio::time::Instant::now() + tokio::time::Duration::from_millis(1000);

    print_header();

    loop {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;

        for sensor in &sensors {
            print_name(sensor, now);

            let by_sp = metadata.sensors_by_sensor_and_sp.get(sensor).unwrap();

            for sp in &sps {
                print_value(if let Some(id) = by_sp.get(sp) {
                    if let Some(value) = values.0.get(id) {
                        match value {
                            Some(value) => {
                                sensor.format(*value, args.parseable)
                            }
                            None => "X".to_string(),
                        }
                    } else {
                        "?".to_string()
                    }
                } else {
                    "-".to_string()
                });
            }

            println!();
        }

        if !args.sleep {
            break;
        }

        tokio::time::sleep_until(wakeup).await;
        wakeup += tokio::time::Duration::from_millis(1000);

        values = sensor_data(mgs_client, metadata.clone()).await?;

        if !args.parseable {
            print_header();
        }
    }

    Ok(())
}

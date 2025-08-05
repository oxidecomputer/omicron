// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the "mgs sensors" subcommand

use anyhow::{Context, bail};
use clap::Args;
use gateway_client::types::MeasurementErrorCode;
use gateway_client::types::MeasurementKind;
use gateway_client::types::SpComponentDetails;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpIgnition;
use multimap::MultiMap;
use nexus_types::inventory::SpType;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Args)]
pub(crate) struct SensorsArgs {
    /// verbose messages
    #[clap(long, short)]
    pub verbose: bool,

    /// restrict to specified sled(s)
    #[clap(long, use_value_delimiter = true)]
    pub sled: Vec<u16>,

    /// exclude sleds rather than include them
    #[clap(long, short)]
    pub exclude: bool,

    /// include switches
    #[clap(long)]
    pub switches: bool,

    /// include PSC
    #[clap(long)]
    pub psc: bool,

    /// print sensors every second
    #[clap(long, short)]
    pub sleep: bool,

    /// parseable output
    #[clap(long, short)]
    pub parseable: bool,

    /// show latencies
    #[clap(long)]
    pub show_latencies: bool,

    /// restrict sensors by type of sensor
    #[clap(
        long,
        short,
        value_name = "sensor type",
        use_value_delimiter = true
    )]
    pub types: Option<Vec<String>>,

    /// restrict sensors by name
    #[clap(
        long,
        short,
        value_name = "sensor name",
        use_value_delimiter = true
    )]
    pub named: Option<Vec<String>>,

    /// simulate using specified file as input
    #[clap(long, short)]
    pub input: Option<String>,

    /// start time, if using an input file
    #[clap(long, value_name = "time", requires = "input")]
    pub start: Option<u64>,

    /// end time, if using an input file
    #[clap(long, value_name = "time", requires = "input")]
    pub end: Option<u64>,

    /// duration, if using an input file
    #[clap(
        long,
        value_name = "seconds",
        requires = "input",
        conflicts_with = "end"
    )]
    pub duration: Option<u64>,
}

impl SensorsArgs {
    fn matches_sp(&self, sp: &SpIdentifier) -> bool {
        match sp.type_ {
            SpType::Sled => {
                let matched = if !self.sled.is_empty() {
                    self.sled.contains(&sp.slot)
                } else {
                    true
                };

                matched != self.exclude
            }
            SpType::Switch => self.switches,
            SpType::Power => self.psc,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Sensor {
    pub name: String,
    pub kind: MeasurementKind,
}

impl Sensor {
    fn units(&self) -> &str {
        match self.kind {
            MeasurementKind::CpuTctl => {
                // Part of me kiiiind of wants to make this "%", as Tctl is
                // always in the range of 0-100 --- it's essentially a sort of
                // abstract "thermal shutdown Badness Percentage". But, nobody
                // else seems to format it as a percentage AFAIK...
                ""
            }
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
                    //
                    // This space is deliberate:  other units (°C, V, A) look
                    // more natural when directly attached to their value --
                    // but RPM looks decidedly unnatural without a space.
                    //
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
            MeasurementKind::CpuTctl => "cpu-tctl",
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
            "cpu-tctl" => Some(MeasurementKind::CpuTctl),
            "temp" | "temperature" => Some(MeasurementKind::Temperature),
            "power" => Some(MeasurementKind::Power),
            "current" => Some(MeasurementKind::Current),
            "voltage" => Some(MeasurementKind::Voltage),
            "input-current" => Some(MeasurementKind::InputCurrent),
            "input-voltage" => Some(MeasurementKind::InputVoltage),
            "speed" => Some(MeasurementKind::Speed),
            _ => None,
        };

        k.map(|kind| Sensor { name: name.to_string(), kind })
    }
}

pub(crate) enum SensorInput<R> {
    MgsClient(gateway_client::Client),
    CsvReader(csv::Reader<R>, csv::Position),
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct SensorId(u32);

#[derive(Debug)]
pub(crate) struct SensorMetadata {
    pub sensors_by_sensor: MultiMap<Sensor, SensorId>,
    pub sensors_by_sensor_and_sp:
        HashMap<Sensor, HashMap<SpIdentifier, SensorId>>,
    pub sensors_by_id:
        HashMap<SensorId, (SpIdentifier, Sensor, DeviceIdentifier)>,
    pub sensors_by_sp: MultiMap<SpIdentifier, SensorId>,
    pub work_by_sp:
        HashMap<SpIdentifier, Vec<(DeviceIdentifier, Vec<SensorId>)>>,
    #[allow(dead_code)]
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
}

struct SensorSpInfo {
    info: Vec<(SpIdentifier, SpInfo)>,
    time: u64,
    latencies: Option<HashMap<SpIdentifier, Duration>>,
}

pub(crate) struct SensorValues {
    pub values: HashMap<SensorId, Option<f32>>,
    pub latencies: Option<HashMap<SpIdentifier, Duration>>,
    pub time: u64,
}

///
/// We identify a device as either a physical device (i.e., when connecting
/// to MGS), or as a field in the CSV header (i.e., when processing data
/// postmortem.  It's handy to have this as enum to allow most of the code
/// to be agnostic to the underlying source, but callers of ['device'] and
/// ['field'] are expected to know which of these they're dealing with.
///
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum DeviceIdentifier {
    Field(usize),
    Device(String),
}

impl DeviceIdentifier {
    fn device(&self) -> &String {
        match self {
            Self::Device(ref device) => device,
            _ => panic!(),
        }
    }

    fn field(&self) -> usize {
        match self {
            Self::Field(field) => *field,
            _ => panic!(),
        }
    }
}

struct SpInfo {
    devices: MultiMap<DeviceIdentifier, (Sensor, Option<f32>)>,
    timestamps: Vec<std::time::Instant>,
}

async fn sp_info(
    mgs_client: gateway_client::Client,
    type_: SpType,
    slot: u16,
) -> Result<SpInfo, anyhow::Error> {
    let mut devices = MultiMap::new();
    let mut timestamps = vec![];

    timestamps.push(std::time::Instant::now());

    //
    // First, get a component list.
    //
    let components = mgs_client.sp_component_list(&type_, slot).await?;
    timestamps.push(std::time::Instant::now());

    //
    // Now, for every component, we're going to get its details:  for those
    // that are sensors (and contain measurements), we will store the name
    // of the sensor as well as the retrieved value.
    //
    for c in &components.components {
        for s in mgs_client
            .sp_component_get(&type_, slot, &c.component)
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
            devices.insert(DeviceIdentifier::Device(c.component.clone()), s);
        }
    }

    timestamps.push(std::time::Instant::now());

    Ok(SpInfo { devices, timestamps })
}

async fn sp_info_mgs(
    mgs_client: &gateway_client::Client,
    args: &SensorsArgs,
) -> Result<SensorSpInfo, anyhow::Error> {
    let mut rval = vec![];
    let mut latencies = HashMap::new();

    //
    // First, get all of the SPs that we can see via Ignition
    //
    let all_sp_list =
        mgs_client.ignition_list().await.context("listing ignition")?;

    let mut sp_list = all_sp_list
        .iter()
        .filter_map(|ignition| {
            if matches!(ignition.details, SpIgnition::Yes { .. })
                && ignition.id.type_ == SpType::Sled
            {
                if args.matches_sp(&ignition.id) {
                    return Some(ignition.id);
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
        let handle =
            tokio::spawn(sp_info(mgs_client.clone(), sp_id.type_, sp_id.slot));

        handles.push((sp_id, handle));
    }

    for (sp_id, handle) in handles {
        match handle.await.unwrap() {
            Ok(info) => {
                let l0 = info.timestamps[1].duration_since(info.timestamps[0]);
                let l1 = info.timestamps[2].duration_since(info.timestamps[1]);

                if args.verbose {
                    eprintln!(
                        "mgs: latencies for {sp_id:?}: {l1:.1?} {l0:.1?}",
                    );
                }

                latencies.insert(sp_id, l0 + l1);
                rval.push((sp_id, info));
            }

            Err(err) => {
                eprintln!("failed to read devices for {:?}: {:?}", sp_id, err);
            }
        }
    }

    if args.verbose {
        eprintln!("total discovery time {:?}", now.elapsed());
    }

    Ok(SensorSpInfo {
        info: rval,
        time: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        latencies: Some(latencies),
    })
}

fn sp_info_csv<R: std::io::Read>(
    reader: &mut csv::Reader<R>,
    position: &mut csv::Position,
    args: &SensorsArgs,
) -> Result<SensorSpInfo, anyhow::Error> {
    let mut sps = vec![];
    let headers = reader.headers()?;

    let expected = ["TIME", "SENSOR", "KIND"];
    let len = expected.len();
    let hlen = headers.len();

    if hlen < len {
        bail!("expected as least {len} fields (found {headers:?})");
    }

    for ndx in 0..len {
        if &headers[ndx] != expected[ndx] {
            bail!(
                "malformed headers: expected {}, found {} ({headers:?})",
                &expected[ndx],
                &headers[ndx]
            );
        }
    }

    for ndx in len..hlen {
        let field = &headers[ndx];
        let parts: Vec<&str> = field.splitn(2, '-').collect();

        if parts.len() != 2 {
            bail!("malformed field \"{field}\"");
        }

        let type_ = match parts[0] {
            "SLED" => SpType::Sled,
            "SWITCH" => SpType::Switch,
            "POWER" => SpType::Power,
            _ => {
                bail!("unknown type {}", parts[0]);
            }
        };

        let slot = parts[1].parse::<u16>().or_else(|_| {
            bail!("invalid slot in \"{field}\"");
        })?;

        let sp = SpIdentifier { type_, slot };

        if args.matches_sp(&sp) {
            sps.push(Some(sp));
        } else {
            sps.push(None);
        }
    }

    let mut iter = reader.records();
    let mut sensors = HashSet::new();
    let mut by_sp = MultiMap::new();
    let mut time = None;

    loop {
        *position = iter.reader().position().clone();

        if let Some(record) = iter.next() {
            let record = record?;

            if record.len() != hlen {
                bail!("bad record length at line {}", position.line());
            }

            if time.is_none() {
                let t = record[0].parse::<u64>().or_else(|_| {
                    bail!("bad time at line {}", position.line());
                })?;

                if let Some(start) = args.start {
                    if t < start {
                        continue;
                    }
                }

                if let Some(end) = args.end {
                    if let Some(start) = args.start {
                        if start > end {
                            bail!(
                                "specified start time is later than end time"
                            );
                        }
                    }

                    if t > end {
                        bail!(
                            "specified end time ({end}) is earlier \
                            than time of earliest record ({t})"
                        );
                    }
                }

                time = Some(t);
            }

            if let Some(sensor) = Sensor::from_string(&record[1], &record[2]) {
                if !sensors.insert(sensor.clone()) {
                    break;
                }

                for (ndx, sp) in sps.iter().enumerate() {
                    if let Some(sp) = sp {
                        let value = match record[ndx + len].parse::<f32>() {
                            Ok(value) => Some(value),
                            _ => {
                                //
                                // We want to distinguish between the device
                                // having an error ("X") and it being absent
                                // ("-"); if it's absent, we don't want to add
                                // it at all.
                                //
                                match &record[ndx + len] {
                                    "X" => {}
                                    "-" => continue,
                                    _ => {
                                        bail!(
                                            "line {}: unrecognized value \
                                            \"{}\" in field {}",
                                            position.line(),
                                            record[ndx + len].to_string(),
                                            ndx + len
                                        );
                                    }
                                }

                                None
                            }
                        };

                        by_sp.insert(sp, (sensor.clone(), value));
                    }
                }
            }
        } else {
            break;
        }
    }

    if time.is_none() {
        bail!("no data found");
    }

    let mut rval = vec![];

    for (field, sp) in sps.iter().enumerate() {
        let mut devices = MultiMap::new();

        if let Some(sp) = sp {
            if let Some(v) = by_sp.remove(sp) {
                devices.insert_many(DeviceIdentifier::Field(field + len), v);
            }

            rval.push((*sp, SpInfo { devices, timestamps: vec![] }));
        }
    }

    Ok(SensorSpInfo { info: rval, time: time.unwrap(), latencies: None })
}

pub(crate) async fn sensor_metadata<R: std::io::Read>(
    input: &mut SensorInput<R>,
    args: &SensorsArgs,
) -> Result<(Arc<SensorMetadata>, SensorValues), anyhow::Error> {
    let by_kind = if let Some(types) = &args.types {
        let mut h = HashSet::new();

        for t in types {
            h.insert(match Sensor::from_string("", t) {
                None => bail!("invalid sensor kind {t}"),
                Some(s) => s.kind,
            });
        }

        Some(h)
    } else {
        None
    };

    let by_name = args
        .named
        .as_ref()
        .map(|named| named.into_iter().collect::<HashSet<_>>());

    let info = match input {
        SensorInput::MgsClient(ref mgs_client) => {
            sp_info_mgs(mgs_client, args).await?
        }
        SensorInput::CsvReader(reader, position) => {
            sp_info_csv(reader, position, args)?
        }
    };

    let mut sensors_by_sensor = MultiMap::new();
    let mut sensors_by_sensor_and_sp = HashMap::new();
    let mut sensors_by_id = HashMap::new();
    let mut sensors_by_sp = MultiMap::new();
    let mut values = HashMap::new();
    let mut work_by_sp = HashMap::new();

    let mut current = 0;
    let time = info.time;

    for (sp_id, info) in info.info {
        let mut sp_work = vec![];

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

                sensors_by_id
                    .insert(id, (sp_id, sensor.clone(), device.clone()));

                if value.is_none() && args.verbose {
                    eprintln!(
                        "mgs: error for {sp_id:?} on {sensor:?} ({device:?})"
                    );
                }

                sensors_by_sensor.insert(sensor.clone(), id);

                let by_sp = sensors_by_sensor_and_sp
                    .entry(sensor)
                    .or_insert_with(|| HashMap::new());
                by_sp.insert(sp_id, id);
                sensors_by_sp.insert(sp_id, id);
                values.insert(id, value);

                device_work.push(id);
            }

            sp_work.push((device, device_work));
        }

        work_by_sp.insert(sp_id, sp_work);
    }

    Ok((
        Arc::new(SensorMetadata {
            sensors_by_sensor,
            sensors_by_sensor_and_sp,
            sensors_by_id,
            sensors_by_sp,
            work_by_sp,
            start_time: args.start,
            end_time: match args.end {
                Some(end) => Some(end),
                None => args.duration.map(|duration| time + duration),
            },
        }),
        SensorValues { values, time, latencies: info.latencies },
    ))
}

async fn sp_read_sensors(
    mgs_client: &gateway_client::Client,
    id: &SpIdentifier,
    metadata: &SensorMetadata,
) -> Result<(Vec<(SensorId, Option<f32>)>, Duration), anyhow::Error> {
    let work = metadata.work_by_sp.get(id).unwrap();
    let mut rval = vec![];

    let start = std::time::Instant::now();

    for (component, ids) in work.iter() {
        for (value, id) in mgs_client
            .sp_component_get(&id.type_, id.slot, component.device())
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

    Ok((rval, start.elapsed()))
}

async fn sp_data_mgs(
    mgs_client: &gateway_client::Client,
    metadata: &Arc<SensorMetadata>,
) -> Result<SensorValues, anyhow::Error> {
    let mut values = HashMap::new();
    let mut latencies = HashMap::new();
    let mut handles = vec![];

    for sp_id in metadata.sensors_by_sp.keys() {
        let mgs_client = mgs_client.clone();
        let id = *sp_id;
        let metadata = Arc::clone(&metadata);

        let handle = tokio::spawn(async move {
            sp_read_sensors(&mgs_client, &id, &metadata).await
        });

        handles.push((id, handle));
    }

    for (id, handle) in handles {
        let (rval, latency) = handle.await.unwrap()?;

        latencies.insert(id, latency);

        for (id, value) in rval {
            values.insert(id, value);
        }
    }

    Ok(SensorValues {
        values,
        latencies: Some(latencies),
        time: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    })
}

fn sp_data_csv<R: std::io::Read + std::io::Seek>(
    reader: &mut csv::Reader<R>,
    position: &mut csv::Position,
    metadata: &SensorMetadata,
) -> Result<SensorValues, anyhow::Error> {
    let headers = reader.headers()?;
    let hlen = headers.len();
    let mut values = HashMap::new();

    reader.seek(position.clone())?;
    let mut iter = reader.records();

    let mut time = None;

    loop {
        *position = iter.reader().position().clone();

        if let Some(record) = iter.next() {
            let record = record?;

            if record.len() != hlen {
                bail!("bad record length at line {}", position.line());
            }

            let now = record[0].parse::<u64>().or_else(|_| {
                bail!("bad time at line {}", position.line());
            })?;

            if let Some(time) = time {
                if now != time {
                    break;
                }
            } else {
                if let Some(end) = metadata.end_time {
                    if now > end {
                        time = Some(0);
                        break;
                    }
                }

                time = Some(now);
            }

            if let Some(sensor) = Sensor::from_string(&record[1], &record[2]) {
                if let Some(ids) = metadata.sensors_by_sensor.get_vec(&sensor) {
                    for id in ids {
                        let (_, _, d) = metadata.sensors_by_id.get(id).unwrap();
                        let value = record[d.field()].parse::<f32>().ok();

                        values.insert(*id, value);
                    }
                }
            } else {
                bail!("bad sensor at line {}", position.line());
            }
        } else {
            time = Some(0);
            break;
        }
    }

    Ok(SensorValues { values, latencies: None, time: time.unwrap() })
}

pub(crate) async fn sensor_data<R: std::io::Read + std::io::Seek>(
    input: &mut SensorInput<R>,
    metadata: &Arc<SensorMetadata>,
) -> Result<SensorValues, anyhow::Error> {
    match input {
        SensorInput::MgsClient(ref mgs_client) => {
            sp_data_mgs(mgs_client, metadata).await
        }
        SensorInput::CsvReader(reader, position) => {
            sp_data_csv(reader, position, &metadata)
        }
    }
}

///
/// Runs `omdb mgs sensors`
///
pub(crate) async fn cmd_mgs_sensors(
    omdb: &crate::Omdb,
    log: &slog::Logger,
    mgs_args: &crate::mgs::MgsArgs,
    args: &SensorsArgs,
) -> Result<(), anyhow::Error> {
    let mut input = if let Some(ref input) = args.input {
        let file = File::open(input)
            .with_context(|| format!("failed to open {input}"))?;
        SensorInput::CsvReader(
            csv::Reader::from_reader(file),
            csv::Position::new(),
        )
    } else {
        SensorInput::MgsClient(mgs_args.mgs_client(omdb, log).await?)
    };

    let (metadata, mut values) = sensor_metadata(&mut input, args).await?;

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
            print!("TIME,SENSOR,KIND");
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

    let print_name = |sensor: &Sensor, now: u64| {
        if !args.parseable {
            print!("{:20} ", sensor.name);
        } else {
            print!("{now},{},{}", sensor.name, sensor.to_kind_string());
        }
    };

    let print_latency = |now: u64| {
        if !args.parseable {
            print!("{:20} ", "LATENCY");
        } else {
            print!("{now},{},{}", "LATENCY", "latency");
        }
    };

    let mut wakeup =
        tokio::time::Instant::now() + tokio::time::Duration::from_millis(1000);

    print_header();

    loop {
        for sensor in &sensors {
            print_name(sensor, values.time);

            let by_sp = metadata.sensors_by_sensor_and_sp.get(sensor).unwrap();

            for sp in &sps {
                print_value(if let Some(id) = by_sp.get(sp) {
                    if let Some(value) = values.values.get(id) {
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

        if args.show_latencies {
            if let Some(latencies) = values.latencies {
                print_latency(values.time);

                for sp in &sps {
                    print_value(if let Some(latency) = latencies.get(sp) {
                        format!("{}ms", latency.as_millis())
                    } else {
                        "?".to_string()
                    });
                }
            }

            println!();
        }

        if !args.sleep {
            if args.input.is_none() {
                break;
            }
        } else {
            tokio::time::sleep_until(wakeup).await;
            wakeup += tokio::time::Duration::from_millis(1000);
        }

        values = sensor_data(&mut input, &metadata).await?;

        if args.input.is_some() && values.time == 0 {
            break;
        }

        if !args.parseable {
            print_header();
        }
    }

    Ok(())
}

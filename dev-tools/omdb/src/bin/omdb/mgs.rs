// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Prototype code for collecting information from systems in the rack

use crate::Omdb;
use anyhow::{bail, Context};
use clap::Args;
use clap::Subcommand;
use futures::StreamExt;
use gateway_client::types::MeasurementErrorCode;
use gateway_client::types::MeasurementKind;
use gateway_client::types::PowerState;
use gateway_client::types::RotSlot;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpComponentDetails;
use gateway_client::types::SpComponentInfo;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpIgnition;
use gateway_client::types::SpIgnitionInfo;
use gateway_client::types::SpIgnitionSystemType;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use multimap::MultiMap;
use tabled::Tabled;

use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};

/// Arguments to the "omdb mgs" subcommand
#[derive(Debug, Args)]
pub struct MgsArgs {
    /// URL of an MGS instance to query
    #[clap(long, env("OMDB_MGS_URL"))]
    mgs_url: Option<String>,

    #[command(subcommand)]
    command: MgsCommands,
}

#[derive(Debug, Subcommand)]
enum MgsCommands {
    /// Dashboard of SPs
    Dashboard(DashboardArgs),

    /// Show information about devices and components visible to MGS
    Inventory(InventoryArgs),

    /// Show information about sensors, as gleaned by MGS
    Sensors(SensorsArgs),
}

#[derive(Debug, Args)]
struct DashboardArgs {}

#[derive(Debug, Args)]
struct InventoryArgs {}

#[derive(Debug, Args)]
struct SensorsArgs {
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

impl MgsArgs {
    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        let mgs_url = match &self.mgs_url {
            Some(cli_or_env_url) => cli_or_env_url.clone(),
            None => {
                eprintln!(
                    "note: MGS URL not specified.  Will pick one from DNS."
                );
                let addrs = omdb
                    .dns_lookup_all(
                        log.clone(),
                        internal_dns::ServiceName::ManagementGatewayService,
                    )
                    .await?;
                let addr = addrs.into_iter().next().expect(
                    "expected at least one MGS address from \
                    successful DNS lookup",
                );
                format!("http://{}", addr)
            }
        };
        eprintln!("note: using MGS URL {}", &mgs_url);
        let mgs_client = gateway_client::Client::new(&mgs_url, log.clone());

        match &self.command {
            MgsCommands::Dashboard(dashboard_args) => {
                cmd_mgs_dashboard(&mgs_client, dashboard_args).await
            }
            MgsCommands::Inventory(inventory_args) => {
                cmd_mgs_inventory(&mgs_client, inventory_args).await
            }
            MgsCommands::Sensors(sensors_args) => {
                cmd_mgs_sensors(&mgs_client, sensors_args).await
            }
        }
    }
}

///
/// Runs `omdb mgs dashboard`
///
async fn cmd_mgs_dashboard(
    _mgs_client: &gateway_client::Client,
    _args: &DashboardArgs,
) -> Result<(), anyhow::Error> {
    anyhow::bail!("not yet");
}

/// Runs `omdb mgs inventory`
///
/// Shows devices and components that are visible to an MGS instance.
async fn cmd_mgs_inventory(
    mgs_client: &gateway_client::Client,
    _args: &InventoryArgs,
) -> Result<(), anyhow::Error> {
    // Report all the SP identifiers that MGS is configured to talk to.
    println!("ALL CONFIGURED SPs\n");
    let mut sp_ids = mgs_client
        .sp_all_ids()
        .await
        .context("listing SP identifiers")?
        .into_inner();
    sp_ids.sort();
    show_sp_ids(&sp_ids)?;
    println!("");

    // Report which SPs are visible via Ignition.
    println!("SPs FOUND THROUGH IGNITION\n");
    let mut sp_list_ignition = mgs_client
        .ignition_list()
        .await
        .context("listing ignition")?
        .into_inner();
    sp_list_ignition.sort_by(|a, b| a.id.cmp(&b.id));
    show_sps_from_ignition(&sp_list_ignition)?;
    println!("");

    // Print basic state about each SP that's visible to ignition.
    println!("SERVICE PROCESSOR STATES\n");
    let mgs_client = std::sync::Arc::new(mgs_client);
    let c = &mgs_client;
    let mut sp_infos =
        futures::stream::iter(sp_list_ignition.iter().filter_map(|ignition| {
            if matches!(ignition.details, SpIgnition::Yes { .. }) {
                Some(ignition.id)
            } else {
                None
            }
        }))
        .then(|sp_id| async move {
            c.sp_get(sp_id.type_, sp_id.slot)
                .await
                .with_context(|| format!("fetching info about SP {:?}", sp_id))
                .map(|s| (sp_id, s))
        })
        .collect::<Vec<Result<_, _>>>()
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok((sp_id, v)) => Some((sp_id, v.into_inner())),
            Err(error) => {
                eprintln!("error: {:?}", error);
                None
            }
        })
        .collect::<Vec<_>>();
    sp_infos.sort();
    show_sp_states(&sp_infos)?;
    println!("");

    // Print detailed information about each SP that we've found so far.
    for (sp_id, sp_state) in &sp_infos {
        show_sp_details(&mgs_client, sp_id, sp_state).await?;
    }

    Ok(())
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

    pub fn to_kind_string(&self) -> &str {
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

    pub fn from_string(name: &str, kind: &str) -> Option<Self> {
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
async fn cmd_mgs_sensors(
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
                sp_type_to_str(&sp.type_).to_uppercase(),
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

fn sp_type_to_str(s: &SpType) -> &'static str {
    match s {
        SpType::Sled => "Sled",
        SpType::Power => "Power",
        SpType::Switch => "Switch",
    }
}

fn show_sp_ids(sp_ids: &[SpIdentifier]) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SpIdRow {
        #[tabled(rename = "TYPE")]
        type_: &'static str,
        slot: u32,
    }

    impl<'a> From<&'a SpIdentifier> for SpIdRow {
        fn from(id: &SpIdentifier) -> Self {
            SpIdRow { type_: sp_type_to_str(&id.type_), slot: id.slot }
        }
    }

    let table_rows = sp_ids.iter().map(SpIdRow::from);
    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", textwrap::indent(&table.to_string(), "    "));
    Ok(())
}

fn show_sps_from_ignition(
    sp_list_ignition: &[SpIgnitionInfo],
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct IgnitionRow {
        #[tabled(rename = "TYPE")]
        type_: &'static str,
        slot: u32,
        system_type: String,
    }

    impl<'a> From<&'a SpIgnitionInfo> for IgnitionRow {
        fn from(value: &SpIgnitionInfo) -> Self {
            IgnitionRow {
                type_: sp_type_to_str(&value.id.type_),
                slot: value.id.slot,
                system_type: match value.details {
                    SpIgnition::No => "-".to_string(),
                    SpIgnition::Yes {
                        id: SpIgnitionSystemType::Gimlet,
                        ..
                    } => "Gimlet".to_string(),
                    SpIgnition::Yes {
                        id: SpIgnitionSystemType::Sidecar,
                        ..
                    } => "Sidecar".to_string(),
                    SpIgnition::Yes {
                        id: SpIgnitionSystemType::Psc, ..
                    } => "PSC".to_string(),
                    SpIgnition::Yes {
                        id: SpIgnitionSystemType::Unknown(v),
                        ..
                    } => format!("unknown: type {}", v),
                },
            }
        }
    }

    let table_rows = sp_list_ignition.iter().map(IgnitionRow::from);
    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", textwrap::indent(&table.to_string(), "    "));
    Ok(())
}

fn show_sp_states(
    sp_states: &[(SpIdentifier, SpState)],
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SpStateRow<'a> {
        #[tabled(rename = "TYPE")]
        type_: &'static str,
        slot: u32,
        model: String,
        serial: String,
        rev: u32,
        hubris: &'a str,
        pwr: &'static str,
        rot_active: String,
    }

    impl<'a> From<&'a (SpIdentifier, SpState)> for SpStateRow<'a> {
        fn from((id, v): &'a (SpIdentifier, SpState)) -> Self {
            SpStateRow {
                type_: sp_type_to_str(&id.type_),
                slot: id.slot,
                model: v.model.clone(),
                serial: v.serial_number.clone(),
                rev: v.revision,
                hubris: &v.hubris_archive_id,
                pwr: match v.power_state {
                    PowerState::A0 => "A0",
                    PowerState::A1 => "A1",
                    PowerState::A2 => "A2",
                },
                rot_active: match &v.rot {
                    RotState::CommunicationFailed { message } => {
                        format!("error: {}", message)
                    }
                    RotState::Enabled { active: RotSlot::A, .. } => {
                        "slot A".to_string()
                    }
                    RotState::Enabled { active: RotSlot::B, .. } => {
                        "slot B".to_string()
                    }
                },
            }
        }
    }

    let table_rows = sp_states.iter().map(SpStateRow::from);
    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", textwrap::indent(&table.to_string(), "    "));
    Ok(())
}

const COMPONENTS_WITH_CABOOSES: &'static [&'static str] = &["sp", "rot"];

async fn show_sp_details(
    mgs_client: &gateway_client::Client,
    sp_id: &SpIdentifier,
    sp_state: &SpState,
) -> Result<(), anyhow::Error> {
    println!(
        "SP DETAILS: type {:?} slot {}\n",
        sp_type_to_str(&sp_id.type_),
        sp_id.slot
    );

    println!("    ROOT OF TRUST\n");
    match &sp_state.rot {
        RotState::CommunicationFailed { message } => {
            println!("        error: {}", message);
        }
        RotState::Enabled {
            active,
            pending_persistent_boot_preference,
            persistent_boot_preference,
            slot_a_sha3_256_digest,
            slot_b_sha3_256_digest,
            transient_boot_preference,
        } => {
            #[derive(Tabled)]
            #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
            struct Row {
                name: &'static str,
                value: String,
            }

            let rows = vec![
                Row {
                    name: "active slot",
                    value: format!("slot {:?}", active),
                },
                Row {
                    name: "persistent boot preference",
                    value: format!("slot {:?}", persistent_boot_preference),
                },
                Row {
                    name: "pending persistent boot preference",
                    value: pending_persistent_boot_preference
                        .map(|s| format!("slot {:?}", s))
                        .unwrap_or_else(|| "-".to_string()),
                },
                Row {
                    name: "transient boot preference",
                    value: transient_boot_preference
                        .map(|s| format!("slot {:?}", s))
                        .unwrap_or_else(|| "-".to_string()),
                },
                Row {
                    name: "slot A SHA3 256 digest",
                    value: slot_a_sha3_256_digest
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                },
                Row {
                    name: "slot B SHA3 256 digest",
                    value: slot_b_sha3_256_digest
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                },
            ];

            let table = tabled::Table::new(rows)
                .with(tabled::settings::Style::empty())
                .with(tabled::settings::Padding::new(0, 1, 0, 0))
                .to_string();
            println!("{}", textwrap::indent(&table.to_string(), "        "));
            println!("");
        }
    }

    let component_list = mgs_client
        .sp_component_list(sp_id.type_, sp_id.slot)
        .await
        .with_context(|| format!("fetching components for SP {:?}", sp_id));
    let list = match component_list {
        Ok(l) => l.into_inner(),
        Err(e) => {
            eprintln!("error: {:#}", e);
            return Ok(());
        }
    };

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SpComponentRow<'a> {
        name: &'a str,
        description: &'a str,
        device: &'a str,
        presence: String,
        serial: String,
    }

    impl<'a> From<&'a SpComponentInfo> for SpComponentRow<'a> {
        fn from(v: &'a SpComponentInfo) -> Self {
            SpComponentRow {
                name: &v.component,
                description: &v.description,
                device: &v.device,
                presence: format!("{:?}", v.presence),
                serial: format!("{:?}", v.serial_number),
            }
        }
    }

    if list.components.is_empty() {
        println!("    COMPONENTS: none found\n");
        return Ok(());
    }

    let table_rows = list.components.iter().map(SpComponentRow::from);
    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("    COMPONENTS\n");
    println!("{}", textwrap::indent(&table.to_string(), "        "));
    println!("");

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct CabooseRow {
        component: String,
        board: String,
        git_commit: String,
        name: String,
        version: String,
    }

    impl<'a> From<(&'a SpIdentifier, &'a SpComponentInfo, SpComponentCaboose)>
        for CabooseRow
    {
        fn from(
            (_sp_id, component, caboose): (
                &'a SpIdentifier,
                &'a SpComponentInfo,
                SpComponentCaboose,
            ),
        ) -> Self {
            CabooseRow {
                component: component.component.clone(),
                board: caboose.board,
                git_commit: caboose.git_commit,
                name: caboose.name,
                version: caboose.version,
            }
        }
    }

    let mut cabooses = Vec::new();
    for c in &list.components {
        if !COMPONENTS_WITH_CABOOSES.contains(&c.component.as_str()) {
            continue;
        }

        for i in 0..1 {
            let r = mgs_client
                .sp_component_caboose_get(
                    sp_id.type_,
                    sp_id.slot,
                    &c.component,
                    i,
                )
                .await
                .with_context(|| {
                    format!(
                        "get caboose for sp type {:?} sp slot {} \
                        component {:?} slot {}",
                        sp_id.type_, sp_id.slot, &c.component, i
                    )
                });
            match r {
                Ok(v) => {
                    cabooses.push(CabooseRow::from((sp_id, c, v.into_inner())))
                }
                Err(error) => {
                    eprintln!("warn: {:#}", error);
                }
            }
        }
    }

    if cabooses.is_empty() {
        println!("    CABOOSES: none found\n");
        return Ok(());
    }

    let table = tabled::Table::new(cabooses)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("    COMPONENT CABOOSES\n");
    println!("{}", textwrap::indent(&table.to_string(), "        "));
    println!("");

    Ok(())
}

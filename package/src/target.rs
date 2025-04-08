// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Type safety for target parsing

use anyhow::{Result, bail};
use clap::ValueEnum;
use omicron_zone_package::target::TargetMap;
use std::collections::BTreeMap;

/// Type of OS image to build
#[derive(Clone, Debug, strum::EnumString, strum::Display, ValueEnum)]
#[strum(serialize_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum Image {
    /// A typical host OS image
    Standard,
    /// A recovery host OS image, intended to bootstrap a Standard image
    Trampoline,
}

/// Classification of the target machine
#[derive(Clone, Debug, strum::EnumString, strum::Display, ValueEnum)]
#[strum(serialize_all = "kebab-case")]
#[clap(rename_all = "kebab-case")]
pub enum Machine {
    /// Use sled agent configuration for a Gimlet
    Gimlet,
    /// Use sled agent configuration for a Gimlet running in isolation
    GimletStandalone,
    /// Use sled agent configuration for a device emulating a Gimlet
    ///
    /// Note that this configuration can actually work on real gimlets,
    /// it just relies on "cargo xtask virtual-hardware create".
    NonGimlet,
}

#[derive(Clone, Debug, strum::EnumString, strum::Display, ValueEnum)]
#[strum(serialize_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum Switch {
    /// Use the "real" Dendrite, that attempts to interact with the Tofino
    Asic,
    /// Use a "stub" Dendrite that does not require any real hardware
    Stub,
    /// Use a "softnpu" Dendrite that uses the SoftNPU asic emulator
    SoftNpu,
}

/// Topology of the sleds within the rack.
#[derive(Clone, Debug, strum::EnumString, strum::Display, ValueEnum)]
#[strum(serialize_all = "kebab-case")]
#[clap(rename_all = "kebab-case")]
pub enum RackTopology {
    /// Use configurations suitable for a multi-sled deployment, such as dogfood
    /// and production racks.
    MultiSled,

    /// Use configurations suitable for a single-sled deployment, such as CI and
    /// dev machines.
    SingleSled,
}

/// Topology of the ClickHouse installation within the rack.
#[derive(Clone, Debug, strum::EnumString, strum::Display, ValueEnum)]
#[strum(serialize_all = "kebab-case")]
#[clap(rename_all = "kebab-case")]
pub enum ClickhouseTopology {
    /// Use configurations suitable for a replicated ClickHouse cluster deployment.
    ReplicatedCluster,

    /// Use configurations suitable for a single-node ClickHouse deployment.
    SingleNode,
}

/// A strongly-typed variant of [`TargetMap`].
#[derive(Clone, Debug)]
pub struct KnownTarget {
    image: Image,
    machine: Option<Machine>,
    switch: Option<Switch>,
    rack_topology: RackTopology,
    clickhouse_topology: ClickhouseTopology,
}

impl KnownTarget {
    /// Creates a new `KnownTarget` from a [`TargetMap`] defined in configuration.
    ///
    /// Real `KnownTarget` instances might have overrides applied to them via
    /// the command line.
    pub fn from_target_map(target: &TargetMap) -> Result<Self> {
        let mut image = Self::default().image;
        let mut machine = None;
        let mut switch = None;
        let mut rack_topology = None;
        let mut clickhouse_topology = None;

        for (k, v) in &target.0 {
            match k.as_str() {
                "image" => {
                    image = v.parse()?;
                }
                "machine" => {
                    machine = Some(v.parse()?);
                }
                "switch" => {
                    switch = Some(v.parse()?);
                }
                "rack-topology" => {
                    rack_topology = Some(v.parse()?);
                }
                "clickhouse-topology" => {
                    clickhouse_topology = Some(v.parse()?);
                }
                _ => {
                    bail!(
                        "Unknown target key {k}\nValid keys include: [{}]",
                        TargetMap::from(Self::default())
                            .0
                            .keys()
                            .cloned()
                            .collect::<Vec<String>>()
                            .join(", "),
                    )
                }
            }
        }

        Self::validate_and_create(
            image,
            machine,
            switch,
            rack_topology.unwrap_or(RackTopology::MultiSled),
            clickhouse_topology.unwrap_or(ClickhouseTopology::SingleNode),
        )
    }

    /// Applies overrides to the target, returning a new `KnownTarget` with the
    /// parameters applied.
    ///
    /// Errors if the new target does not satisfy target constraints.
    pub fn with_overrides(
        &self,
        image: Option<Image>,
        machine: Option<Machine>,
        switch: Option<Switch>,
        rack_topology: Option<RackTopology>,
        clickhouse_topology: Option<ClickhouseTopology>,
    ) -> Result<Self> {
        let image = image.unwrap_or(self.image.clone());
        let machine = machine.or(self.machine.clone());
        let switch = switch.or(self.switch.clone());
        let rack_topology = rack_topology.unwrap_or(self.rack_topology.clone());
        let clickhouse_topology =
            clickhouse_topology.unwrap_or(self.clickhouse_topology.clone());

        Self::validate_and_create(
            image,
            machine,
            switch,
            rack_topology,
            clickhouse_topology,
        )
    }

    /// Creates a new `KnownTarget` from the given parameters, validating
    /// constraints.
    fn validate_and_create(
        image: Image,
        machine: Option<Machine>,
        switch: Option<Switch>,
        rack_topology: RackTopology,
        clickhouse_topology: ClickhouseTopology,
    ) -> Result<Self> {
        if matches!(image, Image::Trampoline) {
            if machine.is_some() {
                bail!(
                    "Trampoline image does not execute the sled agent (do not pass 'machine' flag)"
                );
            }
            if switch.is_some() {
                bail!(
                    "Trampoline image does not contain switch zone (do not pass 'switch' flag)"
                );
            }
        }

        if !matches!(machine, Some(Machine::Gimlet))
            && matches!(switch, Some(Switch::Asic))
        {
            bail!("'switch=asic' is only valid with 'machine=gimlet'");
        }

        Ok(Self { image, machine, switch, rack_topology, clickhouse_topology })
    }
}

impl Default for KnownTarget {
    fn default() -> Self {
        KnownTarget {
            image: Image::Standard,
            machine: Some(Machine::NonGimlet),
            switch: Some(Switch::Stub),
            rack_topology: RackTopology::MultiSled,
            clickhouse_topology: ClickhouseTopology::SingleNode,
        }
    }
}

impl From<KnownTarget> for TargetMap {
    fn from(kt: KnownTarget) -> TargetMap {
        let mut map = BTreeMap::new();
        map.insert("image".to_string(), kt.image.to_string());
        if let Some(machine) = kt.machine {
            map.insert("machine".to_string(), machine.to_string());
        }
        if let Some(switch) = kt.switch {
            map.insert("switch".to_string(), switch.to_string());
        }
        map.insert("rack-topology".to_string(), kt.rack_topology.to_string());
        map.insert(
            "clickhouse-topology".to_string(),
            kt.clickhouse_topology.to_string(),
        );
        TargetMap(map)
    }
}

impl std::fmt::Display for KnownTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let target: TargetMap = self.clone().into();
        target.fmt(f)
    }
}

impl std::str::FromStr for KnownTarget {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let target = TargetMap::from_str(s)?;
        Self::from_target_map(&target)
    }
}

/// Generate a command to build a target, for use in usage strings.
pub fn target_command_help(target_name: &str) -> String {
    format!(
        "{} -t {target_name} target",
        std::env::current_exe().unwrap().display(),
    )
}

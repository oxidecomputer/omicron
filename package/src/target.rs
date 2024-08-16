// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Type safety for target parsing

use anyhow::{bail, Result};
use clap::ValueEnum;
use omicron_zone_package::target::Target;
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

/// A strongly-typed variant of [Target].
#[derive(Clone, Debug)]
pub struct KnownTarget {
    image: Image,
    machine: Option<Machine>,
    switch: Option<Switch>,
    rack_topology: RackTopology,
}

impl KnownTarget {
    pub fn new(
        image: Image,
        machine: Option<Machine>,
        switch: Option<Switch>,
        rack_topology: RackTopology,
    ) -> Result<Self> {
        if matches!(image, Image::Trampoline) {
            if machine.is_some() {
                bail!("Trampoline image does not execute the sled agent (do not pass 'machine' flag)");
            }
            if switch.is_some() {
                bail!("Trampoline image does not contain switch zone (do not pass 'switch' flag)");
            }
        }

        if !matches!(machine, Some(Machine::Gimlet))
            && matches!(switch, Some(Switch::Asic))
        {
            bail!("'switch=asic' is only valid with 'machine=gimlet'");
        }

        Ok(Self { image, machine, switch, rack_topology })
    }
}

impl Default for KnownTarget {
    fn default() -> Self {
        KnownTarget {
            image: Image::Standard,
            machine: Some(Machine::NonGimlet),
            switch: Some(Switch::Stub),
            rack_topology: RackTopology::MultiSled,
        }
    }
}

impl From<KnownTarget> for Target {
    fn from(kt: KnownTarget) -> Target {
        let mut map = BTreeMap::new();
        map.insert("image".to_string(), kt.image.to_string());
        if let Some(machine) = kt.machine {
            map.insert("machine".to_string(), machine.to_string());
        }
        if let Some(switch) = kt.switch {
            map.insert("switch".to_string(), switch.to_string());
        }
        map.insert("rack-topology".to_string(), kt.rack_topology.to_string());
        Target(map)
    }
}

impl std::fmt::Display for KnownTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let target: Target = self.clone().into();
        target.fmt(f)
    }
}

impl std::str::FromStr for KnownTarget {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let target = Target::from_str(s)?;

        let mut image = Self::default().image;
        let mut machine = None;
        let mut switch = None;
        let mut rack_topology = None;

        for (k, v) in target.0.into_iter() {
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
                _ => {
                    bail!(
                        "Unknown target key {k}\nValid keys include: [{}]",
                        Target::from(Self::default())
                            .0
                            .keys()
                            .cloned()
                            .collect::<Vec<String>>()
                            .join(", "),
                    )
                }
            }
        }
        KnownTarget::new(
            image,
            machine,
            switch,
            rack_topology.unwrap_or(RackTopology::MultiSled),
        )
    }
}

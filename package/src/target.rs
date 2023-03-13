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
#[strum(serialize_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum Machine {
    /// Use sled agent configuration for a Gimlet
    Gimlet,
    /// Use sled agent configuration for a device emulating a Gimlet
    ///
    /// Note that this configuration can actually work on real gimlets,
    /// it just relies on the "./tools/create_virtual_hardware.sh" script.
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
}

/// A strongly-typed variant of [Target].
#[derive(Clone, Debug)]
pub struct KnownTarget {
    pub image: Image,
    pub machine: Machine,
    pub switch: Switch,
}

impl Default for KnownTarget {
    fn default() -> Self {
        KnownTarget {
            image: Image::Standard,
            machine: Machine::NonGimlet,
            switch: Switch::Stub,
        }
    }
}

impl From<KnownTarget> for Target {
    fn from(kt: KnownTarget) -> Target {
        let mut map = BTreeMap::new();
        map.insert("image".to_string(), kt.image.to_string());
        map.insert("machine".to_string(), kt.machine.to_string());
        map.insert("switch".to_string(), kt.switch.to_string());
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

        let mut parsed = Self::default();

        for (k, v) in target.0.into_iter() {
            match k.as_str() {
                "image" => {
                    parsed.image = v.parse()?;
                }
                "machine" => {
                    parsed.machine = v.parse()?;
                }
                "switch" => {
                    parsed.switch = v.parse()?;
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
        Ok(parsed)
    }
}

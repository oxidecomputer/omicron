// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with sled agent configuration

use camino::{Utf8Path, Utf8PathBuf};
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use illumos_utils::dladm::CHELSIO_LINK_PREFIX;
use illumos_utils::dladm::Dladm;
use illumos_utils::dladm::FindPhysicalLinkError;
use illumos_utils::dladm::PhysicalLink;
use omicron_common::vlan::VlanID;
use serde::Deserialize;
use sled_hardware::DataLinks;
use sled_hardware::DendriteAsic;
use sled_hardware::ExternalDisks;
use sled_hardware::SledMode;
use slog::Logger;
use sprockets_tls::keys::SprocketsConfig;

use crate::bootstrap::server::StartError;

/// The role a deployment asks of this sled; `auto` lets detection decide.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SledRole {
    Auto,
    #[serde(alias = "gimlet")]
    Sled,
    Scrimlet,
}

/// Switch backend of a deployment, with the parameters it needs.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum Switch {
    TofinoAsic {
        #[serde(default = "default_sidecar_revision")]
        sidecar_revision: String,
    },
    TofinoStub {
        #[serde(default = "default_sidecar_revision")]
        sidecar_revision: String,
    },
    SoftNpuPropolisDevice {
        front_port_count: u8,
        rear_port_count: u8,
    },
    SoftNpuZone {
        front_port_count: u8,
        rear_port_count: u8,
    },
}

impl Switch {
    pub fn asic(&self) -> DendriteAsic {
        match self {
            Switch::TofinoAsic { .. } => DendriteAsic::TofinoAsic,
            Switch::TofinoStub { .. } => DendriteAsic::TofinoStub,
            Switch::SoftNpuPropolisDevice { .. } => {
                DendriteAsic::SoftNpuPropolisDevice
            }
            Switch::SoftNpuZone { .. } => DendriteAsic::SoftNpuZone,
        }
    }

    fn sidecar_revision(&self) -> SidecarRevision {
        match self {
            Switch::TofinoAsic { sidecar_revision }
            | Switch::TofinoStub { sidecar_revision } => {
                SidecarRevision::Physical(sidecar_revision.clone())
            }
            Switch::SoftNpuPropolisDevice {
                front_port_count,
                rear_port_count,
            } => SidecarRevision::SoftPropolis(SoftPortConfig {
                front_port_count: *front_port_count,
                rear_port_count: *rear_port_count,
            }),
            Switch::SoftNpuZone { front_port_count, rear_port_count } => {
                SidecarRevision::SoftZone(SoftPortConfig {
                    front_port_count: *front_port_count,
                    rear_port_count: *rear_port_count,
                })
            }
        }
    }
}

/// How this sled is deployed: the role it is asked to take and the switch
/// backend it runs when it is a scrimlet. Parsed from a [`DeploymentConfig`],
/// which rejects combinations that cannot be resolved at startup, so every
/// value of this type is one sled-agent knows what to do with.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "DeploymentConfig")]
pub struct Deployment {
    sled_mode: SledRole,
    switch: Switch,
}

/// The `deployment` table as written in the sled-agent config. The named
/// kinds fix the role and switch; `custom` spells both out.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
enum DeploymentConfig {
    /// Oxide rack. The hardware monitor detects the Tofino ASIC.
    Production {
        /// Sidecar board revision
        #[serde(default = "default_sidecar_revision")]
        sidecar_revision: String,
    },
    /// Propolis-hosted lab. A SoftNPU device makes the sled a scrimlet.
    Virtual { front_port_count: u8, rear_port_count: u8 },
    /// One host running a SoftNPU zone. Always a scrimlet.
    Standalone { front_port_count: u8, rear_port_count: u8 },
    /// Explicit role and switch backend.
    Custom { sled_mode: SledRole, switch: Switch },
}

fn default_sidecar_revision() -> String {
    "b".to_string()
}

impl TryFrom<DeploymentConfig> for Deployment {
    type Error = String;

    fn try_from(config: DeploymentConfig) -> Result<Self, Self::Error> {
        let (sled_mode, switch) = match config {
            DeploymentConfig::Production { sidecar_revision } => {
                (SledRole::Auto, Switch::TofinoAsic { sidecar_revision })
            }
            DeploymentConfig::Virtual { front_port_count, rear_port_count } => {
                (
                    SledRole::Auto,
                    Switch::SoftNpuPropolisDevice {
                        front_port_count,
                        rear_port_count,
                    },
                )
            }
            DeploymentConfig::Standalone {
                front_port_count,
                rear_port_count,
            } => (
                SledRole::Scrimlet,
                Switch::SoftNpuZone { front_port_count, rear_port_count },
            ),
            DeploymentConfig::Custom { sled_mode, switch } => {
                match (sled_mode, &switch) {
                    // "sled" ignores whatever is attached. The switch still
                    // says whether this is a rack sled with a physical ASIC.
                    (SledRole::Sled, _) => (),
                    // The detected backends decide the role themselves:
                    // the Tofino ASIC through the hardware monitor, the
                    // propolis SoftNPU device at startup. Forcing "scrimlet"
                    // would either wait on the same detection or fail.
                    (
                        SledRole::Auto,
                        Switch::TofinoAsic { .. }
                        | Switch::SoftNpuPropolisDevice { .. },
                    ) => (),
                    (
                        SledRole::Scrimlet,
                        Switch::TofinoAsic { .. }
                        | Switch::SoftNpuPropolisDevice { .. },
                    ) => {
                        return Err(format!(
                            "switch {:?} is detected; sled_mode must be \
                             \"auto\" or \"sled\"",
                            switch.asic()
                        ));
                    }
                    // The stub and zone backends have nothing to detect.
                    (
                        SledRole::Scrimlet,
                        Switch::TofinoStub { .. } | Switch::SoftNpuZone { .. },
                    ) => (),
                    (
                        SledRole::Auto,
                        Switch::TofinoStub { .. } | Switch::SoftNpuZone { .. },
                    ) => {
                        return Err(format!(
                            "switch {:?} has no hardware to detect; sled_mode \
                             must be \"sled\" or \"scrimlet\"",
                            switch.asic()
                        ));
                    }
                }
                (sled_mode, switch)
            }
        };
        Ok(Self { sled_mode, switch })
    }
}

impl Deployment {
    /// Resolve the sled mode. Only the propolis SoftNPU device is probed
    /// here; the Tofino ASIC is the hardware monitor's job.
    pub async fn sled_mode(
        &self,
        log: &Logger,
    ) -> Result<SledMode, StartError> {
        // Parsing leaves the detected backends with "auto" and the stub and
        // zone backends with "scrimlet", so each arm below covers the only
        // role it can carry.
        match (self.sled_mode, &self.switch) {
            (SledRole::Sled, _) => Ok(SledMode::Sled),
            (_, Switch::TofinoAsic { .. }) => Ok(SledMode::Auto),
            (_, Switch::SoftNpuPropolisDevice { .. }) => {
                let log = log.clone();
                // The probe touches devinfo and device nodes, so it may block.
                let found = tokio::task::spawn_blocking(move || {
                    sled_hardware::detect_switch_hardware(&log)
                })
                .await
                .expect("switch detection panicked")
                .map_err(StartError::DetectSwitch)?;
                Ok(match found {
                    Some(asic) => SledMode::Scrimlet { asic },
                    None => SledMode::Sled,
                })
            }
            (
                _,
                switch @ (Switch::TofinoStub { .. }
                | Switch::SoftNpuZone { .. }),
            ) => Ok(SledMode::Scrimlet { asic: switch.asic() }),
        }
    }

    /// Whether this deployment drives a physical sidecar ASIC, and so wants
    /// the services that only ship with those builds.
    pub fn has_physical_asic(&self) -> bool {
        matches!(self.switch, Switch::TofinoAsic { .. })
    }

    /// Sidecar parameters for the switch zone services.
    pub fn sidecar_revision(&self) -> SidecarRevision {
        self.switch.sidecar_revision()
    }
}

/// Sidecar parameters derived from the deployment.
#[derive(Debug, Clone)]
pub enum SidecarRevision {
    Physical(String),
    SoftZone(SoftPortConfig),
    SoftPropolis(SoftPortConfig),
}

impl SidecarRevision {
    pub fn is_physical(&self) -> bool {
        match self {
            Self::Physical(_) => true,
            Self::SoftZone(_) | Self::SoftPropolis(_) => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SoftPortConfig {
    /// Number of front ports
    pub front_port_count: u8,
    /// Number of rear ports
    pub rear_port_count: u8,
}

/// Configuration for a sled agent
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Configuration for the sled agent dropshot server
    ///
    /// If the `bind_address` is set, it will be ignored. The remaining fields
    /// will be respected.
    pub dropshot: ConfigDropshot,
    /// Configuration for the sled agent debug log
    pub log: ConfigLogging,
    /// How this sled is deployed, which selects the switch backend.
    pub deployment: Deployment,
    /// Optional percentage of otherwise-unbudgeted DRAM to reserve for guest
    /// memory, after accounting for expected host OS memory consumption and, if
    /// set, `vmm_reservoir_size_mb`.
    pub vmm_reservoir_percentage: Option<f32>,
    /// Optional DRAM to reserve for guest memory in MiB (mutually exclusive
    /// option with vmm_reservoir_percentage). This can be at most the amount of
    /// otherwise-unbudgeted memory on the slde - a setting high enough to
    /// oversubscribe physical memory results in a `sled-agent` error at
    /// startup.
    pub vmm_reservoir_size_mb: Option<u32>,
    /// Amount of memory to set aside in anticipation of use for services that
    /// will have roughly constant memory use. These are services that may have
    /// zero to one instances on a given sled - internal DNS, MGS, Nexus,
    /// ClickHouse, and so on. For a sled that happens to not run these kinds of
    /// control plane services, this memory is "wasted", but ensures the sled
    /// could run those services if reconfiguration desired it.
    pub control_plane_memory_earmark_mb: Option<u32>,
    /// Optional swap device size in GiB
    pub swap_device_size_gb: Option<u32>,
    /// Optional VLAN ID to be used for tagging guest VNICs.
    pub vlan: Option<VlanID>,
    /// The source of external disks to use.
    pub external_disks: ExternalDisks,
    /// Optionally skip waiting for time synchronization
    pub skip_timesync: Option<bool>,

    /// The data link on which we infer the bootstrap address.
    ///
    /// If unsupplied, we default to:
    ///
    /// - The first physical link on a non-Gimlet machine.
    /// - The first Chelsio link on a Gimlet.
    ///
    /// This allows continued support for development and testing on emulated
    /// systems.
    pub data_link: Option<PhysicalLink>,

    /// The data links sled-agent will use.
    pub data_links: DataLinks,

    /// When running on a scrimlet, tfportd in the switch zone will create links
    /// when it boots, and maghemite in the switch zone is configured to use
    /// those in transit mode in order to transit prefix announcements to sleds.
    ///
    /// For non-gimlet based testing, tfportd will not add create links when it
    /// boots. Map these links into the switch zone for use with the transit
    /// mode maghemite there.
    #[serde(default)]
    pub switch_zone_maghemite_links: Vec<PhysicalLink>,

    /// Settings for sprockets running on the bootstrap network. Includes
    /// root certificates and whether to use local certificate chain or
    /// one over IPCC
    pub sprockets: SprocketsConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config from {path}")]
    Io {
        path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("Failed to parse config from {path}")]
    Parse {
        path: Utf8PathBuf,
        #[source]
        err: anyhow::Error,
    },
    #[error("Loading certificate")]
    Certificate(#[source] anyhow::Error),
    #[error("Could not determine if host is an Oxide sled")]
    SystemDetection(#[source] anyhow::Error),
    #[error("Could not enumerate physical links")]
    FindLinks(#[from] FindPhysicalLinkError),
}

impl Config {
    pub fn from_file<P: AsRef<Utf8Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(&path)
            .map_err(|err| ConfigError::Io { path: path.into(), err })?;
        let config: Self = toml::from_str(&contents).map_err(|err| {
            ConfigError::Parse { path: path.into(), err: err.into() }
        })?;
        Ok(config)
    }

    pub async fn get_link(&self) -> Result<PhysicalLink, ConfigError> {
        if let Some(link) = self.data_link.as_ref() {
            Ok(link.clone())
        } else {
            match self.data_links {
                DataLinks::Virtual { .. } => {
                    Dladm::find_physical().await.map_err(ConfigError::FindLinks)
                }
                DataLinks::Physical => Dladm::list_physical()
                    .await
                    .map_err(ConfigError::FindLinks)?
                    .into_iter()
                    .find(|link| link.0.starts_with(CHELSIO_LINK_PREFIX))
                    .ok_or_else(|| {
                        ConfigError::FindLinks(
                            FindPhysicalLinkError::NoPhysicalLinkFound,
                        )
                    }),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn parse(table: &str) -> Result<Deployment, String> {
        toml::from_str::<Deployment>(table).map_err(|e| e.to_string())
    }

    fn custom(sled_mode: &str, switch: &str) -> Result<Deployment, String> {
        let switch = match switch {
            "tofino_asic" | "tofino_stub" => format!("kind = \"{switch}\""),
            _ => format!(
                "kind = \"{switch}\", front_port_count = 1, rear_port_count = 1"
            ),
        };
        parse(&format!(
            "kind = \"custom\"\nsled_mode = \"{sled_mode}\"\nswitch = {{ {switch} }}"
        ))
    }

    #[test]
    fn named_kinds_normalize() {
        let d = parse("kind = \"production\"").unwrap();
        assert_eq!(d.sled_mode, SledRole::Auto);
        assert!(d.has_physical_asic());
        assert!(matches!(
            d.sidecar_revision(),
            SidecarRevision::Physical(rev) if rev == "b"
        ));

        let d = parse(
            "kind = \"virtual\"\nfront_port_count = 2\nrear_port_count = 4",
        )
        .unwrap();
        assert_eq!(d.sled_mode, SledRole::Auto);
        assert!(!d.has_physical_asic());
        assert!(matches!(
            d.sidecar_revision(),
            SidecarRevision::SoftPropolis(p)
                if p.front_port_count == 2 && p.rear_port_count == 4
        ));

        let d = parse(
            "kind = \"standalone\"\nfront_port_count = 1\nrear_port_count = 1",
        )
        .unwrap();
        assert_eq!(d.sled_mode, SledRole::Scrimlet);
        assert!(matches!(d.sidecar_revision(), SidecarRevision::SoftZone(_)));
    }

    #[test]
    fn custom_role_and_switch_combinations() {
        // "sled" ignores hardware, so any switch is fine.
        for switch in [
            "tofino_asic",
            "tofino_stub",
            "soft_npu_propolis_device",
            "soft_npu_zone",
        ] {
            assert!(custom("sled", switch).is_ok(), "sled + {switch}");
        }
        // Detected backends take "auto", never "scrimlet".
        for switch in ["tofino_asic", "soft_npu_propolis_device"] {
            assert!(custom("auto", switch).is_ok(), "auto + {switch}");
            assert!(custom("scrimlet", switch).is_err(), "scrimlet + {switch}");
        }
        // Fixed backends take "scrimlet", never "auto".
        for switch in ["tofino_stub", "soft_npu_zone"] {
            assert!(custom("scrimlet", switch).is_ok(), "scrimlet + {switch}");
            assert!(custom("auto", switch).is_err(), "auto + {switch}");
        }
        // The old "gimlet" spelling of "sled" still parses.
        assert!(custom("gimlet", "tofino_asic").is_ok());
        // Stray keys are rejected rather than ignored.
        assert!(parse("kind = \"production\"\nfront_port_count = 1").is_err());
        assert!(
            custom("scrimlet", "tofino_stub")
                .map(|_| ())
                .and(
                    parse(
                        "kind = \"custom\"\nsled_mode = \"scrimlet\"\n\
                 switch = { kind = \"tofino_stub\", rear_port_count = 1 }"
                    )
                    .map(|_| ())
                )
                .is_err()
        );
    }

    #[tokio::test]
    async fn sled_mode_without_hardware() {
        let log = slog::Logger::root(slog::Discard, slog::o!());
        let mode = |d: Result<Deployment, String>| async {
            d.unwrap().sled_mode(&log).await.unwrap()
        };
        assert_eq!(mode(parse("kind = \"production\"")).await, SledMode::Auto);
        assert_eq!(mode(custom("sled", "tofino_asic")).await, SledMode::Sled);
        assert_eq!(
            mode(custom("scrimlet", "tofino_stub")).await,
            SledMode::Scrimlet { asic: DendriteAsic::TofinoStub }
        );
        assert_eq!(
            mode(parse(
                "kind = \"standalone\"\nfront_port_count = 1\nrear_port_count = 1"
            ))
            .await,
            SledMode::Scrimlet { asic: DendriteAsic::SoftNpuZone }
        );
    }

    use slog_error_chain::InlineErrorChain;

    #[test]
    fn test_smf_configs() {
        let manifest = std::env::var("CARGO_MANIFEST_DIR")
            .expect("Cannot access manifest directory");
        let smf = Utf8PathBuf::from(manifest).join("../smf/sled-agent");

        let mut configs_seen = 0;
        for variant in smf.read_dir_utf8().unwrap() {
            let variant = variant.unwrap();
            if variant.file_type().unwrap().is_dir() {
                for entry in variant.path().read_dir_utf8().unwrap() {
                    let entry = entry.unwrap();
                    if entry.file_name() == "config.toml" {
                        let path = entry.path();
                        Config::from_file(&path).unwrap_or_else(|e| {
                            panic!(
                                "Failed to parse config {path}: {}",
                                InlineErrorChain::new(&e)
                            )
                        });
                        configs_seen += 1;
                    }
                }
            }
        }
        assert!(configs_seen > 0, "No sled-agent configs found");
    }
}

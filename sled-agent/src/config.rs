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
use sled_hardware::SwitchProbe;
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

/// Switch backend of a `custom` deployment, with the parameters it needs.
/// Flattened into the deployment table: `switch` names the backend and its
/// parameters sit beside it.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "switch")]
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

/// How this sled is deployed. Selects the switch backend and whether the
/// sled is a scrimlet, which is detected where the backend allows it.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Deployment {
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
    /// Explicit sled mode and switch backend.
    Custom {
        sled_mode: SledRole,
        #[serde(flatten)]
        switch: Switch,
    },
}

fn default_sidecar_revision() -> String {
    "b".to_string()
}

impl Deployment {
    /// Reject custom combinations that cannot be resolved at startup.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Deployment::Custom {
                sled_mode: SledRole::Auto,
                switch:
                    switch
                    @ (Switch::TofinoStub { .. } | Switch::SoftNpuZone { .. }),
            } => Err(format!(
                "switch {:?} has no hardware to detect; sled_mode must be \
                 \"sled\" or \"scrimlet\"",
                switch.asic()
            )),
            _ => Ok(()),
        }
    }

    /// Resolve the sled mode: probe for the switch hardware this deployment
    /// can carry, then decide from what was found.
    pub async fn sled_mode(
        &self,
        log: &Logger,
    ) -> Result<SledMode, StartError> {
        let found = match self.probe() {
            Some(probe) => {
                let log = log.clone();
                // The probe touches devinfo and device nodes, so it may block.
                tokio::task::spawn_blocking(move || {
                    sled_hardware::detect_switch_hardware(&log, probe)
                })
                .await
                .expect("switch detection panicked")
                .map_err(StartError::DetectSwitch)?
            }
            None => None,
        };
        self.resolve(found).map_err(StartError::SledModeConfig)
    }

    /// What startup detection should look for, if anything. A sled never
    /// probes, and the stub and zone backends have nothing to find.
    fn probe(&self) -> Option<SwitchProbe> {
        match self {
            Deployment::Production { .. } => Some(SwitchProbe::PhysicalAsic),
            Deployment::Virtual { .. } => Some(SwitchProbe::SoftNpu),
            Deployment::Standalone { .. } => None,
            Deployment::Custom { sled_mode: SledRole::Sled, .. } => None,
            Deployment::Custom {
                switch: Switch::TofinoAsic { .. }, ..
            } => Some(SwitchProbe::PhysicalAsic),
            Deployment::Custom {
                switch: Switch::SoftNpuPropolisDevice { .. },
                ..
            } => Some(SwitchProbe::SoftNpu),
            Deployment::Custom { .. } => None,
        }
    }

    /// Decide the sled mode from what `probe()` found.
    fn resolve(
        &self,
        found: Option<DendriteAsic>,
    ) -> Result<SledMode, &'static str> {
        use Deployment::*;
        Ok(match (self, found) {
            (Production { .. } | Virtual { .. }, Some(asic)) => {
                SledMode::Scrimlet { asic }
            }
            // The ASIC driver can attach after startup; the hardware monitor
            // keeps watching for it.
            (Production { .. }, None) => SledMode::Auto,
            (Virtual { .. }, None) => SledMode::Sled,
            (Standalone { .. }, _) => {
                SledMode::Scrimlet { asic: DendriteAsic::SoftNpuZone }
            }
            (Custom { sled_mode: SledRole::Sled, .. }, _) => SledMode::Sled,
            (Custom { .. }, Some(asic)) => SledMode::Scrimlet { asic },
            (Custom { sled_mode: SledRole::Auto, .. }, None) => {
                match self.probe() {
                    Some(SwitchProbe::PhysicalAsic) => SledMode::Auto,
                    Some(SwitchProbe::SoftNpu) => SledMode::Sled,
                    None => {
                        return Err("switch backend has no hardware to detect");
                    }
                }
            }
            (Custom { sled_mode: SledRole::Scrimlet, switch }, None) => {
                match switch {
                    Switch::SoftNpuPropolisDevice { .. } => {
                        return Err(
                            "sled_mode is scrimlet but no SoftNPU device is \
                             present",
                        );
                    }
                    // A physical ASIC may attach later; force the role now.
                    _ => SledMode::Scrimlet { asic: switch.asic() },
                }
            }
        })
    }

    /// Sidecar parameters for the switch zone services.
    pub fn sidecar_revision(&self) -> SidecarRevision {
        match self {
            Deployment::Production { sidecar_revision } => {
                SidecarRevision::Physical(sidecar_revision.clone())
            }
            Deployment::Virtual { front_port_count, rear_port_count } => {
                SidecarRevision::SoftPropolis(SoftPortConfig {
                    front_port_count: *front_port_count,
                    rear_port_count: *rear_port_count,
                })
            }
            Deployment::Standalone { front_port_count, rear_port_count } => {
                SidecarRevision::SoftZone(SoftPortConfig {
                    front_port_count: *front_port_count,
                    rear_port_count: *rear_port_count,
                })
            }
            Deployment::Custom { switch, .. } => switch.sidecar_revision(),
        }
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
    #[error("Invalid deployment in {path}: {reason}")]
    InvalidDeployment { path: Utf8PathBuf, reason: String },
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
        config.deployment.validate().map_err(|reason| {
            ConfigError::InvalidDeployment { path: path.into(), reason }
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
    use DendriteAsic::*;

    const AUTO: SledRole = SledRole::Auto;
    const SLED: SledRole = SledRole::Sled;
    const SCRIMLET: SledRole = SledRole::Scrimlet;

    fn production() -> Deployment {
        Deployment::Production { sidecar_revision: "b".to_string() }
    }

    fn virtual_lab() -> Deployment {
        Deployment::Virtual { front_port_count: 2, rear_port_count: 4 }
    }

    fn standalone() -> Deployment {
        Deployment::Standalone { front_port_count: 1, rear_port_count: 1 }
    }

    fn custom(sled_mode: SledRole, asic: DendriteAsic) -> Deployment {
        let switch = match asic {
            TofinoAsic => Switch::TofinoAsic { sidecar_revision: "b".into() },
            TofinoStub => Switch::TofinoStub { sidecar_revision: "b".into() },
            SoftNpuPropolisDevice => Switch::SoftNpuPropolisDevice {
                front_port_count: 1,
                rear_port_count: 1,
            },
            SoftNpuZone => {
                Switch::SoftNpuZone { front_port_count: 1, rear_port_count: 1 }
            }
        };
        Deployment::Custom { sled_mode, switch }
    }

    fn scrimlet(asic: DendriteAsic) -> Result<SledMode, &'static str> {
        Ok(SledMode::Scrimlet { asic })
    }

    // Each case: deployment, what the probe found (None when nothing was
    // found or no probe ran), expected mode or a config error.
    #[test]
    fn resolve_table() {
        let err: Result<SledMode, &'static str> = Err("");
        let cases = [
            // Production: a found ASIC is a scrimlet now; nothing found
            // leaves it to the hardware monitor.
            (production(), Some(TofinoAsic), scrimlet(TofinoAsic)),
            (production(), None, Ok(SledMode::Auto)),
            // Virtual: the SoftNPU device decides.
            (
                virtual_lab(),
                Some(SoftNpuPropolisDevice),
                scrimlet(SoftNpuPropolisDevice),
            ),
            (virtual_lab(), None, Ok(SledMode::Sled)),
            // Standalone is always a scrimlet with nothing to detect.
            (standalone(), None, scrimlet(SoftNpuZone)),
            // Custom sled: never a switch zone.
            (custom(SLED, TofinoAsic), None, Ok(SledMode::Sled)),
            (custom(SLED, SoftNpuPropolisDevice), None, Ok(SledMode::Sled)),
            // Custom scrimlet: a physical ASIC may attach later, the zone and
            // stub run as configured, the propolis device must be present.
            (
                custom(SCRIMLET, TofinoAsic),
                Some(TofinoAsic),
                scrimlet(TofinoAsic),
            ),
            (custom(SCRIMLET, TofinoAsic), None, scrimlet(TofinoAsic)),
            (custom(SCRIMLET, TofinoStub), None, scrimlet(TofinoStub)),
            (custom(SCRIMLET, SoftNpuZone), None, scrimlet(SoftNpuZone)),
            (
                custom(SCRIMLET, SoftNpuPropolisDevice),
                Some(SoftNpuPropolisDevice),
                scrimlet(SoftNpuPropolisDevice),
            ),
            (custom(SCRIMLET, SoftNpuPropolisDevice), None, err),
            // Custom auto matches production and virtual.
            (custom(AUTO, TofinoAsic), Some(TofinoAsic), scrimlet(TofinoAsic)),
            (custom(AUTO, TofinoAsic), None, Ok(SledMode::Auto)),
            (
                custom(AUTO, SoftNpuPropolisDevice),
                Some(SoftNpuPropolisDevice),
                scrimlet(SoftNpuPropolisDevice),
            ),
            (custom(AUTO, SoftNpuPropolisDevice), None, Ok(SledMode::Sled)),
            (custom(AUTO, TofinoStub), None, err),
        ];
        for (i, (deployment, found, expected)) in cases.into_iter().enumerate()
        {
            let actual = deployment.resolve(found).map_err(|_| "");
            assert_eq!(actual, expected, "case {i}");
        }
    }

    #[test]
    fn probe_targets() {
        assert_eq!(production().probe(), Some(SwitchProbe::PhysicalAsic));
        assert_eq!(virtual_lab().probe(), Some(SwitchProbe::SoftNpu));
        assert_eq!(standalone().probe(), None);
        assert_eq!(custom(SLED, TofinoAsic).probe(), None);
        assert_eq!(custom(SCRIMLET, TofinoStub).probe(), None);
        assert_eq!(custom(SCRIMLET, SoftNpuZone).probe(), None);
        assert_eq!(
            custom(AUTO, TofinoAsic).probe(),
            Some(SwitchProbe::PhysicalAsic)
        );
        assert_eq!(
            custom(SCRIMLET, SoftNpuPropolisDevice).probe(),
            Some(SwitchProbe::SoftNpu)
        );
    }

    #[test]
    fn custom_deployment_validation() {
        assert!(custom(AUTO, TofinoStub).validate().is_err());
        assert!(custom(AUTO, SoftNpuZone).validate().is_err());
        assert!(custom(SCRIMLET, TofinoStub).validate().is_ok());
        assert!(custom(AUTO, TofinoAsic).validate().is_ok());
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

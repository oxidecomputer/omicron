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
use sled_hardware::ExternalDisks;
use sprockets_tls::keys::SprocketsConfig;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SledMode {
    Auto,
    #[serde(alias = "gimlet")]
    Sled,
    Scrimlet,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, Deserialize)]
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
    /// The sled's mode of operation (auto detect or force gimlet/scrimlet).
    pub sled_mode: SledMode,
    // TODO: Remove once this can be auto-detected.
    pub sidecar_revision: SidecarRevision,
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

    /// Settings for the Support Shell server (RFD 620). If this is absent, no
    /// Support Shell server runs on this sled.
    #[serde(default)]
    pub sush: Option<SushConfig>,
}

/// Configuration for the Support Shell (`sush`) server that runs in the global
/// zone. See RFD 620.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SushConfig {
    /// PEM files holding the trusted root certificates for job requests,
    /// one certificate per file. A job whose signature does not chain to
    /// one of these is refused, so an empty list means no job can ever run.
    pub roots: Vec<Utf8PathBuf>,

    /// Where to record job output before an encrypted dataset is available.
    /// This is on the ramdisk, so it costs global zone memory and does not
    /// survive a reboot.
    #[serde(default = "SushConfig::default_ramdisk_dir")]
    pub ramdisk_dir: Utf8PathBuf,

    /// Maximum output a single job may record while it is being written to
    /// `ramdisk_dir`. Deliberately small: this is memory.
    #[serde(default = "SushConfig::default_ramdisk_max_output_mb")]
    pub ramdisk_max_output_mb: u32,

    /// Maximum output a single job may record once it is being written to an
    /// encrypted dataset.
    #[serde(default = "SushConfig::default_max_output_mb")]
    pub max_output_mb: u32,
}

impl SushConfig {
    // TODO: check that this is the right default. `/var/run/oxide` is tmpfs
    // and is created during bootstrap, but it exists to hold ZFS key files
    // (see `illumos_utils::zfs::KEYPATH_ROOT`), not bulk job output.
    fn default_ramdisk_dir() -> Utf8PathBuf {
        "/var/run/oxide/sush".into()
    }

    fn default_ramdisk_max_output_mb() -> u32 {
        64
    }

    fn default_max_output_mb() -> u32 {
        10 * 1024
    }
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
        let config = toml::from_str(&contents).map_err(|err| {
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

    #[test]
    fn test_sush_config() {
        // Only `roots` is required. The rest have defaults.
        let sush: SushConfig =
            toml::from_str(r#"roots = ["/pkg/sush-root.pem"]"#).unwrap();
        assert_eq!(sush.roots, vec![Utf8PathBuf::from("/pkg/sush-root.pem")]);
        assert_eq!(sush.ramdisk_dir, SushConfig::default_ramdisk_dir());
        assert_eq!(
            sush.ramdisk_max_output_mb,
            SushConfig::default_ramdisk_max_output_mb()
        );
        assert_eq!(sush.max_output_mb, SushConfig::default_max_output_mb());

        let sush: SushConfig = toml::from_str(
            r#"
            roots = []
            ramdisk_dir = "/var/run/sush"
            ramdisk_max_output_mb = 8
            max_output_mb = 128
            "#,
        )
        .unwrap();
        assert!(sush.roots.is_empty());
        assert_eq!(sush.ramdisk_dir, "/var/run/sush");
        assert_eq!(sush.ramdisk_max_output_mb, 8);
        assert_eq!(sush.max_output_mb, 128);
    }
}

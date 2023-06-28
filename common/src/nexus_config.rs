// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration parameters to Nexus that are usually only known
//! at deployment time.

use super::address::{Ipv6Subnet, RACK_PREFIX};
use super::postgres_config::PostgresConfigWithUrl;
use anyhow::anyhow;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DeserializeFromStr;
use serde_with::DisplayFromStr;
use serde_with::DurationSeconds;
use serde_with::SerializeDisplay;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug)]
pub struct LoadError {
    pub path: PathBuf,
    pub kind: LoadErrorKind,
}

#[derive(Debug)]
pub struct InvalidTunable {
    pub tunable: String,
    pub message: String,
}

impl std::fmt::Display for InvalidTunable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid \"{}\": \"{}\"", self.tunable, self.message)
    }
}
impl std::error::Error for InvalidTunable {}

#[derive(Debug)]
pub enum LoadErrorKind {
    Io(std::io::Error),
    Parse(toml::de::Error),
    InvalidTunable(InvalidTunable),
}

impl From<(PathBuf, std::io::Error)> for LoadError {
    fn from((path, err): (PathBuf, std::io::Error)) -> Self {
        LoadError { path, kind: LoadErrorKind::Io(err) }
    }
}

impl From<(PathBuf, toml::de::Error)> for LoadError {
    fn from((path, err): (PathBuf, toml::de::Error)) -> Self {
        LoadError { path, kind: LoadErrorKind::Parse(err) }
    }
}

impl std::error::Error for LoadError {}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            LoadErrorKind::Io(e) => {
                write!(f, "read \"{}\": {}", self.path.display(), e)
            }
            LoadErrorKind::Parse(e) => {
                write!(f, "parse \"{}\": {}", self.path.display(), e.message())
            }
            LoadErrorKind::InvalidTunable(inner) => {
                write!(
                    f,
                    "invalid tunable \"{}\": {}",
                    self.path.display(),
                    inner,
                )
            }
        }
    }
}

impl std::cmp::PartialEq<std::io::Error> for LoadError {
    fn eq(&self, other: &std::io::Error) -> bool {
        if let LoadErrorKind::Io(e) = &self.kind {
            e.kind() == other.kind()
        } else {
            false
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum Database {
    FromDns,
    FromUrl {
        #[serde_as(as = "DisplayFromStr")]
        url: PostgresConfigWithUrl,
    },
}

/// The mechanism Nexus should use to contact the internal DNS servers.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InternalDns {
    /// Nexus should infer the DNS server addresses from this subnet.
    ///
    /// This is a more common usage for production.
    FromSubnet { subnet: Ipv6Subnet<RACK_PREFIX> },
    /// Nexus should use precisely the following address.
    ///
    /// This is less desirable in production, but can give value
    /// in test scenarios.
    FromAddress { address: SocketAddr },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DeploymentConfig {
    /// Uuid of the Nexus instance
    pub id: Uuid,
    /// Uuid of the Rack where Nexus is executing.
    pub rack_id: Uuid,
    /// Dropshot configuration for the external API server.
    pub dropshot_external: ConfigDropshotWithTls,
    /// Dropshot configuration for internal API server.
    pub dropshot_internal: ConfigDropshot,
    /// Describes how Nexus should find internal DNS servers
    /// for bootstrapping.
    pub internal_dns: InternalDns,
    /// DB configuration.
    pub database: Database,
}

impl DeploymentConfig {
    /// Load a `DeploymentConfig` from the given TOML file
    ///
    /// This config object can then be used to create a new `Nexus`.
    /// The format is described in the README.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, LoadError> {
        let path = path.as_ref();
        let file_contents = std::fs::read_to_string(path)
            .map_err(|e| (path.to_path_buf(), e))?;
        let config_parsed: Self = toml::from_str(&file_contents)
            .map_err(|e| (path.to_path_buf(), e))?;
        Ok(config_parsed)
    }
}

/// Thin wrapper around `ConfigDropshot` that adds a boolean for enabling TLS
///
/// The configuration for TLS consists of the list of TLS certificates used.
/// This is dynamic, driven by what's in CockroachDB.  That's why we only need a
/// boolean here.  (If in the future we want to configure other things about
/// TLS, this could be extended.)
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConfigDropshotWithTls {
    /// Regular Dropshot configuration parameters
    #[serde(flatten)]
    pub dropshot: ConfigDropshot,
    /// Whether TLS is enabled (default: false)
    #[serde(default)]
    pub tls: bool,
}

// By design, we require that all config properties be specified (i.e., we don't
// use `serde(default)`).

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AuthnConfig {
    /// allowed authentication schemes for external HTTP server
    pub schemes_external: Vec<SchemeName>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConsoleConfig {
    pub static_dir: PathBuf,
    /// how long a session can be idle before expiring
    pub session_idle_timeout_minutes: u32,
    /// how long a session can exist before expiring
    pub session_absolute_timeout_minutes: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct UpdatesConfig {
    /// Trusted root.json role for the TUF updates repository.
    pub trusted_root: PathBuf,
    /// Default base URL for the TUF repository.
    pub default_base_url: String,
}

/// Optional configuration for the timeseries database.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct TimeseriesDbConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<SocketAddr>,
}

/// Configuration for the `Dendrite` dataplane daemon.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct DpdConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<SocketAddr>,
}

// A deserializable type that does no validation on the tunable parameters.
#[derive(Clone, Debug, Deserialize, PartialEq)]
struct UnvalidatedTunables {
    max_vpc_ipv4_subnet_prefix: u8,
}

/// Tunable configuration parameters, intended for use in test environments or
/// other situations in which experimentation / tuning is valuable.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(try_from = "UnvalidatedTunables")]
pub struct Tunables {
    /// The maximum prefix size supported for VPC Subnet IPv4 subnetworks.
    ///
    /// Note that this is the maximum _prefix_ size, which sets the minimum size
    /// of the subnet.
    pub max_vpc_ipv4_subnet_prefix: u8,
}

// Convert from the unvalidated tunables, verifying each parameter as needed.
impl TryFrom<UnvalidatedTunables> for Tunables {
    type Error = InvalidTunable;

    fn try_from(unvalidated: UnvalidatedTunables) -> Result<Self, Self::Error> {
        Tunables::validate_ipv4_prefix(unvalidated.max_vpc_ipv4_subnet_prefix)?;
        Ok(Tunables {
            max_vpc_ipv4_subnet_prefix: unvalidated.max_vpc_ipv4_subnet_prefix,
        })
    }
}

/// Minimum prefix size supported in IPv4 VPC Subnets.
///
/// NOTE: This is the minimum _prefix_, which sets the maximum subnet size.
pub const MIN_VPC_IPV4_SUBNET_PREFIX: u8 = 8;

/// The number of reserved addresses at the beginning of a subnet range.
pub const NUM_INITIAL_RESERVED_IP_ADDRESSES: usize = 5;

impl Tunables {
    fn validate_ipv4_prefix(prefix: u8) -> Result<(), InvalidTunable> {
        let absolute_max: u8 = 32_u8
            .checked_sub(
                // Always need space for the reserved Oxide addresses, including the
                // broadcast address at the end of the subnet.
                ((NUM_INITIAL_RESERVED_IP_ADDRESSES + 1) as f32)
                .log2() // Subnet size to bit prefix.
                .ceil() // Round up to a whole number of bits.
                as u8,
            )
            .expect("Invalid absolute maximum IPv4 subnet prefix");
        if prefix >= MIN_VPC_IPV4_SUBNET_PREFIX && prefix <= absolute_max {
            Ok(())
        } else {
            Err(InvalidTunable {
                tunable: String::from("max_vpc_ipv4_subnet_prefix"),
                message: format!(
                    "IPv4 subnet prefix must be in the range [0, {}], found: {}",
                    absolute_max,
                    prefix,
                ),
            })
        }
    }
}

/// The maximum prefix size by default.
///
/// There are 6 Oxide reserved IP addresses, 5 at the beginning for DNS and the
/// like, and the broadcast address at the end of the subnet. This size provides
/// room for 2 ** 6 - 6 = 58 IP addresses, which seems like a reasonable size
/// for the smallest subnet that's still useful in many contexts.
pub const MAX_VPC_IPV4_SUBNET_PREFIX: u8 = 26;

impl Default for Tunables {
    fn default() -> Self {
        Tunables { max_vpc_ipv4_subnet_prefix: MAX_VPC_IPV4_SUBNET_PREFIX }
    }
}

/// Background task configuration
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackgroundTaskConfig {
    /// configuration for internal DNS background tasks
    pub dns_internal: DnsTasksConfig,
    /// configuration for external DNS background tasks
    pub dns_external: DnsTasksConfig,
    /// configuration for external endpoint list watcher
    pub external_endpoints: ExternalEndpointsConfig,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DnsTasksConfig {
    /// period (in seconds) for periodic activations of the background task that
    /// reads the latest DNS configuration from the database
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_config: Duration,

    /// period (in seconds) for periodic activations of the background task that
    /// reads the latest list of DNS servers from the database
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_servers: Duration,

    /// period (in seconds) for periodic activations of the background task that
    /// propagates the latest DNS configuration to the latest set of DNS servers
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_propagation: Duration,

    /// maximum number of concurrent DNS server updates
    pub max_concurrent_server_updates: usize,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExternalEndpointsConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
    // Other policy around the TLS certificates could go here (e.g.,
    // allow/disallow wildcard certs, don't serve expired certs, etc.)
}

/// Configuration for a nexus server
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PackageConfig {
    /// Console-related tunables
    pub console: ConsoleConfig,
    /// Server-wide logging configuration.
    pub log: ConfigLogging,
    /// Authentication-related configuration
    pub authn: AuthnConfig,
    /// Timeseries database configuration.
    #[serde(default)]
    pub timeseries_db: TimeseriesDbConfig,
    /// Updates-related configuration. Updates APIs return 400 Bad Request when
    /// this is unconfigured.
    #[serde(default)]
    pub updates: Option<UpdatesConfig>,
    /// Tunable configuration for testing and experimentation
    #[serde(default)]
    pub tunables: Tunables,
    /// `Dendrite` dataplane daemon configuration
    #[serde(default)]
    pub dendrite: DpdConfig,
    /// Background task configuration
    pub background_tasks: BackgroundTaskConfig,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Config {
    /// Configuration parameters known at compile-time.
    #[serde(flatten)]
    pub pkg: PackageConfig,

    /// A variety of configuration parameters only known at deployment time.
    pub deployment: DeploymentConfig,
}

impl Config {
    /// Load a `Config` from the given TOML file
    ///
    /// This config object can then be used to create a new `Nexus`.
    /// The format is described in the README.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, LoadError> {
        let path = path.as_ref();
        let file_contents = std::fs::read_to_string(path)
            .map_err(|e| (path.to_path_buf(), e))?;
        let config_parsed: Self = toml::from_str(&file_contents)
            .map_err(|e| (path.to_path_buf(), e))?;
        Ok(config_parsed)
    }
}

/// List of supported external authn schemes
///
/// Note that the authn subsystem doesn't know about this type.  It allows
/// schemes to be called whatever they want.  This is just to provide a set of
/// allowed values for configuration.
#[derive(
    Clone, Copy, Debug, DeserializeFromStr, Eq, PartialEq, SerializeDisplay,
)]
pub enum SchemeName {
    Spoof,
    SessionCookie,
    AccessToken,
}

impl std::str::FromStr for SchemeName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "spoof" => Ok(SchemeName::Spoof),
            "session_cookie" => Ok(SchemeName::SessionCookie),
            "access_token" => Ok(SchemeName::AccessToken),
            _ => Err(anyhow!("unsupported authn scheme: {:?}", s)),
        }
    }
}

impl std::fmt::Display for SchemeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SchemeName::Spoof => "spoof",
            SchemeName::SessionCookie => "session_cookie",
            SchemeName::AccessToken => "access_token",
        })
    }
}

#[cfg(test)]
mod test {
    use super::Tunables;
    use super::{
        AuthnConfig, Config, ConsoleConfig, LoadError, PackageConfig,
        SchemeName, TimeseriesDbConfig, UpdatesConfig,
    };
    use crate::address::{Ipv6Subnet, RACK_PREFIX};
    use crate::nexus_config::{
        BackgroundTaskConfig, ConfigDropshotWithTls, Database,
        DeploymentConfig, DnsTasksConfig, DpdConfig, ExternalEndpointsConfig,
        InternalDns, LoadErrorKind,
    };
    use dropshot::ConfigDropshot;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingIfExists;
    use dropshot::ConfigLoggingLevel;
    use libc;
    use std::fs;
    use std::net::{Ipv6Addr, SocketAddr};
    use std::path::Path;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::time::Duration;

    /// Generates a temporary filesystem path unique for the given label.
    fn temp_path(label: &str) -> PathBuf {
        let arg0str = std::env::args().next().expect("expected process arg0");
        let arg0 = Path::new(&arg0str)
            .file_name()
            .expect("expected arg0 filename")
            .to_str()
            .expect("expected arg0 filename to be valid Unicode");
        let pid = std::process::id();
        let mut pathbuf = std::env::temp_dir();
        pathbuf.push(format!("{}.{}.{}", arg0, pid, label));
        pathbuf
    }

    /// Load a Config with the given string `contents`.  To exercise
    /// the full path, this function writes the contents to a file first, then
    /// loads the config from that file, then removes the file.  `label` is used
    /// as a unique string for the filename and error messages.  It should be
    /// unique for each test.
    fn read_config(label: &str, contents: &str) -> Result<Config, LoadError> {
        let pathbuf = temp_path(label);
        let path = pathbuf.as_path();
        eprintln!("writing test config {}", path.display());
        fs::write(path, contents).expect("write to tempfile failed");

        let result = Config::from_file(path);
        fs::remove_file(path).expect("failed to remove temporary file");
        eprintln!("{:?}", result);
        result
    }

    // Totally bogus config files (nonexistent, bad TOML syntax)

    #[test]
    fn test_config_nonexistent() {
        let error = Config::from_file(Path::new("/nonexistent"))
            .expect_err("expected config to fail from /nonexistent");
        let expected = std::io::Error::from_raw_os_error(libc::ENOENT);
        assert_eq!(error, expected);
    }

    #[test]
    fn test_config_bad_toml() {
        let error =
            read_config("bad_toml", "foo =").expect_err("expected failure");
        if let LoadErrorKind::Parse(error) = &error.kind {
            assert_eq!(error.span(), Some(5..5));
            // See https://github.com/toml-rs/toml/issues/519
            // assert_eq!(
            //     error.message(),
            //     "unexpected eof encountered at line 1 column 6"
            // );
        } else {
            panic!(
                "Got an unexpected error, expected Parse but got {:?}",
                error
            );
        }
    }

    // Empty config (special case of a missing required field, but worth calling
    // out explicitly)

    #[test]
    fn test_config_empty() {
        let error = read_config("empty", "").expect_err("expected failure");
        if let LoadErrorKind::Parse(error) = &error.kind {
            assert_eq!(error.span(), Some(0..0));
            assert_eq!(error.message(), "missing field `deployment`");
        } else {
            panic!(
                "Got an unexpected error, expected Parse but got {:?}",
                error
            );
        }
    }

    // Success case.  We don't need to retest semantics for either ConfigLogging
    // or ConfigDropshot because those are both tested within Dropshot.  If we
    // add new configuration sections of our own, we will want to test those
    // here (both syntax and semantics).
    #[test]
    fn test_valid() {
        let config = read_config(
            "valid",
            r##"
            [console]
            static_dir = "tests/static"
            session_idle_timeout_minutes = 60
            session_absolute_timeout_minutes = 480
            [authn]
            schemes_external = []
            [log]
            mode = "file"
            level = "debug"
            path = "/nonexistent/path"
            if_exists = "fail"
            [timeseries_db]
            address = "[::1]:8123"
            [updates]
            trusted_root = "/path/to/root.json"
            default_base_url = "http://example.invalid/"
            [tunables]
            max_vpc_ipv4_subnet_prefix = 27
            [deployment]
            id = "28b90dc4-c22a-65ba-f49a-f051fe01208f"
            rack_id = "38b90dc4-c22a-65ba-f49a-f051fe01208f"
            [deployment.dropshot_external]
            bind_address = "10.1.2.3:4567"
            request_body_max_bytes = 1024
            [deployment.dropshot_internal]
            bind_address = "10.1.2.3:4568"
            request_body_max_bytes = 1024
            [deployment.internal_dns]
            type = "from_subnet"
            subnet.net = "::/56"
            [deployment.database]
            type = "from_dns"
            [dendrite]
            address = "[::1]:12224"
            [background_tasks]
            dns_internal.period_secs_config = 1
            dns_internal.period_secs_servers = 2
            dns_internal.period_secs_propagation = 3
            dns_internal.max_concurrent_server_updates = 4
            dns_external.period_secs_config = 5
            dns_external.period_secs_servers = 6
            dns_external.period_secs_propagation = 7
            dns_external.max_concurrent_server_updates = 8
            external_endpoints.period_secs = 9
            "##,
        )
        .unwrap();

        assert_eq!(
            config,
            Config {
                deployment: DeploymentConfig {
                    id: "28b90dc4-c22a-65ba-f49a-f051fe01208f".parse().unwrap(),
                    rack_id: "38b90dc4-c22a-65ba-f49a-f051fe01208f"
                        .parse()
                        .unwrap(),
                    dropshot_external: ConfigDropshotWithTls {
                        tls: false,
                        dropshot: ConfigDropshot {
                            bind_address: "10.1.2.3:4567"
                                .parse::<SocketAddr>()
                                .unwrap(),
                            ..Default::default()
                        }
                    },
                    dropshot_internal: ConfigDropshot {
                        bind_address: "10.1.2.3:4568"
                            .parse::<SocketAddr>()
                            .unwrap(),
                        ..Default::default()
                    },
                    internal_dns: InternalDns::FromSubnet {
                        subnet: Ipv6Subnet::<RACK_PREFIX>::new(
                            Ipv6Addr::LOCALHOST
                        )
                    },
                    database: Database::FromDns,
                },
                pkg: PackageConfig {
                    console: ConsoleConfig {
                        static_dir: "tests/static".parse().unwrap(),
                        session_idle_timeout_minutes: 60,
                        session_absolute_timeout_minutes: 480
                    },
                    authn: AuthnConfig { schemes_external: Vec::new() },
                    log: ConfigLogging::File {
                        level: ConfigLoggingLevel::Debug,
                        if_exists: ConfigLoggingIfExists::Fail,
                        path: "/nonexistent/path".into()
                    },
                    timeseries_db: TimeseriesDbConfig {
                        address: Some("[::1]:8123".parse().unwrap())
                    },
                    updates: Some(UpdatesConfig {
                        trusted_root: PathBuf::from("/path/to/root.json"),
                        default_base_url: "http://example.invalid/".into(),
                    }),
                    tunables: Tunables { max_vpc_ipv4_subnet_prefix: 27 },
                    dendrite: DpdConfig {
                        address: Some(
                            SocketAddr::from_str("[::1]:12224").unwrap()
                        )
                    },
                    background_tasks: BackgroundTaskConfig {
                        dns_internal: DnsTasksConfig {
                            period_secs_config: Duration::from_secs(1),
                            period_secs_servers: Duration::from_secs(2),
                            period_secs_propagation: Duration::from_secs(3),
                            max_concurrent_server_updates: 4,
                        },
                        dns_external: DnsTasksConfig {
                            period_secs_config: Duration::from_secs(5),
                            period_secs_servers: Duration::from_secs(6),
                            period_secs_propagation: Duration::from_secs(7),
                            max_concurrent_server_updates: 8,
                        },
                        external_endpoints: ExternalEndpointsConfig {
                            period_secs: Duration::from_secs(9),
                        }
                    },
                },
            }
        );

        let config = read_config(
            "valid",
            r##"
            [console]
            static_dir = "tests/static"
            session_idle_timeout_minutes = 60
            session_absolute_timeout_minutes = 480
            [authn]
            schemes_external = [ "spoof", "session_cookie" ]
            [log]
            mode = "file"
            level = "debug"
            path = "/nonexistent/path"
            if_exists = "fail"
            [timeseries_db]
            address = "[::1]:8123"
            [deployment]
            id = "28b90dc4-c22a-65ba-f49a-f051fe01208f"
            rack_id = "38b90dc4-c22a-65ba-f49a-f051fe01208f"
            [deployment.dropshot_external]
            bind_address = "10.1.2.3:4567"
            request_body_max_bytes = 1024
            [deployment.dropshot_internal]
            bind_address = "10.1.2.3:4568"
            request_body_max_bytes = 1024
            [deployment.internal_dns]
            type = "from_subnet"
            subnet.net = "::/56"
            [deployment.database]
            type = "from_dns"
            [dendrite]
            address = "[::1]:12224"
            [background_tasks]
            dns_internal.period_secs_config = 1
            dns_internal.period_secs_servers = 2
            dns_internal.period_secs_propagation = 3
            dns_internal.max_concurrent_server_updates = 4
            dns_external.period_secs_config = 5
            dns_external.period_secs_servers = 6
            dns_external.period_secs_propagation = 7
            dns_external.max_concurrent_server_updates = 8
            external_endpoints.period_secs = 9
            "##,
        )
        .unwrap();

        assert_eq!(
            config.pkg.authn.schemes_external,
            vec![SchemeName::Spoof, SchemeName::SessionCookie],
        );
    }

    #[test]
    fn test_bad_authn_schemes() {
        let error = read_config(
            "bad authn.schemes_external",
            r##"
            [console]
            static_dir = "tests/static"
            session_idle_timeout_minutes = 60
            session_absolute_timeout_minutes = 480
            [authn]
            schemes_external = ["trust-me"]
            [log]
            mode = "file"
            level = "debug"
            path = "/nonexistent/path"
            if_exists = "fail"
            [timeseries_db]
            address = "[::1]:8123"
            [deployment]
            id = "28b90dc4-c22a-65ba-f49a-f051fe01208f"
            rack_id = "38b90dc4-c22a-65ba-f49a-f051fe01208f"
            [deployment.dropshot_external]
            bind_address = "10.1.2.3:4567"
            request_body_max_bytes = 1024
            [deployment.dropshot_internal]
            bind_address = "10.1.2.3:4568"
            request_body_max_bytes = 1024
            [deployment.internal_dns]
            type = "from_subnet"
            subnet.net = "::/56"
            [deployment.database]
            type = "from_dns"
            "##,
        )
        .expect_err("expected failure");
        if let LoadErrorKind::Parse(error) = &error.kind {
            assert!(
                error
                    .message()
                    .starts_with("unsupported authn scheme: \"trust-me\""),
                "error = {}",
                error
            );
        } else {
            panic!(
                "Got an unexpected error, expected Parse but got {:?}",
                error
            );
        }
    }

    #[test]
    fn test_invalid_ipv4_prefix_tunable() {
        let error = read_config(
            "invalid_ipv4_prefix_tunable",
            r##"
            [console]
            static_dir = "tests/static"
            session_idle_timeout_minutes = 60
            session_absolute_timeout_minutes = 480
            [authn]
            schemes_external = []
            [log]
            mode = "file"
            level = "debug"
            path = "/nonexistent/path"
            if_exists = "fail"
            [timeseries_db]
            address = "[::1]:8123"
            [updates]
            trusted_root = "/path/to/root.json"
            default_base_url = "http://example.invalid/"
            [tunables]
            max_vpc_ipv4_subnet_prefix = 100
            [deployment]
            id = "28b90dc4-c22a-65ba-f49a-f051fe01208f"
            rack_id = "38b90dc4-c22a-65ba-f49a-f051fe01208f"
            [deployment.dropshot_external]
            bind_address = "10.1.2.3:4567"
            request_body_max_bytes = 1024
            [deployment.dropshot_internal]
            bind_address = "10.1.2.3:4568"
            request_body_max_bytes = 1024
            [deployment.internal_dns]
            type = "from_subnet"
            subnet.net = "::/56"
            [deployment.database]
            type = "from_dns"
            "##,
        )
        .expect_err("Expected failure");
        if let LoadErrorKind::Parse(error) = &error.kind {
            assert!(error.message().starts_with(
                r#"invalid "max_vpc_ipv4_subnet_prefix": "IPv4 subnet prefix must"#,
            ));
        } else {
            panic!(
                "Got an unexpected error, expected Parse but got {:?}",
                error
            );
        }
    }

    #[test]
    fn test_repo_configs_are_valid() {
        // The example config file should be valid.
        let config_path = "../nexus/examples/config.toml";
        println!("checking {:?}", config_path);
        let example_config = Config::from_file(config_path)
            .expect("example config file is not valid");

        // The config file used for the tests should also be valid.  The tests
        // won't clear the runway anyway if this file isn't valid.  But it's
        // helpful to verify this here explicitly as well.
        let config_path = "../nexus/examples/config.toml";
        println!("checking {:?}", config_path);
        let _ = Config::from_file(config_path)
            .expect("test config file is not valid");

        // The partial config file that's used to deploy Nexus must also be
        // valid.  However, it's missing the "deployment" section because that's
        // generated at deployment time.  We'll serialize this section from the
        // example config file (loaded above), append it to the contents of this
        // file, and verify the whole thing.
        #[derive(serde::Serialize)]
        struct DummyConfig {
            deployment: DeploymentConfig,
        }
        let config_path = "../smf/nexus/config-partial.toml";
        println!(
            "checking {:?} with example deployment section added",
            config_path
        );
        let mut contents = std::fs::read_to_string(config_path)
            .expect("failed to read Nexus SMF config file");
        contents.push_str(
            "\n\n\n \
            # !! content below added by test_repo_configs_are_valid()\n\
            \n\n\n",
        );
        let example_deployment = toml::to_string_pretty(&DummyConfig {
            deployment: example_config.deployment,
        })
        .unwrap();
        contents.push_str(&example_deployment);
        let _: Config = toml::from_str(&contents)
            .expect("Nexus SMF config file is not valid");
    }
}

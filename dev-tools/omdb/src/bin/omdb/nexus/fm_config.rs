// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands for fault management configuration

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use anyhow::Context;
use clap::ArgAction;
use clap::Args;
use clap::Subcommand;
use nexus_types::fm::FmConfig;
use nexus_types::fm::FmConfigParam;
use nexus_types::fm::FmConfigSource;
use nexus_types::fm::FmConfigView;
use nexus_types::fm::config::Setting;
use nexus_types::fm::config::settings;
use std::fmt;
use std::num::NonZeroU32;
use std::num::ParseIntError;
use std::str::FromStr;

#[derive(Debug, Args)]
pub struct FmConfigArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Show the current fault management configuration.
    ///
    /// This is an alias for `omdb nexus fm-config show current`.
    Current(ShowOptions),

    /// Show a configuration at a given version
    Show(ShowArgs),

    /// Show the system-defined default values for all config settings, as of
    /// the current Oxide system software version.
    ///
    /// This is an alias for `omdb nexus fm-config show default`.
    ShowDefaults(ShowOptions),

    /// Override one or more config option(s), creating a new version.
    ///
    /// The flags listed under 'Config Options' provide values for settings in
    /// the fault-management config. Any number of these flags may be provided
    /// to set multiple config values. If a flag is *not* provided, the previous
    /// value of that setting (which may be an override from an earlier config
    /// version or the system-defined default) is preserved.
    ///
    /// A value of 'default' may also be provided for any flag. This will
    /// explicitly restore that setting to its system-provided default value.
    /// Note that default values may change in new Oxide system software
    /// releases. Therefore, there is a semantic difference between an override
    /// that explicitly sets a config option to a value that *happens* to be the
    /// default, and restoring the default value through this mechanism: if the
    /// default value changes in a new system software version, the override
    /// will remain the same, while the restored default will use the value
    /// defined by the current software version.
    ///
    /// Use the `omdb nexus fm-config show-defaults` command to view the default
    /// values for all config options.
    Set(SetArgs),
}

#[derive(Debug, Clone, Args)]
struct ShowArgs {
    /// The config version to show
    ///
    /// A value of 'current' or 'latest' selects the currently active config
    /// version, while a value of 'default' displays the default values as
    /// defined by the current Oxide system software release. An integer version
    /// will select that config version if it exists.
    #[clap(value_name = "VERSION|current|default")]
    version: ConfigSelector,

    #[clap(flatten)]
    opts: ShowOptions,
}

#[derive(Debug, Clone, Args)]
struct ShowOptions {
    /// If set, output the config in JSON rather than pretty-printing it.
    #[clap(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct SetArgs {
    /// A comment describing this config change.
    ///
    /// Note that comments are mandatory and may not be empty.
    #[clap(long)]
    comment: String,

    #[clap(flatten, next_help_heading = "Config Options")]
    config: ConfigOpts,
}

// Define the options separately so we can use `group(required = true, multiple
// = true).`
#[derive(Debug, Clone, Args)]
#[group(required = true, multiple = true)]
struct ConfigOpts {
    /// Configures the maximum number of sitreps permitted in the database.
    ///
    /// If the number of sitreps in the database, including orphaned sitreps,
    /// reaches or exceeds this limit, fault management analysis will not
    /// produce new sitreps until some are deleted.
    ///
    /// This must be a non-zero integer, or 'default'. If it is set to
    /// 'default', any previous override will be removed, and the system will
    /// revert this setting to the default value.
    #[clap(long, action = ArgAction::Set)]
    sitrep_limit: Option<Setting<settings::SitrepLimit>>,

    /// Sets the number of sitreps in the history table after which the oldest
    /// sitreps will be removed from the history.
    ///
    /// This must be a non-zero integer, or 'default'. If it is set to
    /// 'default', any previous override will be removed, and the system will
    /// revert this setting to the default value.
    ///
    /// If an integer value is provided, it must be less than the total sitrep
    /// limit.
    #[clap(long, action = ArgAction::Set)]
    history_pruning_threshold:
        Option<Setting<settings::HistoryPruningThreshold>>,

    /// BREAK GLASS IN CASE OF EMERGENCY: COMPLETELY DISABLE FM ANALYSIS
    ///
    /// This must be a boolean ('true' or 'false'), or 'default'. If it is set to
    /// 'default', any previous override will be removed, and the system will
    /// revert this setting to the default value.
    #[clap(long, action = ArgAction::Set)]
    analysis_enabled: Option<Setting<settings::AnalysisEnabled>>,
}

impl ConfigOpts {
    /// Returns an updated `FmConfig` regardless of
    /// whether any values were modified.
    fn update(&self, current: &FmConfig) -> FmConfig {
        FmConfig {
            sitrep_limit: self.sitrep_limit.unwrap_or(current.sitrep_limit),
            history_pruning_threshold: self
                .history_pruning_threshold
                .unwrap_or(current.history_pruning_threshold),
            analysis_enabled: self
                .analysis_enabled
                .unwrap_or(current.analysis_enabled),
        }
    }

    /// Returns an updated `FmConfigParam` if any
    /// values were modified, or `None` if no changes were made.
    fn update_if_modified(
        &self,
        current: &FmConfigView,
        comment: &str,
    ) -> anyhow::Result<Option<FmConfigParam>> {
        let new = self.update(&current.config);
        if new == current.config {
            return Ok(None);
        }
        let version = match current.source {
            FmConfigSource::Default => NonZeroU32::new(1).unwrap(),
            FmConfigSource::Override { version, .. } => {
                version.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!(
                        "cannot update the FM config, as the maximum number of \
                         versions has been reached",
                    )
                })?
            }
        };
        let param =
            FmConfigParam { version, comment: comment.to_owned(), config: new };
        param.validate()?;
        Ok(Some(param))
    }
}

#[derive(Debug, Clone, Copy)]
enum ConfigSelector {
    Default,
    Current,
    Version(NonZeroU32),
}

impl FromStr for ConfigSelector {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("default") {
            return Ok(Self::Default);
        }
        if s.eq_ignore_ascii_case("current") || s.eq_ignore_ascii_case("latest")
        {
            return Ok(Self::Current);
        }
        let version = s.trim_start_matches(['v', 'V']).parse()?;
        Ok(Self::Version(version))
    }
}

impl fmt::Display for ConfigSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => {
                f.write_str("fault management configuration defaults")
            }
            Self::Current => {
                f.write_str("current fault management configuration")
            }
            Self::Version(v) => {
                write!(f, "fault management configuration v{v}")
            }
        }
    }
}

pub async fn cmd_nexus_fm_config(
    omdb: &Omdb,
    client: &nexus_lockstep_client::Client,
    args: &FmConfigArgs,
) -> Result<(), anyhow::Error> {
    match &args.command {
        Commands::Current(opts) => {
            show_config(client, ConfigSelector::Current, opts).await
        }
        Commands::Show(ShowArgs { version, opts }) => {
            show_config(client, *version, opts).await
        }
        Commands::ShowDefaults(opts) => {
            show_config(client, ConfigSelector::Default, opts).await
        }
        Commands::Set(args) => {
            let token = omdb.check_allow_destructive()?;
            set_config(client, args, token).await
        }
    }
}

async fn show_config(
    client: &nexus_lockstep_client::Client,
    selector: ConfigSelector,
    opts: &ShowOptions,
) -> Result<(), anyhow::Error> {
    let config = match selector {
        ConfigSelector::Current => {
            client.fm_config_show_current().await?.into_inner()
        }
        ConfigSelector::Version(version) => {
            client.fm_config_show_version(version.get()).await?.into_inner()
        }
        ConfigSelector::Default => FmConfigView::default(),
    };

    if opts.json {
        serde_json::to_writer_pretty(std::io::stdout().lock(), &config)?;
    } else {
        println!("{selector}:\n{}", config.display_multiline(2));
    }

    Ok(())
}

async fn set_config(
    client: &nexus_lockstep_client::Client,
    args: &SetArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let current_config = client
        .fm_config_show_current()
        .await
        .context(
            "cannot set a new fault management config, because reading the \
             current config failed",
        )?
        .into_inner();
    let Some(new_config) =
        args.config.update_if_modified(&current_config, &args.comment)?
    else {
        println!(
            "no modifications made to current config values:\n{}",
            current_config.display_multiline(2)
        );
        return Ok(());
    };

    client.fm_config_set(&new_config).await?;
    println!(
        "fault management config updated to version {}:\n{}",
        new_config.version,
        // TODO(eliza): it would be nice to display a diff here the way the
        // reconfigurator config does, but a lot of the diff-displaying
        // machinery is currently kind of reconfigurator-specific. Let's figure
        // out what can be generalized later.
        new_config.config.display_multiline(2),
    );

    Ok(())
}

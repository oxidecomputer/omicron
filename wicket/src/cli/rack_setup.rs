// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for rack setup configuration via wicketd.

use crate::ui::defaults::style::BULLET_ICON;
use crate::ui::defaults::style::CHECK_ICON;
use crate::ui::defaults::style::WARN_ICON;
use crate::wicketd::create_wicketd_client;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use clap::Args;
use clap::Subcommand;
use clap::ValueEnum;
use omicron_passwords::Password;
use omicron_passwords::PasswordHashString;
use owo_colors::OwoColorize;
use owo_colors::Style;
use slog::Logger;
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::io::Read;
use std::mem;
use std::net::SocketAddrV6;
use std::time::Duration;
use wicket_common::rack_setup::BgpAuthKey;
use wicket_common::rack_setup::BgpAuthKeyId;
use wicket_common::rack_setup::BgpAuthKeyStatus;
use wicket_common::rack_setup::DisplaySlice;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicketd_client::types::CertificateUploadResponse;
use wicketd_client::types::GetBgpAuthKeyParams;
use wicketd_client::types::NewPasswordHash;
use wicketd_client::types::PutBgpAuthKeyBody;
use wicketd_client::types::PutRssRecoveryUserPasswordHash;
use wicketd_client::types::SetBgpAuthKeyStatus;
use zeroize::Zeroizing;

mod config_toml;

use config_toml::TomlTemplate;

use super::GlobalOpts;

const WICKETD_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Subcommand)]
pub(crate) enum SetupArgs {
    /// Get the current rack configuration as a TOML template
    ///
    /// Save this template to a file, edit it, then upload it via `set-config`.
    GetConfig,

    /// Set the current rack configuration from a filled-in TOML template
    SetConfig,

    /// Reset the configuration to its original (empty) state.
    ///
    /// This will also clear any set password and uploaded certificates/keys.
    ResetConfig,

    /// Set the password for the recovery user of the recovery silo
    SetPassword,

    /// Set one or more BGP authentication keys.
    SetBgpAuthKey(SetBgpAuthKeyArgs),

    // TODO: reset auth keys
    /// Upload a certificate chain
    ///
    /// This cert chain must be PEM encoded. Uploading a cert chain should be
    /// paired with uploading its private key via `upload-key`.
    UploadCert,

    /// Upload the private key of a certificate chain
    ///
    /// This key must be PEM encoded. Uploading a key should be paired with
    /// uploading its cert chain via `upload-cert`.
    UploadKey,
}

impl SetupArgs {
    pub(crate) async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
    ) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        match self {
            SetupArgs::GetConfig => {
                let config = client
                    .get_rss_config()
                    .await
                    .context("error fetching current config from wicketd")?
                    .into_inner();

                let template = TomlTemplate::populate(&config.insensitive);

                // This is intentionally not `println`; our template already
                // includes the final newline.
                print!("{template}");
            }
            SetupArgs::SetConfig => {
                let mut config = String::new();
                slog::info!(log, "reading config from stdin...");
                io::stdin()
                    .read_to_string(&mut config)
                    .context("failed to read config from stdin")?;

                slog::info!(log, "parsing config...");
                let config: PutRssUserConfigInsensitive =
                    toml::de::from_str(&config)
                        .context("failed to parse config TOML")?;

                slog::info!(log, "uploading config to wicketd...");
                client
                    .put_rss_config(&config)
                    .await
                    .context("error uploading config to wicketd")?;

                slog::info!(log, "config upload complete");
            }
            SetupArgs::ResetConfig => {
                slog::info!(log, "instructing wicketd to reset config...");
                client
                    .delete_rss_config()
                    .await
                    .context("failed to clear config")?;
                slog::info!(log, "configuration reset");
            }
            SetupArgs::SetPassword => {
                let hash = read_and_hash_password(&log)?;
                let hash = NewPasswordHash(hash.to_string());

                slog::info!(log, "uploading password hash to wicketd...");
                client
                    .put_rss_config_recovery_user_password_hash(
                        &PutRssRecoveryUserPasswordHash { hash },
                    )
                    .await
                    .context("failed to upload password hash to wicketd")?;
                slog::info!(log, "password set");
            }
            SetupArgs::SetBgpAuthKey(args) => {
                args.exec(&log, &client, global_opts).await?;
            }
            SetupArgs::UploadCert => {
                slog::info!(log, "reading cert from stdin...");
                let mut cert = String::new();
                io::stdin()
                    .read_to_string(&mut cert)
                    .context("failed to read certificate from stdin")?;

                slog::info!(log, "uploading cert to wicketd...");
                let result = client
                    .post_rss_config_cert(&cert)
                    .await
                    .context("failed to upload cert to wicketd")?
                    .into_inner();

                match result {
                    CertificateUploadResponse::WaitingOnCert => {
                        slog::warn!(
                            log,
                            "wicketd waiting on cert \
                            (but we just uploaded it? this is a bug!)"
                        );
                    }
                    CertificateUploadResponse::WaitingOnKey => {
                        slog::info!(
                            log,
                            "certificate uploaded; now upload its key via \
                             `setup upload-key`"
                        );
                    }
                    CertificateUploadResponse::CertKeyAccepted => {
                        slog::info!(
                            log,
                            "certificate uploaded; \
                             matches previously-uploaded key"
                        );
                    }
                    CertificateUploadResponse::CertKeyDuplicateIgnored => {
                        slog::warn!(
                            log,
                            "certificate/key pair valid but already uploaded"
                        );
                    }
                }
            }
            SetupArgs::UploadKey => {
                slog::info!(log, "reading key from stdin...");
                let mut key = Zeroizing::new(String::new());
                io::stdin()
                    .read_to_string(&mut key)
                    .context("failed to read key from stdin")?;

                slog::info!(log, "uploading key to wicketd...");
                let result = client
                    .post_rss_config_key(&key)
                    .await
                    .context("failed to upload key to wicketd")?
                    .into_inner();

                match result {
                    CertificateUploadResponse::WaitingOnCert => {
                        slog::info!(
                            log,
                            "key uploaded; now upload its certificate via \
                             `setup upload-cert`"
                        );
                    }
                    CertificateUploadResponse::WaitingOnKey => {
                        slog::warn!(
                            log,
                            "wicketd waiting on key \
                            (but we just uploaded it? this is a bug!)"
                        );
                    }
                    CertificateUploadResponse::CertKeyAccepted => {
                        slog::info!(
                            log,
                            "key uploaded; \
                             matches previously-uploaded certificate"
                        );
                    }
                    CertificateUploadResponse::CertKeyDuplicateIgnored => {
                        slog::warn!(
                            log,
                            "certificate/key pair valid but already uploaded"
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

fn read_and_hash_password(log: &Logger) -> Result<PasswordHashString> {
    let pass1 = rpassword::prompt_password(
        "Password for recovery user of recovery silo: ",
    )
    .context("failed to read password (do you need to use `ssh -t`?)")?;
    let pass1 = Zeroizing::new(pass1);

    let pass2 = rpassword::prompt_password(
        "Confirm password for recovery user of recovery silo: ",
    )
    .context("failed to read password confirmation")?;
    let pass2 = Zeroizing::new(pass2);

    if pass1 != pass2 {
        bail!("passwords do not match");
    }
    mem::drop(pass2);

    let password = Password::new(&pass1).context("invalid password")?;
    mem::drop(pass1);

    slog::info!(log, "hashing password...");
    let mut hasher = omicron_passwords::Hasher::default();
    let hash = hasher.create_password(&password).context("invalid password")?;

    Ok(hash)
}

#[derive(Debug, Args)]
pub(crate) struct SetBgpAuthKeyArgs {
    #[clap(flatten)]
    spec: BgpAuthKeySpec,

    /// The authentication method.
    #[clap(long, short = 'M', value_enum, default_value_t = BgpAuthMethod::TcpMd5)]
    auth_method: BgpAuthMethod,
}

impl SetBgpAuthKeyArgs {
    async fn exec(
        self,
        log: &Logger,
        client: &wicketd_client::Client,
        global_opts: GlobalOpts,
    ) -> Result<()> {
        let mut styles = Styles::default();
        if global_opts.use_color() {
            styles.colorize();
        }

        slog::debug!(log, "fetching current status for BGP auth keys...");
        let response = client
            .get_bgp_auth_key_info(&GetBgpAuthKeyParams {
                check_valid: self.spec.ids.clone(),
            })
            .await?
            .into_inner();
        if response.data.is_empty() {
            // This can only be hit in the --all case, since we require at
            // least one key ID.
            eprintln!(
                "{}: no BGP authentication keys defined in rack setup config",
                "info".style(styles.bold),
            );
            return Ok(());
        }

        display_auth_key_data(&response.data, &styles);

        // Now see which keys we were going to work on. In the --all case,
        // it'll be all of the unset keys. In the --ids case, it'll be all the
        // ones specified.
        let keys_to_set: Vec<_> = if self.spec.all {
            response
                .data
                .iter()
                .filter_map(|(id, status)| {
                    status.is_unset().then(|| id.clone())
                })
                .collect()
        } else {
            self.spec.ids.clone()
        };

        if keys_to_set.is_empty() {
            // This can only be hit in the --all case, since we require at
            // least one key ID.
            eprintln!(
                "{}: all keys are already set -- no keys to set",
                "info".style(styles.bold)
            );
            return Ok(());
        }

        let len = keys_to_set.len();
        slog::debug!(
            log,
            "setting BGP auth keys";
            "authentication" => %self.auth_method,
            "keys_to_set" => %DisplaySlice(&keys_to_set),
        );
        let display_count = if self.spec.all {
            format!("all {len} unset")
        } else {
            format!("{len}")
        };

        eprintln!(
            "\nsetting {} {} to use {} authentication",
            display_count.style(styles.bold),
            keys_str(len),
            self.auth_method.style(styles.bold),
        );

        let check_icon = CHECK_ICON.style(styles.success);
        let warn_icon = WARN_ICON.style(styles.warning);

        // We're going to add to this count as we set *new* keys.
        let mut set_count =
            response.data.values().filter(|status| status.is_set()).count();

        for (i, key_id) in keys_to_set.iter().enumerate() {
            let status = response.data.get(&key_id).with_context(|| {
                format!(
                    "info for key {} not returned \
                     (should have produced an HTTP error earlier!)",
                    key_id
                )
            })?;
            let curr = i + 1;

            match self.auth_method {
                BgpAuthMethod::TcpMd5 => {
                    let (verb, style) = match status {
                        BgpAuthKeyStatus::Set { .. } => {
                            ("change", styles.warning)
                        }
                        BgpAuthKeyStatus::Unset => ("add", styles.success),
                    };

                    let count_str = format!(" ({curr}/{len}) ");
                    let clen = count_str.len();

                    let prompt = format!(
                        "  {BULLET_ICON}{count_str}{} {}: ",
                        verb.style(style),
                        key_id.style(styles.bold),
                    );

                    let key = read_bgp_md5_key(&prompt)?;
                    let info = key.info().to_string_styled(styles.bold);
                    let response = client
                        .put_bgp_auth_key(&key_id, &PutBgpAuthKeyBody { key })
                        .await
                        .context("failed to set BGP auth key")?;

                    let status = response.into_inner().status;
                    match status {
                        SetBgpAuthKeyStatus::Added => {
                            eprintln!(
                                "{INDENT}{:clen$} {check_icon} key {}: {info}",
                                "",
                                "added".style(styles.bold),
                            );
                            set_count += 1;
                        }
                        SetBgpAuthKeyStatus::Replaced => {
                            eprintln!(
                                "{INDENT}{:clen$} {check_icon} key {}: {info}",
                                "",
                                "replaced".style(styles.bold),
                            );
                            // We're replacing an existing key, so we don't
                            // increment set_count.
                        }
                        SetBgpAuthKeyStatus::Unchanged => {
                            eprintln!(
                                "{INDENT}{:clen$} {warn_icon} key {}: {info}",
                                "",
                                "unchanged".style(styles.warning)
                            );
                            // Same -- don't increment set_count.
                        }
                    }

                    slog::debug!(
                        log,
                        "BGP auth key for {key_id} set";
                        "info" => %info,
                        "status" => ?status
                    );
                }
            }
        }

        println!(
            "{check_icon} {}/{} {} set",
            set_count.style(styles.bold),
            response.data.len().style(styles.bold),
            keys_str(len),
        );

        Ok(())
    }
}

#[derive(Debug, Args)]
#[group(required = true)]
struct BgpAuthKeySpec {
    /// The key IDs to operate on.
    ids: Vec<BgpAuthKeyId>,

    /// Operate on all unset keys (for set), all keys (for reset).
    #[clap(long, conflicts_with = "ids")]
    all: bool,
}

/// BGP authentication method.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum BgpAuthMethod {
    /// TCP-MD5 authentication.
    TcpMd5,
}

impl fmt::Display for BgpAuthMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BgpAuthMethod::TcpMd5 => write!(f, "TCP-MD5"),
        }
    }
}

fn display_auth_key_data(
    data: &BTreeMap<BgpAuthKeyId, BgpAuthKeyStatus>,
    styles: &Styles,
) {
    let set_count = data.values().filter(|status| status.is_set()).count();
    let total_count = data.len();
    eprintln!(
        "current BGP authentication keys ({}/{} set):",
        set_count.style(styles.bold),
        total_count.style(styles.bold),
    );
    for (id, status) in data {
        eprint!("{INDENT}{BULLET_ICON} {}: ", id.style(styles.bold));
        match status {
            BgpAuthKeyStatus::Set { info } => {
                eprintln!(
                    "{} to {}",
                    "set".style(styles.success),
                    info.to_string_styled(styles.bold),
                );
            }
            BgpAuthKeyStatus::Unset => {
                eprintln!("{}", "unset".style(styles.warning));
            }
        }
    }
}

fn read_bgp_md5_key(prompt: &str) -> Result<BgpAuthKey> {
    let key = rpassword::prompt_password(prompt)
        .context("failed to read MD5 authentication key")?;
    Ok(BgpAuthKey::TcpMd5 { key })
}

#[derive(Debug, Default)]
struct Styles {
    bold: Style,
    success: Style,
    warning: Style,
}

impl Styles {
    fn colorize(&mut self) {
        self.bold = Style::new().bold();
        self.success = Style::new().green();
        self.warning = Style::new().yellow();
    }
}

static INDENT: &str = "  ";

fn keys_str(count: usize) -> &'static str {
    match count {
        1 => "key",
        _ => "keys",
    }
}

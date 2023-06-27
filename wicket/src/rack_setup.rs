// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for rack setup configuration via wicketd.

use crate::wicketd::create_wicketd_client;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use clap::Subcommand;
use omicron_passwords::Password;
use omicron_passwords::PasswordHashString;
use slog::Logger;
use std::io;
use std::io::Read;
use std::mem;
use std::net::SocketAddrV6;
use std::time::Duration;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicketd_client::types::CertificateUploadResponse;
use wicketd_client::types::NewPasswordHash;
use wicketd_client::types::PutRssRecoveryUserPasswordHash;
use zeroize::Zeroizing;

mod config_toml;

use config_toml::TomlTemplate;

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
    pub(crate) fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
    ) -> Result<()> {
        let runtime =
            tokio::runtime::Runtime::new().context("creating tokio runtime")?;

        runtime.block_on(self.exec_impl(log, wicketd_addr))
    }

    async fn exec_impl(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
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

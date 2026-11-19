// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-john docs pass and status type

use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use chrono::DateTime;
use chrono::Utc;
use omicron_generation_kinds::WicketSshAuthGeneration;
use schemars::JsonSchema;
use serde::de::Error;
use serde::{Deserialize, Serialize};
use slog_error_chain::InlineErrorChain;
use ssh_key::AuthorizedKeys;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use uuid::Uuid;

#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
pub struct WicketSshAuthPath {
    pub generation: WicketSshAuthGeneration,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct WicketSshAuth {
    pub generation: WicketSshAuthGeneration,
    pub time_created: DateTime<Utc>,
    pub description: String,
    pub kind: WicketSshAuthKind,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WicketSshAuthKind {
    /// `wicket` is publicly accessible to any client.
    Unauthenticated,

    /// `wicket` is only accessible to clients supplying one of the specified
    /// SSH keys.
    PublicKeys {
        /// Set of operator-specified keys allowed to access wicket.
        ///
        /// This set may be empty. If it is empty and `allow_support_keys` is
        /// `false`, no SSH connections to `wicket` will succeed.
        authorized_keys: Vec<WicketSshKey>,

        /// Whether to allow the built-in support keys shipped with the release
        /// access to `wicket`.
        ///
        /// This setting only controls the support keys' access to `wicket`;
        /// even if this is false, the support keys can still be used for
        /// general support operations (support access is strictly more powerful
        /// than access to `wicket`; we recommend leaving this set to `true`).
        allow_support_keys: bool,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct WicketSshKey {
    pub key: String,
    pub description: String,
}

/// Specification of how SSH access to `wicket` on the rack tech port is
/// authenticated.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct WicketSshAuthCreate {
    pub description: String,
    pub kind: WicketSshAuthKindCreate,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WicketSshAuthKindCreate {
    /// `wicket` is publicly accessible to any client.
    Unauthenticated,

    /// `wicket` is only accessible to clients supplying one of the specified
    /// SSH keys.
    PublicKeys {
        /// Set of operator-specified keys allowed to access wicket.
        ///
        /// This set may be empty. If it is empty and `allow_support_keys` is
        /// `false`, no SSH connections to `wicket` will succeed.
        #[serde(deserialize_with = "deserialize_create_authorized_keys")]
        #[schemars(length(max = 32))]
        authorized_keys: Vec<WicketSshKeyCreate>,

        /// Whether to allow the built-in support keys shipped with the release
        /// access to `wicket`.
        ///
        /// This setting only controls the support keys' access to `wicket`;
        /// even if this is false, the support keys can still be used for
        /// general support operations (support access is strictly more powerful
        /// than access to `wicket`; we recommend leaving this set to `true`).
        allow_support_keys: bool,
    },
}

fn deserialize_create_authorized_keys<'de, D>(
    deserializer: D,
) -> Result<Vec<WicketSshKeyCreate>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    // We don't expect operators to supply very many wicket SSH keys, and other
    // parts of the system implicitly assume that (e.g., copying the entire set
    // of keys to every sled's `OmicronSledConfig`); enforce some "unreasonably
    // high" bound just to avoid any unexpected surprises.
    //
    // The baked-in support keys do _not_ count against this number; those are
    // appended by sled-agent based only on the `allow_support_keys` boolean it
    // receives.
    //
    // This must be kept in sync with the `#[schemars(length(max = ...))]` value
    // on the `authorized_keys` field in `WicketSshAuthKindCreate` above (which
    // is required to be a literal).
    const MAX_OPERATOR_KEYS: usize = 32;

    let keys = Vec::<WicketSshKeyCreate>::deserialize(deserializer)?;

    // Enforce upper bound.
    if keys.len() > MAX_OPERATOR_KEYS {
        return Err(D::Error::invalid_length(
            keys.len(),
            &format!("at most {MAX_OPERATOR_KEYS} authorized_keys").as_str(),
        ));
    }

    // Reject any duplicate keys. (These will cause primary key conflicts in the
    // db.)
    let mut unique_keys = BTreeSet::new();
    for k in &keys {
        if !unique_keys.insert(k.key.as_str()) {
            return Err(D::Error::custom(format!(
                "wicket SSH key request contains duplicate key: {:?}",
                k.key.as_str(),
            )));
        }
    }

    Ok(keys)
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct WicketSshKeyCreate {
    pub key: WicketSshAuthorizedKey,
    pub description: String,
}

/// SSH key eligible to access `wicket`.
///
/// This is a single line of an OpenSSH `authorized_keys` file. Options are not
/// allowed. Supported key algorithms are:
///
/// * `ecdsa-sha2-nistp256`
/// * `ecdsa-sha2-nistp384`
/// * `ecdsa-sha2-nistp521`
/// * `sk-ssh-ed25519@openssh.com`
/// * `sk-ecdsa-sha2-nistp256@openssh.com`
/// * `ssh-ed25519`
/// * `ssh-rsa`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(into = "String", try_from = "String")]
pub struct WicketSshAuthorizedKey(String);

impl WicketSshAuthorizedKey {
    // A (gross) test ensures this list matches the doc comment on
    // `WicketSshKey`. If you're updating this list in a new API version, ensure
    // you update both places and copy over the test!
    //
    // This list matches the currently-supported algorithms listed by `sshd` at
    // the time of writing (R23 - R24).
    const ALLOWED_KEY_ALGS: &[ssh_key::Algorithm] = &[
        ssh_key::Algorithm::Ecdsa { curve: ssh_key::EcdsaCurve::NistP256 },
        ssh_key::Algorithm::Ecdsa { curve: ssh_key::EcdsaCurve::NistP384 },
        ssh_key::Algorithm::Ecdsa { curve: ssh_key::EcdsaCurve::NistP521 },
        ssh_key::Algorithm::Rsa { hash: None },
        ssh_key::Algorithm::SkEd25519,
        ssh_key::Algorithm::SkEcdsaSha2NistP256,
        ssh_key::Algorithm::Ed25519,
    ];

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<WicketSshAuthorizedKey> for String {
    fn from(value: WicketSshAuthorizedKey) -> Self {
        value.0
    }
}

impl TryFrom<String> for WicketSshAuthorizedKey {
    // Errors from this conversion get stringified by serde, which only calls
    // `.to_string()` on the error. This will lose the error chain, so we do our
    // own splatting of the full error chain here. Yuck.
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        fn real_impl(value: String) -> anyhow::Result<WicketSshAuthorizedKey> {
            let mut parsed_keys = AuthorizedKeys::new(&value);
            let parsed_key = parsed_keys
                .next()
                .ok_or_else(|| anyhow!("SSH key cannot be empty"))?
                .with_context(|| format!("failed to parse key {value:?}"))?;

            // Validation:
            // * only one key per entry
            // * no authorized_keys options
            // * algorithm is allowed
            if parsed_keys.next().is_some() {
                bail!("SSH key string contains multiple keys: {value:?}");
            }
            if !parsed_key.config_opts().is_empty() {
                bail!("SSH key contains options (none allowed): {value:?}");
            }
            let algorithm = parsed_key.public_key().algorithm();
            if !WicketSshAuthorizedKey::ALLOWED_KEY_ALGS.contains(&algorithm) {
                bail!(
                    "SSH key algorithm `{algorithm}` not supported: \
                 must be one of {}",
                    WicketSshAuthorizedKey::ALLOWED_KEY_ALGS
                        .iter()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                );
            }

            // serialize the parsed key; this will strip out anything that the
            // parser discarded (e.g., leading `#` comment lines)
            let key_only =
                parsed_key.public_key().to_openssh().with_context(|| {
                    format!("failed to serialize {value:?} in OpenSSH format")
                })?;
            Ok(WicketSshAuthorizedKey(key_only))
        }

        real_impl(value).map_err(|e| InlineErrorChain::new(&*e).to_string())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct WicketSshAuthStatus {
    pub current_generation: WicketSshAuthGeneration,
    pub status_time_collected: WicketSshAuthStatusTimeCollected,
    pub sleds: BTreeMap<Uuid, WicketSshAuthStatusSled>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct WicketSshAuthStatusTimeCollected {
    pub started: DateTime<Utc>,
    pub done: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct WicketSshAuthStatusSled {
    pub configured_generation: WicketSshAuthGeneration,
    pub latest: WicketSshAuthStatusSledLatest,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WicketSshAuthStatusSledLatest {
    Unknown,
    Known {
        persisted_generation: Option<WicketSshAuthGeneration>,
        switch_zone_status: WicketSshAuthSwitchZoneStatus,
    },
}

/// Status of wicket's SSH auth settings in a sled's switch zone, if it has one.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WicketSshAuthSwitchZoneStatus {
    /// This sled has no active switch zone.
    NoSwitchZone,

    /// This sled has an active switch zone.
    SwitchZone {
        /// The current auth generation setting in effect in our switch zone.
        effective_auth_gen: WicketSshAuthGeneration,

        /// If the most recent attempt to set a new auth generation failed,
        /// contains the error.
        last_error: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::fs;

    // Very goofy test that reads the current file source, scans it in a
    // cheap-but-fragile way to find the doc comment on `WicketSshKey`, and
    // confirms that comment's list of supported algorithms matches the
    // `WicketSshKey::ALLOWED_KEY_ALGS` constant.
    #[test]
    fn test_key_algorithm_docs_match_constant() {
        // `file!()` gives us the full file path within omicron; strip off to
        // just this crate.
        let this_file_path = file!()
            .strip_prefix("nexus/types/versions/")
            .expect("file lives within expected directory");
        let this_file =
            fs::read_to_string(this_file_path).expect("read this source file");

        // Find the bulleted list of key algorithms in the doc comment on
        // `WicketSshKey`: look for the struct definition, then scan backwards
        // for lines starting with `/// * ` until we hit a `}` or blank line
        // (the preceding type / whitespace).
        let doc_algorithms = {
            let lines = this_file.lines().collect::<Vec<_>>();
            let mut i = lines
                .iter()
                .position(|line| {
                    line.starts_with("pub struct WicketSshKeyValidator(")
                })
                .expect("found WicketSshKeyValidator struct");
            let mut algorithms = BTreeSet::new();
            while i > 0 {
                let line = lines[i];
                if let Some(key) = line.strip_prefix("/// * `") {
                    algorithms.insert(
                        key.split_once('`')
                            .expect("doc comment key ends with `")
                            .0,
                    );
                } else if line == "}" || line.is_empty() {
                    break;
                }
                i -= 1;
            }
            algorithms
        };

        let expected_algorithms = WicketSshAuthorizedKey::ALLOWED_KEY_ALGS
            .iter()
            .map(|k| k.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(doc_algorithms, expected_algorithms);
    }
}

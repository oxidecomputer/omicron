// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::collections::btree_map;
use thiserror::Error;
use wicket_common::rack_setup::BgpAuthKeyInfo;
use wicket_common::rack_setup::BgpAuthKeyStatus;
use wicket_common::rack_setup::DisplaySlice;
use wicketd_commission_types::rack_setup::BgpAuthKey;
use wicketd_commission_types::rack_setup::BgpAuthKeyId;
use wicketd_commission_types::rack_setup::SetBgpAuthKeyStatus;

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum BgpAuthKeyError {
    #[error(
        "key IDs not found: {} (valid key IDs: {})",
        DisplaySlice(.not_found),
        DisplaySlice(.valid_keys),
    )]
    KeyIdsNotFound {
        not_found: Vec<BgpAuthKeyId>,
        valid_keys: Vec<BgpAuthKeyId>,
    },
}

/// BGP auth keys are identified by the key ID.
///
/// It is an invariant that any key IDs defined in `rack_network_config` exist
/// here.
///
/// Currently these are always TCP-MD5 keys.
#[derive(Default, Clone)]
pub struct BgpAuthKeys {
    keys: BTreeMap<BgpAuthKeyId, Option<BgpAuthKey>>,
}

impl BgpAuthKeys {
    pub(crate) fn get(
        &self,
        key_id: &BgpAuthKeyId,
    ) -> Option<&Option<BgpAuthKey>> {
        self.keys.get(key_id)
    }

    pub(crate) fn iter(
        &self,
    ) -> btree_map::Iter<'_, BgpAuthKeyId, Option<BgpAuthKey>> {
        self.keys.iter()
    }

    pub(crate) fn check_valid<'a>(
        &self,
        check_valid: impl IntoIterator<Item = &'a BgpAuthKeyId>,
    ) -> Result<(), BgpAuthKeyError> {
        let not_found: Vec<_> = check_valid
            .into_iter()
            .filter(|key_id| !self.keys.contains_key(key_id))
            .cloned()
            .collect();
        if !not_found.is_empty() {
            return Err(self.make_key_ids_not_found_error(not_found));
        }

        Ok(())
    }

    pub(crate) fn get_data(&self) -> BTreeMap<BgpAuthKeyId, BgpAuthKeyStatus> {
        self.keys
            .iter()
            .map(|(key_id, key)| {
                let status = key
                    .as_ref()
                    .map(|key| BgpAuthKeyStatus::Set {
                        info: BgpAuthKeyInfo::for_key(key),
                    })
                    .unwrap_or(BgpAuthKeyStatus::Unset);
                (key_id.clone(), status)
            })
            .collect()
    }

    pub(crate) fn set_key(
        &mut self,
        key_id: BgpAuthKeyId,
        key: BgpAuthKey,
    ) -> Result<SetBgpAuthKeyStatus, BgpAuthKeyError> {
        match self.keys.entry(key_id.clone()) {
            btree_map::Entry::Occupied(mut entry) => match entry.get() {
                Some(old_key) if old_key == &key => {
                    Ok(SetBgpAuthKeyStatus::Unchanged)
                }
                Some(_) => {
                    entry.insert(Some(key));
                    Ok(SetBgpAuthKeyStatus::Replaced)
                }
                None => {
                    // This is a new key; we don't have it yet.
                    entry.insert(Some(key));
                    Ok(SetBgpAuthKeyStatus::Added)
                }
            },
            btree_map::Entry::Vacant(_) => {
                Err(self.make_key_ids_not_found_error(vec![key_id]))
            }
        }
    }

    /// Sync the key map with a new set of key IDs, preserving existing keys
    /// where possible and dropping keys that are no longer referenced.
    pub(crate) fn sync_keys(
        &mut self,
        new_key_ids: impl IntoIterator<Item = BgpAuthKeyId>,
    ) {
        let mut old_keys = std::mem::take(&mut self.keys);
        self.keys = new_key_ids
            .into_iter()
            .map(|key_id| {
                (
                    key_id.clone(),
                    // For each new key, either grab the corresponding old key,
                    // or initialize to None.
                    old_keys.remove(&key_id).unwrap_or_else(|| None),
                )
            })
            .collect();
    }

    #[must_use]
    fn make_key_ids_not_found_error(
        &self,
        key_ids: Vec<BgpAuthKeyId>,
    ) -> BgpAuthKeyError {
        let valid_key_ids = self.keys.keys().cloned().collect();
        BgpAuthKeyError::KeyIdsNotFound {
            not_found: key_ids,
            valid_keys: valid_key_ids,
        }
    }

    #[cfg(test)]
    pub(crate) fn insert(
        &mut self,
        key_id: BgpAuthKeyId,
        key: Option<BgpAuthKey>,
    ) {
        self.keys.insert(key_id, key);
    }
}

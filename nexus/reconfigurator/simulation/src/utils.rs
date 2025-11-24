// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for internal simulation.

use std::{fmt, hash::Hash};

use indexmap::IndexMap;
use omicron_uuid_kinds::{TypedUuid, TypedUuidKind};
use swrite::{SWrite, swrite};

pub(crate) fn join_comma_or_none<I, T: fmt::Display>(iter: I) -> String
where
    I: IntoIterator<Item = T>,
{
    let mut iter = iter.into_iter();
    match iter.next() {
        Some(first) => iter.fold(first.to_string(), |mut acc, x| {
            swrite!(acc, ", {}", x);
            acc
        }),
        None => "(none)".to_string(),
    }
}

/// Inserts an entry into an `IndexMap` in sorted order. Assumes that the map is
/// sorted by `pred` already.
///
/// `pred` should return true for all entries that are less than the new entry,
/// and false for all entries greater than the new entry. This function uses
/// [`slice::partition_point`] internally; for more, see the documentation for
/// that function. (Generally, `pred` should be of the form `|k, v| k <=
/// to_insert`).
///
/// Returns an error if the key already exists in the map.
pub(crate) fn insert_sorted_by<K, V, P>(
    map: &mut IndexMap<K, V>,
    key: K,
    value: V,
    pred: P,
) -> Result<(), DuplicateKey<K>>
where
    K: Hash + Eq,
    P: FnMut(&K, &V) -> bool,
{
    // It would be nice to use the entry API for this, but that API does mutable
    // access, and doesn't permit a binary search to be run on the map. (There's
    // `VacantEntry::insert_sorted`, but not `VacantEntry::insert_sorted_by`.)
    if map.contains_key(&key) {
        return Err(DuplicateKey(key));
    }

    let ix = map.partition_point(pred);
    map.shift_insert(ix, key, value);

    Ok(())
}

/// Displays a prefix of the given UUID string based on whether `verbose` is
/// true.
pub struct DisplayUuidPrefix<T: TypedUuidKind> {
    uuid: TypedUuid<T>,
    verbose: bool,
}

impl<T: TypedUuidKind> DisplayUuidPrefix<T> {
    pub fn new(uuid: TypedUuid<T>, verbose: bool) -> Self {
        Self { uuid, verbose }
    }
}

impl<T: TypedUuidKind> fmt::Display for DisplayUuidPrefix<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.verbose {
            self.uuid.fmt(f)
        } else {
            // We have a pretty small number of states, so for all practical
            // purposes, the first component of the UUID (8 hex digits, 32 bits)
            // are sufficient. We could potentially improve this to determine
            // unique prefixes using a trie and highlight them like Jujutsu
            // does.
            let bytes = self.uuid.as_fields();
            write!(f, "{:08x}", bytes.0)
        }
    }
}

#[derive(Debug)]
pub(crate) struct DuplicateKey<K>(pub(crate) K);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_sorted_by() {
        let mut map = IndexMap::new();
        map.insert(1, "a");
        map.insert(2, "y");

        insert_sorted_by(&mut map, 3, "x", |_, other| *other <= "x").unwrap();
        insert_sorted_by(&mut map, 4, "t", |_, other| *other <= "t").unwrap();
        insert_sorted_by(&mut map, 5, "b", |_, other| *other <= "b").unwrap();
        // Compares to the same as "x", but will be inserted after 3 because of
        // `<=`.
        insert_sorted_by(&mut map, 6, "x", |_, other| *other <= "x").unwrap();
        insert_sorted_by(&mut map, 7, "z", |_, other| *other <= "z").unwrap();

        let v: Vec<_> = map.into_iter().collect();
        assert_eq!(
            &v,
            &[
                (1, "a"),
                (5, "b"),
                (4, "t"),
                (3, "x"),
                (6, "x"),
                (2, "y"),
                (7, "z"),
            ]
        );
    }
}

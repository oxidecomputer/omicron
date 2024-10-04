// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    borrow::Borrow,
    collections::{hash_map, BTreeSet, HashMap},
    fmt,
    hash::Hash,
};

use derive_where::derive_where;
use serde::{Deserialize, Serialize, Serializer};

/// An append-only 1:1:1 (trijective) map for three keys and a value.
///
/// The storage mechanism is a vector of entries, with indexes into that vector
/// stored in three hashmaps. This allows for efficient lookups by any of the
/// three keys, while preventing duplicates.
///
/// Not totally generic yet, just meant for the deployment use case.
#[derive_where(Clone, Debug, Default)]
pub(crate) struct TriMap<T: TriMapEntry> {
    entries: Vec<T>,
    // Invariant: the values (usize) in these maps are valid indexes into
    // `entries`, and are a 1:1 mapping.
    k1_to_entry: HashMap<T::K1, usize>,
    k2_to_entry: HashMap<T::K2, usize>,
    k3_to_entry: HashMap<T::K3, usize>,
}

impl<T: TriMapEntry + PartialEq> PartialEq for TriMap<T> {
    fn eq(&self, other: &Self) -> bool {
        // Implementing PartialEq for TriMap is tricky because TriMap is not
        // semantically like an IndexMap: two maps are equivalent even if their
        // entries are in a different order. In other words, any permutation of
        // entries is equivalent.
        //
        // We also can't sort the entries because they're not necessarily Ord.
        //
        // So we write a custom equality check that checks that each key in one
        // map points to the same entry as in the other map.

        if self.entries.len() != other.entries.len() {
            return false;
        }

        // Walk over all the entries in the first map and check that they point
        // to the same entry in the second map.
        for (ix, entry) in self.entries.iter().enumerate() {
            let k1 = entry.key1();
            let k2 = entry.key2();
            let k3 = entry.key3();

            // Check that the indexes are the same in the other map.
            let Some(other_ix1) = other.k1_to_entry.get(&k1).copied() else {
                return false;
            };
            let Some(other_ix2) = other.k2_to_entry.get(&k2).copied() else {
                return false;
            };
            let Some(other_ix3) = other.k3_to_entry.get(&k3).copied() else {
                return false;
            };

            if other_ix1 != other_ix2 || other_ix1 != other_ix3 {
                // All the keys were present but they didn't point to the same
                // entry.
                return false;
            }

            // Check that the other map's entry is the same as this map's
            // entry. (This is what we use the `PartialEq` bound on T for.)
            //
            // Because we've checked that other_ix1, other_ix2 and other_ix3
            // are Some(ix), we know that ix is valid and points to the
            // expected entry.
            let other_entry = &other.entries[other_ix1];
            if entry != other_entry {
                eprintln!(
                    "mismatch: ix: {}, entry: {:?}, other_entry: {:?}",
                    ix, entry, other_entry
                );
                return false;
            }
        }

        true
    }
}

// The Eq bound on T ensures that the TriMap forms an equivalence class.
impl<T: TriMapEntry + Eq> Eq for TriMap<T> {}

// Note: Eq and PartialEq are not implemented for TriMap. Implementing them
// would need to be done with care, because TriMap is not semantically like an
// IndexMap: two maps are equivalent even if their entries are in a different
// order.

/// The `Serialize` impl for `TriMap` serializes just the list of entries.
impl<T: TriMapEntry> Serialize for TriMap<T>
where
    T: Serialize,
{
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        // Serialize just the entries -- don't serialize the indexes. We'll
        // rebuild the indexes on deserialization.
        self.entries.serialize(serializer)
    }
}

/// The `Deserialize` impl for `TriMap` deserializes the list of entries and
/// then rebuilds the indexes, producing an error if there are any duplicates.
impl<'de, T: TriMapEntry> Deserialize<'de> for TriMap<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        // First, deserialize the entries.
        let entries = Vec::<T>::deserialize(deserializer)?;

        // Now build a map from scratch, inserting the entries sequentially.
        // This will catch issues with duplicates.
        let mut map = TriMap::new();
        for entry in entries {
            map.insert_no_dups(entry).map_err(serde::de::Error::custom)?;
        }

        Ok(map)
    }
}

pub(crate) trait TriMapEntry: Clone + fmt::Debug {
    type K1: Eq + Hash + Clone + fmt::Debug;
    type K2: Eq + Hash + Clone + fmt::Debug;
    type K3: Eq + Hash + Clone + fmt::Debug;

    fn key1(&self) -> Self::K1;
    fn key2(&self) -> Self::K2;
    fn key3(&self) -> Self::K3;
}

impl<T: TriMapEntry> TriMap<T> {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
            k1_to_entry: HashMap::new(),
            k2_to_entry: HashMap::new(),
            k3_to_entry: HashMap::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.entries.iter()
    }

    /// Checks general invariants of the map.
    ///
    /// The code below always upholds these invariants, but it's useful to have
    /// an explicit check for tests.
    #[cfg(test)]
    fn validate(&self) -> anyhow::Result<()> {
        use anyhow::{ensure, Context};

        // Check that all the maps are of the right size.
        ensure!(
            self.entries.len() == self.k1_to_entry.len(),
            "key1 index has {} entries, but there are {} entries",
            self.k1_to_entry.len(),
            self.entries.len()
        );
        ensure!(
            self.entries.len() == self.k2_to_entry.len(),
            "key2 index has {} entries, but there are {} entries",
            self.k2_to_entry.len(),
            self.entries.len()
        );
        ensure!(
            self.entries.len() == self.k3_to_entry.len(),
            "key3 index has {} entries, but there are {} entries",
            self.k3_to_entry.len(),
            self.entries.len()
        );

        // Check that the indexes are all correct.
        for (ix, entry) in self.entries.iter().enumerate() {
            let key1 = entry.key1();
            let key2 = entry.key2();
            let key3 = entry.key3();

            let ix1 = self.k1_to_entry.get(&key1).context(format!(
                "entry at index {ix} ({entry:?}) has no key1 index"
            ))?;
            let ix2 = self.k2_to_entry.get(&key2).context(format!(
                "entry at index {ix} ({entry:?}) has no key2 index"
            ))?;
            let ix3 = self.k3_to_entry.get(&key3).context(format!(
                "entry at index {ix} ({entry:?}) has no key3 index"
            ))?;

            if *ix1 != ix || *ix2 != ix || *ix3 != ix {
                return Err(anyhow::anyhow!(
                    "entry at index {} has mismatched indexes: key1: {}, key2: {}, key3: {}",
                    ix,
                    ix1,
                    ix2,
                    ix3
                ));
            }
        }

        Ok(())
    }

    /// Inserts a value into the set, returning an error if any duplicates were
    /// added.
    pub(crate) fn insert_no_dups(
        &mut self,
        value: T,
    ) -> Result<(), DuplicateEntry<T>> {
        let mut dups = BTreeSet::new();

        // Check for duplicates *before* inserting the new entry, because we
        // don't want to partially insert the new entry and then have to roll
        // back.
        let e1 = detect_dup_or_insert(
            self.k1_to_entry.entry(value.key1()),
            &mut dups,
        );
        let e2 = detect_dup_or_insert(
            self.k2_to_entry.entry(value.key2()),
            &mut dups,
        );
        let e3 = detect_dup_or_insert(
            self.k3_to_entry.entry(value.key3()),
            &mut dups,
        );

        if !dups.is_empty() {
            return Err(DuplicateEntry {
                new: value,
                dups: dups.iter().map(|ix| self.entries[*ix].clone()).collect(),
            });
        }

        let next_index = self.entries.len();
        self.entries.push(value);
        // e1, e2 and e3 are all Some because if they were None, dups would be
        // non-empty, and we'd have bailed out earlier.
        e1.unwrap().insert(next_index);
        e2.unwrap().insert(next_index);
        e3.unwrap().insert(next_index);

        Ok(())
    }

    pub(crate) fn get1<Q>(&self, key1: &Q) -> Option<&T>
    where
        T::K1: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.k1_to_entry.get(key1).map(|ix| &self.entries[*ix])
    }

    pub(crate) fn get2<Q>(&self, key2: &Q) -> Option<&T>
    where
        T::K2: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.k2_to_entry.get(key2).map(|ix| &self.entries[*ix])
    }

    pub(crate) fn get3<Q>(&self, key3: &Q) -> Option<&T>
    where
        T::K3: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.k3_to_entry.get(key3).map(|ix| &self.entries[*ix])
    }
}

fn detect_dup_or_insert<'a, K>(
    entry: hash_map::Entry<'a, K, usize>,
    dups: &mut BTreeSet<usize>,
) -> Option<hash_map::VacantEntry<'a, K, usize>> {
    match entry {
        hash_map::Entry::Vacant(slot) => Some(slot),
        hash_map::Entry::Occupied(slot) => {
            dups.insert(*slot.get());
            None
        }
    }
}

#[derive(Debug)]
pub struct DuplicateEntry<T: TriMapEntry> {
    new: T,
    dups: Vec<T>,
}

impl<S: TriMapEntry> fmt::Display for DuplicateEntry<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "duplicate entry: {:?} conflicts with existing: {:?}",
            self.new, self.dups
        )
    }
}

impl<T: TriMapEntry> std::error::Error for DuplicateEntry<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use prop::sample::SizeRange;
    use proptest::prelude::*;
    use test_strategy::{proptest, Arbitrary};

    #[derive(
        Clone, Debug, Eq, PartialEq, Arbitrary, Serialize, Deserialize,
    )]
    struct TestEntry {
        key1: u8,
        key2: char,
        key3: String,
        value: String,
    }

    impl TriMapEntry for TestEntry {
        // These types are chosen to represent various kinds of keys in the
        // proptest below.
        //
        // We use u8 since there can only be 256 values, increasing the
        // likelihood of collisions in the proptest below.
        type K1 = u8;
        // char is chosen because the Arbitrary impl for it is biased towards
        // ASCII, increasing the likelihood of collisions.
        type K2 = char;
        // String is a generally open-ended type that probably won't have many
        // collisions.
        type K3 = String;

        fn key1(&self) -> Self::K1 {
            self.key1
        }

        fn key2(&self) -> Self::K2 {
            self.key2
        }

        fn key3(&self) -> Self::K3 {
            self.key3.clone()
        }
    }

    #[test]
    fn test_insert_entry_no_dups() {
        let mut map = TriMap::<TestEntry>::new();

        // Add an element.
        let v1 = TestEntry {
            key1: 0,
            key2: 'a',
            key3: "x".to_string(),
            value: "v".to_string(),
        };
        map.insert_no_dups(v1.clone()).unwrap();

        // Add an exact duplicate, which should error out.
        let error = map.insert_no_dups(v1.clone()).unwrap_err();
        assert_eq!(&error.new, &v1);
        assert_eq!(error.dups, vec![v1.clone()]);

        // Add a duplicate against just key1, which should error out.
        let v2 = TestEntry {
            key1: 0,
            key2: 'b',
            key3: "y".to_string(),
            value: "v".to_string(),
        };
        let error = map.insert_no_dups(v2.clone()).unwrap_err();
        assert_eq!(&error.new, &v2);
        assert_eq!(error.dups, vec![v1.clone()]);

        // Add a duplicate against just key2, which should error out.
        let v3 = TestEntry {
            key1: 1,
            key2: 'a',
            key3: "y".to_string(),
            value: "v".to_string(),
        };
        let error = map.insert_no_dups(v3.clone()).unwrap_err();
        assert_eq!(&error.new, &v3);

        // Add a duplicate against just key3, which should error out.
        let v4 = TestEntry {
            key1: 1,
            key2: 'b',
            key3: "x".to_string(),
            value: "v".to_string(),
        };
        let error = map.insert_no_dups(v4.clone()).unwrap_err();
        assert_eq!(&error.new, &v4);

        // Add an entry that doesn't have any conflicts.
        let v5 = TestEntry {
            key1: 1,
            key2: 'b',
            key3: "y".to_string(),
            value: "v".to_string(),
        };
        map.insert_no_dups(v5.clone()).unwrap();
    }

    /// Represents a naive version of `TriMap` that doesn't have any indexes
    /// and does linear scans.
    #[derive(Debug)]
    struct NaiveTriMap {
        entries: Vec<TestEntry>,
    }

    impl NaiveTriMap {
        fn new() -> Self {
            Self { entries: Vec::new() }
        }

        fn insert_entry_no_dups(
            &mut self,
            entry: TestEntry,
        ) -> Result<(), DuplicateEntry<TestEntry>> {
            let dups = self
                .entries
                .iter()
                .filter(|e| {
                    e.key1 == entry.key1
                        || e.key2 == entry.key2
                        || e.key3 == entry.key3
                })
                .cloned()
                .collect::<Vec<_>>();

            if !dups.is_empty() {
                return Err(DuplicateEntry { new: entry, dups });
            }

            self.entries.push(entry);
            Ok(())
        }
    }

    #[derive(Debug, Arbitrary)]
    enum Operation {
        // Make inserts a bit more common to try and fill up the map.
        #[weight(3)]
        Insert(TestEntry),
        Get1(u8),
        Get2(char),
        Get3(String),
    }

    #[proptest]
    fn proptest_serialize_roundtrip(values: Vec<TestEntry>) {
        let mut map = TriMap::<TestEntry>::new();
        let mut first_error = None;
        for value in values.clone() {
            // Ignore errors from duplicates which are quite possible to occur
            // here, since we're just testing serialization. But store the
            // first error to ensure that deserialization returns errors.
            if let Err(error) = map.insert_no_dups(value) {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        let serialized = serde_json::to_string(&map).unwrap();
        let deserialized: TriMap<TestEntry> =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(map.entries, deserialized.entries, "entries match");
        // All of the indexes should be the same too.
        assert_eq!(
            map.k1_to_entry, deserialized.k1_to_entry,
            "k1 indexes match"
        );
        assert_eq!(
            map.k2_to_entry, deserialized.k2_to_entry,
            "k2 indexes match"
        );
        assert_eq!(
            map.k3_to_entry, deserialized.k3_to_entry,
            "k3 indexes match"
        );

        // Try deserializing the full list of values directly, and see that the
        // error reported is the same as first_error.
        //
        // Here we rely on the fact that a TriMap is serialized as just a
        // vector.
        let serialized = serde_json::to_string(&values).unwrap();
        let res: Result<TriMap<TestEntry>, _> =
            serde_json::from_str(&serialized);
        match (first_error, res) {
            (None, Ok(_)) => {} // No error, should be fine
            (Some(first_error), Ok(_)) => {
                panic!(
                    "expected error ({first_error}), but deserialization succeeded"
                )
            }
            (None, Err(error)) => {
                panic!("unexpected error: {error}, deserialization should have succeeded")
            }
            (Some(first_error), Err(error)) => {
                // first_error is the error from the map, and error is the
                // deserialization error (which should always be a custom
                // error, stored as a string).
                let expected = first_error.to_string();
                let actual = error.to_string();
                assert_eq!(actual, expected, "error matches");
            }
        }
    }

    #[proptest(cases = 16)]
    fn proptest_ops(
        #[strategy(prop::collection::vec(any::<Operation>(), 0..1024))]
        ops: Vec<Operation>,
    ) {
        let mut map = TriMap::<TestEntry>::new();
        let mut naive_map = NaiveTriMap::new();

        // Now perform the operations on both maps.
        for op in ops {
            match op {
                Operation::Insert(entry) => {
                    let map_res = map.insert_no_dups(entry.clone());
                    let naive_res =
                        naive_map.insert_entry_no_dups(entry.clone());

                    assert_eq!(map_res.is_ok(), naive_res.is_ok());
                    if let Err(map_err) = map_res {
                        let naive_err = naive_res.unwrap_err();
                        assert_eq!(map_err.new, naive_err.new);
                        assert_eq!(map_err.dups, naive_err.dups);
                    }

                    map.validate().expect("map should be valid");
                }
                Operation::Get1(key1) => {
                    let map_res = map.get1(&key1);
                    let naive_res =
                        naive_map.entries.iter().find(|e| e.key1 == key1);

                    assert_eq!(map_res, naive_res);
                }
                Operation::Get2(key2) => {
                    let map_res = map.get2(&key2);
                    let naive_res =
                        naive_map.entries.iter().find(|e| e.key2 == key2);

                    assert_eq!(map_res, naive_res);
                }
                Operation::Get3(key3) => {
                    let map_res = map.get3(&key3);
                    let naive_res =
                        naive_map.entries.iter().find(|e| e.key3 == key3);

                    assert_eq!(map_res, naive_res);
                }
            }
        }
    }

    #[proptest(cases = 64)]
    fn proptest_permutation_eq(
        #[strategy(test_entry_permutation_strategy(0..256))] entries: (
            Vec<TestEntry>,
            Vec<TestEntry>,
        ),
    ) {
        let (entries1, entries2) = entries;
        let mut map1 = TriMap::<TestEntry>::new();
        let mut map2 = TriMap::<TestEntry>::new();

        for entry in entries1 {
            map1.insert_no_dups(entry.clone()).unwrap();
        }
        for entry in entries2 {
            map2.insert_no_dups(entry.clone()).unwrap();
        }

        assert_eq_props(map1, map2);
    }

    // Returns a pair of permutations of a set of unique entries.
    fn test_entry_permutation_strategy(
        size: impl Into<SizeRange>,
    ) -> impl Strategy<Value = (Vec<TestEntry>, Vec<TestEntry>)> {
        prop::collection::vec(any::<TestEntry>(), size.into()).prop_perturb(
            |v, mut rng| {
                // It is possible (likely even) that the input vector has
                // duplicates. How can we remove them? The easiest way is to
                // use the TriMap logic that already exists to check for
                // duplicates. Insert all the entries one by one, then get the
                // list.
                let mut map = TriMap::<TestEntry>::new();
                for entry in v {
                    // The error case here is expected -- we're actively
                    // de-duping entries right now.
                    _ = map.insert_no_dups(entry);
                }
                let v = map.entries;

                // Now shuffle the entries. This is a simple Fisher-Yates
                // shuffle (Durstenfeld variant, low to high).
                let mut v2 = v.clone();
                if v.len() < 2 {
                    return (v, v2);
                }
                for i in 0..v2.len() - 2 {
                    let j = rng.gen_range(i..v2.len());
                    v2.swap(i, j);
                }

                (v, v2)
            },
        )
    }

    // Test various conditions for non-equality.
    //
    // It's somewhat hard to capture mutations in a proptest (partly because
    // `TriMap` doesn't support mutating existing entries at the moment), so
    // this is a small example-based test.
    #[test]
    fn test_permutation_eq_examples() {
        let mut map1 = TriMap::<TestEntry>::new();
        let mut map2 = TriMap::<TestEntry>::new();

        // Two empty maps are equal.
        assert_eq!(map1, map2);

        // Insert a single entry into one map.
        let entry = TestEntry {
            key1: 0,
            key2: 'a',
            key3: "x".to_string(),
            value: "v".to_string(),
        };
        map1.insert_no_dups(entry.clone()).unwrap();

        // The maps are not equal.
        assert_ne_props(&map1, &map2);

        // Insert the same entry into the other map.
        map2.insert_no_dups(entry.clone()).unwrap();

        // The maps are now equal.
        assert_eq_props(&map1, &map2);

        {
            // Insert an entry with the same key2 and key3 but a different
            // key1.
            let mut map1 = map1.clone();
            map1.insert_no_dups(TestEntry {
                key1: 1,
                key2: 'b',
                key3: "y".to_string(),
                value: "v".to_string(),
            })
            .unwrap();
            assert_ne_props(&map1, &map2);

            let mut map2 = map2.clone();
            map2.insert_no_dups(TestEntry {
                key1: 2,
                key2: 'b',
                key3: "y".to_string(),
                value: "v".to_string(),
            })
            .unwrap();
            assert_ne_props(&map1, &map2);
        }

        {
            // Insert an entry with the same key1 and key3 but a different
            // key2.
            let mut map1 = map1.clone();
            map1.insert_no_dups(TestEntry {
                key1: 1,
                key2: 'b',
                key3: "y".to_string(),
                value: "v".to_string(),
            })
            .unwrap();
            assert_ne_props(&map1, &map2);

            let mut map2 = map2.clone();
            map2.insert_no_dups(TestEntry {
                key1: 1,
                key2: 'c',
                key3: "y".to_string(),
                value: "v".to_string(),
            })
            .unwrap();
            assert_ne_props(&map1, &map2);
        }

        {
            // Insert an entry with the same key1 and key2 but a different
            // key3.
            let mut map1 = map1.clone();
            map1.insert_no_dups(TestEntry {
                key1: 1,
                key2: 'b',
                key3: "y".to_string(),
                value: "v".to_string(),
            })
            .unwrap();
            assert_ne_props(&map1, &map2);

            let mut map2 = map2.clone();
            map2.insert_no_dups(TestEntry {
                key1: 1,
                key2: 'b',
                key3: "z".to_string(),
                value: "v".to_string(),
            })
            .unwrap();
            assert_ne_props(&map1, &map2);
        }

        {
            // Insert an entry where all the keys are the same, but the value is
            // different.
            let mut map1 = map1.clone();
            map1.insert_no_dups(TestEntry {
                key1: 1,
                key2: 'b',
                key3: "y".to_string(),
                value: "w".to_string(),
            })
            .unwrap();
            assert_ne_props(&map1, &map2);

            let mut map2 = map2.clone();
            map2.insert_no_dups(TestEntry {
                key1: 1,
                key2: 'b',
                key3: "y".to_string(),
                value: "x".to_string(),
            })
            .unwrap();
            assert_ne_props(&map1, &map2);
        }
    }

    /// Assert equality properties.
    ///
    /// The PartialEq algorithm is not obviously symmetric or reflexive, so we
    /// must ensure in our tests that it is.
    #[allow(clippy::eq_op)]
    fn assert_eq_props<T: Eq + fmt::Debug>(a: T, b: T) {
        assert_eq!(a, a, "a == a");
        assert_eq!(b, b, "b == b");
        assert_eq!(a, b, "a == b");
        assert_eq!(b, a, "b == a");
    }

    /// Assert inequality properties.
    ///
    /// The PartialEq algorithm is not obviously symmetric or reflexive, so we
    /// must ensure in our tests that it is.
    #[allow(clippy::eq_op)]
    fn assert_ne_props<T: Eq + fmt::Debug>(a: T, b: T) {
        // Also check reflexivity while we're here.
        assert_eq!(a, a, "a == a");
        assert_eq!(b, b, "b == b");
        assert_ne!(a, b, "a != b");
        assert_ne!(b, a, "b != a");
    }
}

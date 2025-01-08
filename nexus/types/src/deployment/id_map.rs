// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use derive_where::derive_where;
use diffus::edit::Edit;
use diffus::Diffable;
use schemars::JsonSchema;
use serde::de::Error as _;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Serialize;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;

pub trait IdMappable:
    JsonSchema + Serialize + for<'de> Deserialize<'de> + for<'a> Diffable<'a>
{
    type Id: Ord
        + Copy
        + fmt::Display
        + fmt::Debug
        + JsonSchema
        + Serialize
        + for<'de> Deserialize<'de>;

    fn id(&self) -> Self::Id;
}

type Inner<T> = BTreeMap<<T as IdMappable>::Id, T>;

/// A 1:1 map ordered by the entries' IDs.
///
/// `IdMap` is a newtype wrapper around `BTreeMap` that does not allow a
/// mismatch between a key and its associated value (i.e., the keys are
/// guaranteed to be the ID of their associated value). Its implementations of
/// various serialization-related traits all erase the `IdMap` and behave like
/// the inner `BTreeMap` would, although deserialzation performs extra checks to
/// guarantee the key-must-be-its-values-ID property.
///
/// Similar to the constraint that a `BTreeMap`'s keys may not be modified in a
/// way that affects their ordering, the _values_ of an `IdMap` must not be
/// modified in a way that affects their `id()` (as returned by their
/// [`IdMappable`] implementation. This is possible via methods like `get_mut()`
/// but will induce a logic error that may produce panics, invalid
/// serialization, etc. The type stored in `IdMap` is expected to have a stable,
/// never-changing identity.
///
/// An entry-style API is _not_ provided, as it would be relatively unergonomic
/// to provide while enforcing the key-must-be-ID invariant.
#[derive(Clone, Debug, Eq, PartialEq)]
#[derive_where(Default)]
pub struct IdMap<T: IdMappable> {
    inner: Inner<T>,
}

impl<T: IdMappable> IdMap<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn insert(&mut self, entry: T) -> Option<T> {
        self.inner.insert(entry.id(), entry)
    }

    pub fn first(&self) -> Option<&T> {
        self.inner.first_key_value().map(|(_, val)| val)
    }

    pub fn get(&self, key: &T::Id) -> Option<&T> {
        self.inner.get(key)
    }

    pub fn get_mut(&mut self, key: &T::Id) -> Option<RefMut<'_, T>> {
        self.inner.get_mut(key).map(RefMut::new)
    }

    pub fn iter(&self) -> btree_map::Values<'_, T::Id, T> {
        self.inner.values()
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut { inner: self.inner.values_mut() }
    }

    pub fn into_iter(self) -> btree_map::IntoValues<T::Id, T> {
        self.inner.into_values()
    }

    pub fn clear(&mut self) {
        self.inner.clear()
    }

    pub fn retain<F: for<'a, 'b> FnMut(&'a mut RefMut<'b, T>) -> bool>(
        &mut self,
        mut f: F,
    ) {
        self.inner.retain(|_, val| f(&mut RefMut::new(val)))
    }
}

impl<T: IdMappable> FromIterator<T> for IdMap<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let inner = iter.into_iter().map(|entry| (entry.id(), entry)).collect();
        Self { inner }
    }
}

impl<T: IdMappable> IntoIterator for IdMap<T> {
    type Item = T;
    type IntoIter = btree_map::IntoValues<T::Id, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_iter()
    }
}

impl<'a, T: IdMappable> IntoIterator for &'a IdMap<T> {
    type Item = &'a T;
    type IntoIter = btree_map::Values<'a, T::Id, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: IdMappable> IntoIterator for &'a mut IdMap<T> {
    type Item = RefMut<'a, T>;
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T: IdMappable> JsonSchema for IdMap<T> {
    fn schema_name() -> String {
        Inner::<T>::schema_name()
    }

    fn json_schema(
        gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        Inner::<T>::json_schema(gen)
    }
}

impl<T: IdMappable> Serialize for IdMap<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de, T: IdMappable> Deserialize<'de> for IdMap<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdCheckVisitor<T>(PhantomData<T>);

        impl<'d, T: IdMappable> Visitor<'d> for IdCheckVisitor<T> {
            type Value = IdMap<T>;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a map keyed by ID")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'d>,
            {
                let mut inner = Inner::<T>::new();
                while let Some((key, value)) = map.next_entry::<T::Id, T>()? {
                    if key != value.id() {
                        return Err(A::Error::custom(format!(
                            "invalid map: key {} maps to value with ID {}",
                            key,
                            value.id()
                        )));
                    }
                    if let Some(old) = inner.insert(key, value) {
                        return Err(A::Error::custom(format!(
                            "invalid map: duplicate key {}",
                            old.id()
                        )));
                    }
                }
                Ok(IdMap { inner })
            }
        }

        deserializer.deserialize_map(IdCheckVisitor(PhantomData))
    }
}

impl<'a, T: IdMappable + 'a> Diffable<'a> for IdMap<T> {
    type Diff = BTreeMap<&'a T::Id, diffus::edit::map::Edit<'a, T>>;

    fn diff(&'a self, other: &'a Self) -> Edit<'a, Self> {
        match self.inner.diff(&other.inner) {
            Edit::Copy(_) => Edit::Copy(self),
            Edit::Change(change) => Edit::Change(change),
        }
    }
}

/// Wrapper around a `&'a mut T` that panics when dropped if the borrowed
/// value's `id()` has changed since the wrapper was created.
pub struct RefMut<'a, T: IdMappable> {
    original_id: T::Id,
    // Always `Some(_)` until the `RefMut` is consumed by `into_ref()`.
    borrowed: Option<&'a mut T>,
}

impl<T: IdMappable> Drop for RefMut<'_, T> {
    fn drop(&mut self) {
        if let Some(value) = self.borrowed.as_ref() {
            assert_eq!(
                self.original_id,
                value.id(),
                "IdMap values must not change their ID"
            );
        }
    }
}

impl<T: IdMappable> Deref for RefMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.borrowed.as_ref().unwrap()
    }
}

impl<T: IdMappable> DerefMut for RefMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.borrowed.as_mut().unwrap()
    }
}

impl<'a, T: IdMappable> RefMut<'a, T> {
    fn new(borrowed: &'a mut T) -> Self {
        Self { original_id: borrowed.id(), borrowed: Some(borrowed) }
    }

    pub fn into_ref(mut self) -> &'a T {
        let value = self.borrowed.take().unwrap();
        // We stole `value` so `Drop` won't be able to check this invariant;
        // check it ourselves.
        assert_eq!(
            self.original_id,
            value.id(),
            "IdMap values must not change their ID"
        );
        value
    }
}

pub struct IterMut<'a, T: IdMappable> {
    inner: btree_map::ValuesMut<'a, T::Id, T>,
}

impl<'a, T: IdMappable> Iterator for IterMut<'a, T> {
    type Item = RefMut<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(RefMut::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diffus::Diffus;
    use test_strategy::proptest;
    use test_strategy::Arbitrary;

    #[derive(
        Debug,
        Clone,
        PartialEq,
        Eq,
        Arbitrary,
        JsonSchema,
        Serialize,
        Deserialize,
        Diffus,
    )]
    struct TestEntry {
        id: u8,
        val1: u32,
        val2: u32,
    }

    impl IdMappable for TestEntry {
        type Id = u8;

        fn id(&self) -> Self::Id {
            self.id
        }
    }

    #[proptest]
    fn serialization_roundtrip(values: Vec<TestEntry>) {
        let map: IdMap<_> = values.into_iter().collect();

        let serialized = serde_json::to_string(&map).unwrap();
        let deserialized: IdMap<TestEntry> =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(map, deserialized);
    }

    #[proptest]
    fn serialization_is_transparent(values: Vec<TestEntry>) {
        let map: IdMap<_> = values.into_iter().collect();

        let serialized = serde_json::to_string(&map).unwrap();
        let serialized_inner = serde_json::to_string(&map.inner).unwrap();

        assert_eq!(serialized, serialized_inner);
    }

    #[test]
    fn deserialize_rejects_mismatched_keys() {
        let mut map = IdMap::<TestEntry>::new();
        map.insert(TestEntry { id: 1, val1: 2, val2: 3 });

        let mut entries = map.inner;
        entries.get_mut(&1).unwrap().id = 2;

        let serialized = serde_json::to_string(&entries).unwrap();
        let err =
            serde_json::from_str::<IdMap<TestEntry>>(&serialized).unwrap_err();
        let err = err.to_string();

        assert!(
            err.contains("key 1 maps to value with ID 2"),
            "unexpected error message: {err:?}"
        );
    }

    #[test]
    fn deserialize_rejects_duplicates() {
        let serialized = r#"
        {
            "1": {"id": 1, "val1": 2, "val2": 3},
            "1": {"id": 1, "val1": 2, "val2": 3}
        }
        "#;

        let err =
            serde_json::from_str::<IdMap<TestEntry>>(&serialized).unwrap_err();
        let err = err.to_string();

        assert!(
            err.contains("duplicate key 1"),
            "unexpected error message: {err:?}"
        );
    }

    #[test]
    #[should_panic(expected = "IdMap values must not change their ID")]
    fn get_mut_panics_if_id_changes() {
        let mut map = IdMap::<TestEntry>::new();
        map.insert(TestEntry { id: 1, val1: 2, val2: 3 });
        map.get_mut(&1).unwrap().id = 2;
    }

    #[test]
    #[should_panic(expected = "IdMap values must not change their ID")]
    fn iter_mut_panics_if_id_changes() {
        let mut map = IdMap::<TestEntry>::new();
        map.insert(TestEntry { id: 1, val1: 2, val2: 3 });
        for mut entry in map.iter_mut() {
            entry.id = 2;
        }
    }

    #[test]
    #[should_panic(expected = "IdMap values must not change their ID")]
    fn mut_into_iter_panics_if_id_changes() {
        let mut map = IdMap::<TestEntry>::new();
        map.insert(TestEntry { id: 1, val1: 2, val2: 3 });
        for mut entry in &mut map {
            entry.id = 2;
        }
    }

    #[test]
    #[should_panic(expected = "IdMap values must not change their ID")]
    fn retain_panics_if_id_changes() {
        let mut map = IdMap::<TestEntry>::new();
        map.insert(TestEntry { id: 1, val1: 2, val2: 3 });
        map.retain(|entry| {
            entry.id = 2;
            true
        });
    }
}

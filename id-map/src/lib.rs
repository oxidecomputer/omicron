// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Provides [`IdMap`], a collection of values that are able to uniquely
//! identify themselves.

use daft::BTreeMapDiff;
use daft::Diffable;
use derive_where::derive_where;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde::de::Error as _;
use serde::de::Visitor;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::collections::btree_map;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;

/// Bounds required for a type to be stored in an [`IdMap`].
pub trait IdMappable {
    /// The identity type of this value.
    type Id: Ord + fmt::Debug;

    /// Return an owned identity for this value.
    ///
    /// This method is called liberally by [`IdMap`]. For example, mutating a
    /// value in an [`IdMap`] will call this method at least twice in service of
    /// the runtime checks performed by [`RefMut`]. Getting owned `T::Id` values
    /// is expected to be cheap. If your identity type is not `Copy`, it should
    /// be cheap to `Clone`; e.g., `Arc<String>` may be preferable to `String`.
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
/// `IdMap` has the same constraint as `BTreeMap` regarding mutating keys out
/// from under it:
///
/// > It is a logic error for a key to be modified in such a way that the key’s
/// > ordering relative to any other key, as determined by the Ord trait,
/// > changes while it is in the map. This is normally only possible through
/// > Cell, RefCell, global state, I/O, or unsafe code. The behavior resulting
/// > from such a logic error is not specified, but will be encapsulated to the
/// > BTreeMap that observed the logic error and not result in undefined
/// > behavior. This could include panics, incorrect results, aborts, memory
/// > leaks, and non-termination.
///
/// Additionally, `IdMap` has the requirement that when any _values_ are
/// mutated, their ID must not change. This is enforced at runtime via the
/// [`RefMut`] wrapper returned by `get_mut()` and `iter_mut()`. When the
/// wrapper is dropped, it will induce a panic if the ID has changed from what
/// the value had when it was retreived from the map.
#[derive_where(Default)]
#[derive_where(Clone, Debug, Eq, PartialEq; T, T::Id)]
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

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn insert(&mut self, entry: T) -> Option<T> {
        self.inner.insert(entry.id(), entry)
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<T>
    where
        T::Id: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.remove(key)
    }

    pub fn first(&self) -> Option<&T> {
        self.inner.first_key_value().map(|(_, val)| val)
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&T>
    where
        T::Id: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.get(key)
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<RefMut<'_, T>>
    where
        T::Id: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.get_mut(key).map(RefMut::new)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        T::Id: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.contains_key(key)
    }

    pub fn keys(&self) -> btree_map::Keys<'_, T::Id, T> {
        self.inner.keys()
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

    pub fn entry(&mut self, key: T::Id) -> Entry<'_, T> {
        match self.inner.entry(key) {
            btree_map::Entry::Vacant(slot) => {
                Entry::Vacant(VacantEntry::new(slot))
            }
            btree_map::Entry::Occupied(slot) => {
                Entry::Occupied(OccupiedEntry::new(slot))
            }
        }
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

impl<T> JsonSchema for IdMap<T>
where
    T: IdMappable + JsonSchema,
    T::Id: JsonSchema,
{
    fn schema_name() -> String {
        format!("IdMap{}", T::schema_name())
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        Inner::<T>::json_schema(generator)
    }
}

impl<T> Serialize for IdMap<T>
where
    T: IdMappable + Serialize,
    T::Id: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for IdMap<T>
where
    T: IdMappable + Deserialize<'de>,
    T::Id: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdCheckVisitor<T>(PhantomData<T>);

        impl<'d, T> Visitor<'d> for IdCheckVisitor<T>
        where
            T: IdMappable + Deserialize<'d>,
            T::Id: Deserialize<'d>,
        {
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
                            "invalid map: key {:?} maps to value with ID {:?}",
                            key,
                            value.id()
                        )));
                    }
                    if let Some(old) = inner.insert(key, value) {
                        return Err(A::Error::custom(format!(
                            "invalid map: duplicate key {:?}",
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

impl<T: IdMappable + Debug + Eq> Diffable for IdMap<T> {
    type Diff<'daft>
        = BTreeMapDiff<'daft, T::Id, T>
    where
        T: 'daft;

    fn diff<'daft>(&'daft self, other: &'daft Self) -> Self::Diff<'daft> {
        self.inner.diff(&other.inner)
    }
}

/// Wrapper around a `&'a mut T` that panics when dropped if the borrowed
/// value's `id()` has changed since the wrapper was created.
#[derive(Debug)]
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

pub enum Entry<'a, T: IdMappable> {
    Vacant(VacantEntry<'a, T>),
    Occupied(OccupiedEntry<'a, T>),
}

pub struct VacantEntry<'a, T: IdMappable> {
    inner: btree_map::VacantEntry<'a, T::Id, T>,
}

impl<'a, T: IdMappable> VacantEntry<'a, T> {
    fn new(inner: btree_map::VacantEntry<'a, T::Id, T>) -> Self {
        Self { inner }
    }

    /// # Panics
    ///
    /// Panics if `value.id()` does not match the ID used to look up the parent
    /// `Entry`.
    pub fn insert(self, value: T) -> RefMut<'a, T> {
        assert_eq!(
            *self.key(),
            value.id(),
            "VacantEntry::insert() must insert a value with the same ID \
             used to create the entry"
        );
        RefMut::new(self.inner.insert(value))
    }

    pub fn key(&self) -> &T::Id {
        self.inner.key()
    }
}

pub struct OccupiedEntry<'a, T: IdMappable> {
    inner: btree_map::OccupiedEntry<'a, T::Id, T>,
}

impl<'a, T: IdMappable> OccupiedEntry<'a, T> {
    fn new(inner: btree_map::OccupiedEntry<'a, T::Id, T>) -> Self {
        Self { inner }
    }

    pub fn get(&self) -> &T {
        self.inner.get()
    }

    pub fn get_mut(&mut self) -> RefMut<'_, T> {
        RefMut::new(self.inner.get_mut())
    }

    pub fn into_mut(self) -> RefMut<'a, T> {
        RefMut::new(self.inner.into_mut())
    }

    /// # Panics
    ///
    /// Panics if `value.id()` does not match the ID used to look up the parent
    /// `Entry`.
    pub fn insert(&mut self, value: T) -> T {
        assert_eq!(
            self.get().id(),
            value.id(),
            "VacantEntry::insert() must insert a value with the same ID \
             used to create the entry"
        );
        self.inner.insert(value)
    }

    pub fn remove(self) -> T {
        self.inner.remove()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use daft::Diffable;
    use test_strategy::Arbitrary;
    use test_strategy::proptest;

    #[derive(
        Debug,
        Clone,
        PartialEq,
        Eq,
        Arbitrary,
        JsonSchema,
        Serialize,
        Deserialize,
        Diffable,
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

    #[test]
    #[should_panic(expected = "must insert a value with the same ID")]
    fn vacant_entry_panics_if_id_changes_on_insert() {
        let mut map = IdMap::<TestEntry>::new();
        match map.entry(0) {
            Entry::Vacant(slot) => {
                slot.insert(TestEntry { id: 1, val1: 2, val2: 3 });
            }
            Entry::Occupied(_) => unreachable!(),
        }
    }

    #[test]
    #[should_panic(expected = "IdMap values must not change their ID")]
    fn vacant_entry_panics_if_id_changed_after_insert() {
        let mut map = IdMap::<TestEntry>::new();
        match map.entry(1) {
            Entry::Vacant(slot) => {
                let mut val =
                    slot.insert(TestEntry { id: 1, val1: 2, val2: 3 });
                val.id = 2;
            }
            Entry::Occupied(_) => unreachable!(),
        }
    }

    #[test]
    #[should_panic(expected = "must insert a value with the same ID")]
    fn occupied_entry_panics_if_id_changes_on_insert() {
        let mut map = IdMap::<TestEntry>::new();
        map.insert(TestEntry { id: 1, val1: 2, val2: 3 });
        match map.entry(1) {
            Entry::Vacant(_) => unreachable!(),
            Entry::Occupied(mut slot) => {
                slot.insert(TestEntry { id: 2, val1: 2, val2: 3 });
            }
        }
    }

    #[test]
    #[should_panic(expected = "IdMap values must not change their ID")]
    fn occupied_entry_panics_if_id_changed_via_into_mut() {
        let mut map = IdMap::<TestEntry>::new();
        map.insert(TestEntry { id: 1, val1: 2, val2: 3 });
        match map.entry(1) {
            Entry::Vacant(_) => unreachable!(),
            Entry::Occupied(slot) => {
                slot.into_mut().id = 2;
            }
        }
    }

    #[test]
    #[should_panic(expected = "IdMap values must not change their ID")]
    fn occupied_entry_panics_if_id_changed_via_get_mut() {
        let mut map = IdMap::<TestEntry>::new();
        map.insert(TestEntry { id: 1, val1: 2, val2: 3 });
        match map.entry(1) {
            Entry::Vacant(_) => unreachable!(),
            Entry::Occupied(mut slot) => {
                slot.get_mut().id = 2;
            }
        }
    }

    #[test]
    fn methods_allow_borrowed_ids() {
        struct Foo {
            id: String,
            val: i32,
        }

        impl IdMappable for Foo {
            type Id = String;

            fn id(&self) -> Self::Id {
                self.id.clone()
            }
        }

        let mut foos = IdMap::new();
        foos.insert(Foo { id: "foo".to_string(), val: 1 });

        // Id type is `String`, but we can use `&str` for these methods
        assert_eq!(foos.get("foo").unwrap().val, 1);
        assert_eq!(foos.get_mut("foo").unwrap().val, 1);
        assert!(foos.contains_key("foo"));
        assert_eq!(foos.remove("foo").unwrap().val, 1);
    }
}

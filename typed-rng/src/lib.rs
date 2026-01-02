// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Typed RNGs with support for tree-based RNGs.
//!
//! ## [`TypedRng`]
//!
//! This library contains [`TypedRng`], a simple wrapper around a random number
//! generator that generates values of a particular type.
//!
//! At the moment, it only supports stateless value creation, where the
//! `Generatable::generate` method does not have access to anything other than
//! the RNG state. It may be extended in the future with the capability to pass
//! in persistent state.
//!
//! ### Tree-based RNGs
//!
//! Many RNG models are organized in a tree structure, where a parent RNG
//! generates a child RNG. The main benefit to this kind of structure is
//! stability of output: because the different child RNGs are all independent
//! of each other, making more calls to one RNG will not affect any others.
//!
//! The `TypedRng` struct provides a method to generate a new RNG from a parent
//! RNG and a seed. This is useful when you want to generate a new RNG that is
//! independent of the parent RNG, but still deterministic.
//!
//! ### Comparison with property-based testing
//!
//! In a sense, this is a very simple version of how random values are
//! generated with property-based testing, with e.g. `Arbitrary` or proptest's
//! `Strategy`.
//!
//! But with property-based tests, the goal is for small changes in the random
//! input to result in _small_ changes in the output. High-quality libraries
//! like proptest also focus on shrinking.
//!
//! With `Generatable`, the goal is for small changes in the random input (the
//! seed used to initialize the RNG) to result in _large_ changes in output.
//! (However, it is possible to delegate the bulk of the operation to a value
//! generator from a PBT framework). There is also no need for shrinking.
//!
//! Overall, this means that `Generatable` can be used in both testing and
//! production.
//!
//! ## Other functionality
//!
//! This crate also provides two additional convenience functions:
//!
//! - [`from_seed`], which generates a new RNG from a seed.
//! - [`from_parent_and_seed`], which generates a new RNG from a parent RNG and
//!   a seed.
//!
//! Both these methods are short, but more ergonomic and less prone to misuse
//! than using the underlying libraries directly.

use std::{fmt, hash::Hash, marker::PhantomData};

use newtype_uuid::{GenericUuid, TypedUuid, TypedUuidKind};
use rand::rngs::StdRng;
use rand_core::{RngCore, SeedableRng};
use uuid::Uuid;

/// Returns a new RNG generated only from the given seeds `seed` and `extra`.
/// `seed` may be passed down from another caller, e.g. a test, and `extra`
/// should be a fixed value specific to the callsite.
///
/// This takes two hashable arguments rather than one, because when one is
/// passing down a seed, it is all too easy to not include any extra
/// information. That may result in multiple different RNGs generating the same
/// random values. So we expect that callers will also provide bytes of their
/// own, specific to the callsite, and gently guide them towards doing the
/// right thing.
pub fn from_seed<R, H, H2>(seed: H, extra: H2) -> R
where
    R: SeedableRng,
    H: Hash,
    H2: Hash,
{
    // XXX: is Hash really the right thing to use here? That's what
    // rand_seeder uses, but something like https://docs.rs/stable-hash may
    // be more correct.

    let mut seeder = rand_seeder::Seeder::from((seed, extra));
    seeder.into_rng::<R>()
}

/// Generates a new RNG from a parent RNG and a hashable seed.
pub fn from_parent_and_seed<R, R2, H>(parent_rng: &mut R2, seed: H) -> R
where
    R: SeedableRng,
    R2: RngCore,
    H: Hash,
{
    let rng_seed = parent_rng.next_u64();

    let mut seeder = rand_seeder::Seeder::from((rng_seed, seed));
    seeder.into_rng::<R>()
}

/// An RNG that can be used to generate values of a single type.
///
/// This is a convenience wrapper around a random number generator that
/// generates values of a particular type. It works against any type that
/// implements [`Generatable`], and any RNG that implements [`RngCore`].
pub struct TypedRng<T, R> {
    rng: R,
    // PhantomData<fn() -> T> is like PhantomData<T>, but it doesn't inherit
    // Send/Sync from T. See
    // https://doc.rust-lang.org/nomicon/phantom-data.html#table-of-phantomdata-patterns.
    _marker: PhantomData<fn() -> T>,
}

impl<T: Generatable> TypedRng<T, StdRng> {
    /// Returns a new typed RNG from entropy.
    pub fn from_entropy() -> Self {
        Self::new(StdRng::from_os_rng())
    }
}

impl<T, R> TypedRng<T, R>
where
    T: Generatable,
    R: RngCore,
{
    /// Returns a new typed RNG from the given RNG.
    pub fn new(rng: R) -> Self {
        Self { rng, _marker: PhantomData }
    }

    /// Returns a new typed RNG generated from the parent RNG, along with a
    /// seed.
    ///
    /// Many RNG models are organized in a tree structure, where a parent RNG
    /// generates a child RNG. The main benefit to this kind of structure is
    /// stability of output: because the different child RNGs are all
    /// independent of each other, making more calls to one RNG will not affect
    /// any others.
    pub fn from_parent_rng<R2: RngCore, H: Hash>(
        parent_rng: &mut R2,
        seed: H,
    ) -> Self
    where
        R: SeedableRng,
    {
        Self::new(from_parent_and_seed(parent_rng, seed))
    }

    /// Returns a new typed RNG generated only from the given seeds `seed` and
    /// `extra`.
    ///
    /// This takes two hashable arguments rather than one, because when one is
    /// passing down a seed set by e.g. a test, it is all too easy to just pass
    /// down that seed here. That may result in multiple different RNGs
    /// generating the same random values. So we expect that callers will also
    /// provide bytes of their own, specific to the call-site, and gently guide
    /// them towards doing the right thing.
    pub fn from_seed<H, H2>(seed: H, extra: H2) -> Self
    where
        R: SeedableRng,
        H: Hash,
        H2: Hash,
    {
        let mut seeder = rand_seeder::Seeder::from((seed, extra));
        Self::new(seeder.into_rng::<R>())
    }

    /// Sets the seed for this RNG to the given value.
    ///
    /// This takes two hashable arguments rather than one, for much the same
    /// reason as [`Self::from_seed`].
    pub fn set_seed<H, H2>(&mut self, seed: H, extra: H2)
    where
        R: SeedableRng,
        H: Hash,
        H2: Hash,
    {
        let mut seeder = rand_seeder::Seeder::from((seed, extra));
        self.rng = seeder.into_rng::<R>();
    }

    /// Returns a mutable reference to the RNG inside.
    pub fn inner_mut(&mut self) -> &mut R {
        &mut self.rng
    }

    /// Consumes self, returning the RNG inside.
    pub fn into_inner(self) -> R {
        self.rng
    }

    /// Returns the next value.
    pub fn next(&mut self) -> T {
        T::generate(&mut self.rng)
    }
}

// --- Trait impls ---
//
// These have to be done by hand to avoid a dependency on T.

impl<T, R: Clone> Clone for TypedRng<T, R> {
    fn clone(&self) -> Self {
        Self { rng: self.rng.clone(), _marker: PhantomData }
    }
}

impl<T, R: Copy> Copy for TypedRng<T, R> {}

impl<T, R: Default> Default for TypedRng<T, R> {
    fn default() -> Self {
        Self { rng: R::default(), _marker: PhantomData }
    }
}

impl<T, R: fmt::Debug> fmt::Debug for TypedRng<T, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TypedRng").field("rng", &self.rng).finish()
    }
}

impl<T, R: PartialEq> PartialEq for TypedRng<T, R> {
    fn eq(&self, other: &Self) -> bool {
        self.rng == other.rng
    }
}

impl<T, R: Eq> Eq for TypedRng<T, R> {}

/// Represents a value that can be generated.
///
/// This is used to generate random values of a type in a deterministic manner,
/// given a random number generator.
pub trait Generatable {
    fn generate<R: RngCore>(rng: &mut R) -> Self;
}

impl Generatable for Uuid {
    fn generate<R: RngCore>(rng: &mut R) -> Self {
        let mut bytes = [0; 16];
        rng.fill_bytes(&mut bytes);
        // Builder::from_random_bytes will turn the random bytes into a valid
        // UUIDv4. (Parts of the system depend on the UUID actually being valid
        // v4, so it's important that we don't just use `uuid::from_bytes`.)
        uuid::Builder::from_random_bytes(bytes).into_uuid()
    }
}

impl<T: TypedUuidKind> Generatable for TypedUuid<T> {
    fn generate<R: RngCore>(rng: &mut R) -> Self {
        TypedUuid::from_untyped_uuid(Uuid::generate(rng))
    }
}

pub type UuidRng = TypedRng<Uuid, StdRng>;
pub type TypedUuidRng<T> = TypedRng<TypedUuid<T>, StdRng>;

#[cfg(test)]
mod tests {
    use super::*;

    // Test that TypedRng<T, ...> is Send and Sync even if T isn't.
    const _: fn() = || {
        fn assert_send_sync<T: Send + Sync>() {}
        #[expect(dead_code)]
        struct NotSendSync(*mut u8);
        assert_send_sync::<TypedRng<NotSendSync, StdRng>>();
    };
}

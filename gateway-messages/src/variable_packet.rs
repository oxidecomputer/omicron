// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Helper trait to write serde `Serialize`/`Deserialize` implementations that
//! traffic in a header followed by a variable number of elements (the count of
//! which is described by the header). This allows us to live within `hubpack`'s
//! static world with a fixed max size without sending padding for
//! underpopulated messages.

use core::marker::PhantomData;
use serde::de::{DeserializeOwned, Error, Visitor};
use serde::ser::SerializeTuple;
use serde::Serialize;

pub(crate) trait VariablePacket {
    type Header: DeserializeOwned + Serialize;
    type Element: DeserializeOwned + Serialize;

    const MAX_ELEMENTS: usize;
    const DESERIALIZE_NAME: &'static str;

    // construct a header from this instance
    fn header(&self) -> Self::Header;

    // number of elements actually contained in this instance
    fn num_elements(&self) -> u16;

    // `elements` and `elements_mut` can return slices up to
    // `Self::MAX_ELEMENTS` long; the `serialize`/`deserialize` implementations
    // will shorten them to `num_elements()` as needed
    fn elements(&self) -> &[Self::Element];
    fn elements_mut(&mut self) -> &mut [Self::Element];

    // construct an instance from `header` with empty/zero'd elements that
    // `deserialize` will populate before returning
    fn from_header(header: Self::Header) -> Self;

    // We can't `impl<T: VariablePacket> Serialize for T { .. }` due to
    // coherence rules, so instead we'll plop the implementation here, and all
    // our types that implement `VariablePacket` can now have 1-line
    // `Serialize`/`Deserialize` implementations.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let header = self.header();
        let num_elements = usize::from(self.num_elements());

        // serialize ourselves as a tuple containing our header + each element
        let mut tup = serializer.serialize_tuple(1 + num_elements)?;
        tup.serialize_element(&header)?;

        // This is the same as what serde's default serialize implementation
        // does, but we should confirm this generates reasonable code if
        // `Self::Element == u8`. Ideally rustc/llvm will reduce this loop to
        // something approximating memcpy; TODO check this on the stm32.
        for element in &self.elements()[..num_elements] {
            tup.serialize_element(element)?;
        }

        tup.end()
    }

    fn deserialize<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
        Self: Sized,
    {
        struct TupleVisitor<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for TupleVisitor<T>
        where
            T: VariablePacket,
        {
            type Value = T;

            fn expecting(
                &self,
                formatter: &mut core::fmt::Formatter,
            ) -> core::fmt::Result {
                write!(formatter, "{}", T::DESERIALIZE_NAME)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let header: T::Header = match seq.next_element()? {
                    Some(header) => header,
                    None => {
                        return Err(A::Error::custom("missing packet header"))
                    }
                };
                let mut out = T::from_header(header);
                let num_elements = usize::from(out.num_elements());
                if num_elements > T::MAX_ELEMENTS {
                    return Err(A::Error::custom("packet length too long"));
                }
                for element in &mut out.elements_mut()[..num_elements] {
                    *element = match seq.next_element()? {
                        Some(element) => element,
                        None => {
                            return Err(A::Error::custom(
                                "invalid packet length",
                            ))
                        }
                    };
                }
                Ok(out)
            }
        }

        let visitor = TupleVisitor(PhantomData);
        deserializer.deserialize_tuple(1 + Self::MAX_ELEMENTS, visitor)
    }
}

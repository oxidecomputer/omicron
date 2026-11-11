// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # [`Quantile`] Examples
//!
//! ```
//! # // Rename the types crate so the doctests can refer to the public
//! # // `oximeter` crate, not the private impl.
//! # use oximeter_types as oximeter;
//! use oximeter::Quantile;
//! let q = Quantile::new(0.5).unwrap();
//!
//! assert_eq!(q.p(), 0.5);
//! assert_eq!(q.len(), 0);
//! ```
//!
//! ```
//! # // Rename the types crate so the doctests can refer to the public
//! # // `oximeter` crate, not the private impl.
//! # use oximeter_types as oximeter;
//! use oximeter::Quantile;
//! let q = Quantile::from_parts(
//!    0.5,
//!    [0., 1., 2., 3., 4.],
//!    [1, 2, 3, 4, 5],
//!    [1., 3., 5., 7., 9.],
//! );
//! ```
//!
//! ```
//! # // Rename the types crate so the doctests can refer to the public
//! # // `oximeter` crate, not the private impl.
//! # use oximeter_types as oximeter;
//! use oximeter::Quantile;
//! let mut q = Quantile::new(0.5).unwrap();
//! for o in 1..=100 {
//!    q.append(o).unwrap();
//! }
//! assert_eq!(q.estimate().unwrap(), 50.0);
//! ```
//!
//! ```
//! # // Rename the types crate so the doctests can refer to the public
//! # // `oximeter` crate, not the private impl.
//! # use oximeter_types as oximeter;
//! use oximeter::Quantile;
//! let mut q = Quantile::new(0.9).unwrap();
//! q.append(10).unwrap();
//! assert_eq!(q.len(), 1);
//! ```

pub use oximeter_types_versions::latest::quantile::*;

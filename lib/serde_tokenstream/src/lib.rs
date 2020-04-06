//! Serde implementation for proc_macro::TokenStream. This is intended for
//! proc_macro builders who want rich configuration in their custom attributes.
//!
//! If the consumers of your macro use it like this:
//!
//! ```ignore
//! #[my_macro {
//!     settings = {
//!         reticulate_splines = true,
//!         normalizing_power = false,
//!     },
//!     disaster = "tornado"
//! }]
//! ```
//!
//! Your macro probably starts like this:
//!
//! ```ignore
//! #[proc_macro_attribute]
//! pub fn my_macro(
//!     attr: proc_macro::TokenStream,
//!     item: proc_macro::TokenStream,
//! ) -> proc_macro::TokenStream {
//!     ...
//! ```
//!
//! Use `serde_tokenstream` to deserialize `attr` into a structure with the
//! `Deserialize` trait (typically `derive`d):
//!
//! ```ignore
//!     let cfg = from_tokenstream::<Config>(&TokenStream::from(attr))?;
//! ```

mod serde_tokenstream;
pub use crate::serde_tokenstream::from_tokenstream;
pub use crate::serde_tokenstream::Error;
pub use crate::serde_tokenstream::Result;

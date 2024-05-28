// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use proc_macro2::TokenStream;

pub(crate) fn pretty_format(input: TokenStream) -> String {
    let parsed = syn::parse2(input).unwrap();
    prettyplease::unparse(&parsed)
}

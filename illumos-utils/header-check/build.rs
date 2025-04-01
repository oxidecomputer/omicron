// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![deny(warnings)]

use std::convert::TryFrom;
use std::env;
use std::path::PathBuf;

fn main() {
    let mut cfg = ctest2::TestGenerator::new();

    // We cannot proceed without a path to the source
    let gate_dir = match env::var("GATE_SRC").map(PathBuf::try_from) {
        Ok(Ok(dir)) => dir,
        _ => {
            eprintln!("Must specify path to illumos-gate sources with GATE_SRC env var");
            std::process::exit(1);
        }
    };

    let include_paths = [
        "usr/src/head",
        "usr/src/uts/intel",
        "usr/src/uts/common",
    ];
    cfg.include("/usr/include");
    for p in include_paths {
        cfg.include(gate_dir.join(p));
    }

    cfg.header("sys/types.h");
    cfg.header("sys/dkio.h");
    cfg.header("vm/page.h");

    cfg.skip_struct(|ty| match ty {
        "dk_minfo_ext" => false,
        "page" => false,

        _ => true,
    });

    // `ctest2`, unfortunately, is limited to Rust files that are valid under
    // edition="2015". See https://github.com/JohnTitor/ctest2/issues/48 for
    // more.
    //
    // `src/coreadm.rs`' `crate::` use was not valid in 2015, so we can't just
    // use `../src/lib.rs` and walk the whole `illumos-utils` crate like we do
    // in other places we use ctest2. Instead, we have one test harness for each
    // file here, where conveniently we use valid Edition 2015 code.
    //
    // `ctest2` is probably more appropriately used for a `-sys` crate with
    // bindings specifically to illumos' types and headers, where we can be more
    // confident we generally don't and will not need newer edition syntax.
    cfg.generate("../src/page.rs", "page.rs");
    cfg.generate("../src/dkio.rs", "dkio.rs");
}

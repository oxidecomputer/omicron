// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask a4x2-package

// use crate::common::run_subcmd;
use anyhow::Result;
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use serde::Deserialize;
use std::fs;
use std::env;
use xshell::{cmd, Shell};
use walkdir::WalkDir;
use sha2::Digest;
use std::io::{Read, Write};

#[derive(Parser)]
pub struct A4x2PackageArgs {
    /// Bundle omicron live tests into the output tarball
    #[clap(long)]
    live_tests: bool,

    /// Bundle omicron end-to-end-tests package into the output tarball
    #[clap(long)]
    end_to_end_tests: bool,

    /// Are we running in CI or not?
    #[clap(long)]
    ci: bool,
}

#[derive(Deserialize, Debug)]
struct NextestConfig {
    nextest_version: NextestConfigVersion,
}

#[derive(Deserialize, Debug)]
struct NextestConfigVersion {
    // required: String,
    recommended: String,
}

pub fn run_cmd(args: A4x2PackageArgs) -> Result<()> {
    let sh = Shell::new()?;

    let cargo =
    std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));

    // XXX does this make sense as a working directory?
    let work_dir =
        Utf8PathBuf::from(format!("{}/.cache/a4x2-package", &std::env::var("HOME")?));

    // Delete any results from previous runs. We don't mind if there's errors.
    fs::remove_dir_all(&work_dir).ok();

    // Create work dir
    fs::create_dir_all(&work_dir)?;

    // Output. Maybe in CI we want this to be /out
    let out_dir = work_dir.join("out");
    fs::create_dir_all(&out_dir)?;

    // Clone of omicron that we can modify without causing trouble for anyone
    let omicron_dir = work_dir.join("omicron");
    fs::create_dir_all(&omicron_dir)?;

    // Clone local working tree into work dir. This is nabbed from
    // buildomat-at-home
    {
        let src_dir = env::current_dir()?;

        // Get a ref we can checkout from the local working tree
        let mut treeish = cmd!(sh, "git stash create {src_dir}").read()?;
        if treeish.is_empty() {
            treeish = cmd!(sh, "git rev-parse HEAD {src_dir}").read()?;
        }

        // Clone the local source tree into the packaging work dir
        {
            let _popdir = sh.push_dir(&omicron_dir);
            cmd!(sh, "git init").run()?;
            cmd!(sh, "git remote add origin {src_dir}").run()?;
            cmd!(sh, "git fetch origin {treeish}").run()?;
            cmd!(sh, "git checkout {treeish}").run()?;
            cmd!(sh, "./tools/install_builder_prerequisites.sh -yp").run()?;
        }
    }

    // Testbed stuff
    {
        let testbed_dir = work_dir.join("testbed");
        let a4x2_dir = testbed_dir.join("a4x2");

        cmd!(sh,
            "git clone https://github.com/oxidecomputer/testbed {testbed_dir}")
            .run()?;
        let _popdir = sh.push_dir(&a4x2_dir);

        // build a4x2
        cmd!(sh, "{cargo} build --release").run()?;

        // XXX this modifies current working directory, which im not thrilled
        // about. in CI this doesn't matter, but outside CI, what should we do?
        // We could try to save the files (but then we need to make sure we
        // always restore them when we fail), or we could copy/clone the omicron
        // dir somewhere else and then work from that copy. buildomat-at-home
        // has logic to do this apparently.
        cmd!(sh, "./config/build-packages.sh")
            .env("OMICRON", &omicron_dir)
            .run()?;

        cmd!(sh, "./config/fetch-softnpu-artifacts.sh").run()?;

        // Deduplicate cargo-bay output amongst g0-g3. They differ on a few
        // files; deduping the rest is critical to keeping the final archive
        // size manageable for CI. To do this we will
        // - iterate files in g0
        // - check equivalent paths for g1/g3
        // - if hashes match, extract out to `sled-common/`
        let cargo_bay_dir = a4x2_dir.join("cargo-bay");
        let common_dir = cargo_bay_dir.join("sled-common");
        let g0_dir = cargo_bay_dir.join("g0");
        let prefixes = [
            cargo_bay_dir.join("g1"),
            cargo_bay_dir.join("g2"),
            cargo_bay_dir.join("g3"),
        ];
        for ent in WalkDir::new(&g0_dir) {
            let ent = ent?;
            if !ent.file_type().is_file() {
                continue;
            }

            let g0_path = Utf8PathBuf::from_path_buf(ent.path().to_path_buf()).unwrap();
            let g0_hash = sha256(&g0_path)?;

            let base_path = g0_path.strip_prefix(&g0_dir)?;
            let mut can_dedupe = true;
            for prefix in &prefixes {
                let path = prefix.join(base_path);
                // if it doesn't exist, we can't dedupe
                if !path.try_exists()? {
                    can_dedupe = false;
                    break;
                }

                let hash = sha256(&path)?;
                if hash != g0_hash {
                    can_dedupe = false;
                    break;
                }
            }

            if can_dedupe {
                // yay, extract out to the common dir.

                sh.copy_file(&g0_path, &common_dir.join(base_path))?;
                sh.remove_path(&g0_path)?;
                for prefix in &prefixes {
                    sh.remove_path(&prefix.join(base_path))?;
                }
            }
        }


        // At this point, `cargo-bay` is ready for us
        cmd!(sh, "tar -czf {out_dir}/cargo-bay.tar.gz cargo-bay/").run()?;
    }

    if args.end_to_end_tests {
        // XXX
        unimplemented!("didnt do this yet");
    }

    // Generate a bundle with the live tests included and ready to go.
    if args.live_tests {
        let _popdir = sh.push_dir(&omicron_dir);

        // We need nextest available, both to generate the test bundle and to
        // include in the output tarball to execute the bundle on a4x2.
        if args.ci {
            // In CI, we need to download nextest
            let config_str = sh.read_file(".config/nextest.toml")?;
            let nextest_config: NextestConfig = toml::from_str(&config_str)?;

            // This line of bash comes from build-and-test.sh
            // If you want to rewrite this in rust, be my guest. Make sure you
            // - retry correctly
            // - follow redirects correctly
            // - handle http errors
            cmd!(sh, "bash -c")
                .arg(&format!("curl -sSfL --retry 10 https://get.nexte.st/'{}'/illumos | gunzip | tar -xvf - -C ~/.cargo/bin", nextest_config.nextest_version.recommended))
                .run()?;
        } else {
            // Outside of CI, we let error messages take their course, and it's
            // up to the user to figure it out.
        }

        // Build and generate the live tests bundle
        // generates
        // - target/live-tests-archive.tgz
        cmd!(sh, "{cargo} xtask live-tests").run()?;
    }

    println!("hi :)");

    Ok(())
}


fn sha256(path: &Utf8Path) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; 65536];
    let mut file = fs::File::open(path)?;
    let mut ctx = sha2::Sha256::new();
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        ctx.write_all(&buf[0..n])?;
    }

    Ok(ctx.finalize().to_vec())
}
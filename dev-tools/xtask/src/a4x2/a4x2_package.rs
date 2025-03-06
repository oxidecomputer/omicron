// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask a4x2-package

use anyhow::Result;
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use fs_err as fs;
use sha2::Digest;
use std::env;
use std::io::{Read, Write};
use walkdir::WalkDir;
use xshell::{cmd, Shell};

#[derive(Parser)]
pub struct A4x2PackageArgs {
    /// Bundle omicron live tests into the output tarball
    #[clap(long)]
    live_tests: bool,

    /// Bundle omicron end-to-end-tests package into the output tarball
    #[clap(long)]
    end_to_end_tests: bool,


    /// Choose which source of oxidecomputer/testbed to build into the output.
    #[clap(long, default_value_t = String::from("https://github.com/oxidecomputer/testbed"))]
    testbed_source: String,

    /// Choose which branch of oxidecomputer/testbed to build into the output.
    #[clap(long, default_value_t = String::from("main"))]
    testbed_branch: String,
}

struct Environment {
    /// Path to `cargo` command
    cargo: Utf8PathBuf,

    /// Path to `git` command
    git: Utf8PathBuf,

    /// Directory from which xtask a4x2-package was invoked. Should be an
    /// omicron source tree.
    src_dir: Utf8PathBuf,

    /// Directory within which we will build a4x2, omicron, tests, and then
    /// package them
    work_dir: Utf8PathBuf,

    /// Path within `work_dir` where we will place build outputs, prior to
    /// generation of the final tarball artifact.
    out_dir: Utf8PathBuf,

    /// Path within `work_dir` containing a copy of the omicron source tree
    omicron_dir: Utf8PathBuf,

    /// Are we taking the CI codepaths?
    in_ci: bool,
}

pub fn run_cmd(args: A4x2PackageArgs) -> Result<()> {
    let sh = Shell::new()?;

    let env = {
        // Buildomat sets the CI environment variable. We'll work a bit
        // differently in that case (no cleanup, use /work)
        let in_ci = env::var("CI").is_ok();

        let cargo = Utf8PathBuf::from(
            std::env::var("CARGO").unwrap_or(String::from("cargo")),
        );
        let git = Utf8PathBuf::from(
            std::env::var("GIT").unwrap_or(String::from("git")),
        );

        let home_dir = Utf8PathBuf::from(std::env::var("HOME")?);
        let work_dir = if in_ci {
            Utf8PathBuf::from("/work/a4x2-package")
        } else {
            home_dir.join(".cache/a4x2-package")
        };

        let src_dir = Utf8PathBuf::try_from(env::current_dir()?)?;

        // Delete any results from previous runs. We don't mind if there's errors.
        fs::remove_dir_all(&work_dir).ok();
        // Create work dir
        fs::create_dir_all(&work_dir)?;
        sh.change_dir(&work_dir);

        // Output. Maybe in CI we want this to be /out
        let out_dir = work_dir.join("a4x2-package-out");
        fs::create_dir_all(&out_dir)?;

        // Clone of omicron that we can modify without causing trouble for anyone
        let omicron_dir = work_dir.join("omicron");
        fs::create_dir_all(&omicron_dir)?;

        Environment {
            cargo,
            git,
            work_dir,
            src_dir,
            out_dir,
            omicron_dir,
            in_ci,
        }
    };

    prepare_source(&sh, &env)?;

    if args.end_to_end_tests {
        build_end_to_end_tests(&sh, &env)?;
    }

    if args.live_tests {
        build_live_tests(&sh, &env)?;
    }

    // This needs to happen last because it messes with the working tree in a
    // way that end-to-end tests doesnt like when building
    build_a4x2(&sh, &env, &args.testbed_source, &args.testbed_branch)?;

    create_output_artifact(&sh, &env)?;

    Ok(())
}

/// Clone local working tree into work dir. This is nabbed from
/// buildomat-at-home
fn prepare_source(sh: &Shell, env: &Environment) -> Result<()> {
    cmd!(sh, "banner 'prepare'").run()?;

    let git = &env.git;

    // Get a ref we can checkout from the local working tree
    let treeish = {
        let _popdir = sh.push_dir(&env.src_dir);
        let mut treeish = cmd!(sh, "{git} stash create").read()?;
        if treeish.is_empty() {
            // nothing to stash
            eprintln!("Nothing to stash, using most recent commit for clone");
            treeish = cmd!(sh, "{git} rev-parse HEAD").read()?;
        }
        assert_eq!(
            treeish.len(),
            40,
            "treeish should be a 40-character commit hash"
        );
        assert!(
            treeish.chars().all(|c| c.is_ascii_hexdigit()),
            "treeish should be a 40-character commit hash"
        );
        treeish
    };

    // Clone the local source tree into the packaging work dir
    let _popdir = sh.push_dir(&env.omicron_dir);
    cmd!(sh, "{git} init").run()?;

    let src_dir = &env.src_dir;
    cmd!(sh, "{git} remote add origin {src_dir}").run()?;

    cmd!(sh, "{git} fetch origin {treeish}").run()?;
    cmd!(sh, "{git} checkout {treeish}").run()?;

    cmd!(sh, "./tools/install_builder_prerequisites.sh -yp").run()?;

    Ok(())
}

fn build_end_to_end_tests(sh: &Shell, env: &Environment) -> Result<()> {
    // I'm not confident this does what it should, because I don't know much
    // about the end to end tests. This is just going from what the shell script
    // did.
    cmd!(sh, "banner 'end to end'").run()?;
    let _popdir = sh.push_dir(&env.omicron_dir);

    let cargo = &env.cargo;
    cmd!(sh, "{cargo} build -p end-to-end-tests --bin commtest --bin dhcp-server --release").run()?;

    let end_to_end_dir = env.out_dir.join("end-to-end-tests");
    sh.create_dir(&end_to_end_dir)?;
    sh.copy_file("target/release/commtest", &end_to_end_dir)?;
    sh.copy_file("target/release/dhcp-server", &end_to_end_dir)?;

    Ok(())
}

/// Generate a bundle with the live tests included and ready to go.
fn build_live_tests(sh: &Shell, env: &Environment) -> Result<()> {
    cmd!(sh, "banner 'live tests'").run()?;
    let _popdir = sh.push_dir(&env.omicron_dir);

    // We need nextest available, both to generate the test bundle and to
    // include in the output tarball to execute the bundle on a4x2.
    // XXX do we need to test nextest exists & is the right version? need to
    // see if the incidental errors from stuff below already take care of
    // telling someone outside of CI what they need to be doing.
    let nextest_path =
        Utf8PathBuf::from(cmd!(sh, "/usr/bin/which cargo-nextest").read()?);

    // Build and generate the live tests bundle
    // generates
    // - target/live-tests-archive.tgz
    let cargo = &env.cargo;
    cmd!(sh, "{cargo} xtask live-tests").run()?;

    let live_test_bundle_dir = env.work_dir.join(super::LIVE_TEST_BUNDLE_DIR);
    sh.create_dir(&live_test_bundle_dir)?;

    // a tar within a tar, a door behind a door
    cmd!(
        sh,
        "tar -xzf target/live-tests-archive.tgz -C {live_test_bundle_dir}"
    )
    .run()?;

    // and we need nextest to execute it
    sh.copy_file(&nextest_path, &live_test_bundle_dir)?;

    // Finally, the script that will run nextest
    // TODO: should we make this into a rust program that runs in the VM?
    let switch_zone_script_path =
        live_test_bundle_dir.join(super::LIVE_TEST_BUNDLE_SCRIPT);
    let switch_zone_script = r#"
        set -euxo pipefail
        /opt/oxide/omdb/bin/omdb -w nexus blueprints target enable current

        TMPDIR=/var/tmp ./cargo-nextest nextest run \
            --profile=live-tests \
            --archive-file live-tests-archive/omicron-live-tests.tar.zst \
            --workspace-remap live-tests-archive
    "#;
    sh.write_file(&switch_zone_script_path, switch_zone_script)?;
    cmd!(sh, "chmod +x {switch_zone_script_path}").run()?;

    // output tar will have live-tests and nextest. this will get uploaded
    // into one of the a4x2 sleds, which is why it is a self contained tar
    let out_dir = &env.out_dir;

    // intentionally relative path so the tarball gets relative paths
    let _popdir = sh.push_dir(&env.work_dir);
    let bundle_dir_name = live_test_bundle_dir.file_name().unwrap();
    use super::LIVE_TEST_BUNDLE_NAME;
    cmd!(sh, "tar -czf {out_dir}/{LIVE_TEST_BUNDLE_NAME} {bundle_dir_name}/")
        .run()?;

    Ok(())
}

fn build_a4x2(
    sh: &Shell,
    env: &Environment,
    testbed_source: &str,
    testbed_branch: &str,
) -> Result<()> {
    cmd!(sh, "banner testbed").run()?;
    let testbed_dir = env.work_dir.join("testbed");
    let a4x2_dir = testbed_dir.join("a4x2");

    // TODO move this clone & build to prepare, so that if the omicron build
    // takes awhile we wont have lost our git token when we get here. Relevant
    // for CI but not for running locally.
    // TODO: flag to set testbed branch
    let git = &env.git;
    cmd!(
        sh,
        "{git} clone {testbed_source} --branch {testbed_branch} {testbed_dir}"
    )
    .run()?;

    let _popdir = sh.push_dir(&a4x2_dir);

    // build a4x2
    let cargo = &env.cargo;
    cmd!(sh, "{cargo} build --release").run()?;

    sh.copy_file("../target/release/a4x2", &env.out_dir)?;

    // This will modify some omicron configs, build omicron, and package things
    // up for the a4x2 cargo-bay
    cmd!(sh, "./config/build-packages.sh")
        .env("OMICRON", &env.omicron_dir)
        .run()?;

    // debatable whether this should happen in package or deploy
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

        let g0_path =
            Utf8PathBuf::from_path_buf(ent.path().to_path_buf()).unwrap();
        let g0_hash = sha256(&g0_path)?;

        let base_path = g0_path.strip_prefix(&g0_dir)?;
        let mut can_dedupe = true;
        for prefix in &prefixes {
            let path = prefix.join(base_path);
            // if it doesn't exist in all bays, we can't dedupe
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
            let dest = common_dir.join(base_path);
            sh.create_dir(&dest.parent().unwrap())?;
            sh.copy_file(&g0_path, &dest)?;
            sh.remove_path(&g0_path)?;
            for prefix in &prefixes {
                sh.remove_path(&prefix.join(base_path))?;
            }
        }
    }

    // At this point, `cargo-bay` is ready for us
    // We don't actually need all of config/ but it's a small dir
    let out_dir = &env.out_dir;
    cmd!(sh, "mv cargo-bay/ config/ {out_dir}/").run()?;

    Ok(())
}

/// Create a tgz file with everything built during a4x2-package invocation
fn create_output_artifact(sh: &Shell, env: &Environment) -> Result<()> {
    cmd!(sh, "banner bundle").run()?;
    let _popdir = sh.push_dir(&env.work_dir);
    let pkg_dir_name = env.out_dir.file_name().unwrap();

    // Place output in `out/` when running locally, or leave in work dir for CI
    let artifact = Utf8PathBuf::from("a4x2-package-out.tgz");
    let artifact = if env.in_ci {
        env.work_dir.join(artifact)
    } else {
        env.src_dir.join("out").join(artifact)
    };

    cmd!(sh, "tar -czf {artifact} {pkg_dir_name}/").run()?;

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

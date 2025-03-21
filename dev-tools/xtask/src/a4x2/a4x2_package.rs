// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask a4x2-package

use super::cmd;
use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use fs_err as fs;
use sha2::Digest;
use std::env;
use std::io::{Read, Write};
use walkdir::WalkDir;
use xshell::Shell;

/// This script will be placed in the live tests bundle to run the live tests
const LIVE_TESTS_EXECUTION_SCRIPT: &str = r#"
    set -euxo pipefail
    /opt/oxide/omdb/bin/omdb -w nexus blueprints target enable current

    TMPDIR=/var/tmp ./cargo-nextest nextest run \
        --profile=live-tests \
        --archive-file live-tests-archive/omicron-live-tests.tar.zst \
        --workspace-remap live-tests-archive
"#;

#[derive(Parser)]
pub struct A4x2PackageArgs {
    /// Choose which source of oxidecomputer/testbed to build into the output.
    #[clap(long, default_value_t = String::from("https://github.com/oxidecomputer/testbed"))]
    testbed_source: String,

    /// Choose which branch of oxidecomputer/testbed to build into the output.
    #[clap(long, default_value_t = String::from("main"))]
    testbed_branch: String,

    /// Output package is written here.
    #[clap(long, default_value_t = Utf8PathBuf::from(super::DEFAULT_A4X2_PKG_PATH))]
    output: Utf8PathBuf,
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
}

pub fn run_cmd(args: A4x2PackageArgs) -> Result<()> {
    let sh = Shell::new()?;

    let env = {
        let cargo = Utf8PathBuf::from(
            std::env::var("CARGO").unwrap_or(String::from("cargo")),
        );
        let git = Utf8PathBuf::from(
            std::env::var("GIT").unwrap_or(String::from("git")),
        );

        let src_dir = Utf8PathBuf::try_from(env::current_dir()?)?;
        let work_dir = src_dir.join("target/a4x2/package");

        // Delete any results from previous runs.
        if work_dir.try_exists()? {
            fs::remove_dir_all(&work_dir)?;
        }

        // Create work dir
        fs::create_dir_all(&work_dir)?;
        sh.change_dir(&work_dir);

        // Output. Maybe in CI we want this to be /out
        let out_dir = work_dir.join(super::A4X2_PACKAGE_DIR_NAME);
        fs::create_dir_all(&out_dir)?;

        // Clone of omicron that we can modify without causing trouble for anyone
        let omicron_dir = work_dir.join("omicron");
        fs::create_dir_all(&omicron_dir)?;

        Environment { cargo, git, work_dir, src_dir, out_dir, omicron_dir }
    };

    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let output_artifact = canonicalize_parent(&args.output)
        .context("finding absolute path to output artifact")?;

    prepare_source(&sh, &env, &args.testbed_source, &args.testbed_branch)
        .context("preparing source for builds")?;
    build_end_to_end_tests(&sh, &env).context("building end-to-end tests")?;
    build_live_tests(&sh, &env).context("building live tests")?;

    // This needs to happen last because it messes with the working tree in a
    // way that end-to-end tests doesnt like when building
    build_a4x2(&sh, &env).context("building a4x2")?;

    create_output_artifact(&sh, &env, &output_artifact)
        .context("compressing final output tarball")?;

    Ok(())
}

/// Clone local working tree into work dir, and download necessary build deps
fn prepare_source(
    sh: &Shell,
    env: &Environment,
    testbed_source: &str,
    testbed_branch: &str,
) -> Result<()> {
    cmd!(sh, "banner 'prepare'").run()?;

    // Clone testbed as our very first thing, so in buildomat we don't risk
    // our git credentials expiring during the omicron build.
    let testbed_dir = env.work_dir.join("testbed");
    let git = &env.git;
    cmd!(
        sh,
        "{git} clone {testbed_source} --branch {testbed_branch} {testbed_dir}"
    )
    .run()?;

    // We copy the source directory into another work directory to do the build.
    // We do this because a4x2 modifies some files in-place in the omicron tree
    // before running omicron-package.
    //
    // We decided rsync was the best way to do this, while correctly
    // excluding big folders that dont need to be duplicated, and preserving
    // file permissions / symlinks.
    //
    // .git/ is excluded because we duplicate this into the `target/` of the
    // source dir. If it was included, we would not be `cargo clean`-able.
    //
    // With rsync, trailing slashes are load-bearing. Please always have them.
    let src_dir = &env.src_dir;
    let omicron_dir = &env.omicron_dir;
    cmd!(sh, "rsync")
        .arg("--recursive")
        .arg("--links")
        .arg("--perms")
        .arg("--times")
        .arg("--group")
        .arg("--owner")
        .arg("--verbose")
        .arg("--delete-during")
        .arg("--delete-excluded")
        .arg("--exclude=/.git/")
        .arg("--exclude=/target/")
        .arg("--exclude=/out/")
        .arg(format!("{src_dir}/"))
        .arg(format!("{omicron_dir}/"))
        .run()
        .context("cloning omicron source tree into build directory")?;

    let cargo = &env.cargo;
    let _popdir = sh.push_dir(omicron_dir);
    cmd!(sh, "{cargo} xtask download all")
        .run()
        .context("downloading build dependencies")?;

    Ok(())
}

fn build_end_to_end_tests(sh: &Shell, env: &Environment) -> Result<()> {
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
    let nextest_path = Utf8PathBuf::from(
        cmd!(sh, "/usr/bin/which cargo-nextest")
            .read()
            .context("Ensuring nextest is installed to build live tests.")?,
    );

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

    // Finally, the script that will run nextest from the switch zone
    let switch_zone_script_path =
        live_test_bundle_dir.join(super::LIVE_TEST_BUNDLE_SCRIPT);
    sh.write_file(&switch_zone_script_path, LIVE_TESTS_EXECUTION_SCRIPT)?;
    cmd!(sh, "chmod +x {switch_zone_script_path}").run()?;

    // output tar will have live-tests and nextest. this will get uploaded
    // into one of the a4x2 sleds, which is why it is a self contained tar
    let out_dir = &env.out_dir;

    // intentionally relative bundle_dir_name so the tarball gets relative paths
    let bundle_dir_name = live_test_bundle_dir.file_name().unwrap();

    use super::LIVE_TEST_BUNDLE_NAME;
    let _popdir = sh.push_dir(&env.work_dir);
    cmd!(sh, "tar -czf {out_dir}/{LIVE_TEST_BUNDLE_NAME} {bundle_dir_name}/")
        .run()?;

    Ok(())
}

fn build_a4x2(sh: &Shell, env: &Environment) -> Result<()> {
    cmd!(sh, "banner testbed").run()?;
    let testbed_dir = env.work_dir.join("testbed");
    let a4x2_dir = testbed_dir.join("a4x2");

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

        // deduplication only happens to files
        if !ent.file_type().is_file() {
            continue;
        }

        let g0_path =
            Utf8PathBuf::from_path_buf(ent.path().to_path_buf()).unwrap();

        // reference hash. if g1/g2/g3 have this file, and it matches this hash,
        // then we can dedupe.
        let g0_hash = sha256(&g0_path)?;

        // path relative to the start of a cargo bay
        let base_path = g0_path.strip_prefix(&g0_dir)?;

        // Look for proof that we cannot dedupe
        let mut can_dedupe = true;
        for prefix in &prefixes {
            let path = prefix.join(base_path);
            // if it doesn't exist in all bays, we can't dedupe
            if !path.try_exists()? {
                can_dedupe = false;
                break;
            }

            // if the hashes don't match in all bays, we can't dedupe
            let hash = sha256(&path)?;
            if hash != g0_hash {
                can_dedupe = false;
                break;
            }
        }

        // We can dedupe, so remove this file from all the sled-specific bays
        // and place it in the common directory.
        if can_dedupe {
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
fn create_output_artifact(
    sh: &Shell,
    env: &Environment,
    out_path: &Utf8Path,
) -> Result<()> {
    cmd!(sh, "banner bundle").run()?;
    let _popdir = sh.push_dir(&env.work_dir);
    let pkg_dir_name = env.out_dir.file_name().unwrap();
    cmd!(sh, "tar -czvf {out_path} {pkg_dir_name}/").run()?;

    Ok(())
}

/// Canonicalizing fails if a file doesn't exist. This function instead
/// canonicalizes the *parent* directory of the files, and rejoins it with the
/// file name. This produces an absolute path to a file that may or may not
/// exist.
fn canonicalize_parent(path: &Utf8Path) -> Result<Utf8PathBuf> {
    // If the path is just a filename with no parent, add an implicit `.` to
    // match the current working dir.
    let parent = path.parent().unwrap_or_else(|| {
        // wouldn't want to turn an absolute path relative. this case is
        // probably if path == "/"
        if path.is_absolute() { path } else { Utf8Path::new(".") }
    });

    let parent = parent.canonicalize_utf8().with_context(|| {
        format!("canonicalizing parent directory `{}`", parent)
    })?;

    let file = path
        .components()
        .last()
        .with_context(|| {
            format!("extracting last component of path `{}`", path)
        })?
        .as_str();

    Ok(parent.join(file))
}

/// Fully read and hash a file
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

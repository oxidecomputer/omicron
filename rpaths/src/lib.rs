// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Build-time crate for configuring RPATHs for Omicron binaries
//!
//! ## The least you need to know
//!
//! This build-time crate is used by several top-level Omicron crates to set
//! RPATH so that libpq can be found at runtime.  This is necessary because these
//! crates depend on "diesel", which depends on "pq-sys", which links in "libpq".
//! But Cargo/Rust have no built-in way to set the RPATH so that libpq can
//! actually be found at runtime.  (See below.)  So we've developed the pattern
//! here instead.  It works like this:
//!
//! 1. Any crate that depends on pq-sys, directly or not, needs to follow these
//!    instructions.  Generally, we depend on pq-sys _indirectly_, by virtue of
//!    depending on Diesel.
//! 2. Affected crates (e.g., omicron-nexus) have a build.rs that just calls
//!    `omicron_rpath::configure_default_omicron_rpaths()`.
//! 3. These crates must also add a dependency on "pq-sys", usually version "*".
//!    (This dependency is unfortunate but necessary in order for us to get the
//!    metadata emitted by pq-sys that tells it where it found libpq.  Since we
//!    don't directly use pq-sys in the crate, we don't care what version it is.
//!    We specify "*" so that when Cargo dedups our dependency with the one in
//!    Diesel, we pick up whatever would be picked up anyway, and we'll get its
//!    metadata.)
//! 4. At the top level of Omicron (in the workspace Cargo.toml), we use a
//!    patched version of pq-sys that emits metadata that's used by
//!    `configure_default_omicron_rpaths()`.
//!
//! This crate is factored (over-engineered, really) so that we can extend this
//! pattern to other native libraries in the future.
//!
//! ## More details
//!
//! On Unix-like systems, executable binaries and shared libraries can have
//! shared library dependencies.  When a binary is loaded (whether it's an
//! executable or a shared library), the runtime linker (ld.so) locates these
//! dependencies using a combination of environment variables, system
//! configuration, and RPATH entries embedded in the binary itself.  While this
//! process is well-defined, using it correctly can be deceptively tricky and
//! it's often a source of considerable frustration.
//!
//! As of this writing, Cargo has no first-class way to configure the RPATH for
//! binaries that it builds.  This is covered by [rust-lang/cargo#5077][1].  This
//! comes up most often for people trying to expose a native library to Rust, as
//! via [*-sys packages][2].  Typically a Rust program that uses one of these
//! packages will wind up with no RPATH entries.  This will work if, at runtime,
//! the library happens to be in one of the runtime linker's default search paths
//! (e.g., /usr/lib).  This is commonly the case if the library came from the
//! system package manager.  But in general, the library might be in some other
//! path, and you would need to specify LD_LIBRARY_PATH every time you run the
//! program in order for the linker to find the library.  Using LD_LIBRARY_PATH
//! like this is discouraged because it affects more than just the program you're
//! running -- it affects everything that inherits the variable.  You're supposed
//! to include RPATH entries in the binary instead.
//!
//! As of 1.56, Cargo supports the "cargo:rustc-link-arg" instruction for use by
//! [Build Scripts][3] to pass arbitrary options to the linker.  We use that here
//! to tell the linker to include the correct RPATH entry for our one native
//! dependency that's affected by this (libpq, exposed via the pq-sys package).
//!
//! A subtle but critical point here is that the RPATH is knowable only by the
//! system that's building the top-level executable binary.  This mechanism can't
//! go into the *-sys package that wraps the native library because that package
//! cannot know where the library will be found at runtime.  Only whoever (or
//! whatever) is building the software knows that.  Further, Cargo provides no
//! mechanism for a package to emit linker arguments used when building its
//! dependents.  For more discussion on this, see [rust-lang/cargo#9554][4].
//!
//! So we need to emit the linker argument here.  How do we know what value to
//! use?  We take the approach used by most build systems: we use the path where
//! the library was found on the build machine.  But how do we know where that
//! was?  *-sys packages have non-trivial mechanisms for locating the desired
//! library.  We don't want to duplicate those here.  Instead, we make use of
//! metadata emitted by those build scripts, which shows up as "DEP_*"
//! environment variables for us.
//!
//! **Important note:** In order for us to have metadata for these dependencies,
//! we must *directly* depend on them.  This may mean adding an otherwise unused
//! dependency from the top-level package to the *-sys package.
//!
//! (In the limit, it may be wrong for us to use the same path that was used to
//! locate the library on the build machine.  We might want to bundle all of
//! these libraries and use something like `$ORIGIN/../lib`.  We could generalize
//! the mechanism here to pass whatever path we want, possibly specified by some
//! other environment variable like OMICRON_BUILD_RPATH.)
//!
//! [1]: https://github.com/rust-lang/cargo/issues/5077
//! [2]: https://doc.rust-lang.org/cargo/reference/build-scripts.html#-sys-packages
//! [3]: https://doc.rust-lang.org/cargo/reference/build-scripts.html
//! [4]: https://github.com/rust-lang/cargo/issues/9554

/// Tells Cargo to pass linker arguments that specify the right RPATH for Omicron
/// binaries
// This currently assumes that all Omicron binaries link to the same set of
// native libraries.  As a result, we use a fixed list of libraries.  In the
// future, if they depend on different combinations, we can accept different
// arguments here that specify exactly which ones are expected to be found.
pub fn configure_default_omicron_rpaths() {
    internal::configure_default_omicron_rpaths();
    // If no 'rerun-if-*' directives are emitted, cargo conservatively [1]
    // assumes the build-script should be rerun if any file within the
    // package changes. Unfortunately this can result in overzealous,
    // rebuilds, e.g., a change to an integration test triggering a
    // rebuild for its containing package.
    //
    // To get around this we output this dummy directive to ensure a
    // build.rs using just the rpath logic isn't rerun constantly.
    //
    // [1]: https://doc.rust-lang.org/cargo/reference/build-scripts.html#change-detection
    println!("cargo:rerun-if-changed=build.rs");
}

// None of this behavior is needed on MacOS.
#[cfg(not(any(target_os = "illumos", target_os = "linux")))]
mod internal {
    pub fn configure_default_omicron_rpaths() {}
}

#[cfg(any(target_os = "illumos", target_os = "linux"))]
mod internal {
    use std::ffi::OsStr;

    pub fn configure_default_omicron_rpaths() {
        let mut rpaths = Vec::new();

        for env_var_name in RPATH_ENV_VARS {
            let env_var_name = std::ffi::OsString::from(&env_var_name);
            configure_rpaths_from_env_var(&mut rpaths, &env_var_name);
        }

        for r in rpaths {
            println!("{}", emit_rpath(&r));
        }
    }

    /// Environment variables that contain RPATHs we want to use in our built
    /// binaries
    ///
    /// These environment variables are set by Cargo based on metadata emitted by
    /// our dependencies' build scripts.  Since a particular dependency could use
    /// multiple libraries in different paths, each of these environment
    /// variables may itself look like a path, not just a directory.  That is,
    /// these are colon-separated lists of directories.
    ///
    /// Currently, we only do this for libpq ("pq-sys" package), but this pattern
    /// could be generalized for other native libraries.
    pub static RPATH_ENV_VARS: &'static [&'static str] = &["DEP_PQ_LIBDIRS"];

    /// Tells Cargo to pass linker arguments that specify RPATHs from the
    /// environment variable `env_var_name`
    ///
    /// Panics if the environment variable is not set or contains non-UTF8 data.
    /// This might be surprising, since environment variables are optional in
    /// most build-time mechanisms.  We opt for strictness here because in fact
    /// we _do_ expect these to always be set, and if they're not, it's most
    /// likely that somebody has forgotten to include a required dependency.  We
    /// want to tell them that rather than silently produce unrunnable binaries.
    pub fn configure_rpaths_from_env_var(
        rpaths: &mut Vec<String>,
        env_var_name: &OsStr,
    ) {
        // If you see this message, that means that the build script for some
        // Omicron crate is trying to configure RPATHs for a native library, but
        // the environment variable that's supposed to contain the RPATH
        // information for that library is unset.  That most likely means that
        // the crate you're building is lacking a direct dependency on the
        // '*-sys' crate, or else that the '*-sys' crate's build script failed
        // to set this metadata.
        let env_var_value =
            std::env::var_os(env_var_name).unwrap_or_else(|| {
                panic!(
                    "omicron-rpaths: expected {:?} to be set in the \
                    environment, but found it unset.  (Is the current \
                    crate missing a dependency on a *-sys crate?)",
                    env_var_name,
                )
            });

        configure_rpaths_from_path(rpaths, &env_var_value).unwrap_or_else(
            |error| {
                panic!("omicron-rpaths: env var {:?}: {}", env_var_name, error)
            },
        );
    }

    /// Given a colon-separated list of paths in `env_var_value`, append to
    /// `rpaths` the same list of paths.
    fn configure_rpaths_from_path(
        rpaths: &mut Vec<String>,
        env_var_value: &OsStr,
    ) -> Result<(), String> {
        for path in std::env::split_paths(env_var_value) {
            let path_str =
                path.to_str().ok_or_else(|| "contains non-UTF8 data")?;
            rpaths.push(path_str.to_owned());
        }

        Ok(())
    }

    /// Emits the Cargo instruction for a given RPATH.  This is only separated
    /// out to make different parts of this module easier to test.
    pub fn emit_rpath(path_str: &str) -> String {
        format!("cargo:rustc-link-arg=-Wl,-R{}", path_str)
    }

    #[cfg(test)]
    mod tests {
        use std::ffi::OsStr;
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStrExt;

        #[test]
        #[should_panic = "omicron-rpaths: expected \"SHOULD_NOT_EXIST\" \
        to be set in the environment, but found it unset"]
        fn test_configure_rpaths_from_bad_envvar() {
            use super::configure_rpaths_from_env_var;

            let mut v = Vec::new();
            configure_rpaths_from_env_var(
                &mut v,
                &OsString::from("SHOULD_NOT_EXIST"),
            );
        }

        #[test]
        fn test_configure_from_path() {
            use super::configure_rpaths_from_path;

            let mut v = Vec::new();

            configure_rpaths_from_path(
                &mut v,
                &OsString::from("/opt/local/lib:/usr/local/lib"),
            )
            .unwrap();
            assert_eq!(v, vec!["/opt/local/lib", "/usr/local/lib"]);

            configure_rpaths_from_path(&mut v, &OsString::from("foo")).unwrap();
            assert_eq!(v, vec!["/opt/local/lib", "/usr/local/lib", "foo"]);

            configure_rpaths_from_path(
                &mut v,
                &OsString::from("/my/special/lib"),
            )
            .unwrap();
            assert_eq!(
                v,
                vec![
                    "/opt/local/lib",
                    "/usr/local/lib",
                    "foo",
                    "/my/special/lib"
                ]
            );

            let error = configure_rpaths_from_path(
                &mut v,
                &OsStr::from_bytes(b"/foo/b\x80ar"),
            )
            .unwrap_err();
            assert_eq!(error, "contains non-UTF8 data");
        }

        #[test]
        fn test_emit_rpath() {
            use super::emit_rpath;

            assert_eq!(
                "cargo:rustc-link-arg=-Wl,-R/foo/bar",
                emit_rpath("/foo/bar").as_str()
            );
            assert_eq!(
                "cargo:rustc-link-arg=-Wl,-R$ORIGIN/../lib",
                emit_rpath("$ORIGIN/../lib").as_str()
            );
        }
    }
}

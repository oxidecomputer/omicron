// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    apis::{ApiIdent, ManagedApis},
    environment::{BlessedSource, GeneratedSource},
    resolved::{Resolution, Resolved},
    spec::Environment,
    spec_files_generic::ApiSpecFile,
};
use semver::Version;
use std::{collections::BTreeMap, ops::Deref};

pub(crate) fn dump_impl(
    env: &Environment,
    blessed_source: &BlessedSource,
    generated_source: &GeneratedSource,
) -> anyhow::Result<()> {
    let apis = ManagedApis::all()?;

    // Print information about local files.
    let local_files = env.local_source.load(&apis)?;
    dump_structure(
        &local_files.spec_files,
        &local_files.errors,
        &local_files.warnings,
    );

    // Print information about what we found in Git.
    let blessed = blessed_source.load(&apis)?;
    dump_structure(&blessed.spec_files, &blessed.errors, &blessed.warnings);

    // Print information about generated files.
    let generated = generated_source.load(&apis)?;
    dump_structure(
        &generated.spec_files,
        &generated.errors,
        &generated.warnings,
    );

    // Print result of resolving the differences.
    println!("Resolving specs");
    let resolved =
        Resolved::new(env, &apis, &blessed, &generated, &local_files);
    for note in resolved.notes() {
        println!("NOTE: {}", note);
    }
    for problem in resolved.general_problems() {
        println!("PROBLEM: {}", problem);
    }

    for api in apis.iter_apis() {
        let ident = api.ident();
        println!("    API: {}", ident);

        for version in api.iter_versions_semver() {
            print!("        version {}: ", version);

            // unwrap(): there should be a resolution for every managed API
            let resolution =
                resolved.resolution_for_api_version(ident, version).unwrap();
            match resolution {
                Resolution::NoProblems => println!("OK"),
                Resolution::Problems(problems) => {
                    println!("ERROR");
                    for p in problems {
                        println!("    PROBLEM: {}\n", p);
                    }
                }
            }
        }
    }

    Ok(())
}

fn dump_structure<T: Deref<Target = ApiSpecFile>>(
    spec_files: &BTreeMap<ApiIdent, BTreeMap<Version, Vec<T>>>,
    errors: &[anyhow::Error],
    warnings: &[anyhow::Error],
) {
    println!("warnings: {}", warnings.len());
    for w in warnings {
        println!("    warn: {:#}", w);
    }
    println!("errors: {}", errors.len());
    for e in errors {
        println!("    error: {:#}", e);
    }

    for (api_ident, version_map) in spec_files {
        println!("    API: {}", api_ident);
        for (version, files) in version_map {
            println!("        version {}:", version);
            for f in files {
                let api_spec: &ApiSpecFile = f.deref();
                println!(
                    "            file {} (v{})",
                    api_spec.spec_file_name().path(),
                    api_spec.version()
                );
            }
        }
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Xtask for printing USDT probes.

use crate::load_workspace;
use tabled::settings::Style;
use tabled::Table;
use tabled::Tabled;

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct Probe {
    binary: String,
    provider: String,
    probe: String,
    arguments: String,
}

pub(crate) fn print_probes(filter: Option<String>) -> anyhow::Result<()> {
    const SKIP_ME: &[&str] = &["bootstrap", "xtask"];
    const PATHS: &[&str] = &["release", "debug"];
    let workspace = load_workspace()?;

    // Find all local packages and any binaries they contain, and attemp to find
    // contained DTrace probes in the object files.
    let mut entries = Vec::new();
    for bin_target in
        workspace.workspace_packages().iter().flat_map(|package| {
            package.targets.iter().filter(|target| {
                target.is_bin()
                    && !SKIP_ME.contains(&target.name.as_str())
                    && filter
                        .as_ref()
                        .map(|filt| target.name.contains(filt))
                        .unwrap_or(true)
            })
        })
    {
        let maybe_path = PATHS
            .iter()
            .filter_map(|p| {
                let path =
                    workspace.target_directory.join(p).join(&bin_target.name);
                if path.exists() {
                    Some(path)
                } else {
                    None
                }
            })
            .next();
        let Some(path) = maybe_path else {
            continue;
        };
        match usdt::probe_records(&path) {
            Ok(sections) => {
                let all_providers = sections
                    .into_iter()
                    .flat_map(|section| section.providers.into_values());
                for provider in all_providers {
                    for probe in provider.probes.into_values() {
                        let arguments =
                            format!("[{}]", probe.arguments.join(","));
                        entries.push(Probe {
                            binary: bin_target.name.clone(),
                            provider: provider.name.clone(),
                            probe: probe.name,
                            arguments,
                        });
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Failed to extract DTrace probes from '{path}': \
                    {e}, or it may have zero probes"
                );
            }
        }
    }
    println!("{}", Table::new(entries).with(Style::empty()));
    Ok(())
}

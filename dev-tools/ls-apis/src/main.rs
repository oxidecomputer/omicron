// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Show information about Progenitor-based APIs

// XXX-dap wishlist:
// - Fix warnings:
//   - missing client: crucible-control-client
//   - missing client: dsc-client
//   - missing client: omicron-ddm-admin-client
//   - missing that "Maghemite DDM Admin" has another client in
//     "omicron:clients/ddm-admin-client"
//   - missing that "Maghemite DDM Admin" is consumed by sled-agent
// - Inspect all the edges to make sure I understand them and that we're
//   handling them well
// - *Use* this to generate the Asciidoc table
//   - after I've compared the current one with the generated one, make package
//     names into links
// - Find The DAG
// - Take a pass through everything: document, and rethink abstractions a little
//
// Some specific notes:
// - clickhouse-admin has no client yet

use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use clap::{Args, Parser, Subcommand};
use omicron_ls_apis::{
    AllApiMetadata, LoadArgs, ServerComponentName, SystemApis,
};

#[derive(Parser)]
#[command(
    name = "ls-apis",
    bin_name = "ls-apis",
    about = "Show information about Progenitor-based APIs"
)]
struct LsApis {
    /// path to metadata about APIs
    #[arg(long)]
    api_manifest: Option<Utf8PathBuf>,

    /// path to directory with clones of dependent repositories
    #[arg(long)]
    extra_repos: Option<Utf8PathBuf>,

    #[command(subcommand)]
    cmd: Cmds,
}

#[derive(Subcommand)]
enum Cmds {
    /// print out an Asciidoc table summarizing the APIs
    Adoc,
    /// print out each API, what exports it, and what consumes it
    Apis(ShowDepsArgs),
    /// print out APIs exported and consumed by each deployment unit
    DeploymentUnits(DotArgs),
    /// print out APIs exported and consumed, by server component
    Servers(DotArgs),
}

#[derive(Args)]
pub struct ShowDepsArgs {
    /// Show the Rust dependency path resulting in the API dependency
    #[arg(long)]
    show_deps: bool,
}

#[derive(Args)]
pub struct DotArgs {
    /// Show output that can be fed to graphviz (dot)
    #[arg(long)]
    dot: bool,
    /// Show the Rust dependency path resulting in the API dependency
    #[arg(long)]
    show_deps: bool,
}

fn main() -> Result<()> {
    let cli_args = LsApis::parse();
    let load_args = LoadArgs::try_from(&cli_args)?;
    let apis = SystemApis::load(load_args)?;

    match cli_args.cmd {
        Cmds::Adoc => run_adoc(&apis),
        Cmds::Apis(args) => run_apis(&apis, args),
        Cmds::DeploymentUnits(args) => run_deployment_units(&apis, args),
        Cmds::Servers(args) => run_servers(&apis, args),
    }
}

fn run_adoc(apis: &SystemApis) -> Result<()> {
    println!(r#"[cols="1h,2,2,2a,2", options="header"]"#);
    println!("|===");
    println!("|API");
    println!("|Server location (`repo:path`)");
    println!("|Client packages (`repo:path`)");
    println!("|Consumers (`repo:path`; excluding omdb and tests)");
    println!("|Notes");
    println!("");

    let metadata = apis.api_metadata();
    for api in metadata.apis() {
        let server_component = apis.api_producer(&api.client_package_name)?;
        println!("|{}", api.label);
        println!("|{}", apis.adoc_label(server_component)?);
        println!("|{}", apis.adoc_label(&api.client_package_name)?);
        println!("|");

        for (c, _) in apis.api_consumers(&api.client_package_name) {
            println!("* {}", apis.adoc_label(c)?);
        }

        print!("|{}", api.notes.as_deref().unwrap_or("-\n"));
        println!("");
    }

    Ok(())
}

fn run_apis(apis: &SystemApis, args: ShowDepsArgs) -> Result<()> {
    let metadata = apis.api_metadata();
    for api in metadata.apis() {
        println!("{} (client: {})", api.label, api.client_package_name);
        for (s, path) in apis.api_consumers(&api.client_package_name) {
            let (repo_name, package_path) = apis.package_label(s)?;
            println!("    consumed by: {} ({}/{})", s, repo_name, package_path);
            if args.show_deps {
                for p in path.nodes() {
                    println!("        via {}", p);
                }
            }
        }
        println!("");
    }
    Ok(())
}

fn run_deployment_units(apis: &SystemApis, args: DotArgs) -> Result<()> {
    if args.dot {
        println!("{}", apis.dot_by_unit());
    } else {
        let metadata = apis.api_metadata();
        for unit in apis.deployment_units() {
            let server_components = apis.deployment_unit_servers(unit)?;
            println!("{}", unit);
            print_server_components(
                apis,
                metadata,
                server_components,
                "    ",
                args.show_deps,
            )?;
            println!("");
        }
    }

    Ok(())
}

fn print_server_components<'a>(
    apis: &SystemApis,
    metadata: &AllApiMetadata,
    server_components: impl IntoIterator<Item = &'a ServerComponentName>,
    prefix: &str,
    show_deps: bool,
) -> Result<()> {
    for s in server_components.into_iter() {
        let (repo_name, pkg_path) = apis.package_label(s)?;
        println!("{}{} ({}/{})", prefix, s, repo_name, pkg_path);
        for api in metadata
            .apis()
            .filter(|a| apis.api_producer(&a.client_package_name).unwrap() == s)
        {
            println!(
                "{}    exposes: {} (client = {})",
                prefix, api.label, api.client_package_name
            );
        }
        for (c, path) in apis.component_apis_consumed(s) {
            println!("{}    consumes: {}", prefix, c);
            if show_deps {
                for p in path.nodes() {
                    println!("{}        via: {}", prefix, p);
                }
            }
        }

        println!("");
    }
    Ok(())
}

fn run_servers(apis: &SystemApis, args: DotArgs) -> Result<()> {
    if args.dot {
        println!("{}", apis.dot_by_server_component())
    } else {
        let metadata = apis.api_metadata();
        print_server_components(
            apis,
            metadata,
            metadata.server_components(),
            "",
            args.show_deps,
        )?;
    }
    Ok(())
}

impl TryFrom<&LsApis> for LoadArgs {
    type Error = anyhow::Error;

    fn try_from(args: &LsApis) -> Result<Self> {
        let self_manifest_dir_str = std::env::var("CARGO_MANIFEST_DIR")
            .context("expected CARGO_MANIFEST_DIR in environment")?;
        let self_manifest_dir = Utf8PathBuf::from(self_manifest_dir_str);

        let api_manifest_path =
            args.api_manifest.clone().unwrap_or_else(|| {
                self_manifest_dir
                    .join("..")
                    .join("..")
                    .join("api-manifest.toml")
            });
        let extra_repos_path = args.extra_repos.clone().unwrap_or_else(|| {
            self_manifest_dir
                .join("..")
                .join("..")
                .join("out")
                .join("ls-apis")
                .join("checkout")
        });

        Ok(LoadArgs { api_manifest_path, extra_repos_path })
    }
}

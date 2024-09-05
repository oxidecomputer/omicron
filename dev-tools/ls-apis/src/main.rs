// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Show information about Progenitor-based APIs

// XXX-dap some ideas:
// - The current approach only finds consumers that are themselves exporters of
//   APIs.  We want to find other consumers, too.  This is a little tricky
//   because we don't have a way of getting from a package to the list of
//   packages that depend on it.  But perhaps we could create this reverse-index
//   inside the Workspace.  We really only need it for the progenitor clients'
//   reverse dependencies, but we don't know which ones those are until we've
//   computed this for everybody.
//
//   It's also worth noting that we care about this cross-repo: a progenitor
//   client might be in repo X but we want this information for its dependents
//   in repo Y.  So we can't do this entirely at the Workspace level.  All the
//   Workspace can give us is an efficient way to walk reverse dependencies.
// - Add options to the various list commands to print full dependency paths, too
//   - Modify walk_required_deps_recursively() to provide the full path from the
//     root to the package, not just the parent and package found.
//   - Modify the data structures in `Apis` to keep track of not just the
//     dependencies but the path to them
//   - Update the commands to use these

use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use clap::{Args, Parser, Subcommand};
use omicron_ls_apis::{AllApiMetadata, Apis, LoadArgs, ServerComponent};

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
    Apis,
    /// print out APIs exported and consumed by each deployment unit
    DeploymentUnits(DotArgs),
    /// print out APIs exported and consumed, by server component
    Servers,
}

#[derive(Args)]
pub struct DotArgs {
    /// Show output that can be fed to graphviz (dot)
    #[arg(long)]
    dot: bool,
}

fn main() -> Result<()> {
    let cli_args = LsApis::parse();
    let load_args = LoadArgs::try_from(&cli_args)?;
    let apis = Apis::load(load_args)?;

    match cli_args.cmd {
        Cmds::Adoc => run_adoc(&apis),
        Cmds::Apis => run_apis(&apis),
        Cmds::DeploymentUnits(args) => run_deployment_units(&apis, args),
        Cmds::Servers => run_servers(&apis),
    }
}

fn run_adoc(apis: &Apis) -> Result<()> {
    // XXX-dap
    // - missing Clickhouse Admin?
    // - missing that "Maghemite DDM Admin" has another client in
    //   "omicron:clients/ddm-admin-client"
    // - missing that "Maghemite DDM Admin" is consumed by sled-agent
    println!(r#"[cols="1h,2,2,2a,2", options="header"]"#);
    println!("|===");
    println!("|API");
    println!("|Server location (`repo:path`)");
    println!("|Client packages (`repo:path`)");
    // XXX-dap does this approach ignore consumers that are not themselves
    // exporters of APIs?
    println!("|Consumers (`repo:path`; excluding omdb and tests)");
    println!("|Notes");
    println!("");

    let metadata = apis.api_metadata();
    for api in metadata.apis() {
        println!("|{}", api.label);
        // XXX-dap want these to be links
        println!("|{}", apis.adoc_label(&api.server_component)?);
        println!("|{}", apis.adoc_label(&api.client_package_name)?);
        println!("|");

        for c in apis.api_consumers(&api.client_package_name) {
            println!("* {}", apis.adoc_label(c)?);
        }

        print!("|{}", api.notes.as_deref().unwrap_or("-\n"));
        println!("");
    }

    Ok(())
}

fn run_apis(apis: &Apis) -> Result<()> {
    let metadata = apis.api_metadata();
    for api in metadata.apis() {
        println!("{} (client: {})", api.label, api.client_package_name);
        for s in apis.api_consumers(&api.client_package_name) {
            let (repo_name, package_path) = apis.package_label(s)?;
            println!("    consumed by: {} ({}/{})", s, repo_name, package_path);
        }
        println!("");
    }
    Ok(())
}

fn run_deployment_units(apis: &Apis, args: DotArgs) -> Result<()> {
    if args.dot {
        println!("{}", apis.dot_by_unit());
    } else {
        let metadata = apis.api_metadata();
        for (unit, server_components) in apis.all_deployment_unit_components() {
            println!("{}", unit);
            print_server_components(apis, metadata, server_components, "    ")?;
            println!("");
        }
    }

    Ok(())
}

fn print_server_components<'a>(
    apis: &Apis,
    metadata: &AllApiMetadata,
    server_components: impl IntoIterator<Item = &'a ServerComponent>,
    prefix: &str,
) -> Result<()> {
    for s in server_components.into_iter() {
        let (repo_name, pkg_path) = apis.package_label(s)?;
        println!("{}{} ({}/{})", prefix, s, repo_name, pkg_path);
        for api in metadata.apis().filter(|a| a.server_component == *s) {
            println!(
                "{}    exposes: {} (client = {})",
                prefix, api.label, api.client_package_name
            );
        }
        // XXX-dap add mode to print paths
        for c in apis.component_apis_consumed(s) {
            println!("{}    consumes: {}", prefix, c);
        }

        println!("");
    }
    Ok(())
}

fn run_servers(apis: &Apis) -> Result<()> {
    let metadata = apis.api_metadata();
    print_server_components(apis, metadata, metadata.server_components(), "")
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

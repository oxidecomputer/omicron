// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask ls-clients

use anyhow::{anyhow, ensure, Context, Result};
use camino::Utf8Path;
use camino::Utf8PathBuf;
use cargo_metadata::DependencyKind;
use cargo_metadata::Metadata;
use cargo_metadata::Package;
use clap::Args;
use petgraph::dot::Dot;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Display;
use url::Url;

#[derive(Args)]
pub struct LsClientsArgs {
    #[arg(long)]
    api_manifest: Option<Utf8PathBuf>,

    #[arg(long)]
    package_manifest: Option<Utf8PathBuf>,

    #[arg(long)]
    extra_repos: Option<Utf8PathBuf>,

    #[arg(long)]
    adoc: bool,
}

pub fn run_cmd(args: LsClientsArgs) -> Result<()> {
    let manifest_dir = Utf8PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR")
            .context("looking up CARGO_MANIFEST_DIR in environment")?,
    );

    // First, read the metadata we have about APIs.
    let api_manifest_path = args.api_manifest.clone().unwrap_or_else(|| {
        manifest_dir.join("..").join("..").join("api-manifest.toml")
    });
    let api_manifest: AllApiMetadata = toml::from_str(
        &std::fs::read_to_string(&api_manifest_path).with_context(|| {
            format!("read API manifest {:?}", &api_manifest_path)
        })?,
    )
    .with_context(|| format!("parse API manifest {:?}", &api_manifest_path))?;

    let apis_by_client_package: BTreeMap<_, _> = api_manifest
        .apis
        .into_iter()
        .map(|api| (api.client_package_name.clone(), api))
        .collect();

    // Now, for each repo, load its metadata.  Omicron is a little special since
    // we're already in that repo.
    let mut metadata_by_repo = BTreeMap::new();
    metadata_by_repo.insert(
        String::from("omicron"),
        Workspace::load("omicron", None)
            .context("loading Omicron repo metadata")?,
    );
    let extra_repos = args.extra_repos.clone().unwrap_or_else(|| {
        manifest_dir.join("..").join("..").join("extra_repos")
    });
    for repo in &api_manifest.extra_repos {
        metadata_by_repo.insert(
            repo.clone(),
            Workspace::load(repo, Some(&extra_repos))
                .with_context(|| format!("load metadata for repo {}", repo))?,
        );
    }

    let mut metadata_used = BTreeSet::new();
    for (_, workspace) in &metadata_by_repo {
        println!("WORKSPACE: {}", workspace.name);
        for (_, c) in &workspace.progenitor_clients {
            let metadata = apis_by_client_package.get(&c.me.name);
            if metadata.is_none() {
                eprintln!(
                    "missing metadata for client package: {:#}",
                    c.me.name
                );
            }

            metadata_used.insert(c.me.name.clone());
            print_package(c, &args);
        }

        println!("");
    }

    let clients_in_metadata =
        apis_by_client_package.keys().cloned().collect::<BTreeSet<_>>();
    let metadata_extra =
        clients_in_metadata.difference(&metadata_used).collect::<Vec<_>>();
    for extra in metadata_extra {
        eprintln!("unused metadata: {}", extra);
    }

    // Parse the package manifest.
    //    let pkg_file = args
    //        .package_manifest
    //        .as_ref()
    //        .map(|c| Ok::<_, anyhow::Error>(c.clone()))
    //        .unwrap_or_else(|| {
    //            Ok(Utf8PathBuf::from(
    //                std::env::var("CARGO_MANIFEST_DIR")
    //                    .context("looking up CARGO_MANIFEST_DIR in environment")?,
    //            )
    //            .join("..")
    //            .join("..")
    //            .join("package-manifest.toml"))
    //        })?;
    //    let pkgs = parse_packages(&pkg_file)?;
    //    pkgs.dump();

    // for c in &progenitor_clients {
    //     let metadata = apis_by_client_package.remove(c.me.name);
    //     if metadata.is_none() {
    //         eprintln!("missing metadata for client package: {:#}", c.me.name);
    //     }
    //     // print_package(c, &args);
    // }

    // for (client_package, _) in apis_by_client_package {
    //     eprintln!("metadata matches no known package: {}", client_package);
    // }

    let graph = make_graph(
        &apis_by_client_package,
        metadata_by_repo.values().collect(),
    )?;
    println!("{}", graph);

    Ok(())
}

struct ClientPackage {
    me: MyPackage,
    rdeps: Vec<MyPackage>,
}

impl ClientPackage {
    fn new(workspace: &Metadata, me: MyPackage) -> Result<ClientPackage> {
        let rdeps = direct_dependents(workspace, &me.name)?;
        Ok(ClientPackage { me, rdeps })
    }
}

struct MyPackage {
    name: String,
    location: MyPackageLocation,
}

impl MyPackage {
    fn new(workspace: &Metadata, pkg: &Package) -> Result<MyPackage> {
        // Figure out where this thing is.  It's generally one of two places:
        // (1) In a remote repository.  In that case, it will have a "source"
        //     property that's the URL to a package.
        // (2) Inside this workspace.  In that case, it will have no "source",
        //     but it will have a manifest_path that's inside this workspace.
        let location = if let Some(source) = &pkg.source {
            let source_repo_str = &source.repr;
            let repo_name =
                source_repo_name(source_repo_str).with_context(|| {
                    format!("parsing source {:?}", source_repo_str)
                })?;

            // Figuring out where in that repo the package lives is trickier.
            // Here we encode some knowledge of where Cargo would have checked
            // out the repo.
            let cargo_home = std::env::var("CARGO_HOME")
                .context("looking up CARGO_HOME in environment")?;
            let cargo_path =
                Utf8PathBuf::from(cargo_home).join("git").join("checkouts");
            let path =
                pkg.manifest_path.strip_prefix(&cargo_path).map_err(|_| {
                    anyhow!(
                    "expected non-local package manifest path ({:?}) to be \
                     under {:?}",
                    pkg.manifest_path,
                    cargo_path,
                )
                })?;

            // There should be two extra leading directory components here.
            // Remove them.  We've gone too far if the file name isn't right
            // after that.
            let tail: Utf8PathBuf = path.components().skip(2).collect();
            ensure!(
                tail.file_name() == Some("Cargo.toml"),
                "unexpected non-local package manifest path: {:?}",
                pkg.manifest_path
            );

            let path = tail
                .parent()
                .ok_or_else(|| {
                    anyhow!(
                        "unexpected non-local package manifest path: {:?}",
                        pkg.manifest_path
                    )
                })?
                .to_owned();
            MyPackageLocation::RemoteRepo { oxide_github_repo: repo_name, path }
        } else {
            let manifest_path = &pkg.manifest_path;
            let relative_path = manifest_path
                .strip_prefix(&workspace.workspace_root)
                .map_err(|_| {
                    anyhow!(
                    "no \"source\", so assuming this package is inside this \
                     repo, but its manifest path ({:?}) is not under the \
                     workspace root ({:?})",
                    manifest_path,
                    &workspace.workspace_root
                )
                })?;
            // XXX-dap commonize with above
            ensure!(
                relative_path.file_name() == Some("Cargo.toml"),
                "unexpected manifest path for local package: {:?}",
                manifest_path
            );
            let path = relative_path
                .parent()
                .ok_or_else(|| {
                    anyhow!(
                        "unexpected manifest path for local package: {:?}",
                        manifest_path
                    )
                })?
                .to_owned();

            MyPackageLocation::Omicron { path }
        };

        Ok(MyPackage { name: pkg.name.clone(), location })
    }
}

enum MyPackageLocation {
    Omicron { path: Utf8PathBuf },
    RemoteRepo { oxide_github_repo: String, path: Utf8PathBuf },
}

impl Display for MyPackageLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyPackageLocation::Omicron { path } => {
                write!(f, "omicron:{}", path)
            }
            MyPackageLocation::RemoteRepo { oxide_github_repo, path } => {
                write!(f, "{}:{}", oxide_github_repo, path)
            }
        }
    }
}

fn source_repo_name(raw: &str) -> Result<String> {
    let repo_url =
        Url::parse(raw).with_context(|| format!("parsing {:?}", raw))?;
    ensure!(repo_url.scheme() == "git+https", "unsupported URL scheme",);
    ensure!(
        matches!(repo_url.host_str(), Some(h) if h == "github.com"),
        "unexpected URL host (expected \"github.com\")",
    );
    let path_segments: Vec<_> = repo_url
        .path_segments()
        .ok_or_else(|| anyhow!("expected URL to contain path segments"))?
        .collect();
    ensure!(
        path_segments.len() == 2,
        "expected exactly two path segments in URL",
    );
    ensure!(
        path_segments[0] == "oxidecomputer",
        "expected repo under Oxide's GitHub organization",
    );

    Ok(path_segments[1].to_string())
}

fn direct_dependents(
    workspace: &Metadata,
    pkg_name: &str,
) -> Result<Vec<MyPackage>> {
    workspace
        .packages
        .iter()
        .filter_map(|pkg| {
            if pkg.dependencies.iter().any(|dep| {
                matches!(
                    dep.kind,
                    DependencyKind::Normal | DependencyKind::Build
                ) && dep.name == pkg_name
            }) {
                Some(
                    MyPackage::new(workspace, pkg)
                        .with_context(|| format!("package {:?}", pkg.name)),
                )
            } else {
                None
            }
        })
        .collect()
}

// fn parse_packages(pkg_file: &Utf8Path) -> Result<OmicronPackageConfig> {
//     let contents = std::fs::read_to_string(pkg_file)
//         .with_context(|| format!("read package file {:?}", pkg_file))?;
//     let raw_packages =
//         toml::from_str::<omicron_zone_package::config::Config>(&contents)
//             .with_context(|| format!("parse package file {:?}", pkg_file))?;
//     Ok(OmicronPackageConfig::from(raw_packages))
// }
//
// struct OmicronPackageConfig {
//     deployable_zones: Vec<OmicronPackage>,
//     dont_care: Vec<(OmicronPackage, &'static str)>,
//     dont_know: Vec<OmicronPackage>,
// }
//
// struct OmicronPackage {
//     name: String,
//     package: omicron_zone_package::package::Package,
// }
//
// impl From<(String, omicron_zone_package::package::Package)> for OmicronPackage {
//     fn from(
//         (pkgname, package): (String, omicron_zone_package::package::Package),
//     ) -> Self {
//         OmicronPackage { name: pkgname, package }
//     }
// }
//
// impl From<omicron_zone_package::config::Config> for OmicronPackageConfig {
//     fn from(raw: omicron_zone_package::config::Config) -> Self {
//         let mut deployable_zones = Vec::new();
//         let mut dont_care = Vec::new();
//         let mut dont_know = Vec::new();
//         for (pkgname, package) in raw.packages {
//             let ompkg = OmicronPackage::from((pkgname, package));
//
//             match &ompkg.package.output {
//                 omicron_zone_package::package::PackageOutput::Zone {
//                     intermediate_only: true,
//                 } => {
//                     dont_care.push((ompkg, "marked intermediate-only"));
//                 }
//                 omicron_zone_package::package::PackageOutput::Zone {
//                     intermediate_only: false,
//                 } => {
//                     deployable_zones.push(ompkg);
//                 }
//                 omicron_zone_package::package::PackageOutput::Tarball => {
//                     dont_know.push(ompkg);
//                 }
//             }
//         }
//
//         OmicronPackageConfig { deployable_zones, dont_care, dont_know }
//     }
// }
//
// impl OmicronPackageConfig {
//     pub fn dump(&self) {
//         println!("deployable zones");
//         for ompkg in &self.deployable_zones {
//             println!("    {}", ompkg.name);
//         }
//         println!("");
//
//         println!("stuff I think we can ignore");
//         for (ompkg, reason) in &self.dont_care {
//             println!("    {}: {}", ompkg.name, reason);
//         }
//         println!("");
//
//         println!("stuff I'm not sure about yet");
//         for ompkg in &self.dont_know {
//             println!("    {}", ompkg.name);
//         }
//         println!("");
//     }
//     //    pub fn dump(&self) {
//     //        for (pkgname, package) in &self.raw.packages {
//     //            print!("found Omicron package {:?}: ", pkgname);
//     //            match &package.source {
//     //                omicron_zone_package::package::PackageSource::Local {
//     //                    blobs,
//     //                    buildomat_blobs,
//     //                    rust,
//     //                    paths,
//     //                } => {
//     //                    if rust.is_some() {
//     //                        println!("rust package");
//     //                    } else {
//     //                        println!("");
//     //                    }
//     //
//     //                    if let Some(blobs) = blobs {
//     //                        println!("    blobs: ({})", blobs.len());
//     //                        for b in blobs {
//     //                            println!("        {}", b);
//     //                        }
//     //                    }
//     //
//     //                    if let Some(buildomat_blobs) = blobs {
//     //                        println!(
//     //                            "    buildomat blobs: ({})",
//     //                            buildomat_blobs.len()
//     //                        );
//     //                        for b in buildomat_blobs {
//     //                            println!("        {}", b);
//     //                        }
//     //                    }
//     //
//     //                    if !paths.is_empty() {
//     //                        println!("    plus some mapped paths: {}", paths.len());
//     //                    }
//     //                }
//     //                omicron_zone_package::package::PackageSource::Prebuilt {
//     //                    repo,
//     //                    commit,
//     //                    sha256,
//     //                } => {
//     //                    println!("prebuilt from repo: {repo}");
//     //                }
//     //                omicron_zone_package::package::PackageSource::Composite {
//     //                    packages,
//     //                } => {
//     //                    println!(
//     //                        "composite of: {}",
//     //                        packages
//     //                            .iter()
//     //                            .map(|p| format!("{:?}", p))
//     //                            .collect::<Vec<_>>()
//     //                            .join(", ")
//     //                    );
//     //                }
//     //                omicron_zone_package::package::PackageSource::Manual => {
//     //                    println!("ERROR: unsupported manual package");
//     //                }
//     //            }
//     //        }
//     //    }
// }
//
fn print_package(p: &ClientPackage, args: &LsClientsArgs) {
    if !args.adoc {
        println!("  package: {} from {}", p.me.name, p.me.location);
        for d in &p.rdeps {
            println!("    consumer: {} from {}", d.name, d.location);
        }
    } else {
        println!("|?");
        println!("|?");
        println!("|{}", p.me.location);
        print!(
            "|{}",
            p.rdeps
                .iter()
                .map(|d| d.location.to_string())
                .collect::<Vec<_>>()
                .join(",\n ")
        );
        println!("\n");
    }
}

#[derive(Deserialize)]
struct AllApiMetadata {
    extra_repos: Vec<String>,
    apis: Vec<ApiMetadata>,
}

#[derive(Deserialize)]
struct ApiMetadata {
    /// primary key for APIs is the client package name
    client_package_name: String,
    /// human-readable label for the API
    label: String,
    /// package name that the corresponding API lives in
    // XXX-dap unused right now
    server_package_name: String,
    /// package name that the corresponding server lives in
    server_component: String,
    /// name of the unit of deployment
    group: Option<String>,
}

impl ApiMetadata {
    fn group(&self) -> &str {
        self.group.as_ref().unwrap_or_else(|| &self.server_component)
    }
}

struct Workspace {
    name: String,
    metadata: cargo_metadata::Metadata,
    all_packages: BTreeMap<String, Package>, // XXX-dap memory usage
    progenitor_clients: BTreeMap<String, ClientPackage>,
}

impl Workspace {
    pub fn load(name: &str, extra_repos: Option<&Utf8Path>) -> Result<Self> {
        eprintln!("loading metadata for workspace {name}");

        let mut cmd = cargo_metadata::MetadataCommand::new();
        if let Some(extra_repos) = extra_repos {
            cmd.manifest_path(extra_repos.join(name).join("Cargo.toml"));
        }
        let metadata = cmd.exec().context("loading metadata")?;

        // Find all packages with a direct non-dev, non-build dependency on
        // "progenitor".  These generally ought to be suffixed with "-client".
        let progenitor_clients = direct_dependents(&metadata, "progenitor")?
            .into_iter()
            .filter_map(|mypkg| {
                if mypkg.name.ends_with("-client") {
                    Some(ClientPackage::new(&metadata, mypkg))
                } else {
                    eprintln!("ignoring apparent non-client: {}", mypkg.name);
                    None
                }
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|cpkg| (cpkg.me.name.to_owned(), cpkg))
            .collect();

        let all_packages = metadata
            .packages
            .iter()
            .map(|p| (p.name.clone(), p.clone()))
            .collect();

        Ok(Workspace {
            name: name.to_owned(),
            metadata,
            all_packages,
            progenitor_clients,
        })
    }

    pub fn find_package(&self, pkgname: &str) -> Option<&Package> {
        self.all_packages.get(pkgname)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

// XXX-dap Okay, so now I have all the information I think I could want in
// memory.  Which is: I have the metadata, plus the set of all packages
// referenced in all relevant workspaces.  Now what do I do with it?
//
// In the end, my goal is to:
//
// - map "server" packages to top-level deployable components
//   I can do this manually or I can presumably do it with the package manifest.
// - make a DAG
//   - nodes: deployable components (could start with server packages)
//   - edges: component depends on a client of another API
//
// To do this, I need to do ONE of the following:
//
// - for each server package, walk its dependencies recursively and note each of
//   known client packages that it uses
// - for each client package, walk its reverse dependencies recursively and note
//   each of the known server packages
//
// but this is tricky because:
//
// - client packages could have reverse dependencies in any repo, and I will
//   likely find a lot of dups
// - server packages have dependencies in other repos, too
//   - If we start from the server, the entire chain should be represented in
//     the server's workspace's metadata
//   - by contrast, if we start from the client, it may have reverse-deps in
//     other workspaces and we have to try all of them
fn make_graph(
    apis_by_client_package: &BTreeMap<String, ApiMetadata>,
    workspaces: Vec<&Workspace>,
) -> Result<String> {
    let mut graph = petgraph::graph::Graph::new();

    // Some server components have more than one API in them.  Deduplicate the
    // server component names.  We'll iterate these to compute relationships.
    let server_component_groups: BTreeMap<_, _> = apis_by_client_package
        .values()
        .map(|api| (&api.server_component, api.group()))
        .collect();

    // Identify the distinct units of deployment.  Create a graph node for each
    // one.
    let deployment_unit_names: BTreeSet<_> =
        apis_by_client_package.values().map(|api| api.group()).collect();
    let nodes: BTreeMap<_, _> = deployment_unit_names
        .into_iter()
        .map(|name| (name, graph.add_node(name)))
        .collect();

    for (server_pkgname, group) in &server_component_groups {
        // Figure out which workspace has this package.
        // XXX-dap make some data structures maybe?
        let found_in_workspaces: Vec<_> = workspaces
            .iter()
            .filter_map(|w| w.find_package(&server_pkgname).map(|p| (w, p)))
            .collect();
        if found_in_workspaces.is_empty() {
            eprintln!(
                "error: server package {:?} was not found in any workspace",
                server_pkgname
            );
            // XXX-dap exit non-zero
            continue;
        }

        if found_in_workspaces.len() > 1 {
            eprintln!(
                "error: server package {:?} was found in more than one \
                 workspace: {}",
                server_pkgname,
                found_in_workspaces
                    .into_iter()
                    .map(|(w, _)| w.name())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            // XXX-dap exit non-zero
            continue;
        }

        let (found_workspace, found_server_package) = found_in_workspaces[0];
        eprintln!(
            "server package {} found in repo {}",
            server_pkgname,
            found_workspace.name(),
        );

        // Walk the server package's dependencies recursively, keeping track of
        // known clients used.
        let mut clients_used = BTreeSet::new();
        walk_required_deps_recursively(
            &found_workspace,
            &found_server_package,
            &mut |parent: &Package, p: &Package| {
                // TODO
                // omicron_common depends on mg-admin-client solely to impl
                // some `From` conversions.  That makes it look like just about
                // everything depends on mg-admin-client, which isn't true.  We
                // should consider reversing this, since most clients put those
                // conversions into the client rather than omicron_common.  But
                // for now, let's just ignore this particular dependency.
                if p.name == "mg-admin-client"
                    && parent.name == "omicron-common"
                {
                    return;
                }

                // TODO internal-dns depends on dns-service-client to use its
                // types.  They're only used when *configuring* DNS, which is
                // only done in a couple of components.  But many components use
                // internal-dns to *read* DNS.  So like above, this makes it
                // look like everything uses the DNS server API, but that's not
                // true.  We should consider splitting this crate in two.  But
                // for now, just ignore the specific dependency from
                // internal-dns to dns-service-client.  If a consumer actually
                // calls the DNS server, it will have a separate dependency.
                if p.name == "dns-service-client"
                    && (parent.name == "internal-dns")
                {
                    return;
                }

                // TODO nexus-types depends on dns-service-client and
                // gateway-client for defining some types, but again, this
                // doesn't mean that somebody using nexus-types is actually
                // calling out to these services.  If they were, they'd need to
                // have some other dependency on them.
                if parent.name == "nexus-types"
                    && (p.name == "dns-service-client"
                        || p.name == "gateway-client")
                {
                    return;
                }

                // TODO
                // This one's debatable.  Everything with an Oximeter producer
                // talks to Nexus.  But let's ignore those for now.
                // Maybe we could improve this by creating a separate API inside
                // Nexus for this?
                if parent.name == "oximeter-producer"
                    && p.name == "nexus-client"
                {
                    eprintln!(
                        "warning: ignoring legit dependency from \
                         oximeter-producer -> nexus_client"
                    );
                    return;
                }

                if apis_by_client_package.contains_key(&p.name) {
                    clients_used.insert(p.name.clone());
                }
            },
        )
        .with_context(|| {
            format!(
                "iterating dependencies of workspace {:?} package {:?}",
                found_workspace.name(),
                server_pkgname
            )
        })?;

        // unwrap(): we created a node for every group above.
        let my_node = nodes.get(group).unwrap();

        println!("server package {}:", server_pkgname);
        for c in &clients_used {
            println!("    uses client {}", c);

            // unwrap(): The values in "clients_used" were populated above after
            // checking whether they were in `apis_by_client_package` already.
            let other_api = apis_by_client_package.get(c).unwrap();
            // unwrap(): We created nodes for all of the groups up front.
            let other_node = nodes.get(other_api.group()).unwrap();

            graph.add_edge(
                *my_node,
                *other_node,
                &other_api.client_package_name,
            );
        }
        println!("");
    }

    Ok(Dot::new(&graph).to_string())
}

fn walk_required_deps_recursively(
    workspace: &Workspace,
    root: &Package,
    func: &mut dyn FnMut(&Package, &Package),
) -> Result<()> {
    let mut remaining = vec![root];
    let mut seen: BTreeSet<String> = BTreeSet::new();

    while let Some(next) = remaining.pop() {
        for d in &next.dependencies {
            if seen.contains(&d.name) {
                continue;
            }

            seen.insert(d.name.clone());

            if d.optional {
                continue;
            }

            if !matches!(d.kind, DependencyKind::Normal | DependencyKind::Build)
            {
                continue;
            }

            let pkg = workspace.find_package(&d.name).ok_or_else(|| {
                anyhow!(
                    "package {:?} has dependency {:?} not in workspace \
                     metadata",
                    next.name,
                    d.name
                )
            })?;
            func(next, pkg);
            remaining.push(pkg);
        }
    }

    Ok(())
}

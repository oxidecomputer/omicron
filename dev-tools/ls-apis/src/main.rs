// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Show information about Progenitor-based APIs

// XXX-dap some ideas:
// - another cleanup pass
//   - move stuff into separate files
//   - see XXX-dap
// - summarize metadata (e.g., write a table of APIs)
// - asciidoc output

use anyhow::{anyhow, ensure, Context, Result};
use camino::Utf8Path;
use camino::Utf8PathBuf;
use cargo_metadata::DependencyKind;
use cargo_metadata::Metadata;
use cargo_metadata::Package;
use clap::{Args, Parser, Subcommand};
use petgraph::dot::Dot;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Display;
use url::Url;

#[macro_use]
extern crate newtype_derive;

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

    /// path to metadata about Omicron packages
    #[arg(long)]
    package_manifest: Option<Utf8PathBuf>,

    /// path to directory with clones of dependent repositories
    #[arg(long)]
    extra_repos: Option<Utf8PathBuf>,

    #[command(subcommand)]
    cmd: Cmds,
}

#[derive(Subcommand)]
enum Cmds {
    Show(ShowArgs),
}

#[derive(Args)]
pub struct ShowArgs {
    #[arg(long)]
    adoc: bool,
}

fn main() -> Result<()> {
    let cli_args = LsApis::parse();
    let load_args = LoadArgs::try_from(&cli_args)?;
    let apis = Apis::load(load_args)?;

    match cli_args.cmd {
        Cmds::Show(args) => run_show(&apis, args),
    }
}

fn run_show(apis: &Apis, args: ShowArgs) -> Result<()> {
    println!("{}", apis.dot_by_unit());
    Ok(())
}

struct LoadArgs {
    api_manifest_path: Utf8PathBuf,
    package_manifest_path: Utf8PathBuf,
    extra_repos_path: Utf8PathBuf,
}

impl TryFrom<&LsApis> for LoadArgs {
    type Error = anyhow::Error;

    fn try_from(args: &LsApis) -> Result<Self> {
        let self_manifest_dir_str = std::env::var("CARGO_MANIFEST_DIR")
            .context("expected CARGO_MANIFEST_DIR in environment")?;
        let self_manifest_dir = Utf8PathBuf::from(self_manifest_dir_str);

        let api_manifest_path =
            args.api_manifest.clone().unwrap_or_else(|| {
                // XXX TODO-cleanup can these be done in one join call?
                self_manifest_dir
                    .join("..")
                    .join("..")
                    .join("api-manifest.toml")
            });
        let package_manifest_path =
            args.package_manifest.clone().unwrap_or_else(|| {
                self_manifest_dir
                    .join("..")
                    .join("..")
                    .join("package-manifest.toml")
            });
        let extra_repos_path = args.extra_repos.clone().unwrap_or_else(|| {
            self_manifest_dir
                .join("..")
                .join("..")
                .join("out")
                .join("ls-apis")
                .join("checkout")
        });

        Ok(LoadArgs {
            api_manifest_path,
            package_manifest_path,
            extra_repos_path,
        })
    }
}

#[derive(Clone, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct ClientPackageName(String);
NewtypeDebug! { () pub struct ClientPackageName(String); }
NewtypeDeref! { () pub struct ClientPackageName(String); }
NewtypeDerefMut! { () pub struct ClientPackageName(String); }
NewtypeDisplay! { () pub struct ClientPackageName(String); }
NewtypeFrom! { () pub struct ClientPackageName(String); }

#[derive(Clone, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct DeploymentUnit(String);
NewtypeDebug! { () pub struct DeploymentUnit(String); }
NewtypeDeref! { () pub struct DeploymentUnit(String); }
NewtypeDerefMut! { () pub struct DeploymentUnit(String); }
NewtypeDisplay! { () pub struct DeploymentUnit(String); }
NewtypeFrom! { () pub struct DeploymentUnit(String); }

#[derive(Clone, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct ServerPackageName(String);
NewtypeDebug! { () pub struct ServerPackageName(String); }
NewtypeDeref! { () pub struct ServerPackageName(String); }
NewtypeDerefMut! { () pub struct ServerPackageName(String); }
NewtypeDisplay! { () pub struct ServerPackageName(String); }
NewtypeFrom! { () pub struct ServerPackageName(String); }

#[derive(Clone, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct ServerComponent(String);
NewtypeDebug! { () pub struct ServerComponent(String); }
NewtypeDeref! { () pub struct ServerComponent(String); }
NewtypeDerefMut! { () pub struct ServerComponent(String); }
NewtypeDisplay! { () pub struct ServerComponent(String); }
NewtypeFrom! { () pub struct ServerComponent(String); }

struct Apis {
    server_component_units: BTreeMap<ServerComponent, DeploymentUnit>,
    unit_server_components: BTreeMap<DeploymentUnit, Vec<ServerComponent>>,
    deployment_units: BTreeSet<DeploymentUnit>,
    apis_consumed: BTreeMap<ServerComponent, BTreeSet<ClientPackageName>>,
    api_metadata: AllApiMetadata,
}

impl Apis {
    pub fn load(args: LoadArgs) -> Result<Apis> {
        let helper = ApisHelper::load(args)?;
        if !helper.warnings.is_empty() {
            for e in &helper.warnings {
                eprintln!("warning: {:#}", e);
            }
        }

        // Collect the distinct set of deployment units for all the APIs.
        let deployment_units: BTreeSet<_> =
            helper.api_metadata.apis().map(|a| a.group().clone()).collect();

        // Create a mapping from server package to its deployment unit.
        let server_component_units: BTreeMap<_, _> = helper
            .api_metadata
            .apis()
            .map(|a| (a.server_component.clone(), a.group().clone()))
            .collect();

        // Compute a reverse mapping from deployment unit to the server
        // packages deployed inside it.
        let mut unit2components = BTreeMap::new();
        for (server_component, deployment_unit) in &server_component_units {
            let list = unit2components
                .entry(deployment_unit.clone())
                .or_insert_with(Vec::new);
            list.push(server_component.clone());
        }

        // For each server component, determine which client APIs it depends on
        // by walking its dependencies.
        // XXX-dap figure out how to abstract this
        let mut apis_consumed = BTreeMap::new();
        for server_pkgname in helper.api_metadata.server_components() {
            let (workspace, pkg) =
                helper.find_package_workspace(server_pkgname)?;
            let mut clients_used = BTreeSet::new();
            workspace
                .walk_required_deps_recursively(
                    pkg,
                    &mut |parent: &Package, p: &Package| {
                        // TODO
                        // omicron_common depends on mg-admin-client solely to
                        // impl some `From` conversions.  That makes it look
                        // like just about everything depends on
                        // mg-admin-client, which isn't true.  We should
                        // consider reversing this, since most clients put those
                        // conversions into the client rather than
                        // omicron_common.  But for now, let's just ignore this
                        // particular dependency.
                        if p.name == "mg-admin-client"
                            && parent.name == "omicron-common"
                        {
                            return;
                        }

                        // TODO internal-dns depends on dns-service-client to use
                        // its types.  They're only used when *configuring* DNS,
                        // which is only done in a couple of components.  But
                        // many components use internal-dns to *read* DNS.  So
                        // like above, this makes it look like everything uses
                        // the DNS server API, but that's not true.  We should
                        // consider splitting this crate in two.  But for now,
                        // just ignore the specific dependency from internal-dns
                        // to dns-service-client.  If a consumer actually calls
                        // the DNS server, it will have a separate dependency.
                        if p.name == "dns-service-client"
                            && (parent.name == "internal-dns")
                        {
                            return;
                        }

                        // TODO nexus-types depends on dns-service-client and
                        // gateway-client for defining some types, but again,
                        // this doesn't mean that somebody using nexus-types is
                        // actually calling out to these services.  If they
                        // were, they'd need to have some other dependency on
                        // them.
                        if parent.name == "nexus-types"
                            && (p.name == "dns-service-client"
                                || p.name == "gateway-client")
                        {
                            return;
                        }

                        // TODO
                        // This one's debatable.  Everything with an Oximeter
                        // producer talks to Nexus.  But let's ignore those for
                        // now.  Maybe we could improve this by creating a
                        // separate API inside Nexus for this?
                        if parent.name == "oximeter-producer"
                            && p.name == "nexus-client"
                        {
                            eprintln!(
                                "warning: ignoring legit dependency from \
                         oximeter-producer -> nexus_client"
                            );
                            return;
                        }

                        if helper
                            .api_metadata
                            .client_pkgname_lookup(&p.name)
                            .is_some()
                        {
                            clients_used.insert(p.name.clone().into());
                        }
                    },
                )
                .with_context(|| {
                    format!(
                        "iterating dependencies of workspace {:?} package {:?}",
                        workspace.name(),
                        server_pkgname
                    )
                })?;

            apis_consumed.insert(server_pkgname.clone(), clients_used);
        }

        Ok(Apis {
            server_component_units,
            deployment_units,
            unit_server_components: unit2components,
            apis_consumed,
            api_metadata: helper.api_metadata,
        })
    }

    fn all_deployment_unit_components(
        &self,
    ) -> impl Iterator<Item = (&DeploymentUnit, &Vec<ServerComponent>)> {
        self.unit_server_components.iter()
    }

    fn component_apis_consumed(
        &self,
        server_component: &ServerComponent,
    ) -> Box<dyn Iterator<Item = &ClientPackageName> + '_> {
        match self.apis_consumed.get(server_component) {
            Some(l) => Box::new(l.iter()),
            None => Box::new(std::iter::empty()),
        }
    }

    pub fn dot_by_unit(&self) -> String {
        let mut graph = petgraph::graph::Graph::new();
        let nodes: BTreeMap<_, _> = self
            .deployment_units
            .iter()
            .map(|name| (name, graph.add_node(name)))
            .collect();

        // Now walk through the deployment units, walk through each one's server
        // packages, walk through each one of the clients used by those, and
        // create a corresponding edge.
        for (deployment_unit, server_components) in
            self.all_deployment_unit_components()
        {
            let my_node = nodes.get(deployment_unit).unwrap();
            for server_pkg in server_components {
                for client_pkg in self.component_apis_consumed(server_pkg) {
                    let api = self
                        .api_metadata
                        .client_pkgname_lookup(client_pkg)
                        .unwrap();
                    let other_component = &api.server_component;
                    let other_unit = self
                        .server_component_units
                        .get(other_component)
                        .unwrap();
                    let other_node = nodes.get(other_unit).unwrap();
                    graph.add_edge(*my_node, *other_node, client_pkg.clone());
                }
            }
        }

        Dot::new(&graph).to_string()
    }
}

struct ApisHelper {
    api_metadata: AllApiMetadata,
    workspaces: BTreeMap<String, Workspace>,
    warnings: Vec<anyhow::Error>,
}

impl ApisHelper {
    pub fn load(args: LoadArgs) -> Result<ApisHelper> {
        // Load the API manifest.
        let api_metadata: AllApiMetadata =
            parse_toml_file(&args.api_manifest_path)?;

        // Load information about each of the workspaces.
        let mut workspaces = BTreeMap::new();
        workspaces.insert(
            String::from("omicron"),
            Workspace::load("omicron", None)
                .context("loading Omicron workspace metadata")?,
        );

        for repo in ["crucible", "maghemite", "propolis", "dendrite"] {
            workspaces.insert(
                String::from(repo),
                Workspace::load(repo, Some(&args.extra_repos_path))
                    .with_context(|| {
                        format!("load metadata for workspace {:?}", repo)
                    })?,
            );
        }

        // Validate the metadata against what we found in the workspaces.
        let mut client_pkgnames_unused: BTreeSet<_> =
            api_metadata.client_pkgnames().collect();
        let mut errors = Vec::new();
        for (_, workspace) in &workspaces {
            for (client_pkgname, _) in &workspace.progenitor_clients {
                if api_metadata.client_pkgname_lookup(client_pkgname).is_some()
                {
                    // It's possible that we will find multiple references
                    // to the same client package name.  That's okay.
                    client_pkgnames_unused.remove(client_pkgname);
                } else {
                    errors.push(anyhow!(
                        "found client package missing from API manifest: {}",
                        client_pkgname
                    ));
                }
            }
        }

        for c in client_pkgnames_unused {
            errors.push(anyhow!(
                "API manifest refers to unknown client package: {}",
                c
            ));
        }

        Ok(ApisHelper { api_metadata, workspaces, warnings: errors })
    }

    pub fn find_package_workspace(
        &self,
        server_pkgname: &str,
    ) -> Result<(&Workspace, &Package)> {
        // Figure out which workspace has this package.
        let found_in_workspaces: Vec<_> = self
            .workspaces
            .values()
            .filter_map(|w| w.find_package(&server_pkgname).map(|p| (w, p)))
            .collect();
        ensure!(
            !found_in_workspaces.is_empty(),
            "server package {:?} was not found in any workspace",
            server_pkgname
        );
        ensure!(
            found_in_workspaces.len() == 1,
            "server package {:?} was found in more than one workspace: {}",
            server_pkgname,
            found_in_workspaces
                .into_iter()
                .map(|(w, _)| w.name())
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok(found_in_workspaces[0])
    }
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AllApiMetadata {
    apis: Vec<ApiMetadata>,
}

impl AllApiMetadata {
    fn apis(&self) -> impl Iterator<Item = &ApiMetadata> {
        self.apis.iter()
    }

    fn client_pkgnames(&self) -> impl Iterator<Item = &ClientPackageName> {
        self.apis().map(|api| &api.client_package_name)
    }

    fn server_components(&self) -> impl Iterator<Item = &ServerComponent> {
        self.apis().map(|api| &api.server_component)
    }

    fn client_pkgname_lookup(&self, pkgname: &str) -> Option<&ApiMetadata> {
        // XXX-dap this is worth optimizing but it would require a separate type
        // -- this one would be the "raw" type.
        self.apis.iter().find(|api| *api.client_package_name == pkgname)
    }
}

#[derive(Deserialize)]
struct ApiMetadata {
    /// primary key for APIs is the client package name
    client_package_name: ClientPackageName,
    /// human-readable label for the API
    label: String,
    /// package name that the corresponding API lives in
    // XXX-dap unused right now
    server_package_name: ServerPackageName,
    /// package name that the corresponding server lives in
    server_component: ServerComponent,
    /// name of the unit of deployment
    group: Option<DeploymentUnit>,
}

impl ApiMetadata {
    fn group(&self) -> DeploymentUnit {
        self.group
            .clone()
            .unwrap_or_else(|| (*self.server_component).clone().into())
    }
}

struct Workspace {
    name: String,
    all_packages: BTreeMap<String, Package>, // XXX-dap memory usage
    progenitor_clients: BTreeMap<ClientPackageName, ClientPackage>,
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
            .map(|cpkg| (cpkg.me.name.clone().into(), cpkg))
            .collect();

        let all_packages = metadata
            .packages
            .iter()
            .map(|p| (p.name.clone(), p.clone()))
            .collect();

        Ok(Workspace {
            name: name.to_owned(),
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

    fn walk_required_deps_recursively(
        &self,
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

                if !matches!(
                    d.kind,
                    DependencyKind::Normal | DependencyKind::Build
                ) {
                    continue;
                }

                let pkg = self.find_package(&d.name).ok_or_else(|| {
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
}

fn parse_toml_file<T: DeserializeOwned>(path: &Utf8Path) -> Result<T> {
    let s = std::fs::read_to_string(path)
        .with_context(|| format!("read {:?}", path))?;
    toml::from_str(&s).with_context(|| format!("parse {:?}", path))
}

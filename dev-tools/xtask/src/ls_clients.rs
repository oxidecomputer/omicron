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
use std::fmt::Display;
use url::Url;

#[derive(Args)]
pub struct LsClientsArgs {
    #[arg(long)]
    package_manifest: Option<Utf8PathBuf>,

    #[arg(long)]
    adoc: bool,
}

pub fn run_cmd(args: LsClientsArgs) -> Result<()> {
    // Load information about the Cargo workspace.
    let workspace = crate::load_workspace()?;

    // Find all packages with a direct non-dev, non-build dependency on
    // "progenitor".  These generally ought to be suffixed with "-client".
    let progenitor_clients = direct_dependents(&workspace, "progenitor")?
        .into_iter()
        .filter_map(|mypkg| {
            if mypkg.name.ends_with("-client") {
                Some(ClientPackage::new(&workspace, mypkg))
            } else {
                eprintln!("ignoring apparent non-client: {}", mypkg.name);
                None
            }
        })
        .collect::<Result<Vec<_>>>()?;

    // Parse the package manifest.
    let pkg_file = args
        .package_manifest
        .as_ref()
        .map(|c| Ok::<_, anyhow::Error>(c.clone()))
        .unwrap_or_else(|| {
            Ok(Utf8PathBuf::from(
                std::env::var("CARGO_MANIFEST_DIR")
                    .context("looking up CARGO_MANIFEST_DIR in environment")?,
            )
            .join("..")
            .join("..")
            .join("package-manifest.toml"))
        })?;
    let pkgs = parse_packages(&pkg_file)?;
    pkgs.dump();

    for c in &progenitor_clients {
        print_package(c, &args);
    }

    Ok(())
}

struct ClientPackage<'a> {
    me: MyPackage<'a>,
    rdeps: Vec<MyPackage<'a>>,
}

impl<'a> ClientPackage<'a> {
    fn new(
        workspace: &'a Metadata,
        me: MyPackage<'a>,
    ) -> Result<ClientPackage<'a>> {
        let rdeps = direct_dependents(workspace, &me.name)?;
        Ok(ClientPackage { me, rdeps })
    }
}

struct MyPackage<'a> {
    name: &'a str,
    location: MyPackageLocation<'a>,
}

impl<'a> MyPackage<'a> {
    fn new(workspace: &'a Metadata, pkg: &'a Package) -> Result<MyPackage<'a>> {
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
            let path = relative_path.parent().ok_or_else(|| {
                anyhow!(
                    "unexpected manifest path for local package: {:?}",
                    manifest_path
                )
            })?;

            MyPackageLocation::Omicron { path }
        };

        Ok(MyPackage { name: &pkg.name, location })
    }
}

enum MyPackageLocation<'a> {
    Omicron { path: &'a Utf8Path },
    RemoteRepo { oxide_github_repo: String, path: Utf8PathBuf },
}

impl<'a> Display for MyPackageLocation<'a> {
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

fn direct_dependents<'a, 'b>(
    workspace: &'a Metadata,
    pkg_name: &'b str,
) -> Result<Vec<MyPackage<'a>>> {
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

fn parse_packages(pkg_file: &Utf8Path) -> Result<OmicronPackageConfig> {
    let contents = std::fs::read_to_string(pkg_file)
        .with_context(|| format!("read package file {:?}", pkg_file))?;
    let raw_packages =
        toml::from_str::<omicron_zone_package::config::Config>(&contents)
            .with_context(|| format!("parse package file {:?}", pkg_file))?;
    Ok(OmicronPackageConfig::from(raw_packages))
}

struct OmicronPackageConfig {
    deployable_zones: Vec<OmicronPackage>,
    dont_care: Vec<(OmicronPackage, &'static str)>,
    dont_know: Vec<OmicronPackage>,
}

struct OmicronPackage {
    name: String,
    package: omicron_zone_package::package::Package,
}

impl From<(String, omicron_zone_package::package::Package)> for OmicronPackage {
    fn from(
        (pkgname, package): (String, omicron_zone_package::package::Package),
    ) -> Self {
        OmicronPackage { name: pkgname, package }
    }
}

impl From<omicron_zone_package::config::Config> for OmicronPackageConfig {
    fn from(raw: omicron_zone_package::config::Config) -> Self {
        let mut deployable_zones = Vec::new();
        let mut dont_care = Vec::new();
        let mut dont_know = Vec::new();
        for (pkgname, package) in raw.packages {
            let ompkg = OmicronPackage::from((pkgname, package));

            match &ompkg.package.output {
                omicron_zone_package::package::PackageOutput::Zone {
                    intermediate_only: true,
                } => {
                    dont_care.push((ompkg, "marked intermediate-only"));
                }
                omicron_zone_package::package::PackageOutput::Zone {
                    intermediate_only: false,
                } => {
                    deployable_zones.push(ompkg);
                }
                omicron_zone_package::package::PackageOutput::Tarball => {
                    dont_know.push(ompkg);
                }
            }
        }

        OmicronPackageConfig {
            deployable_zones,
            dont_care,
            dont_know,
        }
    }
}

impl OmicronPackageConfig {
    pub fn dump(&self) {
        println!("deployable zones");
        for ompkg in &self.deployable_zones {
            println!("    {}", ompkg.name);
        }
        println!("");

        println!("stuff I think we can ignore");
        for (ompkg, reason) in &self.dont_care {
            println!("    {}: {}", ompkg.name, reason);
        }
        println!("");

        println!("stuff I'm not sure about yet");
        for ompkg in &self.dont_know {
            println!("    {}", ompkg.name);
        }
        println!("");
    }
    //    pub fn dump(&self) {
    //        for (pkgname, package) in &self.raw.packages {
    //            print!("found Omicron package {:?}: ", pkgname);
    //            match &package.source {
    //                omicron_zone_package::package::PackageSource::Local {
    //                    blobs,
    //                    buildomat_blobs,
    //                    rust,
    //                    paths,
    //                } => {
    //                    if rust.is_some() {
    //                        println!("rust package");
    //                    } else {
    //                        println!("");
    //                    }
    //
    //                    if let Some(blobs) = blobs {
    //                        println!("    blobs: ({})", blobs.len());
    //                        for b in blobs {
    //                            println!("        {}", b);
    //                        }
    //                    }
    //
    //                    if let Some(buildomat_blobs) = blobs {
    //                        println!(
    //                            "    buildomat blobs: ({})",
    //                            buildomat_blobs.len()
    //                        );
    //                        for b in buildomat_blobs {
    //                            println!("        {}", b);
    //                        }
    //                    }
    //
    //                    if !paths.is_empty() {
    //                        println!("    plus some mapped paths: {}", paths.len());
    //                    }
    //                }
    //                omicron_zone_package::package::PackageSource::Prebuilt {
    //                    repo,
    //                    commit,
    //                    sha256,
    //                } => {
    //                    println!("prebuilt from repo: {repo}");
    //                }
    //                omicron_zone_package::package::PackageSource::Composite {
    //                    packages,
    //                } => {
    //                    println!(
    //                        "composite of: {}",
    //                        packages
    //                            .iter()
    //                            .map(|p| format!("{:?}", p))
    //                            .collect::<Vec<_>>()
    //                            .join(", ")
    //                    );
    //                }
    //                omicron_zone_package::package::PackageSource::Manual => {
    //                    println!("ERROR: unsupported manual package");
    //                }
    //            }
    //        }
    //    }
}

fn print_package(p: &ClientPackage<'_>, args: &LsClientsArgs) {
    if !args.adoc {
        println!("package: {} from {}", p.me.name, p.me.location);
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

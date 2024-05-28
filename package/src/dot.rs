//! Visualize the package manifest using Graphviz

use anyhow::anyhow;
use omicron_zone_package::config::Config;
use omicron_zone_package::package::PackageOutput;
use omicron_zone_package::package::PackageSource;
use omicron_zone_package::target::Target;
use petgraph::dot::Dot;
use petgraph::graph::EdgeReference;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use std::collections::BTreeMap;

/// A node in our visual representation of the package manifest
///
/// Note that this graph is not the same as the package graph.  It has extra
/// nodes to illustrate things like S3 blobs, Buildomat outputs, and Rust
/// packages that are part of the build (but are not themselves packages).
enum GraphNode {
    /// singleton node at the root of the graph
    TopLevel,

    /// a package from the package manifest
    ///
    /// All other node types are added for illustration only and are derived
    /// from a package's configuration.
    Package { name: String, source_type: &'static str },

    /// an S3 blob that's copied into a package
    Blob { path: String },
    /// local files that are copied into a package
    Paths { paths: Vec<(String, String)> },
    /// output of a buildomat job that's copied into a package
    BuildomatOutput { repo: String },
    /// Rust package in the Omicron repo that's built and then copied into a
    /// package
    OmicronRustCrate { name: String },
    /// artifact that's directly copied into a package
    Manual,
}

// The "display" impl for GraphNode is used to generate the text that appears
// inside each node in the graph
impl std::fmt::Display for GraphNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphNode::TopLevel => write!(f, "omicron-package build"),
            GraphNode::Blob { path } => write!(f, "S3 blob: {:?}", path),
            GraphNode::Paths { paths } => {
                assert!(!paths.is_empty());
                write!(f, "local files:\n")?;
                for p in paths {
                    write!(f, "- {}\n  (to {})\n", p.0, p.1)?;
                }
                Ok(())
            }
            GraphNode::BuildomatOutput { repo } => {
                write!(f, "buildomat output: job {:?}", repo)
            }
            GraphNode::OmicronRustCrate { name } => {
                write!(f, "local Rust package:\n{:?}", name)
            }
            GraphNode::Manual => write!(f, "manually-built artifact"),
            GraphNode::Package { name, source_type } => {
                write!(f, "package {:?}\ntype: {}", name, source_type)
            }
        }
    }
}

// Returns a string that can be passed to dot(1) to visualize a package manifest
pub fn do_dot(
    target: &Target,
    package_config: &Config,
) -> anyhow::Result<String> {
    let packages = &package_config.packages;

    // We'll use petgraph's facilities to build a directed acyclic graph that
    // models the package manifest and then print out the dot(1) input.
    let mut graph = petgraph::graph::Graph::new();

    // Create a top-level node that will depend on all of the packages in the
    // manifest.
    let toplevel = graph.add_node(GraphNode::TopLevel);

    // Begin constructing the graph by creating a node for each package in
    // the manifest.  Store these into `pkg_nodes`, mapping each package's name
    // to the corresponding graph node index.
    let pkg_nodes = packages
        .iter()
        .map(|(pkgname, pkg)| {
            let source_type = match &pkg.source {
                PackageSource::Local { .. } => "local",
                PackageSource::Prebuilt { .. } => "prebuilt",
                PackageSource::Composite { .. } => "composite",
                PackageSource::Manual => "manual",
            };
            let graph_node =
                GraphNode::Package { name: pkgname.to_string(), source_type };
            let node_index = graph.add_node(graph_node);

            // If this is _not_ an intermediate-only package, make the top-level
            // node depend on it.  This is a bit misleading because the
            // top-level node actually depends on _all_ packages, not just
            // non-intermediate ones.  But this is a lot easier to grok.  And it
            // should be equivalent in practice.  (These would only differ if we
            // had intermediate-only packages that were not used by any other
            // packages.  This seems like a mistake.)
            match &pkg.output {
                PackageOutput::Zone { intermediate_only: true } => (),
                PackageOutput::Zone { intermediate_only: false }
                | PackageOutput::Tarball => {
                    graph.add_edge(toplevel, node_index, "");
                }
            };

            (pkgname, node_index)
        })
        .collect::<BTreeMap<_, _>>();

    // Build a map of package output names to package graph node indices so that
    // we can look this up later to find the name of a package on which some
    // other package depends.  In practice, we could just strip off the
    // ".tar.gz" suffix.  But the approach here avoids baking in any assumptions
    // about how these are named.
    let pkg_for_output = packages
        .iter()
        .map(|(pkgname, pkg)| {
            // We only care about the output basename here, so we make up an
            // output directory.
            let pkg_node = pkg_nodes
                .get(pkgname)
                .expect("expected node for package already");
            let output_directory = camino::Utf8Path::new("/nonexistent");
            let output_basename = pkg
                .get_output_path(pkgname, output_directory)
                .file_name()
                .expect("Missing file name")
                .to_string();
            (output_basename, pkg_node)
        })
        .collect::<BTreeMap<_, _>>();

    // Now, iterate the packages again and create additional nodes to illustrate
    // S3 dependencies, buildomat job outputs, local builds (of Rust packages
    // inside Omicron), etc.  Attach edges to these nodes identifying
    // dependencies.
    for (pkgname, pkg) in packages {
        let pkg_node = pkg_nodes
            .get(pkgname)
            .expect("graph node for package should have already been craeted");

        match &pkg.source {
            PackageSource::Prebuilt { repo, .. } => {
                // Make up a node to represent the buildomat download.
                let buildomat_node =
                    graph.add_node(GraphNode::BuildomatOutput {
                        repo: repo.to_string(),
                    });
                graph.add_edge(*pkg_node, buildomat_node, "download");
            }

            PackageSource::Manual => {
                // Make up a node to represent a manually-placed artifact.
                let manual_node = graph.add_node(GraphNode::Manual);
                graph.add_edge(*pkg_node, manual_node, "use");
            }

            PackageSource::Composite { packages } => {
                // For each dependency, find the corresponding package and draw
                // an edge representing the dependency relationship.
                for dependency in packages {
                    let dep_node =
                        pkg_for_output.get(dependency).ok_or_else(|| {
                            anyhow!(
                            "package {:?} has dependency on {:?}, which does \
                            not correspond to the output of any package",
                            pkgname, dependency)
                        })?;
                    graph.add_edge(*pkg_node, **dep_node, "merge");
                }
            }

            PackageSource::Local { blobs, rust, paths, .. } => {
                // Regardless of the type of local package (e.g., files-only or
                // Rust package or whatever), create nodes showing any S3 blobs
                // on which it depends.
                if let Some(blobs) = blobs {
                    for b in blobs {
                        let s3_node = graph
                            .add_node(GraphNode::Blob { path: b.to_string() });
                        graph.add_edge(*pkg_node, s3_node, "download");
                    }
                }

                // Similarly, regardless of the type of local package, create
                // a node showing any local paths that get included in the
                // package.
                if !paths.is_empty() {
                    let paths = paths
                        .iter()
                        .map(|mapping| {
                            Ok((
                                mapping.from.interpolate(&target)?,
                                mapping.to.interpolate(&target)?,
                            ))
                        })
                        .collect::<anyhow::Result<_>>()?;
                    let path_node = graph.add_node(GraphNode::Paths { paths });
                    graph.add_edge(*pkg_node, path_node, "include");
                }

                // If this package represents a corresponding Rust package in
                // the Omicron workspace, create a separate node describing
                // that.  (omicron-package generally treats these as the same
                // thing, but it can be helpful (if pedantic?) to illustrate
                // that, say, the "omicron-nexus" *package* depends on the
                // "omicron-nexus" Rust package, particularly since that's not
                // always the case!)
                if rust.is_some() {
                    let rust_node =
                        graph.add_node(GraphNode::OmicronRustCrate {
                            name: pkgname.to_string(),
                        });
                    graph.add_edge(*pkg_node, rust_node, "build");
                }
            }
        }
    }

    // Build and emit the graphviz ("dot") representation of the graph to a
    // string.
    let raw_dot_graph =
        Dot::with_attr_getters(&graph, &[], &edge_attributes, &node_attributes)
            .to_string();

    // This is pretty gross, but the graph is much more readable when layed out
    // horizontally.  But the petgraph Dot converter doesn't provide for
    // graph-level options.  (We could instead use the `dot` crate.)
    let trailing_dot_graph = raw_dot_graph
        .strip_prefix("digraph {\n")
        .expect("unexpected dot graph format");
    Ok(format!("digraph {{\n    rankdir=\"RL\"\n{}", trailing_dot_graph))
}

// Helper function that returns the Graphviz attributes for each edge in the
// graph
fn edge_attributes(
    _: &Graph<GraphNode, &'static str>,
    _: EdgeReference<&'static str>,
) -> String {
    // Switch the direction of the arrows because we want them to flow from
    // dependency *to* dependent.
    "dir=back color=gray35 fontcolor=gray35".to_string()
}

// Helper function that returns the Graphviz attributes for each node in the
// graph
fn node_attributes(
    _: &Graph<GraphNode, &'static str>,
    noderef: (NodeIndex, &GraphNode),
) -> String {
    let (_, node) = noderef;

    if let GraphNode::TopLevel = node {
        return "fontcolor=gray35 shape=plaintext".to_string();
    }

    let fillcolor = match node {
        GraphNode::TopLevel => unimplemented!(),
        GraphNode::Package { .. } => "1",
        GraphNode::Blob { .. } => "2",
        GraphNode::Paths { .. } => "3",
        GraphNode::BuildomatOutput { .. } => "4",
        GraphNode::OmicronRustCrate { .. } => "5",
        GraphNode::Manual => "6",
    };

    format!(
        "fontcolor=\"/gray35\" \
        pencolor=\"/gray35\" \
        colorscheme=pastel28 \
        fillcolor={} \
        style=filled \
        shape=box",
        fillcolor,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use omicron_zone_package::config::parse_manifest;

    fn dot_output_for(raw_toml: &str) -> Result<String, anyhow::Error> {
        let package_config =
            parse_manifest(raw_toml).expect("test toml was invalid");
        do_dot(&Target::default(), &package_config)
    }

    #[test]
    fn test_dot_output() {
        // Right now, we test this by verifying the output matches exactly what
        // we expect.  The expectation is that if changes are made to the
        // output, someone will eyeball the resulting graph (the visual version,
        // not the graphviz source) and use expectorate to replace the expected
        // output.
        let dot =
            dot_output_for(include_str!("../tests/test-package-manifest.toml"))
                .expect("failed to generate dot output");
        expectorate::assert_contents("tests/test-dot-output.dot", &dot);
    }

    #[test]
    fn test_bad_dependency() {
        let toml = include_str!("../tests/test-package-manifest.toml");
        // add a package with a bogus dependency
        let toml = toml.to_owned()
            + r#"
        [package.oops]
        service_name = "oops"
        source.type = "composite"
        source.packages = [ "dangling.tar.gz" ]
        output.type = "zone"
        "#;
        let error = dot_output_for(&toml).expect_err("expected failure");
        let message = format!("{:#}", error);
        assert_eq!(
            message,
            "package \"oops\" has dependency on \"dangling.tar.gz\", which \
            does not correspond to the output of any package"
        );
    }
}

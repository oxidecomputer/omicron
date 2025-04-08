// A build script is required for cargo to consider build dependencies.
fn main() {
    println!("cargo::rerun-if-changed=build.rs");
}

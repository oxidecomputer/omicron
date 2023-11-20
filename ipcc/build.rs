fn main() {
    #[cfg(target_os = "illumos")]
    {
        println!(
            "cargo:rustc-link-arg=-Wl,-rpath,/usr/platform/oxide/lib/amd64"
        );
        println!(r"cargo:rustc-link-search=/usr/platform/oxide/lib/amd64");
    }
}

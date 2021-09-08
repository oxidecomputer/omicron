fn main() {
    if let Some(rpaths) = std::env::var_os("DEP_PQ_LIBDIRS") {
        for p in std::env::split_paths(&rpaths) {
            println!("cargo:rustc-link-arg=-Wl,-R{}", p.to_string_lossy());
        }
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{io::Write, os::unix::fs::OpenOptionsExt};

use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use clap::Args;

#[derive(Clone, Debug, Args)]
pub(crate) struct CertCreateArgs {
    /// path to where the generated certificate and key files should go
    /// (e.g., "out/initial-" would cause the files to be called
    /// "out/initial-cert.pem" and "out/initial-key.pem")
    #[clap(action)]
    output_base: Utf8PathBuf,

    /// DNS names that the certificate claims to be valid for (subject
    /// alternative names)
    #[clap(action, required = true)]
    server_names: Vec<String>,
}

impl CertCreateArgs {
    pub(crate) async fn exec(&self) -> Result<(), anyhow::Error> {
        let cert =
            rcgen::generate_simple_self_signed(self.server_names.clone())
                .context("generating certificate")?;
        let cert_pem =
            cert.serialize_pem().context("serializing certificate as PEM")?;
        let key_pem = cert.serialize_private_key_pem();

        let cert_path =
            Utf8PathBuf::from(format!("{}cert.pem", self.output_base));
        write_private_file(&cert_path, cert_pem.as_bytes())
            .context("writing certificate file")?;
        println!("wrote certificate to {}", cert_path);

        let key_path =
            Utf8PathBuf::from(format!("{}key.pem", self.output_base));
        write_private_file(&key_path, key_pem.as_bytes())
            .context("writing private key file")?;
        println!("wrote private key to {}", key_path);

        Ok(())
    }
}

#[cfg_attr(not(mac), allow(clippy::useless_conversion))]
fn write_private_file(
    path: &Utf8Path,
    contents: &[u8],
) -> Result<(), anyhow::Error> {
    // The file should be readable and writable by the user only.
    let perms = libc::S_IRUSR | libc::S_IWUSR;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(perms.into()) // into() needed on mac only
        .open(path)
        .with_context(|| format!("open {:?} for writing", path))?;
    file.write_all(contents).with_context(|| format!("write to {:?}", path))
}

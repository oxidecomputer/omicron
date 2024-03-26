// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example test to run in a VM.

#[cfg(all(test, target_os = "illumos"))]
mod test {
    use anyhow::anyhow;
    use gethostname::gethostname;

    #[ignore]
    #[tokio::test]
    async fn launch() -> Result<(), anyhow::Error> {
        if gethostname() == "launchpad_mcduck_test_vm" {
            eprintln!("hello falcon runner: I am inside the VM");
            Ok(())
        } else {
            Err(anyhow!("Test failed: Not running in a VM"))
        }
    }
}

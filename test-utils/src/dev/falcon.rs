// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanisms to launch and control propolis VMs via falcon

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn launch() {
        eprintln!("hello falcon runner: I am inside the VM");
    }
}

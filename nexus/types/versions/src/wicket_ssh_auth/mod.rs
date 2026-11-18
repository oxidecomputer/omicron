// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `WICKET_SSH_AUTH` of the Nexus external API.
//!
//! Adds the `WicketSshAuth` resource and endpoints for changing and viewing the
//! authentication applied to SSH'ing into `wicket`, as described in [RFD 785].
//!
//! [RFD 785]: https://rfd.shared.oxide.computer/rfd/0785

pub mod wicket_ssh_auth;

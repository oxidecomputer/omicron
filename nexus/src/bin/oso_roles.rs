// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simple program to dump the roles found in the Oso policy file

use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_nexus::authz;

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let roles = authz::Authz::new()
        .builtin_roles()
        .map_err(|error| CmdError::Failure(error.to_string()))?;
    for (resource_name, role_list) in roles {
        for role_name in role_list {
            eprintln!("resource {:?} role {:?}", resource_name, role_name);
        }
    }
    Ok(())
}

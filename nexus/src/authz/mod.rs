// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authorization facilities

mod oso_types;
pub use oso_types::Action;
pub use oso_types::DATABASE;

mod context;
pub use context::Authz;
pub use context::Context;

mod actor;

mod roles;
pub use roles::AuthzResource;

mod api_resources;
pub use api_resources::Fleet;
pub use api_resources::FleetChild;
pub use api_resources::Organization;
pub use api_resources::Project;
pub use api_resources::ProjectChild;
pub use api_resources::FLEET;

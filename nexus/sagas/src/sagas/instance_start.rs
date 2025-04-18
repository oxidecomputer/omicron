use nexus_auth::authn;
use serde::{Deserialize, Serialize};

/// Parameters to the instance start saga.
#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub db_instance: nexus_db_model::Instance,

    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,

    /// Why is this instance being started?
    pub reason: InstanceStartReason,
}

/// Reasons an instance may be started.
///
/// Currently, this is primarily used to determine whether the instance's
/// auto-restart timestamp must be updated. It's also included in log messages
/// in the start saga.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum InstanceStartReason {
    /// The instance was automatically started upon being created.
    AutoStart,
    /// The instance was started by a user action.
    User,
    /// The instance has failed and is being automatically restarted by the
    /// control plane.
    AutoRestart,
}

// TODO: move the actual saga over.

/*!
 * Facilities for interacting with Server Controllers.  See RFD 48.
 */

use uuid::Uuid;

/**
 * `ServerController` is our handle for the software service running on a
 * compute server that manages the control plane on that server.  The current
 * implementation is simulated directly in Rust.  The intent is that this object
 * will be implemented using requests to a remote server and the simulation
 * would be moved to the other side of the network.
 */
pub struct ServerController {
    pub id: Uuid,
}

impl ServerController {
    /** Constructs a simulated ServerController with the given uuid. */
    pub fn new_simulated_with_id(id: &Uuid) -> ServerController {
        ServerController {
            id: id.clone(),
        }
    }
}

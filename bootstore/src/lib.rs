//! The two communication paths for the bootstore:
//!
//! RSS -> Sled Agent -> Coordinator -> Storage Nodes
//! Nexus -> Steno -> Sled Agent -> Coordinator -> Storage Nodes
//!
//!
//! Since some trust quorum membership information that is input via RSS must
//! make its way into CockroachDb so that reconfiguration works, we will load
//! that information from the trust quorum database, parse it, and write
//! it to CockroachDB when we start it up.

mod db;
mod db_macros;
//mod server;
mod trust_quorum;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}

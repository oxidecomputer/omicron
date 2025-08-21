// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for clickhouse-admin

use camino::Utf8PathBuf;
use clickhouse_admin_types::OXIMETER_CLUSTER;
use clickward::{BasePorts, Deployment, DeploymentConfig};
use dropshot::test_util::{LogContext, log_prefix_for_test};
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use std::net::TcpListener;

pub const DEFAULT_CLICKHOUSE_ADMIN_BASE_PORTS: BasePorts = BasePorts {
    keeper: 29000,
    raft: 29100,
    clickhouse_tcp: 29200,
    clickhouse_http: 29300,
    clickhouse_interserver_http: 29400,
};

/// Error type for port allocation failures
#[derive(Debug, thiserror::Error)]
pub enum PortAllocationError {
    #[error("No available port ranges found after {attempts} attempts")]
    NoAvailablePorts { attempts: usize },
    #[error("IO error while checking port availability: {0}")]
    IoError(#[from] std::io::Error),
}

/// Allocate a set of available ports for ClickHouse testing
/// 
/// This function finds 5 consecutive available ports starting from a randomized base
/// to avoid conflicts when running tests in parallel.
/// 
/// **Note**: There is a small race condition window between checking port availability
/// and ClickHouse actually binding to them. This is acceptable for test scenarios
/// where the likelihood is low and retry logic handles conflicts.
/// 
/// # Arguments
/// * `base_port` - Starting port to search from (defaults to 20000 if None)
/// * `max_attempts` - Maximum number of port ranges to try (defaults to 100)
/// 
/// # Returns
/// A `BasePorts` struct with available ports, or an error if none found
pub fn allocate_available_ports(
    base_port: Option<u16>,
    max_attempts: Option<usize>,
) -> Result<BasePorts, PortAllocationError> {
    let base = base_port.unwrap_or(20000);
    let attempts = max_attempts.unwrap_or(100);
    
    // Use a deterministic but varied approach: base + (process_id * some_offset)
    // This reduces collisions when multiple test processes run simultaneously  
    let process_offset = std::process::id() as u16 % 500;
    
    for attempt in 0..attempts {
        // Each attempt tries a different offset to avoid conflicts
        let attempt_offset = (attempt as u16) * 37; // Use a prime to spread attempts
        let current_base = base
            .saturating_add(process_offset * 10)
            .saturating_add(attempt_offset);
        
        // Ensure we don't overflow
        if current_base > 65530 {
            continue;
        }
        
        match try_allocate_port_range(current_base) {
            Ok(ports) => return Ok(ports),
            Err(_) => continue, // Try next range
        }
    }
    
    Err(PortAllocationError::NoAvailablePorts { attempts })
}

/// Try to allocate 5 consecutive ports starting from the given base
fn try_allocate_port_range(base_port: u16) -> Result<BasePorts, PortAllocationError> {
    let ports_needed: usize = 5;
    let mut listeners = Vec::new();
    let mut allocated_ports = Vec::new();
    
    // Try to bind all 5 consecutive ports
    for offset in 0..ports_needed {
        let port = base_port + (offset as u16);
        match TcpListener::bind(("127.0.0.1", port)) {
            Ok(listener) => {
                allocated_ports.push(listener.local_addr()?.port());
                listeners.push(listener);
            }
            Err(e) => {
                // Failed to bind this port, cleanup and return error
                drop(listeners); // Release all listeners
                return Err(PortAllocationError::IoError(e));
            }
        }
    }
    
    // All ports successfully allocated
    if allocated_ports.len() == ports_needed {
        // Drop listeners to release ports for immediate use
        drop(listeners);
        
        Ok(BasePorts {
            keeper: allocated_ports[0],
            raft: allocated_ports[1],
            clickhouse_tcp: allocated_ports[2], 
            clickhouse_http: allocated_ports[3],
            clickhouse_interserver_http: allocated_ports[4],
        })
    } else {
        Err(PortAllocationError::NoAvailablePorts { attempts: 1 })
    }
}

pub fn default_clickhouse_cluster_test_deployment(
    path: Utf8PathBuf,
) -> Deployment {
    let config = DeploymentConfig {
        path,
        base_ports: DEFAULT_CLICKHOUSE_ADMIN_BASE_PORTS,
        cluster_name: OXIMETER_CLUSTER.to_string(),
    };

    Deployment::new(config)
}

/// Create a ClickHouse cluster test deployment with dynamically allocated ports
/// 
/// This variant uses dynamic port allocation to avoid conflicts when running
/// tests in parallel.
pub fn clickhouse_cluster_test_deployment_with_dynamic_ports(
    path: Utf8PathBuf,
) -> Result<Deployment, PortAllocationError> {
    let base_ports = allocate_available_ports(None, None)?;
    
    let config = DeploymentConfig {
        path,
        base_ports,
        cluster_name: OXIMETER_CLUSTER.to_string(),
    };
    
    Ok(Deployment::new(config))
}

pub fn default_clickhouse_log_ctx_and_path() -> (LogContext, Utf8PathBuf) {
    let logctx = LogContext::new(
        "clickhouse_cluster",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    let (parent_dir, _prefix) = log_prefix_for_test("clickhouse_cluster");
    let path = parent_dir.join("clickward_test");

    (logctx, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_port_allocation_basic() {
        let ports = allocate_available_ports(Some(30000), Some(10))
            .expect("Should find available ports");
        
        // Verify all ports are different
        let port_vec = vec![
            ports.keeper,
            ports.raft, 
            ports.clickhouse_tcp,
            ports.clickhouse_http,
            ports.clickhouse_interserver_http,
        ];
        
        // Check for uniqueness
        let mut unique_ports = std::collections::HashSet::new();
        for port in &port_vec {
            assert!(unique_ports.insert(*port), "Port {} is duplicated", port);
        }
        
        // Verify ports are consecutive
        let mut sorted_ports = port_vec.clone();
        sorted_ports.sort();
        for i in 1..sorted_ports.len() {
            assert_eq!(
                sorted_ports[i], 
                sorted_ports[i-1] + 1,
                "Ports should be consecutive: {:?}", 
                sorted_ports
            );
        }
    }

    #[test]
    fn test_port_allocation_multiple_calls() {
        // Test that multiple allocations work (they might be the same if 
        // running sequentially in the same process, which is fine)
        let ports1 = allocate_available_ports(Some(31000), Some(10))
            .expect("First allocation should succeed");
        let ports2 = allocate_available_ports(Some(31100), Some(10))  // Different base
            .expect("Second allocation should succeed");
        
        // Both should be valid port ranges
        assert!(ports1.keeper > 0);
        assert!(ports2.keeper > 0);
        
        // Verify all ports in each allocation are consecutive
        assert_eq!(ports1.raft, ports1.keeper + 1);
        assert_eq!(ports1.clickhouse_tcp, ports1.keeper + 2);
        assert_eq!(ports1.clickhouse_http, ports1.keeper + 3);
        assert_eq!(ports1.clickhouse_interserver_http, ports1.keeper + 4);
        
        assert_eq!(ports2.raft, ports2.keeper + 1);
        assert_eq!(ports2.clickhouse_tcp, ports2.keeper + 2);
        assert_eq!(ports2.clickhouse_http, ports2.keeper + 3);
        assert_eq!(ports2.clickhouse_interserver_http, ports2.keeper + 4);
    }
    
    #[test] 
    fn test_port_allocation_edge_cases() {
        // Test with high base port (should handle overflow gracefully)
        let _result = allocate_available_ports(Some(65530), Some(5));
        // This might fail due to lack of available ports, which is expected
        
        // Test with 0 attempts (should fail immediately)
        let result = allocate_available_ports(Some(32000), Some(0));
        assert!(result.is_err());
        
        if let Err(PortAllocationError::NoAvailablePorts { attempts }) = result {
            assert_eq!(attempts, 0);
        } else {
            panic!("Expected NoAvailablePorts error");
        }
    }

    #[test]
    fn test_dynamic_deployment_creation() {
        let temp_dir = camino_tempfile::Utf8TempDir::new()
            .expect("Failed to create temp directory");
        
        let deployment = clickhouse_cluster_test_deployment_with_dynamic_ports(
            temp_dir.path().to_path_buf()
        ).expect("Should create deployment with dynamic ports");
        
        // Verify deployment was created successfully
        // (We can't easily test the actual ports without accessing private fields)
        drop(deployment);
    }
}

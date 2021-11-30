// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::{
    IdentityMetadata, L4Port, L4PortRange, VpcFirewallRule,
    VpcFirewallRuleAction, VpcFirewallRuleDirection, VpcFirewallRuleFilter,
    VpcFirewallRuleHostFilter, VpcFirewallRulePriority,
    VpcFirewallRuleProtocol, VpcFirewallRuleStatus, VpcFirewallRuleTarget,
    VpcFirewallRuleUpdate, VpcFirewallRuleUpdateParams,
};
use std::collections::HashMap;
use std::convert::TryFrom;

use dropshot::test_util::{object_delete, objects_list_page};

pub mod common;
use common::resource_helpers::{
    create_organization, create_project, create_vpc,
};
use common::test_setup;

extern crate slog;

#[tokio::test]
async fn test_vpc_firewall() {
    let cptestctx = test_setup("test_vpc_firewall").await;
    let client = &cptestctx.external_client;

    /* Create a project that we'll use for testing. */
    let org_name = "test-org";
    create_organization(&client, &org_name).await;
    let project_name = "springfield-squidport";
    let vpcs_url =
        format!("/organizations/{}/projects/{}/vpcs", org_name, project_name);
    create_project(&client, &org_name, &project_name).await;

    // Each project has a default VPC. Make sure it has the default rules.
    let default_vpc_firewall = format!("{}/default/firewall/rules", vpcs_url);
    let rules =
        objects_list_page::<VpcFirewallRule>(client, &default_vpc_firewall)
            .await
            .items;
    assert!(is_default_firewall_rules(&rules));

    // Create another VPC and make sure it gets the default rules.
    let other_vpc = "second-vpc";
    let other_vpc_firewall =
        format!("{}/{}/firewall/rules", vpcs_url, other_vpc);
    create_vpc(&client, &org_name, &project_name, &other_vpc).await;
    let rules =
        objects_list_page::<VpcFirewallRule>(client, &other_vpc_firewall)
            .await
            .items;
    assert!(is_default_firewall_rules(&rules));

    // Modify one VPC's firewall
    let mut new_rules = HashMap::new();
    new_rules.insert(
        "deny-all-incoming".parse().unwrap(),
        VpcFirewallRuleUpdate {
            action: VpcFirewallRuleAction::Deny,
            description: "test desc".to_string(),
            status: VpcFirewallRuleStatus::Disabled,
            targets: vec![VpcFirewallRuleTarget::Vpc(
                "default".parse().unwrap(),
            )],
            filters: VpcFirewallRuleFilter {
                hosts: None,
                ports: None,
                protocols: None,
            },
            direction: VpcFirewallRuleDirection::Inbound,
            priority: VpcFirewallRulePriority(100),
        },
    );
    new_rules.insert(
        "allow-icmp".parse().unwrap(),
        VpcFirewallRuleUpdate {
            action: VpcFirewallRuleAction::Allow,
            description: "allow icmp".to_string(),
            status: VpcFirewallRuleStatus::Enabled,
            targets: vec![VpcFirewallRuleTarget::Vpc(
                "default".parse().unwrap(),
            )],
            filters: VpcFirewallRuleFilter {
                hosts: None,
                ports: None,
                protocols: Some(vec![VpcFirewallRuleProtocol::Icmp]),
            },
            direction: VpcFirewallRuleDirection::Inbound,
            priority: VpcFirewallRulePriority(10),
        },
    );
    let update_params =
        VpcFirewallRuleUpdateParams { rules: new_rules.clone() };
    client
        .make_request(
            Method::PUT,
            &default_vpc_firewall,
            Some(update_params),
            StatusCode::OK,
        )
        .await
        .unwrap();

    // Make sure the firewall is changed
    let rules =
        objects_list_page::<VpcFirewallRule>(client, &default_vpc_firewall)
            .await
            .items;
    assert!(!is_default_firewall_rules(&rules));
    assert_eq!(rules.len(), new_rules.len());
    assert_eq!(rules[0].identity.name, "allow-icmp");
    assert_eq!(rules[1].identity.name, "deny-all-incoming");

    // Make sure the other firewall is unchanged
    let rules =
        objects_list_page::<VpcFirewallRule>(client, &other_vpc_firewall)
            .await
            .items;
    assert!(is_default_firewall_rules(&rules));

    // DELETE is unspported
    client
        .make_request_error(
            Method::DELETE,
            &default_vpc_firewall,
            StatusCode::METHOD_NOT_ALLOWED,
        )
        .await;

    // Delete a VPC and ensure we can't read its firewall anymore
    object_delete(client, format!("{}/{}", vpcs_url, other_vpc).as_str()).await;
    client
        .make_request_error(
            Method::GET,
            &other_vpc_firewall,
            StatusCode::NOT_FOUND,
        )
        .await;
}

fn is_default_firewall_rules(rules: &Vec<VpcFirewallRule>) -> bool {
    let default_rules = vec![
        VpcFirewallRule {
            identity: IdentityMetadata {
                id: "4ec28ba7-8f99-4d43-ae48-8f53f68389d5".parse().unwrap(),
                name: "allow-icmp".parse().unwrap(),
                description: "allow inbound ICMP traffic from anywhere"
                    .to_string(),
                time_created: "2021-11-16T00:24:06.027445Z".parse().unwrap(),
                time_modified: "2021-11-16T00:24:06.027445Z".parse().unwrap(),
            },
            status: VpcFirewallRuleStatus::Enabled,
            direction: VpcFirewallRuleDirection::Inbound,
            targets: vec![VpcFirewallRuleTarget::Vpc(
                "default".parse().unwrap(),
            )],
            filters: VpcFirewallRuleFilter {
                hosts: None,
                protocols: Some(vec![VpcFirewallRuleProtocol::Icmp]),
                ports: None,
            },
            action: VpcFirewallRuleAction::Allow,
            priority: VpcFirewallRulePriority(65534),
        },
        VpcFirewallRule {
            identity: IdentityMetadata {
                id: "ec9d884e-3fe9-4207-9244-2bcb7923183e".parse().unwrap(),
                name: "allow-internal-inbound".parse().unwrap(),
                description:
                    "allow inbound traffic to all instances within the \
                VPC if originated within the VPC"
                        .to_string(),
                time_created: "2021-11-16T00:24:06.027447Z".parse().unwrap(),
                time_modified: "2021-11-16T00:24:06.027447Z".parse().unwrap(),
            },
            status: VpcFirewallRuleStatus::Enabled,
            direction: VpcFirewallRuleDirection::Inbound,
            targets: vec![VpcFirewallRuleTarget::Vpc(
                "default".parse().unwrap(),
            )],
            filters: VpcFirewallRuleFilter {
                hosts: Some(vec![VpcFirewallRuleHostFilter::Vpc(
                    "default".parse().unwrap(),
                )]),
                protocols: None,
                ports: None,
            },
            action: VpcFirewallRuleAction::Allow,
            priority: VpcFirewallRulePriority(65534),
        },
        VpcFirewallRule {
            identity: IdentityMetadata {
                id: "dd166833-cd79-4279-beb0-186cadb982ce".parse().unwrap(),
                name: "allow-rdp".parse().unwrap(),
                description:
                    "allow inbound TCP connections on port 3389 from anywhere"
                        .to_string(),
                time_created: "2021-11-16T00:24:06.027404Z".parse().unwrap(),
                time_modified: "2021-11-16T00:24:06.027404Z".parse().unwrap(),
            },
            status: VpcFirewallRuleStatus::Enabled,
            direction: VpcFirewallRuleDirection::Inbound,
            targets: vec![VpcFirewallRuleTarget::Vpc(
                "default".parse().unwrap(),
            )],
            filters: VpcFirewallRuleFilter {
                hosts: None,
                protocols: Some(vec![VpcFirewallRuleProtocol::Tcp]),
                ports: Some(vec![L4PortRange {
                    first: L4Port::try_from(3389).unwrap(),
                    last: L4Port::try_from(3389).unwrap(),
                }]),
            },
            action: VpcFirewallRuleAction::Allow,
            priority: VpcFirewallRulePriority(65534),
        },
        VpcFirewallRule {
            identity: IdentityMetadata {
                id: "4cb76726-4cb6-4bc2-8d32-71c36e3881d4".parse().unwrap(),
                name: "allow-ssh".parse().unwrap(),
                description:
                    "allow inbound TCP connections on port 22 from anywhere"
                        .to_string(),
                time_created: "2021-11-16T00:24:06.027440Z".parse().unwrap(),
                time_modified: "2021-11-16T00:24:06.027440Z".parse().unwrap(),
            },
            status: VpcFirewallRuleStatus::Enabled,
            direction: VpcFirewallRuleDirection::Inbound,
            targets: vec![VpcFirewallRuleTarget::Vpc(
                "default".parse().unwrap(),
            )],
            filters: VpcFirewallRuleFilter {
                hosts: None,
                protocols: Some(vec![VpcFirewallRuleProtocol::Tcp]),
                ports: Some(vec![L4PortRange {
                    first: L4Port::try_from(22).unwrap(),
                    last: L4Port::try_from(22).unwrap(),
                }]),
            },
            action: VpcFirewallRuleAction::Allow,
            priority: VpcFirewallRulePriority(65534),
        },
    ];

    if rules.len() != default_rules.len() {
        return false;
    }
    for (rule, expected_rule) in rules.iter().zip(default_rules) {
        // Check all fields besides uuids and timestamps
        if rule.identity.name != expected_rule.identity.name {
            return false;
        }
        if rule.identity.description != expected_rule.identity.description {
            return false;
        }
        if rule.status != expected_rule.status {
            return false;
        }
        if rule.direction != expected_rule.direction {
            return false;
        }
        if rule.targets != expected_rule.targets {
            return false;
        }
        if rule.filters != expected_rule.filters {
            return false;
        }
        if rule.action != expected_rule.action {
            return false;
        }
        if rule.priority != expected_rule.priority {
            return false;
        }
    }
    true
}

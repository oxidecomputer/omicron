// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils::resource_helpers::{create_project, create_vpc};
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::{
    IdentityMetadata, L4Port, L4PortRange, VpcFirewallRule,
    VpcFirewallRuleAction, VpcFirewallRuleDirection, VpcFirewallRuleFilter,
    VpcFirewallRuleHostFilter, VpcFirewallRulePriority,
    VpcFirewallRuleProtocol, VpcFirewallRuleStatus, VpcFirewallRuleTarget,
    VpcFirewallRuleUpdate, VpcFirewallRuleUpdateParams, VpcFirewallRules,
};
use omicron_nexus::external_api::views::Vpc;
use std::convert::TryFrom;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_vpc_firewall(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let project_name = "springfield-squidport";
    create_project(&client, &project_name).await;

    let project_selector = format!("project={}", project_name);
    // Each project has a default VPC. Make sure it has the default rules.
    let default_vpc_url = format!("/v1/vpcs/default?{}", project_selector);
    let default_vpc: Vpc = NexusRequest::object_get(client, &default_vpc_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();

    let default_vpc_firewall =
        format!("/v1/vpc-firewall-rules?vpc=default&{}", project_selector,);
    let rules = get_rules(client, &default_vpc_firewall).await;
    assert!(rules.iter().all(|r| r.vpc_id == default_vpc.identity.id));
    assert!(is_default_firewall_rules("default", &rules));

    // Create another VPC and make sure it gets the default rules.
    let other_vpc = "second-vpc";
    let other_vpc_selector = format!("{}&vpc={}", project_selector, other_vpc);
    let other_vpc_firewall =
        format!("/v1/vpc-firewall-rules?{}", other_vpc_selector);
    let vpc2 = create_vpc(&client, &project_name, &other_vpc).await;
    let rules = get_rules(client, &other_vpc_firewall).await;
    assert!(rules.iter().all(|r| r.vpc_id == vpc2.identity.id));
    assert!(is_default_firewall_rules(other_vpc, &rules));

    // Modify one VPC's firewall
    let new_rules = vec![
        VpcFirewallRuleUpdate {
            name: "deny-all-incoming".parse().unwrap(),
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
        VpcFirewallRuleUpdate {
            name: "allow-icmp".parse().unwrap(),
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
    ];
    let update_params =
        VpcFirewallRuleUpdateParams { rules: new_rules.clone() };
    let updated_rules = NexusRequest::object_put(
        client,
        &default_vpc_firewall,
        Some(&update_params),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<VpcFirewallRules>()
    .unwrap()
    .rules;
    assert!(!is_default_firewall_rules("default", &updated_rules));
    assert_eq!(updated_rules.len(), new_rules.len());
    assert_eq!(updated_rules[0].identity.name, "allow-icmp");
    assert_eq!(updated_rules[1].identity.name, "deny-all-incoming");

    // Make sure the firewall is changed
    let rules = get_rules(client, &default_vpc_firewall).await;
    assert!(!is_default_firewall_rules("default", &rules));
    assert_eq!(rules.len(), new_rules.len());
    assert_eq!(rules[0].identity.name, "allow-icmp");
    assert_eq!(rules[1].identity.name, "deny-all-incoming");

    // Make sure the other firewall is unchanged
    let rules = get_rules(client, &other_vpc_firewall).await;
    assert!(is_default_firewall_rules(other_vpc, &rules));

    // DELETE is unsupported
    NexusRequest::expect_failure(
        client,
        StatusCode::METHOD_NOT_ALLOWED,
        Method::DELETE,
        &default_vpc_firewall,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Delete a VPC Subnet / VPC and ensure we can't read its firewall anymore
    NexusRequest::object_delete(
        client,
        &format!("/v1/vpc-subnets/default?{}", other_vpc_selector),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    NexusRequest::object_delete(
        client,
        &format!("/v1/vpcs/{}?{}", other_vpc, project_selector),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &other_vpc_firewall,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn get_rules(
    client: &dropshot::test_util::ClientTestContext,
    url: &str,
) -> Vec<VpcFirewallRule> {
    NexusRequest::object_get(client, url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body::<VpcFirewallRules>()
        .unwrap()
        .rules
}

fn is_default_firewall_rules(
    vpc_name: &str,
    rules: &Vec<VpcFirewallRule>,
) -> bool {
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
                vpc_name.parse().unwrap(),
            )],
            filters: VpcFirewallRuleFilter {
                hosts: None,
                protocols: Some(vec![VpcFirewallRuleProtocol::Icmp]),
                ports: None,
            },
            action: VpcFirewallRuleAction::Allow,
            priority: VpcFirewallRulePriority(65534),
            vpc_id: Uuid::new_v4(), // placeholder, not used in comparison
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
                vpc_name.parse().unwrap(),
            )],
            filters: VpcFirewallRuleFilter {
                hosts: Some(vec![VpcFirewallRuleHostFilter::Vpc(
                    vpc_name.parse().unwrap(),
                )]),
                protocols: None,
                ports: None,
            },
            action: VpcFirewallRuleAction::Allow,
            priority: VpcFirewallRulePriority(65534),
            vpc_id: Uuid::new_v4(),
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
                vpc_name.parse().unwrap(),
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
            vpc_id: Uuid::new_v4(),
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

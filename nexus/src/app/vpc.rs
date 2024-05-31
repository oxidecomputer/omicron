// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPCs and firewall rules

use crate::app::project::Project;
use crate::app::saga;
use crate::app::sagas;
use crate::app::SagaContext;
use crate::external_api::params;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::Name;
use nexus_defaults as defaults;
use omicron_common::api::external;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

/// Application level operations on VPCs
#[derive(Clone)]
pub struct Vpc {
    log: Logger,
    datastore: Arc<db::DataStore>,
    sec_client: Arc<saga::SecClient>,
    opctx_sled_lookup: OpContext,
    project: Project,
}

impl Vpc {
    pub fn new(
        log: Logger,
        authz: Arc<authz::Authz>,
        datastore: Arc<db::DataStore>,
        sec_client: Arc<saga::SecClient>,
        project: Project,
    ) -> Vpc {
        let opctx_sled_lookup = OpContext::for_background(
            log.new(o!("component" => "SledLookupForVpc")),
            authz,
            authn::Context::internal_read(),
            Arc::clone(&datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );
        Vpc { log, datastore, sec_client, opctx_sled_lookup, project }
    }

    pub fn vpc_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        vpc_selector: params::VpcSelector,
    ) -> LookupResult<lookup::Vpc<'a>> {
        match vpc_selector {
            params::VpcSelector { vpc: NameOrId::Id(id), project: None } => {
                let vpc = LookupPath::new(opctx, &self.datastore).vpc_id(id);
                Ok(vpc)
            }
            params::VpcSelector {
                vpc: NameOrId::Name(name),
                project: Some(project),
            } => {
                let vpc = self.project
                    .project_lookup(opctx, params::ProjectSelector { project })?
                    .vpc_name_owned(name.into());
                Ok(vpc)
            }
            params::VpcSelector {
                vpc: NameOrId::Id(_),
                project: Some(_),
            } => Err(Error::invalid_request(
                "when providing vpc as an ID, project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "vpc should either be an ID or project should be specified",
            )),
        }
    }

    pub(crate) async fn project_create_vpc(
        &self,
        opctx: &OpContext,
        saga_context: &SagaContext,
        project_lookup: &lookup::Project<'_>,
        params: &params::VpcCreate,
    ) -> CreateResult<db::model::Vpc> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        opctx.authorize(authz::Action::CreateChild, &authz_project).await?;

        let saga_params = sagas::vpc_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            vpc_create: params.clone(),
            authz_project,
        };

        let saga_outputs = self
            .sec_client
            .execute_saga::<sagas::vpc_create::SagaVpcCreate>(
                saga_params,
                saga_context.clone(),
            )
            .await?;

        let (_, db_vpc) = saga_outputs
            .lookup_node_output::<(authz::Vpc, db::model::Vpc)>("vpc")
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from VPC create saga")?;

        Ok(db_vpc)
    }

    pub(crate) async fn vpc_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Vpc> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.datastore.vpc_list(&opctx, &authz_project, pagparams).await
    }

    pub(crate) async fn project_update_vpc(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        params: &params::VpcUpdate,
    ) -> UpdateResult<db::model::Vpc> {
        let (.., authz_vpc) =
            vpc_lookup.lookup_for(authz::Action::Modify).await?;
        self.datastore
            .project_update_vpc(opctx, &authz_vpc, params.clone().into())
            .await
    }

    pub(crate) async fn project_delete_vpc(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
    ) -> DeleteResult {
        let (.., authz_vpc, db_vpc) = vpc_lookup.fetch().await?;

        let authz_vpc_router = authz::VpcRouter::new(
            authz_vpc.clone(),
            db_vpc.system_router_id,
            LookupType::ById(db_vpc.system_router_id),
        );

        // Possibly delete the VPC, then the router and firewall.
        //
        // We must delete the VPC first. This will fail if the VPC still
        // contains at least one subnet, since those are independent containers
        // that track network interfaces as child resources. If we delete the
        // router first, it'll succeed even if the VPC contains Subnets, which
        // means the router is now gone from an otherwise-live subnet.
        //
        // This is a good example of need for the original comment:
        //
        // TODO: This should eventually use a saga to call the
        // networking subsystem to have it clean up the networking resources
        self.datastore.project_delete_vpc(opctx, &db_vpc, &authz_vpc).await?;
        self.datastore.vpc_delete_router(&opctx, &authz_vpc_router).await?;

        // Delete all firewall rules after deleting the VPC, to ensure no
        // firewall rules get added between rules deletion and VPC deletion.
        self.datastore.vpc_delete_all_firewall_rules(&opctx, &authz_vpc).await
    }

    // Firewall rules

    pub(crate) async fn vpc_list_firewall_rules(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
    ) -> ListResultVec<db::model::VpcFirewallRule> {
        nexus_networking::vpc_list_firewall_rules(
            &self.datastore,
            opctx,
            vpc_lookup,
        )
        .await
    }

    pub(crate) async fn vpc_update_firewall_rules(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        params: &VpcFirewallRuleUpdateParams,
    ) -> UpdateResult<Vec<db::model::VpcFirewallRule>> {
        let (.., authz_vpc, db_vpc) =
            vpc_lookup.fetch_for(authz::Action::Modify).await?;
        let rules = db::model::VpcFirewallRule::vec_from_params(
            authz_vpc.id(),
            params.clone(),
        );
        let rules = self
            .datastore
            .vpc_update_firewall_rules(opctx, &authz_vpc, rules)
            .await?;
        self.send_sled_agents_firewall_rules(opctx, &db_vpc, &rules, &[])
            .await?;
        Ok(rules)
    }

    /// Customize the default firewall rules for a particular VPC
    /// by replacing the name `default` with the VPC's actual name.
    pub(crate) async fn default_firewall_rules_for_vpc(
        &self,
        vpc_id: Uuid,
        vpc_name: Name,
    ) -> Result<Vec<db::model::VpcFirewallRule>, Error> {
        let mut rules = db::model::VpcFirewallRule::vec_from_params(
            vpc_id,
            defaults::DEFAULT_FIREWALL_RULES.clone(),
        );
        for rule in rules.iter_mut() {
            for target in rule.targets.iter_mut() {
                match target.0 {
                    external::VpcFirewallRuleTarget::Vpc(ref mut name)
                        if name.as_str() == "default" =>
                    {
                        *name = vpc_name.clone().into()
                    }
                    _ => {
                        return Err(external::Error::internal_error(
                            "unexpected target in default firewall rule",
                        ))
                    }
                }
                if let Some(ref mut filter_hosts) = rule.filter_hosts {
                    for host in filter_hosts.iter_mut() {
                        match host.0 {
                            external::VpcFirewallRuleHostFilter::Vpc(
                                ref mut name,
                            ) if name.as_str() == "default" => {
                                *name = vpc_name.clone().into()
                            }
                            _ => return Err(external::Error::internal_error(
                                "unexpected host filter in default firewall rule"
                            )),
                        }
                    }
                }
            }
        }
        debug!(self.log, "default firewall rules for vpc {}", vpc_name; "rules" => ?&rules);
        Ok(rules)
    }

    pub(crate) async fn send_sled_agents_firewall_rules(
        &self,
        opctx: &OpContext,
        vpc: &db::model::Vpc,
        rules: &[db::model::VpcFirewallRule],
        sleds_filter: &[Uuid],
    ) -> Result<(), Error> {
        nexus_networking::send_sled_agents_firewall_rules(
            &self.datastore,
            opctx,
            vpc,
            rules,
            sleds_filter,
            &self.opctx_sled_lookup,
            &self.log,
        )
        .await
    }

    pub(crate) async fn resolve_firewall_rules_for_sled_agent(
        &self,
        opctx: &OpContext,
        vpc: &db::model::Vpc,
        rules: &[db::model::VpcFirewallRule],
    ) -> Result<Vec<sled_agent_client::types::VpcFirewallRule>, Error> {
        nexus_networking::resolve_firewall_rules_for_sled_agent(
            &self.datastore,
            opctx,
            vpc,
            rules,
            &self.log,
        )
        .await
    }
}

/*!
 * API simulation of an Oxide rack, used for testing and prototyping.
 */

use async_trait::async_trait;
use futures::stream::StreamExt;
use std::collections::BTreeMap;

use crate::api_model::ApiBackend;
use crate::api_model::ApiListResult;
use crate::api_model::ApiModelProject;

/**
 * Maintains simulated state of the Oxide rack.  The current implementation is
 * in-memory only.
 */
pub struct Simulator {
    projects_by_name: BTreeMap<String, SimProject>
}

struct SimProject {
    name: String
}

impl Simulator {
    pub fn new()
        -> Self
    {
        Simulator {
            projects_by_name: BTreeMap::new()
        }
    }

    pub fn project_create(&mut self, project_name: &str)
    {
        let name = project_name.to_string();
        let project = SimProject {
            name: name
        };

        self.projects_by_name.insert(project_name.to_string(), project);
    }
}

#[async_trait]
impl ApiBackend for Simulator {
    async fn projects_list(&'static self)
        -> ApiListResult<ApiModelProject>
    {
        Ok(futures::stream::iter(self.projects_by_name
            .values()
            .map(|sim_project| Ok(ApiModelProject {
                name: sim_project.name.clone()
            }))).boxed())
    }
}

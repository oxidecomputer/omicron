/*!
 * API simulation of an Oxide rack, used for testing and prototyping.
 */

use futures::stream::StreamExt;
use async_trait::async_trait;

use crate::api_model;

/**
 * Maintains simulated state of the Oxide rack.  The current implementation is
 * in-memory only.
 */
pub struct Simulator {
    projects_by_name: std::collections::BTreeMap<String, SimProject>
}

struct SimProject {
    name: String
}

impl Simulator {
    pub fn new()
        -> Self
    {
        Simulator {
            projects_by_name: std::collections::BTreeMap::new()
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
impl api_model::ApiBackend for Simulator {
    async fn projects_list(&'static self)
        -> api_model::ApiListResult<api_model::ApiModelProject>
    {
        Ok(futures::stream::iter(self.projects_by_name
            .values()
            .map(|sim_project| Ok(api_model::ApiModelProject {
                name: sim_project.name.clone()
            }))).boxed())
    }
}

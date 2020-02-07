/*!
 * API simulation of an Oxide rack, used for testing and prototyping.
 */

use async_trait::async_trait;
use futures::lock::Mutex;
use futures::stream::StreamExt;
use std::collections::BTreeMap;

use crate::api_error::ApiError;
use crate::api_model::ApiBackend;
use crate::api_model::ApiCreateResult;
use crate::api_model::ApiListResult;
use crate::api_model::ApiModelProject;
use crate::api_model::ApiModelProjectCreate;

/**
 * SimulatorBuilder is used to initialize and populate a Simulator
 * synchronously, before we've wrapped the guts in a Mutex that can only be
 * locked in an async context.
 */
pub struct SimulatorBuilder {
    projects_by_name: BTreeMap<String, SimProject>
}

impl SimulatorBuilder {
    pub fn new() -> Self
    {
        SimulatorBuilder {
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

    pub fn build(self)
        -> Simulator
    {
        Simulator {
            projects_by_name: Mutex::new(self.projects_by_name)
        }
    }
}

/**
 * Maintains simulated state of the Oxide rack.  The current implementation is
 * in-memory only.
 */
pub struct Simulator {
    projects_by_name: Mutex<BTreeMap<String, SimProject>>
}

#[async_trait]
impl ApiBackend for Simulator {
    async fn projects_list(&self)
        -> ApiListResult<ApiModelProject>
    {
        /*
         * Assemble the list of projects under the lock, then stream the list
         * asynchronously.  This probably seems a little strange (why is this
         * async to begin with?), but the more realistic backend that we're
         * simulating is likely to be streaming this from a database or the
         * like.
         */
        let projects_by_name = self.projects_by_name.lock().await;
        let projects : Vec<Result<ApiModelProject, ApiError>> = projects_by_name
            .values()
            .map(|sim_project| Ok(sim_project.to_model()))
            .collect();

        Ok(futures::stream::iter(projects).boxed())
    }

    async fn project_create(&self, new_project: &ApiModelProjectCreate)
        -> ApiCreateResult<ApiModelProject>
    {
        let mut projects_by_name = self.projects_by_name.lock().await;
        if projects_by_name.contains_key(&new_project.name) {
            // XXX better error
            return Err(ApiError {});
        }

        let newname = &new_project.name;
        let project = SimProject { name: newname.clone() };
        let model_project = project.to_model();

        projects_by_name.insert(newname.clone(), project);
        Ok(model_project)
    }

    async fn project_delete(&self, project_id: &String)
        -> Result<(), ApiError>
    {
        // TODO conflating id with name here
        let mut projects_by_name = self.projects_by_name.lock().await;
        let oldvalue = projects_by_name.remove(project_id);
        if oldvalue.is_none() {
            // XXX better error
            return Err(ApiError {});
        }

        Ok(())
    }

    async fn project_lookup(&self, project_id: &String)
        -> Result<ApiModelProject, ApiError>
    {
        // TODO conflating id with name here
        let projects_by_name = self.projects_by_name.lock().await;
        let sim_project = projects_by_name.get(project_id);

        match sim_project {
            // XXX better error
            None => Err(ApiError {}),
            Some(p) => Ok(p.to_model())
        }
    }
}

/**
 * Representation of a Project within the simulated Backend.  This is
 * potentially distinct from the representation in the API, since the API may
 * contain a stable set of fields, while the underlying representation could
 * change.
 */
struct SimProject {
    name: String
}

impl SimProject {
    /**
     * Create an API representation of this Project.  You could think of this as
     * serializing the internal SimProject structure to pass it to the model
     * layer (which will then serialize _that_ for transfer over HTTP).
     */
    fn to_model(&self) -> ApiModelProject {
        ApiModelProject {
            name: self.name.clone()
        }
    }
}

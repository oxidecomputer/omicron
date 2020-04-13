/*!
 * Data storage interfaces for resources in the Oxide Rack.  Currently, this
 * just stores data in-memory, but the intent is to move this towards something
 * more like a distributed database.
 */

use crate::api_error::ApiError;
use crate::api_model::ApiIdentityMetadata;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiInstanceState;
use crate::api_model::ApiName;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_model::ApiResourceType;
use crate::api_model::DEFAULT_LIST_PAGE_SIZE;
use crate::rack::CreateResult;
use crate::rack::DeleteResult;
use crate::rack::ListResult;
use crate::rack::LookupResult;
use crate::rack::PaginationParams;
use crate::rack::UpdateResult;
use chrono::Utc;
use futures::lock::Mutex;
use futures::stream::StreamExt;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

pub struct RackDataStore {
    /** projects in the rack, indexed by name */
    projects_by_name: Mutex<BTreeMap<ApiName, Arc<ApiProject>>>,
    /** project instances, indexed by project name, then by instance name */
    instances_by_project_name:
        Mutex<BTreeMap<ApiName, BTreeMap<ApiName, Arc<ApiInstance>>>>,
}

impl RackDataStore {
    pub fn new() -> RackDataStore {
        RackDataStore {
            projects_by_name: Mutex::new(BTreeMap::new()),
            instances_by_project_name: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn project_create(
        &self,
        new_project: &ApiProjectCreateParams,
    ) -> CreateResult<ApiProject> {
        self.project_create_with_id(Uuid::new_v4(), new_project).await
    }

    pub async fn project_create_with_id(
        &self,
        new_uuid: Uuid,
        new_project: &ApiProjectCreateParams,
    ) -> CreateResult<ApiProject> {
        let mut projects_by_name = self.projects_by_name.lock().await;
        let mut project_instances = self.instances_by_project_name.lock().await;
        let newname = &new_project.identity.name;
        if projects_by_name.contains_key(&newname) {
            assert!(project_instances.contains_key(&newname));
            return Err(ApiError::ObjectAlreadyExists {
                type_name: ApiResourceType::Project,
                object_name: String::from(new_project.identity.name.clone()),
            });
        }

        assert!(!project_instances.contains_key(&newname));
        let now = Utc::now();
        let project = Arc::new(ApiProject {
            identity: ApiIdentityMetadata {
                id: new_uuid,
                name: newname.clone(),
                description: new_project.identity.description.clone(),
                time_created: now.clone(),
                time_modified: now.clone(),
            },
            generation: 1,
        });

        let rv = Arc::clone(&project);
        projects_by_name.insert(newname.clone(), project);
        project_instances.insert(newname.clone(), BTreeMap::new());
        Ok(rv)
    }

    pub async fn project_lookup(
        &self,
        name: &ApiName,
    ) -> LookupResult<ApiProject> {
        let mut projects = self.projects_by_name.lock().await;
        let project =
            collection_lookup(&mut projects, name, ApiResourceType::Project)?;
        let rv = Arc::clone(project);
        Ok(rv)
    }

    pub async fn projects_list(
        &self,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiProject> {
        let projects_by_name = self.projects_by_name.lock().await;
        collection_list(&projects_by_name, pagparams).await
    }

    pub async fn project_delete(&self, name: &ApiName) -> DeleteResult {
        let mut projects = self.projects_by_name.lock().await;
        let mut project_instances = self.instances_by_project_name.lock().await;
        projects.remove(name).ok_or_else(|| ApiError::ObjectNotFound {
            type_name: ApiResourceType::Project,
            object_name: String::from(name.clone()),
        })?;
        project_instances
            .remove(name)
            .expect("project existed but had no instances collection");
        Ok(())
    }

    pub async fn project_update(
        &self,
        name: &ApiName,
        new_params: &ApiProjectUpdateParams,
    ) -> UpdateResult<ApiProject> {
        let now = Utc::now();
        let mut projects = self.projects_by_name.lock().await;

        let oldproject: Arc<ApiProject> =
            projects.remove(name).ok_or_else(|| ApiError::ObjectNotFound {
                type_name: ApiResourceType::Project,
                object_name: String::from(name.clone()),
            })?;
        let newname = &new_params
            .identity
            .name
            .as_ref()
            .unwrap_or(&oldproject.identity.name);
        let newdescription = &new_params
            .identity
            .description
            .as_ref()
            .unwrap_or(&oldproject.identity.description);
        let newgen = oldproject.generation + 1;

        let newvalue = Arc::new(ApiProject {
            identity: ApiIdentityMetadata {
                id: oldproject.identity.id.clone(),
                name: (*newname).clone(),
                description: (*newdescription).clone(),
                time_created: oldproject.identity.time_created.clone(),
                time_modified: now.clone(),
            },
            generation: newgen,
        });

        let rv = Arc::clone(&newvalue);
        projects.insert(newvalue.identity.name.clone(), newvalue);
        Ok(rv)
    }

    /*
     * Instances
     */

    pub async fn project_list_instances(
        &self,
        project_name: &ApiName,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiInstance> {
        /*
         * TODO-cleanup a common function for looking up instances that returns
         * an appropriate ApiError would avoid this weird call where we ignore
         * the value (and in several other places as well).
         */
        self.project_lookup(project_name).await?;
        let project_instances = self.instances_by_project_name.lock().await;
        let instances = project_instances
            .get(project_name)
            .expect("project existed but had no instance collection");
        collection_list(&instances, pagparams).await
    }

    pub async fn project_create_instance(
        &self,
        project_name: &ApiName,
        params: &ApiInstanceCreateParams,
    ) -> CreateResult<ApiInstance> {
        let now = Utc::now();
        let newname = params.identity.name.clone();

        let projects = self.projects_by_name.lock().await;

        let project = collection_lookup(
            &projects,
            project_name,
            ApiResourceType::Project,
        )?;

        let mut project_instances = self.instances_by_project_name.lock().await;
        let instances = project_instances
            .get_mut(project_name)
            .expect("project existed but had no instance collection");

        if instances.contains_key(&newname) {
            return Err(ApiError::ObjectAlreadyExists {
                type_name: ApiResourceType::Instance,
                object_name: String::from(newname),
            });
        }

        let instance = Arc::new(ApiInstance {
            identity: ApiIdentityMetadata {
                id: Uuid::new_v4(),
                name: params.identity.name.clone(),
                description: params.identity.description.clone(),
                time_created: now.clone(),
                time_modified: now.clone(),
            },
            project_id: project.identity.id.clone(),
            ncpus: params.ncpus,
            memory: params.memory,
            boot_disk_size: params.boot_disk_size,
            hostname: params.hostname.clone(),
            /* TODO-debug: add state timestamp */
            state: ApiInstanceState::Starting,
        });

        let rv = Arc::clone(&instance);
        instances.insert(newname, instance);
        Ok(rv)
    }

    pub async fn project_lookup_instance(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> LookupResult<ApiInstance> {
        let projects = self.projects_by_name.lock().await;
        /* TODO-cleanup we're just doing this to handle the error case */
        collection_lookup(&projects, project_name, ApiResourceType::Project)?;
        let project_instances = self.instances_by_project_name.lock().await;
        let instances = project_instances
            .get(project_name)
            .expect("project existed but had no instance collection");
        let instance = collection_lookup(
            &instances,
            instance_name,
            ApiResourceType::Instance,
        )?;
        Ok(Arc::clone(instance))
    }

    pub async fn project_delete_instance(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> DeleteResult {
        let mut projects = self.projects_by_name.lock().await;
        /* TODO-cleanup we're just doing this to handle the error case */
        collection_lookup(
            &mut projects,
            project_name,
            ApiResourceType::Project,
        )?;
        let mut project_instances = self.instances_by_project_name.lock().await;
        let instances = project_instances
            .get_mut(project_name)
            .expect("project existed but had no instance collection");
        instances.remove(instance_name).ok_or_else(|| {
            ApiError::ObjectNotFound {
                type_name: ApiResourceType::Instance,
                object_name: String::from(instance_name.clone()),
            }
        })?;
        Ok(())
    }
}

/**
 * List a page of items from a collection.
 */
async fn collection_list<KeyType, ValueType>(
    tree: &BTreeMap<KeyType, Arc<ValueType>>,
    pagparams: &PaginationParams<KeyType>,
) -> ListResult<ValueType>
where
    KeyType: std::cmp::Ord,
    ValueType: Send + Sync + 'static,
{
    /* TODO-cleanup this logic should be in a wrapper function? */
    let limit = pagparams.limit.unwrap_or(DEFAULT_LIST_PAGE_SIZE);

    /*
     * We assemble the list of results that we're going to return now.  If the
     * caller is holding a lock, they'll be able to release it right away.  This
     * also makes the lifetime of the return value much easier.
     */
    let collect_items =
        |iter: &mut dyn Iterator<Item = (&KeyType, &Arc<ValueType>)>| {
            iter.take(limit)
                .map(|(_, arcitem)| Ok(Arc::clone(&arcitem)))
                .collect::<Vec<Result<Arc<ValueType>, ApiError>>>()
        };

    let items = match &pagparams.marker {
        None => collect_items(&mut tree.iter()),
        /*
         * NOTE: This range is inclusive on the low end because that
         * makes it easier for the client to know that it hasn't missed
         * some items in the namespace.  This does mean that clients
         * have to know to skip the first item on each page because
         * it'll be the same as the last item on the previous page.
         * TODO-cleanup would it be a problem to just make this an
         * exclusive bound?  It seems like you couldn't fail to see any
         * items that were present for the whole scan, which seems like
         * the main constraint.
         */
        Some(start_value) => collect_items(&mut tree.range(start_value..)),
    };

    Ok(futures::stream::iter(items).boxed())
}

/*
 * TODO-cleanup: for consistency and generality it would be nice if we could
 * make this take a KeyType type parameters, but I'm not sure how to specify the
 * bound that &KeyType: Into<String>
 */
fn collection_lookup<'a, 'b, ValueType>(
    tree: &'b BTreeMap<ApiName, Arc<ValueType>>,
    name: &'a ApiName,
    resource_type: ApiResourceType,
) -> Result<&'b Arc<ValueType>, ApiError> {
    Ok(tree.get(name).ok_or_else(|| ApiError::ObjectNotFound {
        type_name: resource_type,
        object_name: String::from(name.clone()),
    })?)
}

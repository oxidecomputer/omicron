/*!
 * Implementation of APIs for the Oxide Rack
 */

use crate::api_error::ApiError;
use crate::api_model::ApiIdentityMetadata;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiInstanceState;
use crate::api_model::ApiName;
use crate::api_model::ApiObject;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_model::ApiResourceType;
use crate::api_model::DEFAULT_LIST_PAGE_SIZE;
use chrono::Utc;
use futures::future::ready;
use futures::lock::Mutex;
use futures::stream::Stream;
use futures::stream::StreamExt;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

/*
 * These type aliases exist primarily to make it easier to be consistent about
 * return values from this module.
 */

/** Result of a create operation for the specified type. */
pub type CreateResult<T> = Result<Arc<T>, ApiError>;
/** Result of a delete operation for the specified type. */
pub type DeleteResult = Result<(), ApiError>;
/** Result of a list operation that returns an ObjectStream. */
pub type ListResult<T> = Result<ObjectStream<T>, ApiError>;
/** Result of a lookup operation for the specified type. */
pub type LookupResult<T> = Result<Arc<T>, ApiError>;
/** Result of an update operation for the specified type. */
pub type UpdateResult<T> = Result<Arc<T>, ApiError>;

/** A stream of Results, each potentially representing an object in the API. */
pub type ObjectStream<T> =
    Pin<Box<dyn Stream<Item = Result<Arc<T>, ApiError>> + Send>>;

#[derive(Deserialize)]
pub struct PaginationParams<NameType> {
    pub marker: Option<NameType>,
    pub limit: Option<usize>,
}

/**
 * Given an `ObjectStream<ApiObject>` (for some specific `ApiObject` type),
 * return a vector of the objects' views.  Any failures are ignored.
 * TODO-hardening: Consider how to better deal with these failures.  We should
 * probably at least log something.
 */
pub async fn to_view_list<T: ApiObject>(
    object_stream: ObjectStream<T>,
) -> Vec<T::View> {
    object_stream
        .filter(|maybe_object| ready(maybe_object.is_ok()))
        .map(|maybe_object| maybe_object.unwrap().to_view())
        .collect::<Vec<T::View>>()
        .await
}

/**
 * Represents the state of the Oxide rack that we're managing.
 */
pub struct OxideRack {
    /*
     * TODO-cleanup the data here about the contents of the rack should probably
     * be behind some other abstraction (like a "datastore"?).
     */
    /** Projects and instances in the rack. */
    projects_by_name: Arc<Mutex<BTreeMap<ApiName, Arc<ApiProject>>>>,
}

/*
 * TODO Is it possible to make some of these operations more generic?  A
 * particularly good example is probably list() (or even lookup()), where
 * with the right type parameters, generic code can be written to work on all
 * types.
 * TODO update and delete need to accommodate both with-etag and don't-care
 * TODO audit logging ought to be part of this structure and its functions
 */
impl OxideRack {
    pub fn new() -> OxideRack {
        OxideRack {
            projects_by_name: Arc::new(Mutex::new(BTreeMap::new())),
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
        if projects_by_name.contains_key(&new_project.identity.name) {
            return Err(ApiError::ObjectAlreadyExists {
                type_name: ApiResourceType::Project,
                object_name: String::from(new_project.identity.name.clone()),
            });
        }

        let now = Utc::now();
        let newname = &new_project.identity.name;
        let project = Arc::new(ApiProject {
            instances: Mutex::new(BTreeMap::new()),
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
        projects.remove(name).ok_or_else(|| ApiError::ObjectNotFound {
            type_name: ApiResourceType::Project,
            object_name: String::from(name.clone()),
        })?;
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

        let old_instances = oldproject.instances.lock().await;
        let newvalue = Arc::new(ApiProject {
            instances: Mutex::new(old_instances.clone()),
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
        let project = self.project_lookup(project_name).await?;
        let instances = project.instances.lock().await;
        collection_list(&instances, pagparams).await
    }

    pub async fn project_create_instance(
        &self,
        project_name: &ApiName,
        params: &ApiInstanceCreateParams,
    ) -> CreateResult<ApiInstance> {
        let now = Utc::now();
        let newname = params.identity.name.clone();

        let mut projects = self.projects_by_name.lock().await;
        let project = collection_lookup(
            &mut projects,
            project_name,
            ApiResourceType::Project,
        )?;
        let mut instances = project.instances.lock().await;
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
        let mut projects = self.projects_by_name.lock().await;
        let project = collection_lookup(
            &mut projects,
            project_name,
            ApiResourceType::Project,
        )?;
        let instances = project.instances.lock().await;
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
        let project = collection_lookup(
            &mut projects,
            project_name,
            ApiResourceType::Project,
        )?;
        let mut instances = project.instances.lock().await;

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

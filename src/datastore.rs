/*!
 * Data storage interfaces for resources in the Oxide system.  Currently, this
 * just stores data in-memory, but the intent is to move this towards something
 * more like a distributed database.
 */

use crate::api_error::ApiError;
use crate::api_model::ApiDisk;
use crate::api_model::ApiDiskState;
use crate::api_model::ApiIdentityMetadata;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiInstanceRuntimeState;
use crate::api_model::ApiName;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_model::ApiResourceType;
use crate::api_model::DEFAULT_LIST_PAGE_SIZE;
use crate::controller::CreateResult;
use crate::controller::DeleteResult;
use crate::controller::ListResult;
use crate::controller::LookupResult;
use crate::controller::PaginationParams;
use crate::controller::UpdateResult;
use chrono::Utc;
use futures::lock::Mutex;
use futures::stream::StreamExt;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

pub struct ControlDataStore {
    data: Mutex<CdsData>,
}

/*
 * TODO-cleanup: We could clean up the internal interfaces for projects here by
 * storing projects_by_id (a map from Uuid to Arc<ApiProject>).  We may want to
 * change `projects_by_name` to map from ApiName to Uuid.  This will allow a
 * clearer interface for getting information about a project by either id or
 * name without duplicating a reference to the project.
 */
struct CdsData {
    /** projects in the system, indexed by name */
    projects_by_name: BTreeMap<ApiName, Arc<ApiProject>>,
    /** project instances, indexed by project name, then by instance name */
    instances_by_project_id:
        BTreeMap<Uuid, BTreeMap<ApiName, Arc<ApiInstance>>>,
    /** project instances, indexed by Uuid */
    instances_by_id: BTreeMap<Uuid, Arc<ApiInstance>>,

    /** disks, indexed by Uuid */
    disks_by_id: BTreeMap<Uuid, Arc<ApiDisk>>,
    /** index mapping project name to tree of disks for that project. */
    disks_by_project_id: BTreeMap<Uuid, BTreeMap<ApiName, Uuid>>,
}

/*
 * TODO-cleanup
 * We should consider cleaning up the consumers' interface here.  Right now,
 * instances are sometimes operated on by (project_name, instance_name) tuple;
 * other times you provide an ApiInstance directly (from which we have the id).
 * Disks take the ApiDisk directly.  When you attach a disk, you provide the
 * project name, instance name, and disk name.
 *
 * One idea would be that there's a way to look up a project, disk, or instance
 * by name (and project *id*, for instances and disks), and after that, you have
 * to operate using the whole object (instead of its name).
 */
impl ControlDataStore {
    pub fn new() -> ControlDataStore {
        ControlDataStore {
            data: Mutex::new(CdsData {
                projects_by_name: BTreeMap::new(),
                instances_by_project_id: BTreeMap::new(),
                instances_by_id: BTreeMap::new(),
                disks_by_id: BTreeMap::new(),
                disks_by_project_id: BTreeMap::new(),
            }),
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
        let newname = &new_project.identity.name;

        let mut data = self.data.lock().await;
        assert!(!data.instances_by_project_id.contains_key(&new_uuid));
        assert!(!data.disks_by_project_id.contains_key(&new_uuid));
        if data.projects_by_name.contains_key(&newname) {
            return Err(ApiError::ObjectAlreadyExists {
                type_name: ApiResourceType::Project,
                object_name: String::from(new_project.identity.name.clone()),
            });
        }

        let now = Utc::now();
        let project = Arc::new(ApiProject {
            identity: ApiIdentityMetadata {
                id: new_uuid.clone(),
                name: newname.clone(),
                description: new_project.identity.description.clone(),
                time_created: now.clone(),
                time_modified: now.clone(),
            },
            generation: 1,
        });

        let rv = Arc::clone(&project);
        let projects_by_name = &mut data.projects_by_name;
        projects_by_name.insert(newname.clone(), project);
        data.instances_by_project_id.insert(new_uuid.clone(), BTreeMap::new());
        data.disks_by_project_id.insert(new_uuid, BTreeMap::new());
        Ok(rv)
    }

    pub async fn project_lookup(
        &self,
        name: &ApiName,
    ) -> LookupResult<ApiProject> {
        let data = self.data.lock().await;
        let project = collection_lookup(
            &data.projects_by_name,
            name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
        Ok(Arc::clone(project))
    }

    pub async fn projects_list(
        &self,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiProject> {
        let data = self.data.lock().await;
        collection_page(&data.projects_by_name, pagparams)
    }

    pub async fn project_delete(&self, name: &ApiName) -> DeleteResult {
        let mut data = self.data.lock().await;
        let project_id = {
            let project = collection_lookup(
                &data.projects_by_name,
                name,
                ApiResourceType::Project,
                &ApiError::not_found_by_name,
            )?;

            project.identity.id.clone()
        };
        let instances = data.instances_by_project_id.get(&project_id).unwrap();

        if instances.len() > 0 {
            return Err(ApiError::InvalidRequest {
                message: String::from("project still has instances"),
            });
        }

        data.instances_by_project_id.remove(&project_id).unwrap();
        data.disks_by_project_id.remove(&project_id).unwrap();
        data.projects_by_name.remove(name).unwrap();
        Ok(())
    }

    pub async fn project_update(
        &self,
        name: &ApiName,
        new_params: &ApiProjectUpdateParams,
    ) -> UpdateResult<ApiProject> {
        let now = Utc::now();
        let mut data = self.data.lock().await;
        let projects = &mut data.projects_by_name;

        let oldproject: Arc<ApiProject> =
            projects.remove(name).ok_or_else(|| {
                ApiError::not_found_by_name(ApiResourceType::Project, name)
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
        let data = self.data.lock().await;
        let project = collection_lookup(
            &data.projects_by_name,
            project_name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
        let project_instances = &data.instances_by_project_id;
        let instances = project_instances
            .get(&project.identity.id)
            .expect("project existed but had no instance collection");
        collection_page(&instances, pagparams)
    }

    pub async fn project_create_instance(
        &self,
        project_name: &ApiName,
        params: &ApiInstanceCreateParams,
        runtime_initial: &ApiInstanceRuntimeState,
    ) -> CreateResult<ApiInstance> {
        let now = Utc::now();
        let newname = params.identity.name.clone();

        let mut data = self.data.lock().await;

        let project_id = {
            let project = collection_lookup(
                &data.projects_by_name,
                project_name,
                ApiResourceType::Project,
                &ApiError::not_found_by_name,
            )?;
            project.identity.id.clone()
        };

        let instances = data
            .instances_by_project_id
            .get_mut(&project_id)
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
            project_id: project_id,
            ncpus: params.ncpus,
            memory: params.memory,
            boot_disk_size: params.boot_disk_size,
            hostname: params.hostname.clone(),
            runtime: runtime_initial.clone(),
        });

        instances.insert(newname, Arc::clone(&instance));
        data.instances_by_id
            .insert(instance.identity.id.clone(), Arc::clone(&instance));
        Ok(instance)
    }

    pub async fn project_lookup_instance(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> LookupResult<ApiInstance> {
        let data = self.data.lock().await;
        let project = collection_lookup(
            &data.projects_by_name,
            project_name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
        let project_instances = &data.instances_by_project_id;
        let instances = project_instances
            .get(&project.identity.id)
            .expect("project existed but had no instance collection");
        let instance = collection_lookup(
            &instances,
            instance_name,
            ApiResourceType::Instance,
            &ApiError::not_found_by_name,
        )?;
        Ok(Arc::clone(instance))
    }

    pub async fn project_delete_instance(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> DeleteResult {
        let mut data = self.data.lock().await;
        let project_id = {
            let project = collection_lookup(
                &data.projects_by_name,
                project_name,
                ApiResourceType::Project,
                &ApiError::not_found_by_name,
            )?;
            project.identity.id.clone()
        };
        let project_instances = &mut data.instances_by_project_id;
        let instances = project_instances
            .get_mut(&project_id)
            .expect("project existed but had no instance collection");
        let instance = instances.remove(instance_name).ok_or_else(|| {
            ApiError::not_found_by_name(
                ApiResourceType::Instance,
                instance_name,
            )
        })?;
        data.instances_by_id.remove(&instance.identity.id).unwrap();
        Ok(())
    }

    pub async fn instance_lookup_by_id(
        &self,
        id: &Uuid,
    ) -> LookupResult<ApiInstance> {
        let data = self.data.lock().await;
        Ok(Arc::clone(collection_lookup(
            &data.instances_by_id,
            id,
            ApiResourceType::Instance,
            &ApiError::not_found_by_id,
        )?))
    }

    /*
     * TODO-correctness This ought to take some kind of generation counter or
     * etag that can be used for optimistic concurrency control inside the
     * datastore.
     * TODO-cleanup We really ought to refactor this so that you don't need to
     * update two data structures.
     */
    pub async fn instance_update(
        &self,
        new_instance: Arc<ApiInstance>,
    ) -> Result<(), ApiError> {
        let id = new_instance.identity.id.clone();
        let instance_name = new_instance.identity.name.clone();
        let mut data = self.data.lock().await;
        let old_name = {
            let old_instance = collection_lookup(
                &data.instances_by_id,
                &id,
                ApiResourceType::Instance,
                &ApiError::not_found_by_id,
            )?;

            assert_eq!(old_instance.identity.id, id);
            old_instance.identity.name.clone()
        };

        /*
         * In case this update changes the name of the instance, remove it from
         * the list of instances in the project and re-add it with the new name.
         */
        let instances = data
            .instances_by_project_id
            .get_mut(&new_instance.project_id)
            .unwrap();
        instances.remove(&old_name).unwrap();
        instances.insert(instance_name, Arc::clone(&new_instance));
        data.instances_by_id.insert(id, Arc::clone(&new_instance)).unwrap();
        Ok(())
    }

    /**
     * List disks associated with a given instance.
     */
    pub async fn instance_list_disks(
        &self,
        instance: &Arc<ApiInstance>,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiDisk> {
        /*
         * For most of the other queries made to the data store, we keep data
         * structures indexing what we need.  And in a real database, we
         * probably would do that here too.  For this use-case, it doesn't seem
         * worthwhile.
         */
        let instance_id = &instance.identity.id;
        let project_id = &instance.project_id;

        let data = self.data.lock().await;
        let all_disks = &data.disks_by_id;
        let project_disks =
            data.disks_by_project_id.get(project_id).ok_or_else(|| {
                ApiError::not_found_by_id(ApiResourceType::Project, project_id)
            })?;
        let instance_disks_by_name = project_disks
            .iter()
            .map(|(disk_name, disk_id)| {
                (disk_name.clone(), Arc::clone(all_disks.get(disk_id).unwrap()))
            })
            .filter(|(_, disk)| match disk.runtime.disk_state {
                ApiDiskState::Attaching(id) if *instance_id == id => true,
                ApiDiskState::Attached(id) if *instance_id == id => true,
                ApiDiskState::Detaching(id) if *instance_id == id => true,

                ApiDiskState::Creating => false,
                ApiDiskState::Detached => false,
                ApiDiskState::Faulted => false,
                ApiDiskState::Destroyed => false,
                _ => false,
            })
            .collect::<BTreeMap<ApiName, Arc<ApiDisk>>>();
        collection_page(&instance_disks_by_name, pagparams)
    }

    /*
     * Disks
     */

    pub async fn project_list_disks(
        &self,
        project_name: &ApiName,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiDisk> {
        let data = self.data.lock().await;
        let project_id = {
            let project = collection_lookup(
                &data.projects_by_name,
                &project_name,
                ApiResourceType::Project,
                &ApiError::not_found_by_name,
            )?;
            project.identity.id.clone()
        };
        let all_disks = &data.disks_by_id;
        let disks_by_project = &data.disks_by_project_id;
        let project_disks = disks_by_project
            .get(&project_id)
            .expect("project existed but had no disk collection");
        collection_page_via_id(&project_disks, &pagparams, &all_disks)
    }

    pub async fn project_lookup_disk(
        &self,
        project_name: &ApiName,
        disk_name: &ApiName,
    ) -> LookupResult<ApiDisk> {
        let data = self.data.lock().await;
        let project = collection_lookup(
            &data.projects_by_name,
            project_name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
        let disks_by_project = &data.disks_by_project_id;
        let project_disks = disks_by_project
            .get(&project.identity.id)
            .expect("project existed but had no disk collection");
        Ok(Arc::clone(collection_lookup_via_id(
            project_disks,
            &data.disks_by_id,
            disk_name,
            ApiResourceType::Disk,
            &ApiError::not_found_by_name,
        )?))
    }

    pub async fn disk_create(
        &self,
        disk: Arc<ApiDisk>,
    ) -> CreateResult<ApiDisk> {
        let mut data = self.data.lock().await;

        let disk_id = &disk.identity.id;
        if let Some(_) = data.disks_by_id.get(&disk_id) {
            panic!("attempted to add disk that already exists");
        }

        let project_id = &disk.project_id;
        let project_disks =
            data.disks_by_project_id.get_mut(project_id).ok_or_else(|| {
                ApiError::not_found_by_id(ApiResourceType::Project, project_id)
            })?;

        let disk_name = &disk.identity.name;
        if let Some(_) = project_disks.get(&disk_name) {
            return Err(ApiError::ObjectAlreadyExists {
                type_name: ApiResourceType::Disk,
                object_name: String::from(disk_name.clone()),
            });
        }

        project_disks.insert(disk_name.clone(), disk_id.clone());
        data.disks_by_id.insert(disk_id.clone(), Arc::clone(&disk));
        Ok(disk)
    }

    pub async fn disk_lookup_by_id(&self, id: &Uuid) -> LookupResult<ApiDisk> {
        let data = self.data.lock().await;
        Ok(Arc::clone(collection_lookup(
            &data.disks_by_id,
            id,
            ApiResourceType::Disk,
            &ApiError::not_found_by_id,
        )?))
    }

    /*
     * TODO-correctness This ought to take some kind of generation counter or
     * etag that can be used for optimistic concurrency control inside the
     * datastore.
     * TODO-cleanup Can this be commonized with instance_update()?  It's awfully
     * parallel.
     */
    pub async fn disk_update(
        &self,
        new_disk: Arc<ApiDisk>,
    ) -> Result<(), ApiError> {
        let id = new_disk.identity.id.clone();
        let disk_name = new_disk.identity.name.clone();
        let mut data = self.data.lock().await;
        let old_name = {
            let old_disk = collection_lookup(
                &data.disks_by_id,
                &id,
                ApiResourceType::Disk,
                &ApiError::not_found_by_id,
            )?;

            assert_eq!(old_disk.identity.id, id);
            old_disk.identity.name.clone()
        };

        /*
         * In case this update changes the name, remove it from the list of
         * instances in the project and re-add it with the new name.
         */
        let disks =
            data.disks_by_project_id.get_mut(&new_disk.project_id).unwrap();
        disks.remove(&old_name).unwrap();
        disks.insert(disk_name, id);
        data.disks_by_id.insert(id, Arc::clone(&new_disk)).unwrap();
        Ok(())
    }

    /*
     * TODO-correctness This ought to take some kind of generation counter or
     * etag that can be used for optimistic concurrency control inside the
     * datastore.
     * TODO-cleanup Can this be commonized with instance_delete()?
     */
    pub async fn disk_delete(
        &self,
        new_disk: Arc<ApiDisk>,
    ) -> Result<(), ApiError> {
        let id = new_disk.identity.id.clone();
        let mut data = self.data.lock().await;
        let old_name = {
            let old_disk = collection_lookup(
                &data.disks_by_id,
                &id,
                ApiResourceType::Disk,
                &ApiError::not_found_by_id,
            )?;

            assert_eq!(old_disk.identity.id, id);
            old_disk.identity.name.clone()
        };

        let disks =
            data.disks_by_project_id.get_mut(&new_disk.project_id).unwrap();
        disks.remove(&old_name).unwrap();
        data.disks_by_id.remove(&id).unwrap();
        Ok(())
    }
}

/**
 * List a page of items from a collection.
 */
/*
 * TODO-cleanup this is only public because we haven't built servers into the
 * datastore yet so the controller needs this interface.
 */
pub fn collection_page<'a, 'b, KeyType, ValueType>(
    search_tree: &'a BTreeMap<KeyType, Arc<ValueType>>,
    pagparams: &'b PaginationParams<KeyType>,
) -> ListResult<ValueType>
where
    KeyType: std::cmp::Ord,
    ValueType: Send + Sync + 'static,
{
    /*
     * We assemble the list of results that we're going to return now.  If the
     * caller is holding a lock, they'll be able to release it right away.  This
     * also makes the lifetime of the return value much easier.
     */
    let list = collection_page_as_iter(search_tree, pagparams)
        .map(|(_, v)| Ok(Arc::clone(v)))
        .collect::<Vec<Result<Arc<ValueType>, ApiError>>>();
    Ok(futures::stream::iter(list).boxed())
}

fn collection_page_as_iter<'a, 'b, KeyType, ValueType>(
    search_tree: &'a BTreeMap<KeyType, ValueType>,
    pagparams: &'b PaginationParams<KeyType>,
) -> Box<dyn Iterator<Item = (&'a KeyType, &'a ValueType)> + 'a>
where
    KeyType: std::cmp::Ord,
{
    let limit = pagparams.limit.unwrap_or(DEFAULT_LIST_PAGE_SIZE);
    match &pagparams.marker {
        None => Box::new(search_tree.iter().take(limit)),
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
        Some(start_value) => {
            Box::new(search_tree.range(start_value..).take(limit))
        }
    }
}

fn collection_lookup<'a, 'b, KeyType, ValueType>(
    tree: &'b BTreeMap<KeyType, Arc<ValueType>>,
    lookup_key: &'a KeyType,
    resource_type: ApiResourceType,
    mkerror: &dyn Fn(ApiResourceType, &KeyType) -> ApiError,
) -> Result<&'b Arc<ValueType>, ApiError>
where
    KeyType: std::cmp::Ord,
{
    tree.get(lookup_key).ok_or_else(|| mkerror(resource_type, lookup_key))
}

fn collection_lookup_via_id<'a, 'b, KeyType, IdType, ValueType>(
    search_tree: &'b BTreeMap<KeyType, IdType>,
    value_tree: &'b BTreeMap<IdType, Arc<ValueType>>,
    lookup_key: &'a KeyType,
    resource_type: ApiResourceType,
    mkerror: &dyn Fn(ApiResourceType, &KeyType) -> ApiError,
) -> Result<&'b Arc<ValueType>, ApiError>
where
    KeyType: std::cmp::Ord,
    IdType: std::cmp::Ord,
{
    /*
     * The lookup into `search_tree` can fail if the object does not exist.
     * However, if that lookup suceeds, we must find the value in `value_tree`
     * or else our data structures are internally inconsistent.
     */
    let id = search_tree
        .get(lookup_key)
        .ok_or_else(|| mkerror(resource_type, lookup_key))?;
    Ok(value_tree.get(id).unwrap())
}

pub fn collection_page_via_id<KeyType, IdType, ValueType>(
    search_tree: &BTreeMap<KeyType, IdType>,
    pagparams: &PaginationParams<KeyType>,
    value_tree: &BTreeMap<IdType, Arc<ValueType>>,
) -> ListResult<ValueType>
where
    KeyType: std::cmp::Ord,
    IdType: std::cmp::Ord,
    ValueType: Send + Sync + 'static,
{
    let list = collection_page_as_iter(search_tree, pagparams)
        .map(|(_, id)| Ok(Arc::clone(value_tree.get(id).unwrap())))
        .collect::<Vec<Result<Arc<ValueType>, ApiError>>>();
    Ok(futures::stream::iter(list).boxed())
}

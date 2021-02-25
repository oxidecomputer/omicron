/*!
 * Simulated (in-memory) data storage for the Oxide control plane
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
use crate::api_model::DataPageParams;
use crate::api_model::PaginationOrder::Ascending;
use crate::api_model::PaginationOrder::Descending;

use crate::api_model::CreateResult;
use crate::api_model::DeleteResult;
use crate::api_model::ListResult;
use crate::api_model::LookupResult;
use crate::api_model::UpdateResult;

use chrono::Utc;
use futures::lock::Mutex;
use futures::stream::StreamExt;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use uuid::Uuid;

/**
 * Data storage interface exposed to the rest of OXC
 *
 * All the data is stored in the `data` field, protected by one big lock.
 */
pub struct ControlDataStore {
    data: Mutex<CdsData>,
}

/**
 * Contains the actual data structures to store control plane objects
 *
 * The methods exposed here should reflect what we expect would be exposed if
 * this were a traditional database or a distributed SQL-like database, since
 * that's ultimately what we expect to put here.
 */
struct CdsData {
    /** projects in the system */
    projects_by_id: BTreeMap<Uuid, Arc<ApiProject>>,
    /** index mapping project name to project id */
    projects_by_name: BTreeMap<ApiName, Uuid>,
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
 * by name (and project *id*, for instances and disks), and after that, you
 * always have to operate using the whole object (instead of its name).
 * Really, this should resemble what we expect to get from the real database.
 *
 * On the other hand: sagas create a use case where we want to be able to
 * do the lookup early in the saga and get back a token that can be used in
 * later steps.  That could be the id, but that might result in lots of extra
 * lookups.  That could be the object itself, but then that thing needs to be
 * serializable, and the database can't store its own state there.
 */
impl ControlDataStore {
    pub fn new() -> ControlDataStore {
        ControlDataStore {
            data: Mutex::new(CdsData {
                projects_by_id: BTreeMap::new(),
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
        assert!(!data.projects_by_id.contains_key(&new_uuid));
        if data.projects_by_name.contains_key(&newname) {
            return Err(ApiError::ObjectAlreadyExists {
                type_name: ApiResourceType::Project,
                object_name: String::from(new_project.identity.name.clone()),
            });
        }

        let now = Utc::now();
        let project = Arc::new(ApiProject {
            identity: ApiIdentityMetadata {
                id: new_uuid,
                name: newname.clone(),
                description: new_project.identity.description.clone(),
                time_created: now,
                time_modified: now,
            },
            generation: 1,
        });

        let rv = Arc::clone(&project);
        data.projects_by_id.insert(new_uuid, project);
        data.projects_by_name.insert(newname.clone(), new_uuid);
        data.instances_by_project_id.insert(new_uuid, BTreeMap::new());
        data.disks_by_project_id.insert(new_uuid, BTreeMap::new());
        Ok(rv)
    }

    pub async fn project_lookup(
        &self,
        name: &ApiName,
    ) -> LookupResult<ApiProject> {
        let data = self.data.lock().await;
        let project = collection_lookup_via_id(
            &data.projects_by_name,
            &data.projects_by_id,
            name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
        Ok(Arc::clone(project))
    }

    pub async fn projects_list_by_id(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<ApiProject> {
        let data = self.data.lock().await;
        collection_page(&data.projects_by_id, pagparams)
    }

    pub async fn projects_list_by_name(
        &self,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiProject> {
        let data = self.data.lock().await;
        collection_page_via_id(
            &data.projects_by_name,
            pagparams,
            &data.projects_by_id,
        )
    }

    pub async fn project_delete(&self, name: &ApiName) -> DeleteResult {
        let mut data = self.data.lock().await;
        let project_id = *collection_lookup(
            &data.projects_by_name,
            name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
        let instances = data.instances_by_project_id.get(&project_id).unwrap();

        if !instances.is_empty() {
            return Err(ApiError::InvalidRequest {
                message: String::from("project still has instances"),
            });
        }

        data.instances_by_project_id.remove(&project_id).unwrap();
        data.disks_by_project_id.remove(&project_id).unwrap();
        data.projects_by_id.remove(&project_id).unwrap();
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

        let project_id =
            data.projects_by_name.remove(name).ok_or_else(|| {
                ApiError::not_found_by_name(ApiResourceType::Project, name)
            })?;
        let oldproject = data.projects_by_id.get(&project_id).unwrap();
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
                id: project_id,
                name: (*newname).clone(),
                description: (*newdescription).clone(),
                time_created: oldproject.identity.time_created,
                time_modified: now,
            },
            generation: newgen,
        });

        let rv = Arc::clone(&newvalue);
        data.projects_by_name
            .insert(newvalue.identity.name.clone(), project_id);
        data.projects_by_id.insert(project_id, newvalue);
        Ok(rv)
    }

    /*
     * Instances
     */

    pub async fn project_list_instances(
        &self,
        project_name: &ApiName,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiInstance> {
        let data = self.data.lock().await;
        let project_id = collection_lookup(
            &data.projects_by_name,
            project_name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
        let project_instances = &data.instances_by_project_id;
        let instances = project_instances
            .get(&project_id)
            .expect("project existed but had no instance collection");
        collection_page(&instances, pagparams)
    }

    pub async fn project_create_instance(
        &self,
        instance_id: &Uuid,
        project_name: &ApiName,
        params: &ApiInstanceCreateParams,
        runtime_initial: &ApiInstanceRuntimeState,
    ) -> CreateResult<ApiInstance> {
        let now = Utc::now();
        let newname = params.identity.name.clone();

        let mut data = self.data.lock().await;

        let project_id = *collection_lookup(
            &data.projects_by_name,
            project_name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;

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
                id: *instance_id,
                name: params.identity.name.clone(),
                description: params.identity.description.clone(),
                time_created: now,
                time_modified: now,
            },
            project_id,
            ncpus: params.ncpus,
            memory: params.memory,
            boot_disk_size: params.boot_disk_size,
            hostname: params.hostname.clone(),
            runtime: runtime_initial.clone(),
        });

        instances.insert(newname, Arc::clone(&instance));
        data.instances_by_id
            .insert(instance.identity.id, Arc::clone(&instance));
        Ok(instance)
    }

    pub async fn project_lookup_instance(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> LookupResult<ApiInstance> {
        let data = self.data.lock().await;
        let project_id = collection_lookup(
            &data.projects_by_name,
            project_name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
        let project_instances = &data.instances_by_project_id;
        let instances = project_instances
            .get(&project_id)
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
        let project_id = *collection_lookup(
            &data.projects_by_name,
            project_name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
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
     */
    pub async fn instance_update(
        &self,
        new_instance: Arc<ApiInstance>,
    ) -> Result<(), ApiError> {
        let id = new_instance.identity.id;
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
        pagparams: &DataPageParams<'_, ApiName>,
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
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiDisk> {
        let data = self.data.lock().await;
        let project_id = collection_lookup(
            &data.projects_by_name,
            &project_name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
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
        let project_id = collection_lookup(
            &data.projects_by_name,
            project_name,
            ApiResourceType::Project,
            &ApiError::not_found_by_name,
        )?;
        let disks_by_project = &data.disks_by_project_id;
        let project_disks = disks_by_project
            .get(&project_id)
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
        if data.disks_by_id.get(&disk_id).is_some() {
            panic!("attempted to add disk that already exists");
        }

        let project_id = &disk.project_id;
        let project_disks =
            data.disks_by_project_id.get_mut(project_id).ok_or_else(|| {
                ApiError::not_found_by_id(ApiResourceType::Project, project_id)
            })?;

        let disk_name = &disk.identity.name;
        if project_disks.get(&disk_name).is_some() {
            return Err(ApiError::ObjectAlreadyExists {
                type_name: ApiResourceType::Disk,
                object_name: String::from(disk_name.clone()),
            });
        }

        project_disks.insert(disk_name.clone(), *disk_id);
        data.disks_by_id.insert(*disk_id, Arc::clone(&disk));
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
        let id = new_disk.identity.id;
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
         * disks in the project and re-add it with the new name.
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
        let id = new_disk.identity.id;
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
 * List a page of items from a collection `search_tree` that maps lookup keys
 * directly to the actual objects
 *
 * For objects that are stored using two mappings (one from lookup keys to ids,
 * and one from ids to values), see [`collection_page_via_id`].
 */
/*
 * TODO-cleanup this is only public because we haven't built Servers into the
 * datastore yet so the controller needs this interface.
 */
pub fn collection_page<KeyType, ValueType>(
    search_tree: &BTreeMap<KeyType, Arc<ValueType>>,
    pagparams: &DataPageParams<'_, KeyType>,
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

/**
 * Returns a page of items from a collection `search_tree` as an iterator
 */
fn collection_page_as_iter<'a, 'b, KeyType, ValueType>(
    search_tree: &'a BTreeMap<KeyType, ValueType>,
    pagparams: &'b DataPageParams<'_, KeyType>,
) -> Box<dyn Iterator<Item = (&'a KeyType, &'a ValueType)> + 'a>
where
    KeyType: std::cmp::Ord,
{
    let limit = pagparams.limit.get();
    match (pagparams.direction, &pagparams.marker) {
        (Ascending, None) => Box::new(search_tree.iter().take(limit)),
        (Descending, None) => Box::new(search_tree.iter().rev().take(limit)),
        (Ascending, Some(start_value)) => Box::new(
            search_tree
                .range((Bound::Excluded(*start_value), Bound::Unbounded))
                .take(limit),
        ),
        (Descending, Some(start_value)) => Box::new(
            search_tree
                .range((Bound::Unbounded, Bound::Excluded(*start_value)))
                .rev()
                .take(limit),
        ),
    }
}

/**
 * Look up a single item in a collection `tree` by its key `lookup_key`, where
 * `tree` maps the lookup key directly to the item that the caller is looking
 * for
 *
 * This is a convenience function used to generate an appropriate `ApiError` if
 * the object is not found.
 *
 * Some resources are stored by mapping a lookup key first to an id, and then
 * looking up this id in a separate tree.  For that kind of lookup, use
 * [`collection_lookup_via_id`].
 */
fn collection_lookup<'a, 'b, KeyType, ValueType>(
    tree: &'b BTreeMap<KeyType, ValueType>,
    lookup_key: &'a KeyType,
    resource_type: ApiResourceType,
    mkerror: &dyn Fn(ApiResourceType, &KeyType) -> ApiError,
) -> Result<&'b ValueType, ApiError>
where
    KeyType: std::cmp::Ord,
{
    tree.get(lookup_key).ok_or_else(|| mkerror(resource_type, lookup_key))
}

/**
 * Look up a single item in a collection `value_tree` by first looking up its id
 * in `search_tree` and then finding the item with that id in `value_tree`.
 */
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

/**
 * List a page of objects for objects that are stored using two mappings: one
 * from lookup keys to ids called `search_tree`, and one from ids to values
 * called `value_tree`
 */
pub fn collection_page_via_id<KeyType, IdType, ValueType>(
    search_tree: &BTreeMap<KeyType, IdType>,
    pagparams: &DataPageParams<'_, KeyType>,
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

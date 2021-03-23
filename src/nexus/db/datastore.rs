/*!
 * Primary control plane interface for database read and write operations
 */

use super::Pool;
use crate::api_error::ApiError;
use crate::api_model::ApiIdentityMetadata;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiInstanceRuntimeState;
use crate::api_model::ApiName;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiResourceType;
use crate::api_model::CreateResult;
use crate::api_model::LookupResult;
use chrono::Utc;
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

pub struct DataStore {
    pool: Arc<Pool>,
}

impl DataStore {
    pub fn new(pool: Arc<Pool>) -> Self {
        DataStore { pool }
    }

    pub async fn project_create_with_id(
        &self,
        new_id: &Uuid,
        new_project: &ApiProjectCreateParams,
    ) -> CreateResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        // XXX Error handling here needs to explicitly handle case of duplicate
        // key value (code 23505).
        let rows = client
            .query(
                "INSERT INTO Project \
            (id, name, description, time_created, time_metadata_updated,
            time_deleted) VALUES ($1, $2, $3, $4, $5, NULL) \
            RETURNING \
            id, name, description, time_created, time_metadata_updated,
            time_deleted",
                &[
                    new_id,
                    &new_project.identity.name.as_str(),
                    &new_project.identity.description,
                    &now,
                    &now,
                ],
            )
            .await
            .map_err(sql_error)?;
        let row = match rows.len() {
            1 => &rows[0],
            len => {
                return Err(ApiError::internal_error(&format!(
                    "expected 1 row from INSERT query, but found {}",
                    len
                )))
            }
        };

        // XXX With this design, do we really want to use Arc?
        Ok(Arc::new(ApiProject::try_from(row)?))
    }

    pub async fn project_lookup_id_by_name(
        &self,
        name: &ApiName,
    ) -> Result<Uuid, ApiError> {
        let client = self.pool.acquire().await?;

        let rows = client
            .query(
                "SELECT id FROM Project WHERE \
            name = $1 AND time_deleted IS NULL LIMIT 2",
                &[&name.as_str()],
            )
            .await
            .map_err(sql_error)?;
        match rows.len() {
            1 => Ok(rows[0].try_get(0).map_err(sql_error)?),
            0 => {
                Err(ApiError::not_found_by_name(ApiResourceType::Project, name))
            }
            /*
             * TODO-design This should really include a bunch more information,
             * like the SQL and the type of object we were expecting.
             */
            _ => Err(ApiError::internal_error(&format!(
                "expected 1 row from database query, but found {}",
                rows.len()
            ))),
        }
    }

    pub async fn project_create_instance(
        &self,
        instance_id: &Uuid,
        project_id: &Uuid,
        params: &ApiInstanceCreateParams,
        runtime_initial: &ApiInstanceRuntimeState,
    ) -> CreateResult<ApiInstance> {
        // XXX
        todo!();
    }

    pub async fn instance_lookup_by_id(
        &self,
        id: &Uuid,
    ) -> LookupResult<ApiInstance> {
        // XXX
        todo!();
    }

    pub async fn instance_update(
        &self,
        new_instance: Arc<ApiInstance>,
    ) -> Result<(), ApiError> {
        // XXX
        todo!();
    }
}

/* XXX Should this be From<>?  May want more context */
fn sql_error(e: tokio_postgres::Error) -> ApiError {
    use tokio_postgres::error::SqlState;

    match e.code() {
        None => ApiError::InternalError {
            message: format!("unexpected database error: {}", e.to_string()),
        },
        Some(code) => ApiError::InternalError {
            message: format!(
                "unexpected database error (code {}): {}",
                code.code(),
                e.to_string()
            ),
        },
    }
}

impl TryFrom<&tokio_postgres::Row> for ApiProject {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        // XXX really need some kind of context for these errors
        let name_str: &str = value.try_get("name").map_err(sql_error)?;
        let name = ApiName::try_from(name_str).map_err(|e| {
            ApiError::internal_error(&format!(
                "database project.name {:?}: {}",
                name_str, e
            ))
        })?;
        // XXX What to do with non-NULL time_deleted?
        Ok(ApiProject {
            generation: 1, // XXX
            identity: ApiIdentityMetadata {
                id: value.try_get("id").map_err(sql_error)?,
                name,
                description: value.try_get("description").map_err(sql_error)?,
                time_created: value
                    .try_get("time_created")
                    .map_err(sql_error)?,
                // XXX is it time_updated or time_metadata_updated
                time_modified: value
                    .try_get("time_metadata_updated")
                    .map_err(sql_error)?,
            },
        })
    }
}

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
use crate::api_model::ApiProjectUpdateParams;
use crate::api_model::ApiResourceType;
use crate::api_model::CreateResult;
use crate::api_model::DataPageParams;
use crate::api_model::DeleteResult;
use crate::api_model::ListResult;
use crate::api_model::LookupResult;
use crate::api_model::UpdateResult;
use chrono::Utc;
use futures::Future;
use futures::StreamExt;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio_postgres::row::Row;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

pub struct DataStore {
    pool: Arc<Pool>,
}

impl DataStore {
    pub fn new(pool: Arc<Pool>) -> Self {
        DataStore { pool }
    }

    /// Create a project
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
                    &new_project.identity.name,
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

    /// Fetch metadata for a project
    pub async fn project_fetch(
        &self,
        project_name: &ApiName,
    ) -> LookupResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let rows = client
            .query(
                "SELECT id, name, description, time_created, \
                time_metadata_updated FROM Project \
                WHERE time_deleted IS NULL AND name = $1 LIMIT 2",
                &[&project_name],
            )
            .await
            .map_err(sql_error)?;
        match rows.len() {
            1 => Ok(Arc::new(ApiProject::try_from(&rows[0])?)),
            0 => Err(ApiError::not_found_by_name(
                ApiResourceType::Project,
                project_name,
            )),
            len => Err(ApiError::internal_error(&format!(
                "expected at most one row from database query, but found {}",
                len
            ))),
        }
    }

    /// Delete a project
    pub async fn project_delete(&self, project_name: &ApiName) -> DeleteResult {
        /*
         * XXX TODO-correctness This needs to check whether the project has any
         * resources in it.  One way to do that would be to define a certain
         * kind of generation number, maybe called a "resource-creation
         * generation number" ("rcgen").  Every resource creation bumps the
         * "rgen" of the parent project.  This operation fetches the current
         * rgen of the project, then checks for various kinds of resources, then
         * does the following UPDATE that sets time_deleted _conditional_ on the
         * rgen being the same.  If this updates 0 rows, either the project is
         * gone already or a new resource has been created.  (We'll want to make
         * sure that we report the correct error!)  We'll want to think more
         * carefully about this scheme, and maybe alternatives.  (Another idea
         * would be to have resource creation and deletion update a regular
         * counter.  But isn't that the same as denormalizing this piece of
         * information?)
         *
         * Can we do all this in one big query, maybe with a few CTEs?  (e.g.,
         * something like:
         *
         * WITH project AS
         *     (select id from Project where time_deleted IS NULL and name =
         *     $1),
         *     project_instances as (select id from Instance where time_deleted
         *     IS NULL and project_id = project.id LIMIT 1),
         *     project_disks as (select id from Disk where time_deleted IS NULL
         *     and project_id = project.id LIMIT 1),
         *
         *     UPDATE Project set time_deleted = $1 WHERE time_deleted IS NULL
         *         AND id = project.id AND project_instances ...
         *
         * I'm not sure how to finish that SQL, and moreover, I'm not sure it
         * solves the problem.  You can still create a new instance after
         * listing the instances.  So I guess you still need the "rcgen".
         * Still, you could potentially combine these to do it all in one go:
         *
         * WITH project AS
         *     (select id,rcgen from Project where time_deleted IS NULL and
         *     name = $1),
         *     project_instances as (select id from Instance where time_deleted
         *     IS NULL and project_id = project.id LIMIT 1),
         *     project_disks as (select id from Disk where time_deleted IS NULL
         *     and project_id = project.id LIMIT 1),
         *
         *     UPDATE Project set time_deleted = $1 WHERE time_deleted IS NULL
         *         AND id = project.id AND rcgen = project.rcgen AND
         *         project_instances ...
         */
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        let rows = client
            .query(
                "UPDATE Project SET time_deleted = $1 WHERE \
                time_deleted IS NULL AND name = $2 LIMIT 2 \
                RETURNING id, name, description, time_created, \
                time_metadata_updated",
                &[&now, &project_name],
            )
            .await
            .map_err(sql_error)?;
        /* TODO-log log the returned row(s) */
        match rows.len() {
            1 => Ok(()),
            0 => Err(ApiError::not_found_by_name(
                ApiResourceType::Project,
                project_name,
            )),
            len => Err(ApiError::internal_error(&format!(
                "expected at most one row from database query, but found {}",
                len
            ))),
        }
    }

    /// Look up the id for a project based on its name
    pub async fn project_lookup_id_by_name(
        &self,
        name: &ApiName,
    ) -> Result<Uuid, ApiError> {
        let client = self.pool.acquire().await?;

        let rows = client
            .query(
                "SELECT id FROM Project WHERE \
            name = $1 AND time_deleted IS NULL LIMIT 2",
                &[&name],
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

    /// List a page of projects by id
    pub async fn projects_list_by_id(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let rows = sql_pagination(
            &client,
            "SELECT id, name, description, time_created, time_metadata_updated \
            FROM Project WHERE time_deleted IS NULL",
            "id",
            pagparams,
        ).await?;
        let list = rows
            .iter()
            .map(|row| ApiProject::try_from(row).map(Arc::new))
            .collect::<Vec<Result<Arc<ApiProject>, ApiError>>>();
        Ok(futures::stream::iter(list).boxed())
    }

    /// List a page of projects by name
    pub async fn projects_list_by_name(
        &self,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let rows = sql_pagination(
            &client,
            "SELECT id, name, description, time_created, time_metadata_updated \
            FROM Project WHERE time_deleted IS NULL",
            "name",
            pagparams
        ).await?;
        let list = rows
            .iter()
            .map(|row| ApiProject::try_from(row).map(Arc::new))
            .collect::<Vec<Result<Arc<ApiProject>, ApiError>>>();
        Ok(futures::stream::iter(list).boxed())
    }

    /// Updates a project by name (clobbering update -- no etag)
    pub async fn project_update(
        &self,
        project_name: &ApiName,
        update_params: &ApiProjectUpdateParams,
    ) -> UpdateResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();

        // XXX handle name conflict error explicitly
        let mut sql = String::from(
            "UPDATE Project \
            SET time_metadata_updated = $1 ",
        );
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&now];

        if let Some(new_name) = &update_params.identity.name {
            sql.push_str(&format!(", name = ${} ", params.len() + 1));
            params.push(new_name);
        }

        if let Some(new_description) = &update_params.identity.description {
            sql.push_str(&format!(", description = ${} ", params.len() + 1));
            params.push(new_description);
        }

        sql.push_str(&format!(
            " WHERE name = ${} AND time_deleted IS NULL LIMIT 2 RETURNING \
            id, name, description, time_created, time_metadata_updated,
            time_deleted",
            params.len() + 1
        ));
        params.push(project_name);

        let rows =
            client.query(sql.as_str(), &params).await.map_err(sql_error)?;
        match rows.len() {
            0 => Err(ApiError::not_found_by_name(
                ApiResourceType::Project,
                project_name,
            )),
            1 => Ok(Arc::new(ApiProject::try_from(&rows[0])?)),
            len => Err(ApiError::internal_error(&format!(
                "expected 1 row from UPDATE query, but found {}",
                len
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

/*
 * XXX building SQL like this sucks (obviously).  The 'static str here is just
 * to make it less likely we accidentally put a user-provided string here if
 * this sicence experiment escapes the lab.
 */
async fn sql_pagination<'a, T: ToSql + Send + Sync>(
    client: &'a tokio_postgres::Client,
    base_sql: &'static str,
    column_name: &'static str,
    pagparams: &'a DataPageParams<'a, T>,
) -> Result<Vec<Row>, ApiError> {
    let (operator, order) = match pagparams.direction {
        dropshot::PaginationOrder::Ascending => (">", "ASC"),
        dropshot::PaginationOrder::Descending => ("<", "DESC"),
    };

    let limit = i64::from(pagparams.limit.get());
    let query_result = if let Some(marker_value) = &pagparams.marker {
        let sql = format!(
            "{} AND {} {} $1 ORDER BY {} {} LIMIT $2",
            base_sql, column_name, operator, column_name, order
        );
        client.query(sql.as_str(), &[&marker_value, &limit]).await
    } else {
        let sql =
            format!("{} ORDER BY {} {} LIMIT $1", base_sql, column_name, order);
        client.query(sql.as_str(), &[&limit]).await
    };

    query_result.map_err(sql_error)
}

impl ToSql for ApiName {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<
        tokio_postgres::types::IsNull,
        Box<dyn std::error::Error + Sync + Send>,
    >
    where
        Self: Sized,
    {
        self.as_str().to_sql(ty, out)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool
    where
        Self: Sized,
    {
        <&str as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<
        tokio_postgres::types::IsNull,
        Box<dyn std::error::Error + Sync + Send>,
    > {
        self.as_str().to_sql_checked(ty, out)
    }
}

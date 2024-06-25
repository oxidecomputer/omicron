// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on timeseries schema.

use super::DataStore;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::raw_query_builder::QueryBuilder;
use crate::db::DbConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::Connection;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use diesel::BoolExpressionMethods as _;
use diesel::ExpressionMethods as _;
use diesel::JoinOnDsl as _;
use diesel::QueryDsl as _;
use diesel::SelectableHelper as _;
use nexus_auth::context::OpContext;
use nexus_db_model::schema::timeseries_field::dsl as field_dsl;
use nexus_db_model::schema::timeseries_field_by_version::dsl as version_dsl;
use nexus_db_model::schema::timeseries_schema::dsl as schema_dsl;
use nexus_db_model::Generation;
use nexus_db_model::TimeseriesAuthzScopeEnum;
use nexus_db_model::TimeseriesDatumTypeEnum;
use nexus_db_model::TimeseriesField;
use nexus_db_model::TimeseriesFieldByVersion;
use nexus_db_model::TimeseriesFieldSourceEnum;
use nexus_db_model::TimeseriesFieldTypeEnum;
use nexus_db_model::TimeseriesSchema;
use nexus_db_model::TimeseriesUnitsEnum;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::MessagePair;
use oximeter::schema::default_schema_version;
use std::collections::BTreeMap;
use std::num::NonZeroU8;

/// Helper used to select a timeseries version.
#[derive(Clone, Copy, Debug)]
enum VersionCheck {
    /// Select a timeseries no greater than this version.
    NoGreater(NonZeroU8),
    /// Select a timeseries with exactly this version.
    Exact(NonZeroU8),
}

impl VersionCheck {
    const fn version(&self) -> NonZeroU8 {
        match self {
            VersionCheck::NoGreater(v) => *v,
            VersionCheck::Exact(v) => *v,
        }
    }
}

/// Type alias for a tuple of database records selected from all three
/// timeseries schema tables, such as during a JOIN.
type SchemaTuple =
    (TimeseriesSchema, TimeseriesField, TimeseriesFieldByVersion);

impl DataStore {
    /// Load timeseries schema from their static definitions.
    ///
    /// **IMPORTANT**: This method does no authorization, and should only be
    /// called from a Nexus populator that loads data on startup.
    pub async fn load_timeseries_schema(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<()> {
        for schema in oximeter::all_timeseries_schema() {
            self.upsert_timeseries_schema(opctx, schema).await?
        }
        Ok(())
    }

    /// Fetch a page of timeseries schema from the database.
    pub async fn list_timeseries_schema(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (oximeter::TimeseriesName, NonZeroU8)>,
    ) -> ListResultVec<oximeter::TimeseriesSchema> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // Create the pagination marker.
        //
        // Note that we always need _something_ to put in the WHERE clause, to
        // avoid tripping the full-table-scan check, so on the first page use an
        // empty name (which should always sort before a real name) and the
        // default version.
        let (timeseries_name, version) = match pagparams.marker {
            Some((n, v)) => (n.to_string(), *v),
            None => (String::new(), default_schema_version()),
        };

        // Selecting the schema involves a 3-table JOIN, matching up rows from
        // the following tables:
        //
        // - `timeseries_schema`
        // - `timeseries_field`
        // - `timeseries_version_field`
        //
        // We match up all tables on the timeseries name; and the versioned
        // table additionally on the field name.
        let query = schema_dsl::timeseries_schema
            // JOIN between the timeseries schema and field tables just using
            // the timeseries name
            .inner_join(
                field_dsl::timeseries_field
                    .on(schema_dsl::timeseries_name
                        .eq(field_dsl::timeseries_name)),
            )
            // JOIN between that and the field-by-version table using:
            // - The timeseries name for the schema and field-by-version
            //   table.
            // - The field name for the field and field-by-version table
            .inner_join(
                version_dsl::timeseries_field_by_version.on(
                    schema_dsl::timeseries_name
                        .eq(version_dsl::timeseries_name)
                        .and(field_dsl::name.eq(version_dsl::field_name)),
                ),
            )
            // Select the record type from each table.
            .select(SchemaTuple::as_select())
            // Filter by the pagination marker, and limit the results to the
            // requested page size.
            .filter(
                schema_dsl::timeseries_name
                    .eq(timeseries_name.clone())
                    .and(version_dsl::version.gt(i16::from(version.get()))),
            )
            .or_filter(schema_dsl::timeseries_name.gt(timeseries_name.clone()))
            .order(schema_dsl::timeseries_name.asc())
            .then_order_by(version_dsl::version.asc())
            .limit(i64::from(pagparams.limit.get()));

        // Select the actual rows from the JOIN result.
        let rows = query
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // Rebuild the list of schema, by merging all the fields from each
        // version with the row from the `timeseries_schema` table. We'll do
        // this by building a mapping from name/version to the current schema,
        // and add fields into it as we process the JOIN result.
        let mut schema = BTreeMap::<
            (oximeter::TimeseriesName, NonZeroU8),
            oximeter::TimeseriesSchema,
        >::new();
        for (schema_row, field_row, versioned_row) in rows.into_iter() {
            let Ok(timeseries_name) = oximeter::TimeseriesName::try_from(
                schema_row.timeseries_name.as_str(),
            ) else {
                return Err(Error::internal_error(&format!(
                    "found invalid timeseries name in the database: '{}'",
                    schema_row.timeseries_name,
                )));
            };

            // Find the current schema, or add the one built from this current
            // row of the schema table. This will have an empty set of fields,
            // which will always be added to after looking up the schema.
            let Some(version) = NonZeroU8::new(*versioned_row.version) else {
                return Err(Error::internal_error(&format!(
                    "database contains invalid version \
                    number of 0 for timeseries '{}'",
                    timeseries_name,
                )));
            };
            let key = (timeseries_name.clone(), version);
            let is_new = schema
                .entry(key)
                .or_insert_with(|| {
                    schema_row
                        .into_bare_schema(version)
                        .expect("fallible parts checked above (name, version)")
                })
                .field_schema
                .insert(field_row.into());
            if !is_new {
                return Err(Error::internal_error(&format!(
                    "while fetching the schema for timeseries '{}' \
                    version {}, the field '{}' appears duplicated",
                    timeseries_name, version, versioned_row.field_name,
                )));
            }
        }

        Ok(schema.into_values().collect())
    }

    /// Fetch a single timeseries schema by name and version.
    pub async fn fetch_timeseries_schema(
        &self,
        opctx: &OpContext,
        timeseries_name: &oximeter::TimeseriesName,
        version: NonZeroU8,
    ) -> LookupResult<oximeter::TimeseriesSchema> {
        self.fetch_timeseries_schema_impl(
            opctx,
            timeseries_name,
            VersionCheck::Exact(version),
        )
        .await
        .map(|(schema, _generation)| schema)
    }

    /// Insert or update a timeseries schema.
    ///
    /// This will only modify the description columns if a record already
    /// exists, leaving things like the actual datum type and fields alone. Those
    /// cannot be changed within a timeseries version.
    async fn upsert_timeseries_schema(
        &self,
        opctx: &OpContext,
        schema: &oximeter::TimeseriesSchema,
    ) -> CreateResult<()> {
        // We first fetch the latest version of the timeseries no greater than
        // this one.
        //
        // The goal here is to handle three cases:
        //
        // - We are inserting a brand new schema
        // - We are adding a new _version_ of a schema, of which we've seen
        // previous versions
        // - We are updating the exact same version of the schema.
        //
        // Each of these cases is distinguished by fetching the latest schema
        // not greater than the current version. The return value of that for
        // each of the above situations is, correspondingly:
        //
        // - an `Error::NotFound`
        // - a schema with a previous version
        // - a schema with the exact same version
        //
        // And in each of these cases, we take the following actions:
        //
        // # Brand new schema
        //
        // In this case, we have found no records for the schema at all. We
        // insert a new record in the `timeseries_schema` table, and new records
        // in both the `timeseries_field` table and
        // `timeseries_field_by_version` table.
        let maybe_existing = self
            .fetch_latest_timeseries_schema(
                opctx,
                &schema.timeseries_name,
                schema.version,
            )
            .await;
        match maybe_existing {
            Ok((existing_schema, generation)) => {
                if existing_schema.version == schema.version {
                    self.upsert_existing_timeseries_schema(
                        opctx,
                        schema,
                        Some(existing_schema),
                        generation,
                    )
                    .await
                } else if existing_schema.version < schema.version {
                    self.upsert_existing_timeseries_schema(
                        opctx, schema, None, generation,
                    )
                    .await
                } else {
                    unreachable!();
                }
            }
            Err(Error::NotFound { .. }) => {
                self.insert_new_timeseries_schema(opctx, schema).await
            }
            Err(e) => Err(e),
        }
    }

    /// Fetch the timeseries schema by name with a version no greater than
    /// `version`.
    async fn fetch_latest_timeseries_schema(
        &self,
        opctx: &OpContext,
        timeseries_name: &oximeter::TimeseriesName,
        version: NonZeroU8,
    ) -> LookupResult<(oximeter::TimeseriesSchema, Generation)> {
        self.fetch_timeseries_schema_impl(
            opctx,
            timeseries_name,
            VersionCheck::NoGreater(version),
        )
        .await
    }

    /// Internal implementation to fetch records from the DB and reconstitute
    /// the `oximeter::TimeseriesSchema` from them, if possible.
    async fn fetch_timeseries_schema_impl(
        &self,
        opctx: &OpContext,
        timeseries_name: &oximeter::TimeseriesName,
        version_check: VersionCheck,
    ) -> LookupResult<(oximeter::TimeseriesSchema, Generation)> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // 3-table JOIN between the schema, fields, and fields-by-version. All
        // use the timeseries name. See `list_timeseries_schema()` for a more
        // detailed explanation.
        let rows = match version_check {
            VersionCheck::NoGreater(version) => {
                self.fetch_timeseries_schema_no_greater_than_version(
                    &conn,
                    timeseries_name,
                    version,
                )
                .await?
            }
            VersionCheck::Exact(version) => {
                self.fetch_timeseries_schema_exact_version(
                    &conn,
                    timeseries_name,
                    version,
                )
                .await?
            }
        };

        // We may have selected more than one version of the schema, if we
        // looked up the schema no greater than our current version. Filter
        // things down so that we're only looking at the latest version of it.
        let Some(extracted_version) =
            rows.iter().map(|(_, _, version_row)| version_row.version).max()
        else {
            return Err(Error::non_resourcetype_not_found(format!(
                "Timeseries '{}' version {} not found",
                timeseries_name,
                version_check.version(),
            )));
        };
        let mut rows = rows.into_iter().filter(|(_, _, version_row)| {
            version_row.version == extracted_version
        });
        let Some(extracted_version) = NonZeroU8::new(*extracted_version) else {
            return Err(Error::internal_error(
                "while fetching the schema for timeseries '{}' \
                version {}, version of 0 was returned from the DB",
            ));
        };

        // Extract just the schema itself from the JOIN result.
        //
        // This has no fields, but has all the other metadata for the schema.
        let Some((schema_row, field_row, version_row)) = rows.next() else {
            // Safety: We just found the max version, or returned an error if
            // the list of rows was empty, and then filtered down the rows to
            // those with the max version. That came from the rows themselves,
            // so the iterator cannot be empty.
            unreachable!();
        };

        // Construct the bare schem, using the _retrieved_ version number. We
        // may have selected something older depending on how this method was
        // called.
        let generation = schema_row.generation;
        let mut schema = schema_row.into_bare_schema(extracted_version)?;

        // Attach all field rows, including the one we already fetched above.
        let first_row = std::iter::once((field_row, version_row));
        let remaining_rows = rows.map(|(_, f, v)| (f, v));
        for (field_row, version_row) in first_row.chain(remaining_rows) {
            if field_row.generation != generation
                || version_row.generation != generation
            {
                return Err(Error::internal_error(&format!(
                    "while fetching the schema for timeseries '{}' \
                    version {}, generation numbers do not all match, \
                    schema generation = {}, field generation = {}, \
                    versioned field generation = {}",
                    timeseries_name,
                    extracted_version,
                    generation.0,
                    field_row.generation.0,
                    version_row.generation.0,
                )));
            }
            let is_new = schema.field_schema.insert(field_row.into());
            if !is_new {
                return Err(Error::internal_error(&format!(
                    "while fetching the schema for timeseries '{}' \
                    version {}, the field '{}' appears duplicated",
                    timeseries_name, extracted_version, version_row.field_name,
                )));
            }
        }
        Ok((schema, generation))
    }

    /// Select a timeseries schema from the database by name, whose version is
    /// less than or equal to the provided version.
    ///
    /// NOTE: This and `fetch_timeseries_schema_exact_version` are different
    /// functions to avoid descending into diesel trait bound hell, which is
    /// what happens if we try to specify just the comparison on the version
    /// column itself (e.g., version = 1 or version <= 1).
    async fn fetch_timeseries_schema_no_greater_than_version(
        &self,
        conn: &Connection<DbConnection>,
        timeseries_name: &oximeter::TimeseriesName,
        version: NonZeroU8,
    ) -> Result<Vec<SchemaTuple>, Error> {
        schema_dsl::timeseries_schema
            .inner_join(
                field_dsl::timeseries_field
                    .on(schema_dsl::timeseries_name
                        .eq(field_dsl::timeseries_name)),
            )
            .inner_join(
                version_dsl::timeseries_field_by_version.on(
                    schema_dsl::timeseries_name
                        .eq(version_dsl::timeseries_name)
                        .and(field_dsl::name.eq(version_dsl::field_name)),
                ),
            )
            .select(SchemaTuple::as_select())
            .filter(
                schema_dsl::timeseries_name
                    .eq(timeseries_name.to_string())
                    .and(version_dsl::version.le(i16::from(version.get()))),
            )
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Select a timeseries schema from the database by name, whose version is
    /// exactly equal to the provided version.
    async fn fetch_timeseries_schema_exact_version(
        &self,
        conn: &Connection<DbConnection>,
        timeseries_name: &oximeter::TimeseriesName,
        version: NonZeroU8,
    ) -> Result<Vec<SchemaTuple>, Error> {
        schema_dsl::timeseries_schema
            .inner_join(
                field_dsl::timeseries_field
                    .on(schema_dsl::timeseries_name
                        .eq(field_dsl::timeseries_name)),
            )
            .inner_join(
                version_dsl::timeseries_field_by_version.on(
                    schema_dsl::timeseries_name
                        .eq(version_dsl::timeseries_name)
                        .and(field_dsl::name.eq(version_dsl::field_name)),
                ),
            )
            .select(SchemaTuple::as_select())
            .filter(
                schema_dsl::timeseries_name
                    .eq(timeseries_name.to_string())
                    .and(version_dsl::version.eq(i16::from(version.get()))),
            )
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Upsert a timeseries schema with a pre-existing version.
    ///
    /// There are two flavors of this function, depending on whether the
    /// `existing_schema` argument is Some or None.
    ///
    /// # `existing_schema` is Some
    ///
    /// This is intended to operrate when the following conditions obtain:
    ///
    /// - We have a matching record in the `timeseries_schema` table;
    /// - We have matching records in the `timeseries_field` table; and
    /// - We have matching records in the `timeseries_field_by_version` table.
    ///
    /// In other words, we've already seen this exact timeseries schema, at this
    /// exact same version number.
    ///
    /// In this case we update:
    ///
    /// - the `timeseries_schema` columns `description`, `time_modified` and
    /// `generation`
    /// - the `timeseries_field` columns `description`, `time_modified` and
    /// `generation`
    ///
    /// Nothing else is inserted.
    ///
    /// # `existing_schema` is None
    ///
    /// This is intended to operate when the following conditions obtain:
    ///
    /// - We have a matching record in the `timeseries_schema` table;
    /// - We have matching records in the `timeseries_field` table; and
    /// - We have **no** matching records in the `timeseries_field_by_version`
    /// table.
    ///
    /// In other words, we have seen _some_ previous version of the schema, but
    /// not this exact version.
    ///
    /// In this case, we update:
    ///
    /// - the `timeseries_schema` columns `description`, `time_modified` and
    /// `generation`
    /// - the `timeseries_field` columns `description`, `time_modified` and
    /// `generation`
    ///
    /// And we insert the field version rows themselves, with the same
    /// `generation` as the other tables.
    ///
    /// # OCC
    ///
    /// In both cases above, we make the update of the timeseries schema table
    /// conditional on the generation number not having changed. If it _has_
    /// changed, we generate an error that fails the entire transaction. This
    /// avoids concurrent modifications.
    async fn upsert_existing_timeseries_schema(
        &self,
        opctx: &OpContext,
        schema: &oximeter::TimeseriesSchema,
        existing_schema: Option<oximeter::TimeseriesSchema>,
        existing_generation: Generation,
    ) -> CreateResult<()> {
        if let Some(existing_schema) = &existing_schema {
            if !schema.eq_ignoring_descriptions(existing_schema) {
                return Err(Error::conflict(&format!(
                    "Timeseries '{}' version {} appears to already \
                    have a record in the database, but whose datum type, \
                    units, authz scope, or field schema differ",
                    schema.timeseries_name, schema.version,
                )));
            }
        }

        // Collect the data for each table derived from the new schema, using
        // the next generation for OCC.
        let generation = Generation::from(existing_generation.next());
        let field_rows = TimeseriesField::for_schema(schema, generation);
        let fields_by_version =
            TimeseriesFieldByVersion::for_schema(schema, generation);
        let conn = self.pool_connection_authorized(opctx).await?;

        // Build out a CTE to update the descriptions, modification times, and
        // generations only. Nothing else changes.
        let mut builder = QueryBuilder::new()
            .sql("\
                WITH updated_schema AS (\
                UPDATE timeseries_schema \
                SET (\
                    target_description, \
                    metric_description, \
                    time_modified, \
                    generation\
                ) = ("
            )
            .param()
            .bind::<sql_types::Text, _>(schema.description.target.clone())
            .sql(", ")
            .param()
            .bind::<sql_types::Text, _>(schema.description.metric.clone())
            .sql(", NOW(), timeseries_schema.generation + 1) WHERE timeseries_name = ")
            .param()
            .bind::<sql_types::Text, _>(schema.timeseries_name.to_string())
            .sql(" AND generation = ")
            .param()
            .bind::<sql_types::Int8, _>(existing_generation)
            .sql(" RETURNING timeseries_name), ");

        // Update description, modification time, and generation of each field.
        //
        // Because this requires updating different rows with different values,
        // it's written as an `INSERT ... ON CONFLICT ... DO UPDATE`
        builder = builder
            .sql("updated_fields AS (INSERT INTO timeseries_field VALUES ");
        let n_rows = field_rows.len();
        for (i, field_row) in field_rows.into_iter().enumerate() {
            let tail = if i == n_rows - 1 { ")" } else { "), " };
            builder = builder
                .sql("(")
                .param()
                .bind::<sql_types::Text, _>(field_row.timeseries_name)
                .sql(", ")
                .param()
                .bind::<sql_types::Text, _>(field_row.name)
                .sql(", ")
                .param()
                .bind::<TimeseriesFieldSourceEnum, _>(field_row.source)
                .sql(", ")
                .param()
                .bind::<TimeseriesFieldTypeEnum, _>(field_row.type_)
                .sql(", ")
                .param()
                .bind::<sql_types::Text, _>(field_row.description)
                .sql(", ")
                .param()
                .bind::<sql_types::Timestamptz, _>(field_row.time_created)
                .sql(", ")
                .param()
                .bind::<sql_types::Timestamptz, _>(field_row.time_modified)
                .sql(", ")
                .param()
                .bind::<sql_types::Int8, _>(field_row.generation)
                .sql(tail);
        }
        builder = builder
            .sql(
                " \
            ON CONFLICT (timeseries_name, name) \
            DO UPDATE SET \
                (description, time_modified, generation) = \
                (excluded.description, NOW(), timeseries_field.generation + 1) \
            WHERE timeseries_field.timeseries_name = ",
            )
            .param()
            .bind::<sql_types::Text, _>(schema.timeseries_name.to_string())
            .sql(" AND timeseries_field.generation = ")
            .param()
            .bind::<sql_types::Int8, _>(existing_generation)
            .sql(" RETURNING timeseries_name, name AS field_name)");

        // And insert the row for each version / field. This works differently
        // if we're inserting a new version or updating an existing version. If
        // an existing one, then we model it similar to above: an `INSERT ... ON
        // CONFLICT ... DO UPDATE`. Otherwise, we just insert the rows directly.
        builder = builder.sql(
            ", \
            inserted_versions AS (\
            INSERT INTO timeseries_field_by_version \
            VALUES ",
        );
        let n_rows = fields_by_version.len();
        for (i, row) in fields_by_version.into_iter().enumerate() {
            let tail = if i == n_rows - 1 { ") " } else { "), " };
            builder = builder
                .sql("(")
                .param()
                .bind::<sql_types::Text, _>(row.timeseries_name)
                .sql(", ")
                .param()
                .bind::<sql_types::Int2, _>(row.version)
                .sql(", ")
                .param()
                .bind::<sql_types::Text, _>(row.field_name)
                .sql(", ")
                .param()
                .bind::<sql_types::Int8, _>(row.generation)
                .sql(tail);
        }

        // Close the last CTE and return the sanity-checking data.
        builder = if let Some(existing_schema) = &existing_schema {
            builder
                .sql(
                    "\
                ON CONFLICT (timeseries_name, field_name, version) \
                DO UPDATE SET \
                    generation = timeseries_field_by_version.generation + 1 \
                WHERE timeseries_field_by_version.timeseries_name = ",
                )
                .param()
                .bind::<sql_types::Text, _>(schema.timeseries_name.to_string())
                .sql(" AND timeseries_field_by_version.version = ")
                .param()
                .bind::<sql_types::Int2, _>(i16::from(
                    existing_schema.version.get(),
                ))
                .sql(" AND timeseries_field_by_version.generation = ")
                .param()
                .bind::<sql_types::Int8, _>(existing_generation)
        } else {
            builder
        };
        builder = builder
            .sql(" \
                RETURNING timeseries_name, field_name) \
                SELECT COUNT(*) \
                FROM updated_schema, updated_fields, inserted_versions \
                WHERE updated_schema.timeseries_name = updated_fields.timeseries_name \
                AND updated_schema.timeseries_name = inserted_versions.timeseries_name"
            );

        // Actually run the main query.
        builder
            .query::<sql_types::Int8>()
            .get_result_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .and_then(|n_rows: i64| {
                if n_rows == 0 {
                    let op = if existing_schema.is_some() {
                        "update an existing version"
                    } else {
                        "insert a new version"
                    };
                    let message = MessagePair::new_full(
                        format!(
                            "concurrent modification detected while \
                            inserting timeseries '{}' version {}",
                            schema.timeseries_name, schema.version,
                        ),
                        format!(
                            "used schema generation {} from the \
                            database for OCC, but we modified zero \
                            rows when attempting to {} of timeseries \
                            '{}' version {}. that implies the \
                            generation changed between our select \
                            and the insert that would modify the \
                            records.",
                            *existing_generation,
                            op,
                            schema.timeseries_name,
                            schema.version,
                        ),
                    );
                    Err(Error::Conflict { message })
                } else {
                    Ok(())
                }
            })
    }

    /// Insert a brand new timeseries schema, where we have no previous
    /// versions.
    ///
    /// In this case, we insert records in all three related tables with
    /// generation = 1. We detect and log conflicts, but return success in that
    /// case.
    async fn insert_new_timeseries_schema(
        &self,
        opctx: &OpContext,
        schema: &oximeter::TimeseriesSchema,
    ) -> CreateResult<()> {
        // Collect the data for each table derived from the new schema. These
        // all have generation 1, since we're assuming we're inserting the first
        // records.
        let generation = Generation::new();
        let schema_row = TimeseriesSchema::new(schema, generation);
        let field_rows = TimeseriesField::for_schema(schema, generation);
        let fields_by_version =
            TimeseriesFieldByVersion::for_schema(schema, generation);
        let conn = self.pool_connection_authorized(opctx).await?;

        // We'll build out a CTE to insert all the records at once. First, the
        // (single) record of the timeseries schema itself.
        let mut builder = QueryBuilder::new()
            .sql(
                "WITH \
                inserted_schema AS (\
                INSERT INTO timeseries_schema \
                VALUES (",
            )
            .param()
            .bind::<sql_types::Text, _>(schema_row.timeseries_name)
            .sql(", ")
            .param()
            .bind::<TimeseriesAuthzScopeEnum, _>(schema_row.authz_scope)
            .sql(", ")
            .param()
            .bind::<sql_types::Text, _>(schema_row.target_description)
            .sql(", ")
            .param()
            .bind::<sql_types::Text, _>(schema_row.metric_description)
            .sql(", ")
            .param()
            .bind::<TimeseriesDatumTypeEnum, _>(schema_row.datum_type)
            .sql(", ")
            .param()
            .bind::<TimeseriesUnitsEnum, _>(schema_row.units)
            .sql(", ")
            .param()
            .bind::<sql_types::Timestamptz, _>(schema_row.time_created)
            .sql(", ")
            .param()
            .bind::<sql_types::Timestamptz, _>(schema_row.time_modified)
            .sql(", ")
            .param()
            .bind::<sql_types::Int8, _>(schema_row.generation)
            .sql(
                ") \
                RETURNING timeseries_schema.generation AS generation), \
                inserted_fields AS (\
                INSERT INTO timeseries_field \
                VALUES ",
            );

        // Insert the row for each field.
        let n_rows = field_rows.len();
        for (i, field_row) in field_rows.into_iter().enumerate() {
            let tail = if i == n_rows - 1 { ")" } else { "), " };
            builder = builder
                .sql("(")
                .param()
                .bind::<sql_types::Text, _>(field_row.timeseries_name)
                .sql(", ")
                .param()
                .bind::<sql_types::Text, _>(field_row.name)
                .sql(", ")
                .param()
                .bind::<TimeseriesFieldSourceEnum, _>(field_row.source)
                .sql(", ")
                .param()
                .bind::<TimeseriesFieldTypeEnum, _>(field_row.type_)
                .sql(", ")
                .param()
                .bind::<sql_types::Text, _>(field_row.description)
                .sql(", ")
                .param()
                .bind::<sql_types::Timestamptz, _>(field_row.time_created)
                .sql(", ")
                .param()
                .bind::<sql_types::Timestamptz, _>(field_row.time_modified)
                .sql(", ")
                .param()
                .bind::<sql_types::Int8, _>(field_row.generation)
                .sql(tail);
        }

        // And insert the row for each version / field.
        builder = builder.sql(
            " \
                RETURNING 1 AS dummy1), \
                inserted_versions AS (\
                INSERT INTO timeseries_field_by_version \
                VALUES ",
        );
        let n_rows = fields_by_version.len();
        for (i, row) in fields_by_version.into_iter().enumerate() {
            let tail = if i == n_rows - 1 { ")" } else { "), " };
            builder = builder
                .sql("(")
                .param()
                .bind::<sql_types::Text, _>(row.timeseries_name)
                .sql(", ")
                .param()
                .bind::<sql_types::Int2, _>(row.version)
                .sql(", ")
                .param()
                .bind::<sql_types::Text, _>(row.field_name)
                .sql(", ")
                .param()
                .bind::<sql_types::Int8, _>(row.generation)
                .sql(tail);
        }

        // Close the last CTE and return the sanity-checking data.
        builder = builder.sql(
            " \
                RETURNING 1 as dummy2) \
                SELECT COUNT(*) FROM inserted_fields",
        );

        // Actually run the main query.
        builder
            .query::<sql_types::Int8>()
            .get_result_async(&*conn)
            .await
            .map_err(|e| match e {
                DieselError::DatabaseError(
                    DatabaseErrorKind::UniqueViolation,
                    ..,
                ) => {
                    // Someone else inserted between when we first looked for a
                    // schema (and didn't find one), and now, so bail out with a
                    // conflict error.
                    Error::conflict(format!(
                        "Failed to insert schema for timeseries '{}'. \
                        Records for it were added between the initial \
                        check and our attempt to insert it",
                        schema.timeseries_name,
                    ))
                }
                e => public_error_from_diesel(e, ErrorHandler::Server),
            })
            .and_then(|count: i64| {
                let Ok(count) = usize::try_from(count) else {
                    return Err(Error::internal_error(&format!(
                        "Unable to convert sanity check count from i64 \
                        to usize! Count of inserted rows in timeseries_field \
                        table is {}",
                        count,
                    )));
                };
                if count == schema.field_schema.len() {
                    Ok(())
                } else {
                    Err(Error::internal_error(&format!(
                        "Expected to insert {} rows in the timeseries_field \
                        table when inserting new schema, but looks like we \
                        inserted {} instead",
                        schema.field_schema.len(),
                        count,
                    )))
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::db::datastore::test_utils::datastore_test;
    use crate::db::raw_query_builder::QueryBuilder;
    use async_bb8_diesel::AsyncRunQueryDsl as _;
    use chrono::Utc;
    use dropshot::PaginationOrder;
    use nexus_db_model::Generation;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::DataPageParams;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev;
    use oximeter::schema::AuthzScope;
    use oximeter::schema::FieldSource;
    use oximeter::schema::TimeseriesDescription;
    use oximeter::schema::TimeseriesSchema;
    use oximeter::schema::Units;
    use oximeter::DatumType;
    use oximeter::FieldSchema;
    use oximeter::FieldType;
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;

    #[tokio::test]
    async fn correctly_recall_timeseries_schema() {
        let logctx = dev::test_setup_log("correctly_recall_timeseries_schema");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let mut expected_schema = vec![
            TimeseriesSchema {
                timeseries_name: "foo:bar".try_into().unwrap(),
                description: TimeseriesDescription {
                    target: "a target".into(),
                    metric: "a metric".into(),
                },
                field_schema: BTreeSet::from([
                    FieldSchema {
                        name: "f0".into(),
                        field_type: FieldType::Uuid,
                        source: FieldSource::Target,
                        description: "target field 0".into(),
                    },
                    FieldSchema {
                        name: "f1".into(),
                        field_type: FieldType::Bool,
                        source: FieldSource::Metric,
                        description: "metric field 1".into(),
                    },
                ]),
                datum_type: DatumType::HistogramI64,
                version: 1.try_into().unwrap(),
                authz_scope: AuthzScope::Fleet,
                units: Units::Count,
                created: Utc::now(),
            },
            TimeseriesSchema {
                timeseries_name: "foo:bar".try_into().unwrap(),
                description: TimeseriesDescription {
                    target: "a target".into(),
                    metric: "a metric".into(),
                },
                field_schema: BTreeSet::from([
                    FieldSchema {
                        name: "f0".into(),
                        field_type: FieldType::Uuid,
                        source: FieldSource::Target,
                        description: "target field 0".into(),
                    },
                    FieldSchema {
                        name: "f1".into(),
                        field_type: FieldType::Bool,
                        source: FieldSource::Metric,
                        description: "metric field 1".into(),
                    },
                ]),
                datum_type: DatumType::HistogramI64,
                version: 2.try_into().unwrap(),
                authz_scope: AuthzScope::Fleet,
                units: Units::Count,
                created: Utc::now(),
            },
            TimeseriesSchema {
                timeseries_name: "baz:quux".try_into().unwrap(),
                description: TimeseriesDescription {
                    target: "a target".into(),
                    metric: "a metric".into(),
                },
                field_schema: BTreeSet::from([
                    FieldSchema {
                        name: "f0".into(),
                        field_type: FieldType::String,
                        source: FieldSource::Target,
                        description: "target field 0".into(),
                    },
                    FieldSchema {
                        name: "f1".into(),
                        field_type: FieldType::IpAddr,
                        source: FieldSource::Metric,
                        description: "metric field 1".into(),
                    },
                ]),
                datum_type: DatumType::U64,
                version: 1.try_into().unwrap(),
                authz_scope: AuthzScope::ViewableToAll,
                units: Units::Count,
                created: Utc::now(),
            },
        ];
        for schema in expected_schema.iter() {
            datastore
                .upsert_timeseries_schema(&opctx, schema)
                .await
                .expect("expected to insert valid schema");
        }
        let pagparams = DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(u32::MAX).unwrap(),
        };
        let schema = datastore
            .list_timeseries_schema(&opctx, &pagparams)
            .await
            .expect("expected to list previously-inserted schema");

        // The DB will always return them sorted by name / version, so we'll do
        // that to our inputs to compare easily.
        expected_schema.sort_by(|a, b| {
            a.timeseries_name
                .cmp(&b.timeseries_name)
                .then(a.version.cmp(&b.version))
        });
        for (actual, expected) in schema.iter().zip(expected_schema.iter()) {
            println!("actual\n{actual:#?}");
            println!("expected\n{expected:#?}\n\n");
        }
        assert!(
            schema
                .iter()
                .zip(expected_schema.iter())
                .all(|(lhs, rhs)| lhs.eq_ignoring_creation_time(rhs)),
            "Failed to recall expected timeseries schema from database",
        );
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn upsert_schema_updates_descriptions() {
        let logctx = dev::test_setup_log("upsert_schema_updates_descriptions");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let original_schema = TimeseriesSchema {
            timeseries_name: "foo:bar".try_into().unwrap(),
            description: TimeseriesDescription {
                target: "a target".into(),
                metric: "a metric".into(),
            },
            field_schema: BTreeSet::from([
                FieldSchema {
                    name: "f0".into(),
                    field_type: FieldType::Uuid,
                    source: FieldSource::Target,
                    description: "target field 0".into(),
                },
                FieldSchema {
                    name: "f1".into(),
                    field_type: FieldType::Bool,
                    source: FieldSource::Metric,
                    description: "metric field 1".into(),
                },
            ]),
            datum_type: DatumType::HistogramI64,
            version: 1.try_into().unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Count,
            created: Utc::now(),
        };
        datastore
            .upsert_timeseries_schema(&opctx, &original_schema)
            .await
            .expect("expected to insert valid schema");

        // Upsert a schema with modified descriptions.
        let mut modified_schema = original_schema.clone();
        modified_schema.description.target = "a new target".into();
        modified_schema.description.metric = "a new metric".into();
        let field = modified_schema.field_schema.pop_first().unwrap();
        modified_schema.field_schema.replace(FieldSchema {
            description: "new target field 0".into(),
            ..field
        });
        datastore
            .upsert_timeseries_schema(&opctx, &modified_schema)
            .await
            .expect("expected to insert schema with only new descriptions");

        let fetched_schema = datastore
            .fetch_timeseries_schema(
                &opctx,
                &original_schema.timeseries_name,
                original_schema.version,
            )
            .await
            .expect("expected to fetch updated timeseries schema");

        // CockroachDB rounds timestamps to 6 digits (microsecond precision),
        // see https://www.cockroachlabs.com/docs/stable/timestamp#precision for
        // details. We compare the schema ignoring the timestamps, and then
        // assert that the timestamps are within that precision as well.
        assert!(
            fetched_schema.eq_ignoring_creation_time(&modified_schema),
            "Failed to upsert new schema with modified descriptions",
        );
        let nanos = (fetched_schema.created - modified_schema.created)
            .num_nanoseconds()
            .expect("nanos should not overflow");
        assert!(
            nanos <= 1000,
            "Expected creation timestamps to be equal within \
            CRDB precision of 1 microsecond, but the difference \
            is {}ns",
            nanos,
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn upsert_schema_fails_with_new_datum_type() {
        let logctx =
            dev::test_setup_log("upsert_schema_fails_with_new_datum_type");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let original_schema = TimeseriesSchema {
            timeseries_name: "foo:bar".try_into().unwrap(),
            description: TimeseriesDescription {
                target: "a target".into(),
                metric: "a metric".into(),
            },
            field_schema: BTreeSet::from([FieldSchema {
                name: "f0".into(),
                field_type: FieldType::Uuid,
                source: FieldSource::Target,
                description: "target field 0".into(),
            }]),
            datum_type: DatumType::HistogramI64,
            version: 1.try_into().unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Count,
            created: Utc::now(),
        };
        datastore
            .upsert_timeseries_schema(&opctx, &original_schema)
            .await
            .expect("expected to insert valid schema");

        // Upsert a schema with modified datum type. This should not be
        // possible, given the way we construct timeseries schema, but we
        // disallow it here in any case.
        let modified_schema = oximeter::TimeseriesSchema {
            datum_type: DatumType::F64,
            ..original_schema.clone()
        };
        let err = datastore
            .upsert_timeseries_schema(&opctx, &modified_schema)
            .await
            .expect_err("expected to insert schema with only new descriptions");
        assert!(
            matches!(err, Error::Conflict { .. }),
            "Expected a conflict when attempting to insert \
            an existing timeseries schema with a new datum \
            type, but found: {err:#?}",
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn upsert_schema_fails_on_concurrent_modification() {
        let logctx = dev::test_setup_log(
            "upsert_schema_fails_on_concurrent_modification",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let original_schema = TimeseriesSchema {
            timeseries_name: "foo:bar".try_into().unwrap(),
            description: TimeseriesDescription {
                target: "a target".into(),
                metric: "a metric".into(),
            },
            field_schema: BTreeSet::from([FieldSchema {
                name: "f0".into(),
                field_type: FieldType::Uuid,
                source: FieldSource::Target,
                description: "target field 0".into(),
            }]),
            datum_type: DatumType::HistogramI64,
            version: 1.try_into().unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Count,
            created: Utc::now(),
        };
        datastore
            .upsert_timeseries_schema(&opctx, &original_schema)
            .await
            .expect("expected to insert valid schema");

        // The goal here is to ensure that we fail our OCC check, if the
        // generation of the existing schema changes between when we read it,
        // determine what "upserting" means, and then actually perform the
        // upsert query.
        //
        // We know that the above is the first generation, so let's directly
        // muck with it to simulate a concurrent modification from another
        // client.
        let n_rows = QueryBuilder::new()
            .sql(
                "\
                UPDATE timeseries_schema \
                SET generation = generation + 1 \
                WHERE timeseries_name = 'foo:bar' \
                AND generation = 1",
            )
            .query::<diesel::sql_types::Int8>()
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(n_rows, 1, "Expected to modify exactly one row");

        let n_rows = QueryBuilder::new()
            .sql(
                "\
                UPDATE timeseries_field \
                SET generation = generation + 1 \
                WHERE timeseries_name = 'foo:bar'
                AND name = 'f0' \
                AND generation = 1",
            )
            .query::<diesel::sql_types::Int8>()
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(n_rows, 1, "Expected to modify exactly one row");

        let n_rows = QueryBuilder::new()
            .sql(
                "\
                UPDATE timeseries_field_by_version \
                SET generation = generation + 1 \
                WHERE timeseries_name = 'foo:bar'
                AND field_name = 'f0' \
                AND generation = 1",
            )
            .query::<diesel::sql_types::Int8>()
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(n_rows, 1, "Expected to modify exactly one row");

        // Now, let's pretend that we had previously fetched exactly the same
        // version of the schema, at that same generation. We'll attempt update
        // the descriptions, which is a no-op, but should still happen at the
        // database itself.
        let result = datastore
            .upsert_existing_timeseries_schema(
                &opctx,
                &original_schema,
                Some(original_schema.clone()),
                Generation::new(), // It _was_ generation 1 when it was "fetched"
            )
            .await;
        assert!(
            matches!(result, Err(Error::Conflict { .. })),
            "Expected a conflict error when a schema generation number \
            changed between selecting the original data and inserting \
            our modified records."
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}

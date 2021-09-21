//! Macro for creating an identity struct for tables.

/// Produces a new "IdentityMetadata" object within a "$table_identity"
/// module, containing a common set of identity-related types along
/// with conversions to/from the external API type.
///
/// Example:
///
/// ```
/// // Produces "instance_identity::IdentityMetadata", which can be
/// // directly embedded for usage within the "instance" table.
/// define_identity!(instance);
/// ```
//
//  TODO TODO TODO TODO TODO
//  ... and now for the sad truth.
//
//  This should be a proc macro. A derive one, probably.
//
//  #[derive(IdentityMetadata)]
//
//  - Would already have access to "table_name", no need for
//  repetition. Could use it like diesel does in the Selectable
//  example.
//  - 99% sure we can just directly emit "ProjectIdentity" as
//  we need.
//  - Also add the shortcut accessor functions (id, name),
//  etc directly to the target structure.
//
//
macro_rules! define_identity {
    {$table:ident} => {
        paste::paste! {
            mod [<$table _identity>] {
                use chrono::{DateTime, Utc};
                use omicron_common::api::external;
                use uuid::Uuid;

                #[derive(Clone, Debug, Selectable, Queryable, Insertable)]
                #[table_name = "crate::db::schema::" [< $table >] ]
                pub struct IdentityMetadata {
                    pub id: Uuid,
                    pub name: external::Name,
                    pub description: String,
                    pub time_created: DateTime<Utc>,
                    pub time_modified: DateTime<Utc>,
                    pub time_deleted: Option<DateTime<Utc>>,
                }

                impl IdentityMetadata {
                    pub fn new(id: Uuid, params: external::IdentityMetadataCreateParams) -> Self {
                        let now = Utc::now();
                        Self {
                            id,
                            name: params.name,
                            description: params.description,
                            time_created: now,
                            time_modified: now,
                            time_deleted: None,
                        }
                    }
                }

                impl Into<external::IdentityMetadata> for IdentityMetadata {
                    fn into(self) -> external::IdentityMetadata {
                        external::IdentityMetadata {
                            id: self.id,
                            name: self.name,
                            description: self.description,
                            time_created: self.time_created,
                            time_modified: self.time_modified,
                        }
                    }
                }

                impl From<external::IdentityMetadata> for IdentityMetadata {
                    fn from(metadata: external::IdentityMetadata) -> Self {
                        Self {
                            id: metadata.id,
                            name: metadata.name,
                            description: metadata.description,
                            time_created: metadata.time_created,
                            time_modified: metadata.time_modified,
                            time_deleted: None,
                        }
                    }
                }
            }
        }
    };
}

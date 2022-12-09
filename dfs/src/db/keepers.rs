//! The Keepers table is a junction table between File and Node, defining which nodes hold which files

use serde::{Deserialize, Serialize};
use sqlx::query;

use super::{
    schema::{Schema, SqlxQuery},
    Database,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeepersRow {
    pub id: i32,
    pub file_id: i32,
    pub node_id: i32,
}

pub struct Keepers<'a>(&'a Database);

impl<'a> Schema<'a> for Keepers<'a> {
    const TABLENAME: &'static str = "keepers";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_keepers.sql"))
    }

    fn with(db: &'a Database) -> Self {
        Self(db)
    }
}
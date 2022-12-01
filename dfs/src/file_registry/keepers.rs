//! The Keepers table is a junction table between File and Node, defining which nodes hold which files

use serde::{Deserialize, Serialize};
use sqlx::query;

use super::schema::{Schema, SqlxQuery};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Keepers {
    pub id: i32,
    pub file_id: i32,
    pub node_id: i32,
}

impl Schema for Keepers {
    const TABLENAME: &'static str = "keepers";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_keepers.sql"))
    }
}

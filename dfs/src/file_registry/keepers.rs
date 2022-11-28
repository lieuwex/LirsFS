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
        query(
            "
            CREATE TABLE IF NOT EXISTS ? (
                id          integer primary key,
                file_id     integer
                node_id     integer
                FOREIGN KEY(node_id) REFERENCES node(id)
                FOREIGN KEY(file_id) REFERENCES file(id)
            );
        ",
        )
        .bind(Self::TABLENAME)
    }
}

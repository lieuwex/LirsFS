//! The File table holds information about every file in the LirsFs

use sqlx::query;

use super::schema::Schema;

#[derive(Clone, Debug, PartialEq)]

pub struct File {
    pub file_id: i32,

    pub file_path: String,

    pub content_hash: u64,

    pub replication_factor: u32,
}

impl Schema for File {
    const TABLENAME: &'static str = "files";

    fn create_table_query(
    ) -> query::Query<'static, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'static>> {
        query(
            "
            CREATE TABLE IF NOT EXISTS ? ( 
                id integer primary key, 
                path text not null, 
                hash blob not null, 
                replication_factor integer not null 
            );  
            ",
        )
        .bind(Self::TABLENAME)
    }
}

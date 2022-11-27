//! The File table holds information about every file in the LirsFs

use sea_orm::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "file")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub file_id: i32,

    pub file_path: String,

    pub content_hash: u64,

    pub replication_factor: u32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl Related<super::node::Entity> for Entity {
    fn to() -> RelationDef {
        super::keepers::Relation::Node.def()
    }

    fn via() -> Option<RelationDef> {
        Some(super::keepers::Relation::File.def().rev())
    }
}

impl ActiveModelBehavior for ActiveModel {}

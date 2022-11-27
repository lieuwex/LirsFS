//! The Node table keeps track of every compute node in the LirsFs
//!
use sea_orm::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "node")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub node_id: i32,

    pub name: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl Related<super::file::Entity> for Entity {
    fn to() -> RelationDef {
        super::keepers::Relation::File.def()
    }

    fn via() -> Option<RelationDef> {
        Some(super::keepers::Relation::Node.def().rev())
    }
}

impl ActiveModelBehavior for ActiveModel {}

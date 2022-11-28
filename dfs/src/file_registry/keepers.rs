//! The Keepers table is a junction table between File and Node, defining which nodes hold which files

#[derive(Clone, Debug, PartialEq)]
pub struct Model {
    pub id: i32,
    pub file_id: i32,
    pub node_id: i32,
}

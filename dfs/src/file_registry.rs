pub mod file;
pub mod keepers;
pub mod node;
pub mod schema;

// pub struct FileRegistry {
//     db: DatabaseConnection,
// }

// impl FileRegistry {
//     pub async fn new() -> Self {
//         Self {
//             db: Database::connect(CONFIG.file_registry.clone())
//                 .await
//                 .unwrap_or_else(|err| {
//                     panic!("Could not connect to SQLite database for FileRegistry: {err}")
//                 }),
//         }
//     }

//     pub async fn create_file() {}
// }

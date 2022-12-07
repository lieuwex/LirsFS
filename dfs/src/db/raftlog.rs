use async_raft::{
    raft::{Entry, EntryConfigChange, EntryPayload, MembershipConfig},
    AppData,
};
use sqlx::{query, Pool, QueryBuilder, Sqlite, SqlitePool};

use crate::client_req::AppClientRequest;

use super::{
    curr_snapshot, db,
    schema::{Schema, SqlxQuery},
};

/// SQLite does not handle u64 types; only i64.
/// Therefore we save the raft log id (also called `index`) as an i64,
/// but in our application, use it as a u64.
pub type RaftLogId = u64;

/// SQLite does not handle u64 types; only i64.
/// Therefore we save the raft term as an i64,
/// but in our application, use it as a u64.
pub type RaftLogTerm = u64;

/// Store operations that the Raft cluster should perform as raw bytes, serialized by `bincode`.
pub type RaftLogEntry = Vec<u8>;

pub enum RaftLogEntryType {
    Blank = 0,
    Normal = 1,
    ConfigChange = 2,
    SnapShotPointer = 3,
}

impl<T: AppData> From<&EntryPayload<T>> for RaftLogEntryType {
    fn from(e: &EntryPayload<T>) -> Self {
        match e {
            EntryPayload::Blank => RaftLogEntryType::Blank,
            EntryPayload::Normal(_) => RaftLogEntryType::Normal,
            EntryPayload::ConfigChange(_) => RaftLogEntryType::ConfigChange,
            EntryPayload::SnapshotPointer(_) => RaftLogEntryType::SnapShotPointer,
        }
    }
}

pub struct RaftLogRow {
    id: RaftLogId,
    term: RaftLogTerm,
    entry: RaftLogEntry,
    entry_type: RaftLogEntryType,
}

/// Repository that is backed by the `raftlog` table in the SQLite database.
/// An instance of this struct represents a single entry in that table.
/// From the outside, however, you interact with the table through associated methods that accept [RaftLogId]s and [Entry<AppClientRequest>]s.
/// Use these associated methods to perform CRUD operations.
#[derive(Clone, Debug)]
pub struct RaftLog(Pool<Sqlite>);

impl RaftLog {
    pub async fn delete_range(&self, from: RaftLogId, to: RaftLogId) {
        let from = from as i64;
        let to = to as i64;
        query!(
            "
            DELETE FROM raftlog
            WHERE id >= ? AND id < ?;",
            from,
            to
        )
        .execute(&self.0)
        .await
        .unwrap_or_else(|err| {
            panic!(
                "Could not delete id range {}-{} from {}: {:?}",
                from,
                to,
                Self::TABLENAME,
                err
            )
        });
    }

    pub async fn delete_from(&self, from: RaftLogId) {
        let from = from as i64;
        query!(
            "
            DELETE FROM raftlog
            WHERE id >= ?;
        ",
            from
        )
        .execute(&self.0)
        .await
        .unwrap_or_else(|err| {
            panic!(
                "Could not delete from id {} from {}: {:?}",
                from,
                Self::TABLENAME,
                err
            )
        });
    }

    /// Inserts the given Raft log entries into the SQLite database.
    pub async fn insert(&self, entries: &[Entry<AppClientRequest>]) {
        let mut query = QueryBuilder::<Sqlite>::new("INSERT INTO raftlog (id,term,entry) ");
        let values_to_insert = entries.iter().map(RaftLogRow::from);

        query.push_values(
            values_to_insert,
            |mut b,
             RaftLogRow {
                 id,
                 term,
                 entry,
                 entry_type,
             }| {
                b.push_bind(id as i64)
                    .push_bind(term as i64)
                    .push_bind(entry)
                    .push_bind(entry_type as i64);
            },
        );
        query.build().execute(&self.0).await.unwrap_or_else(|err| {
            panic!(
                "Could not insert log entry/entries into {}: {:?}",
                Self::TABLENAME,
                err
            )
        });
    }

    /// Retrieves the given range of raft log entries and serializes them into [Entry<AppClientRequest>]s.
    pub async fn get_range(&self, from: RaftLogId, to: RaftLogId) -> Vec<Entry<AppClientRequest>> {
        let from = from as i64;
        let to = to as i64;
        query!(
            "
            SELECT * 
            FROM raftlog
            WHERE id >= ? AND id < ?;
        ",
            from,
            to
        )
        .fetch_all(&self.0)
        .await
        .unwrap_or_else(|err| {
            panic!(
                "Could not get log entries with id {}-{} from {}: {}",
                from,
                to,
                Self::TABLENAME,
                err
            )
        })
        .iter()
        .map(|record| Entry {
            term: record.term as RaftLogTerm,
            index: record.id as RaftLogId,
            // TODO: Better deserialization error handling
            payload: bincode::deserialize(&record.entry).expect("Deserializing entry failed"),
        })
        .collect::<Vec<Entry<AppClientRequest>>>()
    }

    /// Retrieves the raft log entry with the given `id` and serializes it into [Entry<AppClientRequest>]s.
    /// Returns [None] if entry is not found.
    pub async fn get_by_id(&self, id: RaftLogId) -> Option<Entry<AppClientRequest>> {
        let id = id as i64;
        query!(
            "
            SELECT * 
            FROM raftlog
            WHERE id == ?
        ",
            id
        )
        .fetch_optional(&self.0)
        .await
        .unwrap_or_else(|err| {
            panic!(
                "Could not get log entry with id {} from {}: {}",
                id,
                Self::TABLENAME,
                err
            )
        })
        .map(|record| {
            Entry {
                term: record.term as RaftLogTerm,
                index: record.id as RaftLogId,
                // TODO: Better deserialization error handling
                payload: bincode::deserialize(&record.entry).expect("Deserializing entry failed"),
            }
        })
    }

    /// Get the last known membership config before the specified Raft log entry
    pub async fn get_last_membership_before(&self, before: RaftLogId) -> Option<MembershipConfig> {
        let before = before as i64;
        query!(
            "
            SELECT *
            FROM raftlog
            WHERE entry_type == ? AND id <= ?;
        ",
            RaftLogEntryType::ConfigChange as i64,
            before
        )
        .fetch_one(&self.0)
        .await
        .map_or_else(
            |err| match err {
                sqlx::Error::RowNotFound => None,
                _ => panic!("Error getting last config change: {:#?}", err),
            },
            |record| {
                // TODO: Better deserialization error handling
                let deserialized: Entry<AppClientRequest> =
                    bincode::deserialize(&record.entry).expect("Deserialization error");
                match deserialized.payload {
                    EntryPayload::ConfigChange(EntryConfigChange { membership }) => {
                        Some(membership)
                    }
                    _ => unreachable!(),
                }
            },
        )
    }
}

impl Schema for RaftLog {
    const TABLENAME: &'static str = "raftlog";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_raftlog.sql"))
    }

    fn with(db: &SqlitePool) -> Self {
        Self(db.clone())
    }
}

impl From<&Entry<AppClientRequest>> for RaftLogRow {
    fn from(entry: &Entry<AppClientRequest>) -> Self {
        Self {
            id: entry.index,
            term: entry.term,
            entry: bincode::serialize(&entry).unwrap_or_else(|err| {
                panic!("Error serializing log entry {:#?}: {:?}", entry, err)
            }),
            entry_type: RaftLogEntryType::from(&entry.payload),
        }
    }
}

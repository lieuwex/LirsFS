use crate::operation::Operation;
use async_raft::raft::{Entry, EntryNormal, EntryPayload};
use serde::{Deserialize, Serialize};
use sqlx::{query, QueryBuilder, Sqlite};

use crate::client_req::AppClientRequest;

use super::schema::{db, Schema, SqlxQuery};

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

/// Repository that is backed by the `raftlog` table in the SQLite database.
/// An instance of this struct represents a single entry in that table.
/// From the outside, however, you interact with the table through associated methods that accept [RaftLogId]s and [Entry<AppClientRequest>]s.
/// Use these associated methods to perform CRUD operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftLog {
    id: RaftLogId,
    term: RaftLogTerm,
    entry: RaftLogEntry,
}

impl RaftLog {
    pub async fn delete_range(from: RaftLogId, to: RaftLogId) {
        let from = from as i64;
        let to = to as i64;
        query!(
            "
            DELETE FROM raftlog
            WHERE id >= ? AND id < ?;",
            from,
            to
        )
        .execute(db())
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

    pub async fn delete_from(from: RaftLogId) {
        let from = from as i64;
        query!(
            "
            DELETE FROM raftlog
            WHERE id >= ?;
        ",
            from
        )
        .execute(db())
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
    pub async fn insert(entries: &[Entry<AppClientRequest>]) {
        let mut query = QueryBuilder::<Sqlite>::new("INSERT INTO raftlog (id,term,entry) ");
        let values_to_insert = entries.iter().map(RaftLog::from);

        query.push_values(values_to_insert, |mut b, RaftLog { id, term, entry }| {
            b.push_bind(id as i64)
                .push_bind(term as i64)
                .push_bind(entry);
        });
        query.build().execute(db()).await.unwrap_or_else(|err| {
            panic!(
                "Could not insert log entry/entries into {}: {:?}",
                Self::TABLENAME,
                err
            )
        });
    }

    pub async fn get_range(from: RaftLogId, to: RaftLogId) -> Vec<Entry<AppClientRequest>> {
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
        .fetch_all(db())
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
        .collect::<Vec<_>>()
    }
}

impl Schema for RaftLog {
    const TABLENAME: &'static str = "raftlog";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_raftlog.sql"))
    }
}

impl From<&Entry<AppClientRequest>> for RaftLog {
    fn from(entry: &Entry<AppClientRequest>) -> Self {
        Self {
            id: entry.index,
            term: entry.term,
            entry: bincode::serialize(&entry).unwrap_or_else(|err| {
                panic!("Error serializing log entry {:#?}: {:?}", entry, err)
            }),
        }
    }
}
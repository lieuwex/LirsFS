use anyhow::{anyhow, Result};
use async_raft::{
    raft::{Entry, EntryConfigChange, EntryPayload, EntrySnapshotPointer, MembershipConfig},
    AppData,
};
use sqlx::{query, QueryBuilder, Sqlite, SqliteConnection};

use crate::client_req::AppClientRequest;

use super::{
    errors::raftlog_deserialize_error,
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

impl TryFrom<i64> for RaftLogEntryType {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RaftLogEntryType::Blank),
            1 => Ok(RaftLogEntryType::Normal),
            2 => Ok(RaftLogEntryType::ConfigChange),
            3 => Ok(RaftLogEntryType::SnapShotPointer),
            _ => Err(anyhow!("Invalid RaftLogEntryType: {}", value)),
        }
    }
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
/// You interact with the table through associated methods that accept [RaftLogId]s and [Entry<AppClientRequest>]s.
/// Use these associated methods to perform CRUD operations.
#[derive(Clone, Debug)]
pub struct RaftLog;

impl RaftLog {
    /// Delete all Raft log entries in the provided range [from, to) in the database given by `conn`.
    /// Returns the amount of entries deleted.
    pub async fn delete_range(
        conn: &mut SqliteConnection,
        from: RaftLogId,
        to: RaftLogId,
    ) -> Result<u64> {
        let from = from as i64;
        let to = to as i64;
        query!(
            "
            DELETE FROM raftlog
            WHERE id >= ? AND id < ?;",
            from,
            to
        )
        .execute(conn)
        .await
        .map_or_else(
            |err| {
                Err(anyhow!(
                    "Could not delete id range {}-{} from {}: {:?}",
                    from,
                    to,
                    Self::TABLENAME,
                    err
                ))
            },
            |res| Ok(res.rows_affected()),
        )
    }

    /// Deletes all Raft log entries more than or equal to the provided id in the database given by `conn`.
    /// Returns the amount of entries deleted.
    pub async fn delete_from(conn: &mut SqliteConnection, from: RaftLogId) -> Result<u64> {
        let from = from as i64;
        query!(
            "
            DELETE FROM raftlog
            WHERE id >= ?;
        ",
            from
        )
        .execute(conn)
        .await
        .map_or_else(
            |err| {
                Err(anyhow!(
                    "Could not delete from id {} from {}: {:?}",
                    from,
                    Self::TABLENAME,
                    err
                ))
            },
            |res| Ok(res.rows_affected()),
        )
    }

    /// Insert the given Raft log entries into the SQLite database given by `conn`.
    pub async fn insert<'b, I>(conn: &mut SqliteConnection, entries: I)
    where
        I: IntoIterator<Item = &'b Entry<AppClientRequest>>,
    {
        let mut query = QueryBuilder::<Sqlite>::new("INSERT INTO raftlog (id,term,entry) ");
        let values_to_insert = entries.into_iter().map(RaftLogRow::from);

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
        query.build().execute(conn).await.unwrap_or_else(|err| {
            panic!(
                "Could not insert log entry/entries into {}: {:?}",
                Self::TABLENAME,
                err
            )
        });
    }

    /// Retrieves the given range [from, to) of Raft log entries from the database given by `conn`, and serializes them into [Entry<AppClientRequest>]s.
    pub async fn get_range(
        conn: &mut SqliteConnection,
        from: RaftLogId,
        to: RaftLogId,
    ) -> Result<Vec<Entry<AppClientRequest>>> {
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
        .fetch_all(conn)
        .await
        .map_or_else(
            |err| {
                Err(anyhow!(
                    "Could not get log entries with id {}-{} from {}: {}",
                    from,
                    to,
                    Self::TABLENAME,
                    err
                ))
            },
            Ok,
        )?
        .iter()
        .try_fold(Vec::new(), |mut vec, record| {
            vec.push(Entry {
                term: record.term as RaftLogTerm,
                index: record.id as RaftLogId,
                payload: bincode::deserialize(&record.entry).map_err(raftlog_deserialize_error)?,
            });
            Ok(vec)
        })
    }

    /// Retrieves the raft log entry with the given `id` and serializes it into [Entry<AppClientRequest>]s.
    /// Returns [None] if entry is not found.
    pub async fn get_by_id(
        conn: &mut SqliteConnection,
        id: RaftLogId,
    ) -> Result<Option<Entry<AppClientRequest>>> {
        let id = id as i64;
        query!(
            "
            SELECT * 
            FROM raftlog
            WHERE id == ?
        ",
            id
        )
        .fetch_optional(conn)
        .await?
        .map(|record| {
            Ok(Entry {
                term: record.term as RaftLogTerm,
                index: record.id as RaftLogId,
                payload: bincode::deserialize(&record.entry).map_err(raftlog_deserialize_error)?,
            })
        })
        .transpose()
    }

    /// Get the last known membership config before the specified Raft log entry
    pub async fn get_last_membership_before(
        conn: &mut SqliteConnection,
        before: RaftLogId,
    ) -> Result<Option<MembershipConfig>> {
        let before = before as i64;
        query!(
            "
            SELECT entry
            FROM raftlog
            WHERE entry_type == ? AND id <= ?;
        ",
            RaftLogEntryType::ConfigChange as i64,
            before
        )
        .fetch_optional(conn)
        .await?
        .map_or_else(
            || Ok(None),
            |record| {
                let deserialized: Entry<AppClientRequest> =
                    bincode::deserialize(&record.entry).map_err(raftlog_deserialize_error)?;
                match deserialized.payload {
                    EntryPayload::ConfigChange(EntryConfigChange { membership }) => {
                        Ok(Some(membership))
                    }
                    _ => unreachable!(),
                }
            },
        )
    }

    /// Get the last known membership. Will use membership of a snapshot if encountered.
    pub async fn get_last_membership(
        conn: &mut SqliteConnection,
    ) -> Result<Option<MembershipConfig>> {
        let record = query!(
            "
            SELECT entry 
            FROM 
                raftlog,
                (   
                    SELECT MAX(id) as maxid
                    FROM raftlog
                    WHERE entry_type = ? OR entry_type = ?
                )
            WHERE id = maxid
        ",
            RaftLogEntryType::SnapShotPointer as i64,
            RaftLogEntryType::ConfigChange as i64,
        )
        .fetch_optional(conn)
        .await?;
        if let Some(record) = record {
            let entry: Entry<AppClientRequest> =
                bincode::deserialize(&record.entry).map_err(raftlog_deserialize_error)?;
            match entry.payload {
                EntryPayload::ConfigChange(EntryConfigChange { membership }) => {
                    Ok(Some(membership))
                }
                EntryPayload::SnapshotPointer(EntrySnapshotPointer { membership, .. }) => {
                    Ok(Some(membership))
                }
                _ => unreachable!(),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn get_last_log_entry_id_term(
        conn: &mut SqliteConnection,
    ) -> Result<Option<(RaftLogId, RaftLogTerm)>> {
        query!(
            "
            SELECT id, term
            FROM raftlog
            WHERE id=(SELECT MAX(id) FROM raftlog)
        "
        )
        .fetch_optional(conn)
        .await?
        .map(|record| Ok((record.id as RaftLogId, record.term as RaftLogTerm)))
        .transpose()
    }
}

impl Schema for RaftLog {
    const TABLENAME: &'static str = "raftlog";

    fn create_table_query() -> SqlxQuery {
        query(include_str!("../../sql/create_raftlog.sql"))
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

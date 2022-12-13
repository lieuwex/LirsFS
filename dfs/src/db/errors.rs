use std::error::Error;

use anyhow::anyhow;

pub fn raftlog_deserialize_error(err: impl Error) -> anyhow::Error {
    anyhow!("Error deserializing raftlog entry. Most likely the encoding of a raftlog entry's request has been changed and is incompatible with the current version of the application. Error: {:#?}", err)
}

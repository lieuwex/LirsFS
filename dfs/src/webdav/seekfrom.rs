use std::io;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum SeekFrom {
    Start(u64),
    End(i64),
    Current(i64),
}

impl From<io::SeekFrom> for SeekFrom {
    fn from(v: io::SeekFrom) -> Self {
        match v {
            io::SeekFrom::Start(v) => SeekFrom::Start(v),
            io::SeekFrom::End(v) => SeekFrom::End(v),
            io::SeekFrom::Current(v) => SeekFrom::Current(v),
        }
    }
}

impl From<SeekFrom> for io::SeekFrom {
    fn from(v: SeekFrom) -> Self {
        match v {
            SeekFrom::Start(v) => io::SeekFrom::Start(v),
            SeekFrom::End(v) => io::SeekFrom::End(v),
            SeekFrom::Current(v) => io::SeekFrom::Current(v),
        }
    }
}

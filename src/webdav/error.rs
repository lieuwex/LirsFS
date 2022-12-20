use thiserror::Error;
use tracing::error;
use webdav_handler::fs::FsError;

#[derive(Error, Debug)]
pub enum FileSystemError {
    #[error(transparent)]
    Webdav(#[from] FsError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<FileSystemError> for FsError {
    fn from(value: FileSystemError) -> Self {
        match value {
            FileSystemError::Webdav(e) => e,
            FileSystemError::Other(e) => {
                // worst hack in my life, really
                let s = format!("{:?}", e);
                let e = if s.contains("NotImplemented") {
                    FsError::NotImplemented
                } else if s.contains("GeneralFailure") {
                    FsError::GeneralFailure
                } else if s.contains("Exists") {
                    FsError::Exists
                } else if s.contains("NotFound") {
                    FsError::NotFound
                } else if s.contains("Forbidden") {
                    FsError::Forbidden
                } else if s.contains("InsufficientStorage") {
                    FsError::InsufficientStorage
                } else if s.contains("LoopDetected") {
                    FsError::LoopDetected
                } else if s.contains("PathTooLong") {
                    FsError::PathTooLong
                } else if s.contains("TooLarge") {
                    FsError::TooLarge
                } else if s.contains("IsRemote") {
                    FsError::IsRemote
                } else {
                    error!(
                        "Caught webdav filesystem error (to GeneralFailure): {:?}",
                        e
                    );
                    return FsError::GeneralFailure;
                };
                error!("webdav error to {}", e);
                e
            }
        }
    }
}

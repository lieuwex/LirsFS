use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use camino::Utf8PathBuf;
use sqlx::Either;
use tokio::sync::{
    oneshot, MappedMutexGuard, Mutex, MutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard,
    RwLock,
};

pub struct QueueReadHandle<'a>(Either<OwnedRwLockReadGuard<()>, &'a QueueWriteHandle>);

impl<'a> From<&'a QueueWriteHandle> for QueueReadHandle<'a> {
    fn from(h: &'a QueueWriteHandle) -> Self {
        Self(Either::Right(h))
    }
}

pub struct QueueWriteHandle(Option<oneshot::Sender<()>>);

impl QueueWriteHandle {
    fn new(tx: oneshot::Sender<()>) -> Self {
        Self(Some(tx))
    }
}

impl Drop for QueueWriteHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

#[derive(Debug, Default)]
struct QueueItem {
    lock: Arc<RwLock<()>>,
    waiters: HashMap<u64, oneshot::Sender<()>>,
}

#[derive(Debug)]
pub struct Queue {
    items: Mutex<HashMap<Utf8PathBuf, QueueItem>>,
}

impl Queue {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            items: Mutex::new(HashMap::new()),
        })
    }

    async fn entry(&self, path: Utf8PathBuf) -> MappedMutexGuard<QueueItem> {
        let lock = self.items.lock().await;
        MutexGuard::try_map(lock, |items| Some(items.entry(path).or_default())).unwrap()
    }

    // TODO: we should submit get_write jobs on startup when knowingly waiting for jobs to finish
    // in the `outstanding_writes` table.

    /// Submit and hold a write lock for the file at `path` for write command `serial`.
    /// This function will wait until a write lock is acquired.
    /// Write lock will be held until the write completes AND the resulint QueueHandle is dropped.
    pub async fn get_write(
        self: Arc<Self>,
        path: Utf8PathBuf,
        serial: u64,
    ) -> Result<QueueWriteHandle> {
        // channel to indicate that the caller of `get_write` is finished with the lock.
        let (htx, hrx) = oneshot::channel();

        // channel to indicate that the write lock has been acquired.
        let (ltx, lrx) = oneshot::channel();

        let this = self.clone();
        tokio::spawn(async move {
            // channel to indicate that the write operation has been completed
            let (otx, orx) = oneshot::channel();

            // we hold an entry lock for this whole task.
            let write_lock: OwnedRwLockWriteGuard<()> = {
                // hold a lock on the items, this is as shortlived as posisble so that other tasks
                // can mark this write as finished! ...
                let mut entry = this.entry(path).await;
                assert!(entry.waiters.insert(serial, otx).is_none());
                // ... which is the reason we are creating an _owned_ guard here here.
                entry.lock.clone().write_owned().await
            };

            // Notify that the write lock has been acquired.
            ltx.send(()).unwrap();

            // We wait for _both_ the channels to be done.
            orx.await.unwrap();
            hrx.await.unwrap();

            // And finally drop it.
            // This means that we will hold the write lock until both the caller of `get_write` is
            // done and the write operation `serial` is comitted.
            drop(write_lock);
        });

        // Wait until the write lock has been required before returning.
        lrx.await?;

        // Return a QueueHandle, if this is dropped we mark hrx as ready.
        Ok(QueueWriteHandle::new(htx))
    }

    pub async fn get_read(&self, path: Utf8PathBuf) -> QueueReadHandle<'static> {
        let lock = {
            let entry = self.entry(path).await;
            entry.lock.clone()
        };
        QueueReadHandle(Either::Left(lock.read_owned().await))
    }
}

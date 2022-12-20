use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use anyhow::Result;
use camino::{Utf8Path, Utf8PathBuf};
use sqlx::Either;
use tokio::sync::{
    broadcast, oneshot, MappedMutexGuard, Mutex, MutexGuard, OwnedRwLockReadGuard,
    OwnedRwLockWriteGuard, RwLock,
};

use crate::client_req::RequestId;

/// A read handle to a file in the queue.
// Either::Left means that we own a plain lock read in the queue.
// Either::Right is a reference to a write handle, so that we can coerce a write handle to a read
// handle, which is obviosuly correct.
pub struct QueueReadHandle<'a>(Either<OwnedRwLockReadGuard<()>, &'a QueueWriteHandle>);

impl<'a> From<&'a QueueWriteHandle> for QueueReadHandle<'a> {
    fn from(h: &'a QueueWriteHandle) -> Self {
        Self(Either::Right(h))
    }
}

/// A write handle to a file in the queue.
// The reason we have an Option here is to allow us to send the value in the drop function, `send`
// consumes the sender, but we only get a `&mut self` reference in the `drop` function.
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
    waiters: HashMap<RequestId, broadcast::Sender<()>>,
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

    // REVIEW: should this only be called on success?
    /// Mark the write command with ID `req_id` as finished.
    pub async fn write_committed(&self, path: &Utf8Path, req_id: RequestId) -> Result<()> {
        let mut items = self.items.lock().await;

        if let Some(item) = items.get_mut(path) {
            if let Some(waiter) = item.waiters.remove(&req_id) {
                waiter.send(())?;
            }
        }

        Ok(())
    }

    // TODO: we should submit get_write jobs on startup when knowingly waiting for jobs to finish
    // in the `outstanding_writes` table.

    // TODO: also place write job in the `outstanding_writes` table.

    /// Submit and hold a write lock for the file at `path` for write command with ID `req_id`.
    /// This function will wait until a write lock is acquired.
    /// Write lock will be held until the write completes AND the resulint QueueHandle is dropped.
    pub async fn write(
        self: Arc<Self>,
        path: Utf8PathBuf,
        req_id: Option<RequestId>,
    ) -> Result<QueueWriteHandle> {
        // channel to indicate that the caller of `get_write` is finished with the lock.
        let (htx, hrx) = oneshot::channel();

        // channel to indicate that the write lock has been acquired.
        let (ltx, lrx) = oneshot::channel();

        let this = self.clone();
        tokio::spawn(async move {
            // we hold an entry lock for this whole task.
            let (write_lock, orx): (OwnedRwLockWriteGuard<()>, _) = {
                // hold a lock on the items, this is as shortlived as posisble so that other tasks
                // can mark this write as finished! ...
                let mut entry = this.entry(path).await;

                // channel to indicate that the write operation has been completed
                let orx = req_id.map(|req_id| match entry.waiters.entry(req_id) {
                    Entry::Vacant(v) => {
                        let (otx, orx) = broadcast::channel(1);
                        v.insert(otx);
                        orx
                    }
                    Entry::Occupied(o) => o.get().subscribe(),
                });

                // ... which is the reason we are creating an _owned_ guard here here.
                let lock = entry.lock.clone().write_owned().await;

                (lock, orx)
            };

            // Notify that the write lock has been acquired.
            ltx.send(()).unwrap();

            // We wait for _both_ the channels to be done.
            if let Some(mut orx) = orx {
                orx.recv().await.unwrap();
            }
            hrx.await.unwrap();

            // And finally drop it.
            // This means that we will hold the write lock until both the caller of `get_write` is
            // done and the write operation with ID `req_id` is comitted.
            drop(write_lock);
        });

        // Wait until the write lock has been required before returning.
        lrx.await?;

        // Return a QueueHandle, if this is dropped we mark hrx as ready.
        Ok(QueueWriteHandle::new(htx))
    }

    pub async fn read(&self, path: Utf8PathBuf) -> QueueReadHandle<'static> {
        let lock = {
            let entry = self.entry(path).await;
            entry.lock.clone()
        };
        QueueReadHandle(Either::Left(lock.read_owned().await))
    }
}

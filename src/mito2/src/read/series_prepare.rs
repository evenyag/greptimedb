// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Complete-preparation barrier for the buffered SeriesScan prototype.
//!
//! Preparation is scheduled immediately, independently of output polling. A
//! manifest owns its payload; an empty payload is a completed contribution.
//! Dropping the last consumer cancels preparation, including unpolled outputs.

use std::future::Future;
use std::sync::Arc;

use futures::future::{AbortHandle, Abortable};
use snafu::ResultExt;
use tokio::sync::oneshot;

use crate::error::{JoinSnafu, Result, ScanSeriesSnafu, UnexpectedSnafu};

/// Cancels the owned producer when the last readiness receiver disappears.
struct PreparationOwner(AbortHandle);

impl Drop for PreparationOwner {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One partition's complete, exclusively owned preparation result.
pub(crate) struct ReadinessManifest<T> {
    pub(crate) partition: usize,
    pub(crate) payload: T,
}

/// Receives a complete manifest without driving preparation.
pub(crate) struct ReadinessReceiver<T> {
    receiver: oneshot::Receiver<Result<ReadinessManifest<T>>>,
    _owner: Arc<PreparationOwner>,
}

impl<T> ReadinessReceiver<T> {
    pub(crate) async fn ready(self) -> Result<ReadinessManifest<T>> {
        self.receiver.await.map_err(|_| {
            UnexpectedSnafu {
                reason: "series preparation stopped before publishing readiness",
            }
            .build()
        })?
    }
}

/// Starts one independently scheduled preparation job with a publication barrier.
///
/// The producer must return exactly one owned payload per output partition.
/// Publication never waits for a consumer to poll or release channel capacity.
pub(crate) fn start_preparation<T, F>(partitions: usize, prepare: F) -> Vec<ReadinessReceiver<T>>
where
    T: Send + 'static,
    F: Future<Output = Result<Vec<T>>> + Send + 'static,
{
    let (abort, registration) = AbortHandle::new_pair();
    let owner = Arc::new(PreparationOwner(abort));
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..partitions)
        .map(|_| {
            let (sender, receiver) = oneshot::channel();
            (
                sender,
                ReadinessReceiver {
                    receiver,
                    _owner: owner.clone(),
                },
            )
        })
        .unzip();
    // A supervisor converts producer panics into errors for every live consumer.
    common_runtime::spawn_query(async move {
        let task = common_runtime::spawn_query(Abortable::new(prepare, registration));
        let result = match task.await.context(JoinSnafu) {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => return,
            Err(error) => Err(error),
        };
        let result = result.and_then(|payloads| {
            if payloads.len() != partitions {
                return UnexpectedSnafu {
                    reason: format!(
                        "series preparation returned {} manifests for {partitions} partitions",
                        payloads.len()
                    ),
                }
                .fail();
            }
            Ok(payloads)
        });
        match result {
            Ok(payloads) => {
                for (partition, (sender, payload)) in senders.into_iter().zip(payloads).enumerate()
                {
                    let _ = sender.send(Ok(ReadinessManifest { partition, payload }));
                }
            }
            Err(error) => {
                let error = Arc::new(error);
                for sender in senders {
                    let _ = sender.send(Err(error.clone()).context(ScanSeriesSnafu));
                }
            }
        }
    });
    receivers
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    async fn ready<T>(receiver: ReadinessReceiver<T>) -> Result<ReadinessManifest<T>> {
        tokio::time::timeout(Duration::from_secs(5), receiver.ready())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn sequential_and_never_polled_partitions() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut receivers = start_preparation(8, async move {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok((0..8).map(|p| vec![p; p]).collect::<Vec<_>>())
        })
        .into_iter();
        // Seven outputs have never been polled when the first becomes ready.
        let first = ready(receivers.next().unwrap()).await.unwrap();
        assert_eq!(first.partition, 0);
        assert!(first.payload.is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        for (partition, receiver) in receivers.enumerate() {
            let manifest = ready(receiver).await.unwrap();
            assert_eq!(manifest.partition, partition + 1);
            assert_eq!(manifest.payload, vec![partition + 1; partition + 1]);
        }
    }

    #[tokio::test]
    async fn preparation_starts_without_any_poll() {
        let (done, observed) = oneshot::channel();
        let receivers = start_preparation(8, async move {
            done.send(()).unwrap();
            Ok(vec![(); 8])
        });
        tokio::time::timeout(Duration::from_secs(5), observed)
            .await
            .unwrap()
            .unwrap();
        drop(receivers);
    }

    #[tokio::test]
    async fn error_and_wrong_manifest_count_reach_all_consumers() {
        for wrong_count in [false, true] {
            let receivers = start_preparation::<(), _>(8, async move {
                if wrong_count {
                    Ok(vec![])
                } else {
                    UnexpectedSnafu {
                        reason: "injected preparation error",
                    }
                    .fail()
                }
            });
            for receiver in receivers {
                assert!(ready(receiver).await.is_err());
            }
        }
    }

    #[tokio::test]
    async fn panic_reaches_all_consumers() {
        let receivers = start_preparation::<(), _>(2, async {
            panic!("injected producer panic");
        });
        for receiver in receivers {
            assert!(ready(receiver).await.is_err());
        }
    }

    struct DropSignal(Option<oneshot::Sender<()>>);
    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn last_consumer_drop_cancels_and_releases_producer() {
        let (started, running) = oneshot::channel();
        let (dropped, mut released) = oneshot::channel();
        let mut receivers = start_preparation::<(), _>(2, async move {
            let _owned = DropSignal(Some(dropped));
            started.send(()).unwrap();
            futures::future::pending().await
        });
        running.await.unwrap();
        drop(receivers.pop());
        // A remaining, unpolled consumer still owns the job.
        assert!(matches!(
            released.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(receivers);
        tokio::time::timeout(Duration::from_secs(5), released)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn dropped_partition_releases_its_unpublished_payload() {
        let (publish, gate) = oneshot::channel();
        let (dropped, released) = oneshot::channel();
        let mut receivers = start_preparation(2, async move {
            gate.await.unwrap();
            Ok(vec![DropSignal(Some(dropped)), DropSignal(None)])
        });
        drop(receivers.remove(0));
        publish.send(()).unwrap();
        let manifest = ready(receivers.remove(0)).await.unwrap();
        assert_eq!(manifest.partition, 1);
        tokio::time::timeout(Duration::from_secs(5), released)
            .await
            .unwrap()
            .unwrap();
        // None also represents a valid owned empty payload.
        drop(manifest);
    }
}

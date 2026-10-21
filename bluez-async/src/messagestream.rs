use dbus::Message;
use dbus::nonblock::{MsgMatch, SyncConnection};
use futures::Stream;
use futures::channel::mpsc::UnboundedReceiver;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Wrapper for a stream of D-Bus messages which automatically removes the `MsgMatch` from the D-Bus
/// connection when it is dropped.
pub struct MessageStream {
    msg_match: Option<MsgMatch>,
    events: UnboundedReceiver<Message>,
    connection: Arc<SyncConnection>,
}

impl MessageStream {
    pub fn new(msg_match: MsgMatch, connection: Arc<SyncConnection>) -> Self {
        let (msg_match, events) = msg_match.msg_stream();
        Self {
            msg_match: Some(msg_match),
            events,
            connection,
        }
    }
}

impl Stream for MessageStream {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.events).poll_next(cx)
    }
}

impl Drop for MessageStream {
    fn drop(&mut self) {
        let connection = self.connection.clone();
        let Some(msg_match) = self.msg_match.take() else {
            return;
        };
        // Drop can run after the runtime or D-Bus subscription has already closed.
        // Cleanup is best-effort and must never abort a release-mode SDK host.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            // Keep the receiver alive until the match is removed. Otherwise a queued
            // signal can fail delivery and remove the local token before cleanup runs.
            let (_, empty_receiver) = futures::channel::mpsc::unbounded();
            let events = std::mem::replace(&mut self.events, empty_receiver);
            runtime.spawn(async move {
                if let Err(error) = connection.remove_match(msg_match.token()).await {
                    log::warn!(
                        "Failed to remove D-Bus message subscription during cleanup: {error}"
                    );
                }
                drop(events);
            });
        }
    }
}

#[cfg(test)]
mod cleanup_tests {
    use super::*;
    use dbus::{
        message::MatchRule,
        nonblock::{Process, Proxy},
    };
    use std::{collections::HashMap, time::Duration};

    async fn check_cleanup(deliver_queued_signal: bool) {
        let (resource, connection) = dbus_tokio::connection::new_session_sync().unwrap();
        let resource_task = tokio::spawn(async { resource.await });
        let owner = connection.unique_name().to_string();
        let rule = MatchRule::new_signal("com.brainco.CleanupTest", "Tick");
        let msg_match = connection.add_match(rule).await.unwrap();
        let stream = MessageStream::new(msg_match, connection.clone());
        drop(stream);
        if deliver_queued_signal {
            // A current-thread runtime defers spawned cleanup until the next await.
            connection.process_one(
                Message::new_signal("/com/brainco/test", "com.brainco.CleanupTest", "Tick")
                    .unwrap(),
            );
        }
        let proxy = Proxy::new(
            "org.freedesktop.DBus",
            "/org/freedesktop/DBus",
            Duration::from_secs(1),
            connection,
        );
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (rules,): (HashMap<String, Vec<String>>,) = proxy
                    .method_call("org.freedesktop.DBus.Debug.Stats", "GetAllMatchRules", ())
                    .await
                    .unwrap();
                if !rules
                    .get(&owner)
                    .into_iter()
                    .flatten()
                    .any(|rule| rule.contains("com.brainco.CleanupTest"))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        resource_task.abort();
        assert!(result.is_ok(), "message-stream cleanup leaked a bus match");
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires an isolated dbus-run-session with Debug.Stats"]
    async fn message_stream_cleanup_with_queued_signal() {
        check_cleanup(true).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires an isolated dbus-run-session with Debug.Stats"]
    async fn message_stream_cleanup_without_queued_signal() {
        check_cleanup(false).await;
    }
}

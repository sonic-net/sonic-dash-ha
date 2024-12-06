pub mod payload;
mod table;
mod table_watcher;

use swbus_actor::prelude::*;
use swss_common::{ConsumerStateTable, SubscriberStateTable, ZmqConsumerStateTable};
use table::Table;
use table_watcher::{TableWatcher, TableWatcherMessage};
use tokio::sync::{
    mpsc::{channel, Sender},
    oneshot,
};
use tokio_util::task::AbortOnDropHandle;

/// A bridge that converts between Swbus messages and swss tables.
pub struct SwssCommonBridge {
    outbox_tx: Option<oneshot::Sender<Outbox>>,
    tw_msg_tx: Sender<TableWatcherMessage>,
    _table_watcher_task: AbortOnDropHandle<()>,
}

impl SwssCommonBridge {
    pub fn new_consumer_state_table(table: ConsumerStateTable) -> Self {
        Self::new(table)
    }

    pub fn new_subscriber_state_table(table: SubscriberStateTable) -> Self {
        Self::new(table)
    }

    pub fn new_zmq_consumer_state_table(table: ZmqConsumerStateTable) -> Self {
        Self::new(table)
    }

    fn new<T: Table + 'static>(table: T) -> Self {
        let (tw_msg_tx, tw_msg_rx) = channel(1024);
        let (outbox_tx, outbox_rx) = oneshot::channel();
        let table_watcher = TableWatcher::new(table, outbox_rx, tw_msg_rx);
        let _table_watcher_task = AbortOnDropHandle::new(tokio::spawn(table_watcher.run()));
        Self {
            tw_msg_tx,
            outbox_tx: Some(outbox_tx),
            _table_watcher_task,
        }
    }
}

impl Actor for SwssCommonBridge {
    async fn init(&mut self, outbox: Outbox) {
        self.outbox_tx
            .take()
            .unwrap()
            .send(outbox.clone())
            .unwrap_or_else(|_| unreachable!("outbox_tx.send failed"));
    }

    async fn handle_message(&mut self, message: IncomingMessage, outbox: Outbox) {
        match &message.body {
            // Requests are encoded an TableWatcherMessage. Decode it and send it to the TableWatcher
            MessageBody::Request(req) => match payload::decode_table_watcher_message(&req.payload) {
                Ok(tw_msg) => {
                    self.tw_msg_tx.send(tw_msg).await.expect("TableWatcher task died");
                    let msg = OutgoingMessage::ok_response(message);
                    outbox.send(msg).await;
                }

                Err(e) => {
                    let msg = OutgoingMessage::error_response(message, SwbusErrorCode::InvalidPayload, e.to_string());
                    outbox.send(msg).await;
                }
            },
            MessageBody::Response(_) => (),
        }
    }

    async fn handle_message_failure(&mut self, _id: MessageId, subscriber: ServicePath, _outbox: Outbox) {
        // If a message failed to be delivered, we unsubscribe the client so we don't waste bandwidth on them in the future
        self.tw_msg_tx
            .send(TableWatcherMessage::Unsubscribe { subscriber })
            .await
            .expect("TableWatcher task died");
    }
}

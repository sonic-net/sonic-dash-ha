use crate::actor_message::{actor_msg_to_swbus_msg, ActorMessage};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use swbus_edge::{
    simple_client::{MessageId, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusMessage},
};
use tokio::time::{interval, Interval};

const RESEND_TIME: Duration = Duration::from_secs(60);

pub struct Outgoing {
    swbus_client: Arc<SimpleSwbusEdgeClient>,
    resend_interval: Interval,
    unacked_messages: HashMap<MessageId, UnackedMessage>,

    /// Messages that will be sent if the actor logic succeeds, or dropped if it fails.
    queued_messages: Vec<UnackedMessage>,
}

impl Outgoing {
    /// Enqueue a message for sending, if the actor callback succeeds.
    ///
    /// If the actor callback fails, the message will be dropped.
    pub fn send(&mut self, dest: ServicePath, msg: ActorMessage) {
        let swbus_message = actor_msg_to_swbus_msg(&msg, dest, &self.swbus_client);
        let time_sent = Instant::now();
        self.queued_messages.push({
            UnackedMessage {
                actor_message: msg,
                swbus_message,
                time_sent,
            }
        });
    }

    pub(crate) fn new(swbus_client: Arc<SimpleSwbusEdgeClient>) -> Self {
        let mut resend_interval = interval(RESEND_TIME);
        resend_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            swbus_client,
            resend_interval,
            unacked_messages: HashMap::new(),
            queued_messages: Vec::new(),
        }
    }

    /// Actor logic succeeded, so send out messages.
    pub(crate) async fn send_queued_messages(&mut self) {
        for msg in self.queued_messages.drain(..) {
            self.swbus_client
                .send_raw(msg.swbus_message.clone())
                .await
                .expect("Sending swbus message failed");

            let id = msg.swbus_message.header.as_ref().unwrap().id;
            self.unacked_messages.insert(id, msg);
        }
    }

    /// Actor logic failed, so don't send any messages.
    pub(crate) fn drop_queued_messages(&mut self) {
        self.queued_messages.clear();
    }

    /// Received a successful response for a prior request.
    pub(crate) fn ack_message(&mut self, id: MessageId) {
        self.unacked_messages.remove(&id);
    }

    /// Run the resend/maintenence loop. Returned future must be polled to run it.
    pub(crate) async fn drive_resend_loop(&mut self) {
        loop {
            self.resend_interval.tick().await;

            // Drop messages that have been unacked for over an hour, as a memory leak failsafe
            self.unacked_messages
                .retain(|_, msg| msg.time_sent.elapsed() < Duration::from_secs(3600));

            // Resend unacked messages
            for msg in self.unacked_messages.values() {
                if msg.time_sent.elapsed() >= self.resend_interval.period() {
                    self.swbus_client
                        .send_raw(msg.swbus_message.clone())
                        .await
                        .expect("Sending swbus message failed");
                }
            }
        }
    }
}

#[derive(Debug)]
struct UnackedMessage {
    actor_message: ActorMessage,
    swbus_message: SwbusMessage,
    time_sent: Instant,
}

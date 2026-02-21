use super::get_unix_time;
use crate::actor_message::{actor_msg_to_swbus_msg, ActorMessage};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use swbus_edge::{
    simple_client::{MessageId, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode, SwbusMessage},
};
use tokio::time::{interval, Interval};
use tracing::debug;

const RESEND_TIME: Duration = Duration::from_secs(60);

/// Outgoing state table - messages to send to other actors.
pub struct Outgoing {
    swbus_client: Arc<SimpleSwbusEdgeClient>,
    resend_interval: Interval,
    unacked_messages: HashMap<MessageId, UnackedMessage>,

    /// Messages that will be sent if the actor logic succeeds, or dropped if it fails.
    queued_messages: Vec<UnackedMessage>,

    /// Record of sent messages, purely for GetActorState
    sent_messages: HashMap<String, SentMessageEntry>,
}

impl Outgoing {
    /// Enqueue a message for sending, if the actor callback succeeds.
    ///
    /// If the actor callback fails, the message will be dropped.
    pub fn send(&mut self, dest: ServicePath, msg: ActorMessage) {
        let swbus_message = actor_msg_to_swbus_msg(&msg, dest, &self.swbus_client);
        let time_sent = SystemTime::now();
        self.queued_messages.push({
            UnackedMessage {
                actor_message: msg,
                swbus_message,
                time_sent,
            }
        });
    }

    pub fn send_with_delay(&mut self, dest: ServicePath, msg: ActorMessage, delay: Duration) {
        let swbus_message = actor_msg_to_swbus_msg(&msg, dest, &self.swbus_client);
        let now = SystemTime::now();
        let time_sent = now.checked_add(delay).expect("SystemTime overflowed!");
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
            sent_messages: HashMap::new(),
        }
    }

    /// Actor logic succeeded, so send out messages.
    pub(crate) async fn send_queued_messages(&mut self) {
        for msg in self.queued_messages.drain(..) {
            debug!("Sending message: {msg:?}");
            self.swbus_client
                .send_raw(msg.swbus_message.clone())
                .await
                .expect("Sending swbus message failed");

            let id = msg.swbus_message.header.as_ref().unwrap().id;
            let actor_msg = msg.actor_message.clone();

            // Update the table for GetActorState
            match self.sent_messages.get_mut(msg.key()) {
                Some(entry) => entry.new_message_sent(actor_msg, id),
                None => {
                    let key = msg.key().to_string();
                    self.sent_messages.insert(key, SentMessageEntry::new(actor_msg, id));
                }
            }

            // Add to unacked messages/resend queue
            self.unacked_messages.insert(id, msg);
        }
    }

    /// Actor logic failed, so don't send any messages.
    pub(crate) fn drop_queued_messages(&mut self) {
        self.queued_messages.clear();
    }

    /// Handle a response to a sent message.
    pub(crate) fn handle_response(
        &mut self,
        id: MessageId,
        error_code: SwbusErrorCode,
        error_message: &str,
        source: ServicePath,
    ) {
        let Some(unacked_message) = self.unacked_messages.get(&id) else {
            // Response for message that was already acked. Ignore it.
            return;
        };

        // Update the table for GetActorState
        self.sent_messages
            .get_mut(unacked_message.key())
            .unwrap()
            .response_received(id, error_code, error_message, source);

        // Response was successfully acked. Remove it from unacked messages/resend queue
        if error_code == SwbusErrorCode::Ok {
            self.unacked_messages.remove(&id);
        }
    }

    /// Run the resend/maintenence loop. Returned future must be polled to run it.
    pub(crate) async fn drive_resend_loop(&mut self) {
        loop {
            self.resend_interval.tick().await;

            // Drop messages that have been unacked for over an hour, as a memory leak failsafe
            self.unacked_messages
                .retain(|_, msg| Duration::from_secs(get_elapsed_time(&msg.time_sent)) < Duration::from_secs(3600));

            // Resend unacked messages
            for msg in self.unacked_messages.values() {
                if Duration::from_secs(get_elapsed_time(&msg.time_sent)) >= self.resend_interval.period() {
                    self.swbus_client
                        .send_raw(msg.swbus_message.clone())
                        .await
                        .expect("Sending swbus message failed");

                    // Update the table for GetActorState
                    self.sent_messages.get_mut(msg.key()).unwrap().message_resent();
                }
            }
        }
    }

    pub fn from_my_sp(&self, resource_type: &str, resource_id: &str) -> ServicePath {
        let mut sp = self.swbus_client.get_service_path().clone();
        sp.resource_type = resource_type.into();
        sp.resource_id = resource_id.into();
        sp
    }

    pub fn common_bridge_sp<T>(&self) -> ServicePath
    where
        T: sonic_common::SonicDbTable + 'static,
    {
        let resource_id = format!("{}|{}", T::db_name(), T::table_name());
        self.from_my_sp("swss-common-bridge", &resource_id)
    }

    /// Check if there are no unacked messages
    pub fn ready_for_delete(&self) -> bool {
        self.unacked_messages.is_empty()
    }

    pub(crate) fn dump_state(&self) -> OutgoingStateData {
        let state_data = OutgoingStateData {
            outgoing_queued: self.queued_messages.clone(),
            outgoing_sent: self
                .sent_messages
                .iter()
                .map(|(key, entry)| (key.clone(), entry.clone()))
                .collect(),
        };
        state_data
    }
}

pub fn get_elapsed_time(systime: &SystemTime) -> u64 {
    match systime.elapsed() {
        Ok(elapsed) => elapsed.as_secs(),
        Err(_) => 0, // If the system time went backwards, return 0
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct OutgoingStateData {
    pub outgoing_queued: Vec<UnackedMessage>,
    pub outgoing_sent: HashMap<String, SentMessageEntry>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct UnackedMessage {
    pub actor_message: ActorMessage,
    pub swbus_message: SwbusMessage,
    pub time_sent: SystemTime,
}

impl UnackedMessage {
    fn key(&self) -> &str {
        &self.actor_message.key
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SentMessageEntry {
    /// The most recent message sent with this key.
    pub msg: ActorMessage,
    /// The id of the most recent message sent with this key.
    pub id: MessageId,
    /// The first time a message was sent with this key, in unix seconds.
    #[allow(dead_code)] // TODO: GetActorState will read this when it is implemented
    pub created_time: u64,
    /// The most recent time a message was sent with this key, in unix seconds.
    pub last_updated_time: u64,
    /// The most recent time a message was sent OR resent with this key, in unix seconds.
    pub last_sent_time: u64,
    /// How many times this key has been updated.
    pub version: u64,
    /// Whether the latest message has been acked.
    pub acked: bool,
    /// The latest response to the latest message.
    pub response: Option<String>,
    /// Where the latest response came from.
    pub response_source: Option<ServicePath>,
}

impl SentMessageEntry {
    fn new(msg: ActorMessage, id: MessageId) -> Self {
        Self {
            msg,
            id,
            created_time: get_unix_time(),
            last_updated_time: get_unix_time(),
            last_sent_time: get_unix_time(),
            version: 1,
            acked: false,
            response: None,
            response_source: None,
        }
    }

    fn new_message_sent(&mut self, msg: ActorMessage, id: MessageId) {
        self.msg = msg;
        self.id = id;
        self.last_updated_time = get_unix_time();
        self.last_sent_time = get_unix_time();
        self.version += 1;
        self.acked = false;
        self.response = None;
        self.response_source = None;
    }

    fn message_resent(&mut self) {
        self.last_sent_time = get_unix_time();
    }

    fn response_received(
        &mut self,
        request_id: MessageId,
        response_code: SwbusErrorCode,
        response_message: &str,
        response_source: ServicePath,
    ) {
        if request_id == self.id {
            self.response_source = Some(response_source);
            if response_code == SwbusErrorCode::Ok {
                self.acked = true;
                self.response = Some(String::from("Ok"));
            } else {
                self.acked = false;
                self.response = Some(format!("{response_code:?} ({response_message})"));
            }
        }
    }
}

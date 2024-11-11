use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Weak},
    time::Duration,
};

use swbus_edge::SwbusMessage;
use tokio::time::Instant;

pub type MessageId = u64;

/// Settings that determine the behavior of [`ResendQueue`].
#[derive(Debug, Copy, Clone)]
pub struct ResendQueueConfig {
    /// How long to wait for an ack before sending a message again.
    pub resend_time: Duration,

    /// How many times to retry a message before giving up.
    pub max_tries: u32,
}

/// Outgoing messages and associated state necessary to resend them if they go unacknowledged.
pub struct ResendQueue {
    config: ResendQueueConfig,

    /// The messages that actually need to be resent.
    unacked_messages: HashMap<MessageId, Arc<Box<SwbusMessage>>>,

    /// A queue of messages that *may* need to be resent.
    /// Messages are ordered earliest at the front to latest at the back.
    queue: VecDeque<QueuedMessage>,
}

impl ResendQueue {
    pub fn new(config: ResendQueueConfig) -> Self {
        Self {
            config,
            unacked_messages: HashMap::new(),
            queue: VecDeque::new(),
        }
    }

    /// Add a message to the ResendQueue. Assumes the message has already been sent once.
    pub fn enqueue(&mut self, message: SwbusMessage) {
        let id = message.header.id;
        let strong_message = Arc::new(Box::new(message));
        let weak_message = Arc::downgrade(&strong_message);
        self.unacked_messages.insert(id, strong_message);
        self.queue.push_back(QueuedMessage {
            message: weak_message,
            tries: 1,
            resend_at: Instant::now() + self.config.resend_time,
        });
    }

    /// The time at which a message *may* need to be resent.
    /// If this returns `Some(instant)`, the caller should sleep until the returned instant, then call `next_resend` to get updates.
    /// If this returns `None`, no messages are queued.
    pub fn next_resend_instant(&self) -> Option<Instant> {
        self.queue.front().map(|m| m.resend_at)
    }

    /// Get an update about the next message that needs to be resent or that went stale.
    pub fn next_resend(&mut self) -> Option<ResendMessage> {
        let now = Instant::now();

        loop {
            match self.queue.front() {
                Some(peek) if peek.resend_at <= now => {
                    let mut queued_msg = self.queue.pop_front().unwrap();

                    match queued_msg.message.upgrade() {
                        Some(msg) if queued_msg.tries >= self.config.max_tries => {
                            // This message has been resent too may times.
                            // We are going to drop it, and tell the caller.
                            let id = msg.header.id;
                            self.unacked_messages.remove(&id);
                            return Some(ResendMessage::TooManyTries(id));
                        }
                        Some(msg) => {
                            // This message should be retried right now.
                            // We will requeue it, and tell the caller to resend it.
                            queued_msg.resend_at += self.config.resend_time;
                            queued_msg.tries += 1;
                            self.queue.push_back(queued_msg);
                            return Some(ResendMessage::Resend(msg));
                        }
                        None => {
                            // The message has already been dropped, because message was acknowledged.
                            // This, we can ignore this entry and continue the loop.
                        }
                    }
                }

                // Either the queue is empty, or no message needs to be retried yet (because now < top.resend_at)
                _ => return None,
            }
        }
    }

    /// Iterator that calls `next_resend` until it returns `None`
    pub fn iter_resend<'a>(&'a mut self) -> impl Iterator<Item = ResendMessage> + 'a {
        std::iter::from_fn(|| self.next_resend())
    }

    /// Signal that a message was acknowledged and no longer needs to be resent. Removes the message
    /// with this id from the resend queue.
    pub fn message_acknowledged(&mut self, id: MessageId) {
        self.unacked_messages.remove(&id);
    }
}

pub enum ResendMessage {
    /// This message needs to be resent right now
    Resend(Arc<Box<SwbusMessage>>),

    /// The message that was in this slot went stale and was dropped
    TooManyTries(MessageId),
}

/// A message awaiting an ack from the recipient.
struct QueuedMessage {
    /// A copy of the content of the message, so it can be resent.
    ///
    /// If this Weak is broken, that means the message was acked and removed
    /// from ResendQueue::unacked_messages, so we should ignore this entry.
    message: Weak<Box<SwbusMessage>>,

    /// How many times the message has been sent so far
    tries: u32,

    /// The next time at which to resend the message
    resend_at: Instant,
}

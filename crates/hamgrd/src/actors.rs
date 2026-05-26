//! Actors
//!
//! <https://github.com/r12f/SONiC/blob/user/r12f/hamgrd/doc/smart-switch/high-availability/smart-switch-ha-hamgrd.md#2-key-actors>
pub mod dpu;
pub mod ha_scope;
pub mod ha_set;
pub mod vdpu;

#[cfg(test)]
pub mod test;
use anyhow::Result as AnyhowResult;
use futures_util::FutureExt;
use sonic_common::SonicDbTable;
use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use swbus_actor::{spawn, Actor, ActorMessage};
use swbus_edge::swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_edge::swbus_proto::result::*;
use swbus_edge::swbus_proto::swbus::{swbus_message::Body, DataRequest, ServicePath, SwbusErrorCode, SwbusMessage};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{
    KeyOpFieldValues, KeyOperation, ProducerStateTable, SubscriberStateTable, ZmqClient, ZmqProducerStateTable,
};
use swss_common_bridge::{consumer::ConsumerBridge, producer::spawn_producer_bridge};
use tokio::sync::mpsc::{channel, Receiver};
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Best-effort extraction of a panic message from `catch_unwind`'s payload.
fn panic_payload_to_string(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

pub trait DbBasedActor: Actor {
    fn name() -> &'static str;
    fn table_name() -> &'static str;
    fn new(key: String) -> AnyhowResult<Self>
    where
        Self: Sized;

    async fn start_actor_creator<T>(edge_runtime: Arc<SwbusEdgeRuntime>) -> AnyhowResult<Vec<ConsumerBridge>>
    where
        Self: Sized,
        T: SonicDbTable + 'static,
    {
        let ac = ActorCreator::new(
            edge_runtime.new_sp(Self::name(), ""),
            edge_runtime.clone(),
            false,
            |key: String| -> AnyhowResult<Self> { Self::new(key) },
        );

        tokio::task::spawn(ac.run());

        let config_db = crate::db_for_table::<T>().await?;
        let sst = SubscriberStateTable::new_async(config_db, T::table_name(), None, None).await?;
        let addr = crate::common_bridge_sp::<T>(&edge_runtime);
        let base_addr = edge_runtime.get_base_sp();
        Ok(vec![ConsumerBridge::spawn::<T, _, _, _>(
            edge_runtime.clone(),
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                let mut addr = base_addr.clone();
                addr.resource_type = Self::name().to_owned();
                addr.resource_id = kfv.key.clone();
                (addr, T::table_name().to_owned())
            },
            |_| true,
        )])
    }
}

pub struct ActorCreator<F, T>
where
    F: Fn(String) -> AnyhowResult<T>,
    T: Actor,
{
    sp: ServicePath,
    rt: Arc<SwbusEdgeRuntime>,
    handler_rx: Receiver<SwbusMessage>,
    create_fn: F,
    id_generator: MessageIdGenerator,
}
// Connection worker facade
impl<F, T> ActorCreator<F, T>
where
    F: Fn(String) -> AnyhowResult<T>,
    T: Actor,
{
    pub fn new(sp: ServicePath, rt: Arc<SwbusEdgeRuntime>, public: bool, create_fn: F) -> Self {
        let (handler_tx, handler_rx) = channel::<SwbusMessage>(10);

        if public {
            rt.add_handler(sp.clone(), handler_tx);
        } else {
            rt.add_private_handler(sp.clone(), handler_tx);
        }

        Self {
            sp,
            rt,
            handler_rx,
            create_fn,
            id_generator: MessageIdGenerator::new(),
        }
    }

    pub async fn run(mut self) {
        loop {
            let msg = match self.handler_rx.recv().await {
                Some(m) => m,
                None => {
                    // All senders dropped; the route is gone, so we can safely exit.
                    error!(
                        "ActorCreator {}: handler channel closed, exiting",
                        self.sp.to_swbusd_service_path()
                    );
                    return;
                }
            };

            // Per-message handling must never panic, otherwise the run task
            // dies, the receiver is dropped, and the still-registered route
            // returns "channel closed" forever after.
            let result = AssertUnwindSafe(self.handle_received_message(&msg))
                .catch_unwind()
                .await;

            match result {
                Ok(Ok(())) => {
                    // forward the message to the actor that is just spawned
                    if self.rt.send(msg).await.is_err() {
                        error!("Failed to send response to swbus");
                    }
                }
                Ok(Err(swbus_err)) => {
                    let (code, err_msg) = match swbus_err {
                        SwbusError::ConnectionError { code, detail } => (code, detail.to_string()),
                        SwbusError::InternalError { code, detail } => (code, detail),
                        SwbusError::InputError { code, detail } => (code, detail),
                        SwbusError::RouteError { code, detail } => (code, detail),
                    };
                    let Some(req_header) = msg.header.as_ref() else {
                        error!("Cannot send error response: incoming message has no header");
                        continue;
                    };
                    let response = SwbusMessage::new_response(
                        req_header,
                        Some(&self.sp),
                        code,
                        &err_msg,
                        self.id_generator.generate(),
                        None,
                    );
                    if self.rt.send(response).await.is_err() {
                        error!("Failed to send response to swbus");
                    }
                }
                Err(panic) => {
                    let panic_msg = panic_payload_to_string(&panic);
                    error!(
                        "ActorCreator {}: panic while handling message: {}",
                        self.sp.to_swbusd_service_path(),
                        panic_msg
                    );
                    // Best-effort: tell the sender we failed so it doesn't hang.
                    if let Some(req_header) = msg.header.as_ref() {
                        let response = SwbusMessage::new_response(
                            req_header,
                            Some(&self.sp),
                            SwbusErrorCode::Fail,
                            &format!("ActorCreator panicked: {panic_msg}"),
                            self.id_generator.generate(),
                            None,
                        );
                        if self.rt.send(response).await.is_err() {
                            error!("Failed to send panic response to swbus");
                        }
                    }
                    // Continue the loop; the route stays valid.
                }
            }
        }
    }

    async fn handle_received_message(&self, msg: &SwbusMessage) -> Result<()> {
        let header = msg
            .header
            .as_ref()
            .ok_or_else(|| SwbusError::input(SwbusErrorCode::InvalidPayload, "missing message header".to_string()))?;
        let source = header
            .source
            .as_ref()
            .ok_or_else(|| SwbusError::input(SwbusErrorCode::InvalidPayload, "missing message source".to_string()))?;
        let destination = header.destination.as_ref().ok_or_else(|| {
            SwbusError::input(
                SwbusErrorCode::InvalidPayload,
                "missing message destination".to_string(),
            )
        })?;
        // todo: alternatively, check message type
        if source.resource_type != "swss-common-bridge" {
            // todo: log a message
            return Err(SwbusError::route(
                SwbusErrorCode::NoRoute,
                "actor doesn't exist: not from common-bridge".to_string(),
            ));
        }
        if let Some(Body::DataRequest(DataRequest { payload })) = &msg.body {
            match ActorMessage::deserialize(payload) {
                Ok(actor_msg) => {
                    let kfv: KeyOpFieldValues = actor_msg.deserialize_data().map_err(|_| {
                        SwbusError::input(
                            SwbusErrorCode::InvalidPayload,
                            "cannot decode as ActorMessage".to_string(),
                        )
                    })?;

                    if kfv.operation == KeyOperation::Del {
                        return Err(SwbusError::route(
                            SwbusErrorCode::NoRoute,
                            "actor doesn't exist: won't create actor for DEL kfv".to_string(),
                        ));
                    }
                    // create_fn / spawn are user-supplied and must not be allowed to
                    // panic the run loop. Catch any panic and turn it into a Fail.
                    let key = kfv.key.clone();
                    let create_result = std::panic::catch_unwind(AssertUnwindSafe(|| (self.create_fn)(key)));
                    let actor = match create_result {
                        Ok(Ok(actor)) => actor,
                        Ok(Err(e)) => {
                            let mut sp = self.sp.clone();
                            sp.resource_id = kfv.key.clone();
                            return Err(SwbusError::internal(
                                SwbusErrorCode::Fail,
                                format!("Failed to create actor {}. Error: {}", sp.to_swbusd_service_path(), e),
                            ));
                        }
                        Err(panic) => {
                            let mut sp = self.sp.clone();
                            sp.resource_id = kfv.key.clone();
                            return Err(SwbusError::internal(
                                SwbusErrorCode::Fail,
                                format!(
                                    "Panic while creating actor {}: {}",
                                    sp.to_swbusd_service_path(),
                                    panic_payload_to_string(&panic)
                                ),
                            ));
                        }
                    };
                    let res_type = destination.resource_type.clone();
                    let res_id = destination.resource_id.clone();
                    if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(|| spawn(actor, &res_type, &res_id)))
                    {
                        return Err(SwbusError::internal(
                            SwbusErrorCode::Fail,
                            format!(
                                "Panic while spawning actor {}/{}: {}",
                                res_type,
                                res_id,
                                panic_payload_to_string(&panic)
                            ),
                        ));
                    }
                }
                Err(_) => {
                    // log a message
                    return Err(SwbusError::input(
                        SwbusErrorCode::InvalidPayload,
                        "cannot decode as ActorMessage".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

pub async fn spawn_consumer_bridge_for_actor<T>(
    edge_runtime: Arc<SwbusEdgeRuntime>,
    actor_name: &'static str,
    actor_id: Option<&str>,
    single_entry: bool,
) -> AnyhowResult<ConsumerBridge>
where
    T: SonicDbTable + 'static,
{
    spawn_consumer_bridge_for_actor_with_selector::<T, _>(edge_runtime, actor_name, actor_id, single_entry, |_| true)
        .await
}

/// Spawn a consumer bridge for an actor. The bridge subscribes to the given db and table.
/// The bridge will send messages to the actor when there is update on the table. If actor_id is
/// provided, the message destination is `sp(actor_name, actor_id)`. Otherwise, the
/// service path is constructed as `sp(actor_name, key)`, where key is the key of the table entry.
/// The ActorMessage payload is the KeyOpFieldValues of the table entry. If single_entry is set,
/// the key of the ActorMessage is the table_name. Otherwise, the key is table_name|key.
/// selector is a function that takes a KeyOpFieldValues and returns a boolean. If the function
/// returns true, the message is sent to the actor. Otherwise, the message is ignored.
///
pub async fn spawn_consumer_bridge_for_actor_with_selector<T, F>(
    edge_runtime: Arc<SwbusEdgeRuntime>,
    actor_name: &'static str,
    actor_id: Option<&str>,
    single_entry: bool,
    selector: F,
) -> AnyhowResult<ConsumerBridge>
where
    T: SonicDbTable + 'static,
    F: Fn(&KeyOpFieldValues) -> bool + Sync + Send + 'static,
{
    let db = crate::db_for_table::<T>().await?;

    let sst = SubscriberStateTable::new_async(db, T::table_name(), None, None).await?;

    let addr = crate::common_bridge_sp::<T>(&edge_runtime);

    if let Some(actor_id) = actor_id {
        let sp = edge_runtime.new_sp(actor_name, actor_id);
        Ok(ConsumerBridge::spawn::<T, _, _, _>(
            edge_runtime,
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                let key = match single_entry {
                    true => T::table_name().to_owned(),
                    false => format!("{}|{}", T::table_name(), kfv.key),
                };
                (sp.clone(), key)
            },
            selector,
        ))
    } else {
        let base_addr = edge_runtime.get_base_sp();
        Ok(ConsumerBridge::spawn::<T, _, _, _>(
            edge_runtime,
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                let mut addr = base_addr.clone();
                addr.resource_type = actor_name.to_owned();
                addr.resource_id = kfv.key.clone();
                (
                    addr,
                    match single_entry {
                        true => T::table_name().to_owned(),
                        false => format!("{}|{}", T::table_name(), kfv.key),
                    },
                )
            },
            selector,
        ))
    }
}

pub async fn spawn_zmq_producer_bridge<T>(
    edge_runtime: Arc<SwbusEdgeRuntime>,
    zmq_endpoint: &str,
) -> AnyhowResult<JoinHandle<()>>
where
    T: SonicDbTable + 'static,
{
    if let Ok(zmqc) = ZmqClient::new(zmq_endpoint) {
        let dpu_appl_db = crate::db_for_table::<T>().await?;
        let zpst = ZmqProducerStateTable::new(dpu_appl_db, T::table_name(), zmqc, true).unwrap();

        let sp = crate::common_bridge_sp::<T>(&edge_runtime);
        info!(
            "spawned ZMQ producer bridge for {} at {}",
            T::table_name(),
            sp.to_longest_path()
        );
        Ok(spawn_producer_bridge(edge_runtime.clone(), sp, zpst))
    } else {
        anyhow::bail!("Failed to connect to ZMQ server at {}", zmq_endpoint);
    }
}

pub async fn spawn_vanilla_producer_bridge<T>(edge_runtime: Arc<SwbusEdgeRuntime>) -> AnyhowResult<JoinHandle<()>>
where
    T: SonicDbTable + 'static,
{
    let db = crate::db_for_table::<T>().await?;
    let pst = ProducerStateTable::new(db, T::table_name()).unwrap();

    let sp = crate::common_bridge_sp::<T>(&edge_runtime);
    info!(
        "spawned producer bridge for {} at {}",
        T::table_name(),
        sp.to_longest_path()
    );
    Ok(spawn_producer_bridge(edge_runtime.clone(), sp, pst))
}

#[cfg(test)]
mod actor_creator_tests {
    //! Unit tests for [`ActorCreator`].
    //!
    //! Drives the message loop in [`ActorCreator::run`] directly (no DB / consumer-bridge).
    //! The most important case is the regression test for the panic-recovery fix:
    //! a panicking `create_fn` must not kill the run loop or orphan the route.
    use super::*;
    use crate::actors::test;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use swbus_actor::{set_global_runtime_if_unset, Actor, Context, State};
    use swbus_edge::simple_client::{IncomingMessage, MessageBody, OutgoingMessage, SimpleSwbusEdgeClient};
    use swbus_edge::swbus_proto::swbus::{ServicePath, SwbusErrorCode};

    /// Trivial actor used as the product of `create_fn`.
    struct TestActor;
    impl Actor for TestActor {
        async fn handle_message(
            &mut self,
            _state: &mut State,
            _key: &str,
            _ctx: &mut Context,
        ) -> swbus_actor::Result<()> {
            Ok(())
        }
    }

    /// Drives `create_fn` behaviour from the outside.
    #[derive(Clone, Copy)]
    enum CreateBehaviour {
        Ok,
        Err,
        Panic,
    }

    /// Build a CommonBridge-style request payload for [`ActorCreator`].
    fn build_payload(kfv_key: &str, op: &str) -> Vec<u8> {
        // Shape produced by ConsumerBridge: ActorMessage { key: <table>, data: KeyOpFieldValues }.
        let am = swbus_actor::ActorMessage::new(
            "TestTable",
            &serde_json::json!({
                "key": kfv_key,
                "operation": op,
                "field_values": {},
            }),
        )
        .unwrap();
        am.serialize()
    }

    /// Send a request to `dest` from `client` and wait for the matching response.
    async fn send_and_recv(
        client: &SimpleSwbusEdgeClient,
        dest: ServicePath,
        payload: Vec<u8>,
    ) -> (SwbusErrorCode, String) {
        let msg = OutgoingMessage {
            destination: dest,
            body: MessageBody::Request { payload },
        };
        let sent_id = client.send(msg).await.unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or(Duration::ZERO);
            let recv = tokio::time::timeout(remaining, client.recv())
                .await
                .expect("timed out waiting for response");
            match recv {
                Some(IncomingMessage {
                    body:
                        MessageBody::Response {
                            request_id,
                            error_code,
                            error_message,
                            ..
                        },
                    ..
                }) if request_id == sent_id => return (error_code, error_message),
                Some(_) => continue, // unrelated, keep waiting
                None => panic!("client receiver closed"),
            }
        }
    }

    #[tokio::test]
    async fn actor_creator_robustness() {
        // All scenarios share one global ActorRuntime (process-wide static),
        // so they run sequentially inside a single #[tokio::test].
        sonic_common::log::init_logger_for_test();

        let runtime = test::create_actor_runtime(0, "10.0.0.0", "10::").await;
        let edge = runtime.get_swbus_edge();
        // ActorCreator uses the *global* swbus_actor::spawn, so we must install
        // a global runtime. _if_unset keeps us safe if any other test already did.
        set_global_runtime_if_unset(runtime);

        let create_calls = Arc::new(AtomicU32::new(0));
        let behaviour = Arc::new(Mutex::new(CreateBehaviour::Ok));

        let actor_name = "TestActor";
        let creator_sp = edge.new_sp(actor_name, "");

        let create_calls_cl = create_calls.clone();
        let behaviour_cl = behaviour.clone();
        let ac = ActorCreator::<_, TestActor>::new(creator_sp.clone(), edge.clone(), true, move |_key| {
            create_calls_cl.fetch_add(1, Ordering::SeqCst);
            // Copy the behaviour out and drop the lock guard *before* doing
            // anything that may panic, otherwise the panic poisons the Mutex.
            let b = *behaviour_cl.lock().unwrap_or_else(|poison| poison.into_inner());
            match b {
                CreateBehaviour::Ok => Ok(TestActor),
                CreateBehaviour::Err => anyhow::bail!("create_fn intentionally failed"),
                CreateBehaviour::Panic => panic!("create_fn intentionally panicked"),
            }
        });
        let creator_handle = tokio::task::spawn(ac.run());

        // Test client posing as the swss-common-bridge.
        let mut bridge_sp = edge.get_base_sp();
        bridge_sp.resource_type = "swss-common-bridge".to_string();
        bridge_sp.resource_id = "TestDb|TestTable".to_string();
        let client = SimpleSwbusEdgeClient::new(edge.clone(), bridge_sp.clone(), true, false);

        // ---- 1. SET kfv -> Ok, actor is spawned at sp(actor_name, key) ----
        let dest = edge.new_sp(actor_name, "key_ok_1");
        let (code, msg) = send_and_recv(&client, dest.clone(), build_payload("key_ok_1", "Set")).await;
        assert_eq!(code, SwbusErrorCode::Ok, "unexpected error: {msg}");
        assert_eq!(create_calls.load(Ordering::SeqCst), 1);
        for _ in 0..50 {
            if edge.has_handler(&dest) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(edge.has_handler(&dest), "spawned actor's route was not registered");

        // ---- 2. DEL kfv -> NoRoute; creator stays alive ----
        let dest = edge.new_sp(actor_name, "key_del");
        let (code, _msg) = send_and_recv(&client, dest, build_payload("key_del", "Del")).await;
        assert_eq!(code, SwbusErrorCode::NoRoute);
        assert_eq!(
            create_calls.load(Ordering::SeqCst),
            1,
            "create_fn must not be called for DEL"
        );

        // ---- 3. wrong source (not swss-common-bridge) -> NoRoute ----
        let mut wrong_source = edge.get_base_sp();
        wrong_source.resource_type = "not-a-bridge".to_string();
        wrong_source.resource_id = "x".to_string();
        let wrong_client = SimpleSwbusEdgeClient::new(edge.clone(), wrong_source, true, false);
        let dest = edge.new_sp(actor_name, "key_wrong_src");
        let (code, _msg) = send_and_recv(&wrong_client, dest, build_payload("key_wrong_src", "Set")).await;
        assert_eq!(code, SwbusErrorCode::NoRoute);

        // ---- 4. malformed payload -> InvalidPayload ----
        let dest = edge.new_sp(actor_name, "key_garbage");
        let (code, _msg) = send_and_recv(&client, dest, b"not a json actor message".to_vec()).await;
        assert_eq!(code, SwbusErrorCode::InvalidPayload);

        // ---- 5. create_fn returns Err -> Fail (internal) ----
        *behaviour.lock().unwrap() = CreateBehaviour::Err;
        let dest = edge.new_sp(actor_name, "key_create_err");
        let calls_before = create_calls.load(Ordering::SeqCst);
        let (code, msg) = send_and_recv(&client, dest, build_payload("key_create_err", "Set")).await;
        assert_eq!(code, SwbusErrorCode::Fail, "got {code:?}: {msg}");
        assert_eq!(create_calls.load(Ordering::SeqCst), calls_before + 1);

        // ---- 6. create_fn panics -> Fail, run loop survives (regression) ----
        *behaviour.lock().unwrap() = CreateBehaviour::Panic;
        let dest = edge.new_sp(actor_name, "key_panic");
        let (code, msg) = send_and_recv(&client, dest, build_payload("key_panic", "Set")).await;
        assert_eq!(code, SwbusErrorCode::Fail, "got {code:?}: {msg}");

        // The original bug would have killed ActorCreator::run, dropped the
        // handler receiver, and turned every subsequent message into
        // "Failed to send message to local handler: channel closed".
        // Confirm the creator is still serving requests.
        *behaviour.lock().unwrap() = CreateBehaviour::Ok;
        let dest = edge.new_sp(actor_name, "key_after_panic");
        let calls_before = create_calls.load(Ordering::SeqCst);
        let (code, msg) = send_and_recv(&client, dest, build_payload("key_after_panic", "Set")).await;
        assert_eq!(
            code,
            SwbusErrorCode::Ok,
            "ActorCreator did not recover from create_fn panic: {msg}"
        );
        assert_eq!(
            create_calls.load(Ordering::SeqCst),
            calls_before + 1,
            "create_fn was not invoked after panic recovery"
        );
        assert!(!creator_handle.is_finished(), "ActorCreator::run unexpectedly exited");

        creator_handle.abort();
        let _ = creator_handle.await;
    }
}

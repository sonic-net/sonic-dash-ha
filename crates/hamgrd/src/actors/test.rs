use crate::RuntimeData;
use serde_json::Value;
use std::{collections::HashMap, future::Future, time::Duration};
use std::{net::Ipv4Addr, sync::Arc};
use swbus_actor::{ActorMessage, ActorRuntime};
use swbus_edge::{
    simple_client::{IncomingMessage, MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode},
    SwbusEdgeRuntime,
};

async fn timeout<T, Fut: Future<Output = T>>(fut: Fut) -> Result<T, tokio::time::error::Elapsed> {
    const TIMEOUT: Duration = Duration::from_millis(1000);
    tokio::time::timeout(TIMEOUT, fut).await
}

#[macro_export]
macro_rules! send {
    (key: $key:expr, data: $data:tt) => {
        send!(@build $key, $data, swbus_edge::swbus_proto::swbus::ServicePath::from_string("unknown.unknown.unknown/hamgrd/0/test/test").unwrap(), false)
    };
    (key: $key:expr, data: $data:tt, addr: $addr:expr) => {
        send!(@build $key, $data, $addr, false)
    };
    (key: $key:expr, data: $data:tt, fail) => {
        send!(@build $key, $data, swbus_edge::swbus_proto::swbus::ServicePath::from_string("unknown.unknown.unknown/hamgrd/0/test/test").unwrap(), true)
    };
    (key: $key:expr, data: $data:tt, addr: $addr:expr, fail) => {
        send!(@build $key, $data, $addr, true)
    };

    (@build $key:expr, $data:tt, $addr:expr, $fail:expr) => {
        $crate::actors::test::Command::Send {
            key: String::from($key),
            data: serde_json::json!($data),
            addr: $addr,
            fail: $fail,
        }
    };
}
pub use send;

#[macro_export]
macro_rules! recv {
    (key: $key:expr, data: $data:tt, addr: $addr:expr) => {
        $crate::actors::test::Command::Recv {
            key: String::from($key),
            data: serde_json::json!($data),
            addr: $addr.clone(),
        }
    };
}
pub use recv;

pub enum Command {
    Send {
        key: String,
        data: Value,
        addr: ServicePath,
        fail: bool,
    },
    Recv {
        key: String,
        data: Value,
        addr: ServicePath,
    },
}

pub async fn run_commands(runtime: &ActorRuntime, aut: ServicePath, commands: &[Command]) {
    use Command::*;

    let mut clients: HashMap<ServicePath, SimpleSwbusEdgeClient> = HashMap::new();

    // Pre-populate clients
    for cmd in commands {
        match cmd {
            Send { addr, .. } | Recv { addr, .. } => {
                if !clients.contains_key(addr) {
                    let client = SimpleSwbusEdgeClient::new(runtime.get_swbus_edge(), addr.clone(), true);
                    clients.insert(addr.clone(), client);
                }
            }
        }
    }

    // Execute commands
    for cmd in commands {
        match cmd {
            Send { key, data, addr, fail } => {
                let client = &clients[addr];
                let am = ActorMessage::new(key, data).unwrap();
                let msg = OutgoingMessage {
                    destination: aut.clone(),
                    body: MessageBody::Request {
                        payload: am.serialize(),
                    },
                };
                let sent_id = client.send(msg).await.unwrap();

                print!("Sent {key}, ");

                if *fail {
                    print!("expecting Fail, ");
                } else {
                    print!("expecting Ok, ");
                }

                let (error_code, error_msg) = match timeout(client.recv()).await {
                    Ok(Some(IncomingMessage {
                        body:
                            MessageBody::Response {
                                request_id,
                                error_code,
                                error_message,
                                response_body: None,
                            },
                        ..
                    })) if request_id == sent_id => (error_code, error_message),
                    Err(_) => {
                        println!("got Timeout");
                        panic!("Timed out waiting for response")
                    }
                    m => unreachable!("Some other message received: {m:#?}"),
                };

                if *fail && error_code == SwbusErrorCode::Fail {
                    println!("got Fail ({error_msg})");
                } else if !*fail && error_code == SwbusErrorCode::Ok {
                    println!("got Ok");
                } else {
                    println!("got {error_code:?} ({error_msg})");
                    panic!("Got unexpected response");
                }
            }

            Recv { key, data, addr } => {
                let client = &clients[addr];
                print!("Receiving {key}, ");
                let (am, request_id) = match timeout(client.recv()).await {
                    Ok(Some(IncomingMessage {
                        body: MessageBody::Request { payload },
                        id: request_id,
                        ..
                    })) => (ActorMessage::deserialize(&payload).unwrap(), request_id),
                    Err(_) => {
                        println!("got Timeout");
                        panic!("Timed out waiting for request")
                    }
                    m => unreachable!("Some other message received: {m:#?}"),
                };

                println!("got {}", am.key);
                assert_eq!(&am.key, key);
                assert_eq!(&am.data, data);

                let ack = OutgoingMessage {
                    destination: aut.clone(),
                    body: MessageBody::Response {
                        request_id,
                        error_code: SwbusErrorCode::Ok,
                        error_message: "".to_string(),
                        response_body: None,
                    },
                };
                client.send(ack).await.unwrap();
            }
        }
    }
}

pub async fn create_edge_runtime() -> SwbusEdgeRuntime {
    let mut swbus_edge: SwbusEdgeRuntime = SwbusEdgeRuntime::new(
        "none".to_string(),
        ServicePath::from_string("unknown.unknown.unknown/hamgrd/0").unwrap(),
    );
    swbus_edge.start().await.unwrap();
    swbus_edge
}

pub async fn create_actor_runtime(slot: u32, npu_ipv4: &str) -> ActorRuntime {
    let mut edge = create_edge_runtime().await;

    let runtime_data = RuntimeData::new(
        slot,
        Some(npu_ipv4.parse::<Ipv4Addr>().expect("Bad ipv4 address is provided")),
        None,
    );

    edge.set_runtime_env(Box::new(runtime_data));
    ActorRuntime::new(Arc::new(edge))
}

use std::{mem, time::Duration};
use swbus_actor::{Actor, ActorMessage, ActorRuntime, Context, Result, State};
use swbus_edge::{swbus_proto::swbus::ConnectionType, swbus_proto::swbus::ServicePath, SwbusEdgeRuntime};
use tokio::{
    sync::oneshot::{channel, Sender},
    time::timeout,
};

fn sp(name: &str) -> ServicePath {
    ServicePath::from_string(&format!("test.test.test/test/test/test/{name}")).unwrap()
}

#[tokio::test]
async fn echo() {
    let mut swbus_edge = SwbusEdgeRuntime::new("none".to_string(), sp("none"), ConnectionType::InNode);
    swbus_edge.start().await.unwrap();
    let actor_runtime: ActorRuntime = ActorRuntime::new(swbus_edge.into());
    swbus_actor::set_global_runtime(actor_runtime);

    let (notify_done, is_done) = channel();

    swbus_actor::spawn(EchoServer, "test", "echo");
    swbus_actor::spawn(EchoClient(notify_done), "test", "client");

    timeout(Duration::from_secs(3), is_done)
        .await
        .expect("timeout")
        .unwrap();

    {
        // Access the actor runtime to get the service path and check if handler exists
        let runtime_guard = swbus_actor::get_global_runtime();
        let runtime = runtime_guard.as_ref().unwrap();
        let sp = runtime.sp("test", "client");
        let has_handler = runtime.get_swbus_edge().has_handler(&sp);
        assert!(!has_handler);
    }

    let (notify_done, is_done) = channel();

    // spawn the same actor to make sure the previous actor cleans up properly
    swbus_actor::spawn(EchoClient(notify_done), "test", "client");

    timeout(Duration::from_secs(3), is_done)
        .await
        .expect("timeout")
        .unwrap();
}

struct EchoClient(Sender<()>);

impl EchoClient {
    fn notify_done(&mut self) {
        mem::replace(&mut self.0, channel().0).send(()).unwrap();
    }
}

impl Actor for EchoClient {
    async fn init(&mut self, state: &mut State) -> Result<()> {
        let sp = state.outgoing().from_my_sp("test", "echo");
        state.outgoing().send(sp, ActorMessage::new("0", &0)?);
        Ok(())
    }

    async fn handle_message(&mut self, state: &mut State, key: &str, context: &mut Context) -> Result<()> {
        let count = key.parse::<u32>().unwrap();

        // Assert that the incoming table has messages 0..=count still cached
        for i in 0..=count {
            let n = state
                .incoming()
                .get_or_fail(&format!("{i}"))?
                .deserialize_data::<u32>()?;
            assert_eq!(n, i);
        }

        if count == 1000 {
            self.notify_done();
            // terminate this actor
            context.stop();
        } else {
            state
                .outgoing()
                .send(sp("echo"), ActorMessage::new(format!("{}", count + 1), &(count + 1))?);
        }
        Ok(())
    }
}

struct EchoServer;

impl Actor for EchoServer {
    async fn handle_message(&mut self, state: &mut State, key: &str, _context: &mut Context) -> Result<()> {
        let entry = state.incoming().get_entry(key).unwrap();
        let source = entry.source.clone();
        let msg = entry.msg.clone();
        state.outgoing().send(source, msg);
        Ok(())
    }
}

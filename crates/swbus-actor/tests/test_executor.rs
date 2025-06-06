use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap, fs::File, future::pending, io::BufReader, mem, path::PathBuf, sync::Arc, time::Duration,
};
use swbus_actor::{Actor, ActorMessage, ActorRuntime, Context, State};
use swbus_edge::{swbus_proto::swbus::ServicePath, SwbusEdgeRuntime};
use swss_common::{DbConnector, Table};
use swss_common_testing::{random_string, Redis};
use tokio::{
    sync::mpsc::{channel, Sender},
    time::timeout,
};

#[derive(Serialize, Deserialize)]
struct TestSpec {
    spawn: Vec<String>,
    actors: HashMap<String, TestActorSpec>,
}

#[derive(Serialize, Deserialize)]
struct TestActorSpec {
    #[serde(default)]
    init: Vec<Action>,
    handle_message: HashMap<String, Vec<Action>>,
}

#[derive(Serialize, Deserialize)]
struct HandleMessage {
    key: String,
    val: String,
    action: Vec<Action>,
}

#[derive(Serialize, Deserialize, Debug)]
enum Action {
    AddInternalTable { key: String },
    AssertEq(String, String),
    Print(String),
    Read { key: String, field: String, var: String },
    Write { key: String, field: String, val: String },
    Send { to: String, key: String, val: String },
    Finish,
    If { eq: (String, String), then: Vec<Action> },
}

#[derive(Default)]
struct Environment(HashMap<String, String>);

impl Environment {
    fn clear(&mut self) {
        self.0.clear();
    }

    fn set(&mut self, key: &str, val: &str) {
        self.0.insert(key.to_string(), val.to_string());
    }

    fn eval(&self, expr: &str) -> String {
        let mut iter = expr.chars();
        let mut out = String::new();

        while !iter.as_str().is_empty() {
            match iter.next().unwrap() {
                '<' => {
                    let name = iter.by_ref().take_while(|c| *c != '>').collect::<String>();
                    out.push_str(&self.0[&name]);
                }
                '+' => {
                    let lhs = out.parse::<i64>().expect("Expr not a number");
                    let rhs = self.eval(iter.as_str()).parse::<i64>().expect("Expr not a number");
                    return format!("{}", lhs + rhs);
                }
                c if c.is_whitespace() => {}
                c => out.push(c),
            }
        }

        out
    }
}

#[test]
fn environment() {
    let mut env = Environment::default();
    env.set("a", "5");
    env.set("b", "10");
    env.set("c", "15");
    assert_eq!(env.eval("<a> + <b> + <c> + 1"), "31");
    assert_eq!(env.eval("<a>aaa"), "5aaa");
}

#[test]
fn json_tests() {
    // Discover tests
    let tests_dir = std::env::current_dir().unwrap().join("tests");
    #[rustfmt::skip]
    let tests = std::fs::read_dir(&tests_dir)
        .unwrap()
        .map(|res| res.unwrap().path())
        .filter(|p| p.extension().unwrap_or_default() == "json")
        .map(|p| (p.clone(), serde_json::from_reader(BufReader::new(File::open(p).unwrap())).unwrap()))
        .collect::<Vec<(PathBuf, TestSpec)>>();

    for (path, t) in tests {
        println!("Running test {}", path.display());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let maybe_err = rt.block_on(run_test(t));
        rt.shutdown_background();
        if let Some(err) = maybe_err {
            panic!("[{}]: {err}", path.display());
        }
    }
}

async fn run_test(mut t: TestSpec) -> Option<&'static str> {
    let redis = Redis::start();
    let (notify_done, mut recv_done) = channel::<()>(1);

    let mut swbus_edge = SwbusEdgeRuntime::new(
        "<none>".into(),
        ServicePath::from_string("test.test.test/test/test").unwrap(),
    );
    swbus_edge.start().await.unwrap();
    let actor_rt = ActorRuntime::new(Arc::new(swbus_edge));
    swbus_actor::set_global_runtime(actor_rt);

    for s in t.spawn {
        let actor = TestActor {
            name: s.clone(),
            db: redis.db_connector(),
            notify_done: notify_done.clone(),
            spec: t.actors.remove(&s).expect(&format!("No actor in test json named {s}")),
            env: Environment::default(),
        };

        swbus_actor::spawn(actor, "test", &s);
    }

    if timeout(Duration::from_secs(5), recv_done.recv()).await.is_err() {
        Some("test timed out")
    } else {
        None
    }
}

fn sp(s: &str) -> ServicePath {
    ServicePath::from_string(&format!("test.test.test/test/test/test/{s}")).unwrap()
}

struct TestActor {
    name: String,
    db: DbConnector,
    notify_done: Sender<()>,
    spec: TestActorSpec,
    env: Environment,
}

impl TestActor {
    async fn run_action(&mut self, action: &Action, state: &mut State) -> Result<()> {
        use Action::*;

        let (internal, _, outgoing) = state.get_all();
        match action {
            AddInternalTable { key } => {
                let db = self.db.clone_timeout_async(3000).await?;
                let tbl = Table::new_async(db, &random_string()).await?;
                internal.add(key, tbl, "swss_key").await;
            }
            AssertEq(a, b) => {
                let a = self.env.eval(a);
                let b = self.env.eval(b);
                assert_eq!(a, b);
            }
            Print(s) => println!("[{}] \"{}\" = {}", self.name, s, self.env.eval(s)),
            Read { key, field, var } => {
                let key = self.env.eval(key);
                let field = self.env.eval(field);
                let var = self.env.eval(var);
                self.env.set(&var, internal.get(&key)[&field].to_str()?);
            }
            Write { key, field, val } => {
                let key = self.env.eval(key);
                let field = self.env.eval(field);
                let val = self.env.eval(val);
                internal.get_mut(&key).insert(field, val.into());
            }
            Send { to, key, val } => {
                let to = self.env.eval(to);
                let key = self.env.eval(key);
                let val = self.env.eval(val);
                outgoing.send(sp(&to), ActorMessage::new(key, &val)?);
            }
            Finish => {
                self.notify_done.send(()).await?;
                pending::<()>().await;
            }
            If { eq: (a, b), then } => {
                if self.env.eval(a) == self.env.eval(b) {
                    for action in then {
                        Box::pin(self.run_action(action, state)).await?;
                    }
                }
            }
        }

        Ok(())
    }
}

impl Actor for TestActor {
    async fn init(&mut self, state: &mut State) -> Result<()> {
        for action in mem::take(&mut self.spec.init) {
            self.run_action(&action, state).await?;
        }
        Ok(())
    }

    async fn handle_message(&mut self, state: &mut State, key: &str, _context: &mut Context) -> Result<()> {
        let entry = state.incoming().get_entry(key).unwrap();
        self.env.clear();
        self.env.set("source", &entry.source.resource_id);
        self.env.set("key", key);
        self.env.set("val", &entry.msg.deserialize_data::<String>()?);

        let handle_message_map = mem::take(&mut self.spec.handle_message);
        if let Some(actions) = handle_message_map.get(key).or_else(|| handle_message_map.get("*")) {
            for action in actions {
                self.run_action(&action, state).await?;
            }
        }
        self.spec.handle_message = handle_message_map;

        Ok(())
    }
}

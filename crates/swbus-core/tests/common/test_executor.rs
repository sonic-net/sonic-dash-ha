use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::BufReader;
use swbus_core::mux::core_runtime::SwbusCoreRuntime;
use swbus_core::mux::route_config::RoutesConfig;
use swbus_core::mux::route_config::*;
use swbus_edge::core_client::SwbusCoreClient;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant};

//3 seconds receive timeout
pub const RECEIVE_TIMEOUT: u32 = 3;
lazy_static! {
    static ref TOPO: std::sync::Mutex<Topo> = std::sync::Mutex::new(Topo::default());
}

/// The Topo struct contains the server jobs and clients' TX and RX of its message queues.
#[derive(Default)]
struct Topo {
    pub name: String,
    /// The server jobs are the tokio tasks that run the swbusd servers.
    pub server_jobs: Vec<JoinHandle<()>>,
    /// The client_receivers are the message queues of the clients to receive messages.
    pub client_receivers: HashMap<String, mpsc::Receiver<SwbusMessage>>,
    /// The client_senders are the message queues of the clients to send messages.
    pub client_senders: HashMap<String, mpsc::Sender<SwbusMessage>>,
}

/// The test case data including the name, topo, description, and test steps.
/// topo is optional and if it is not provided, the test will be skipped if it doesn't match current topo.
/// The test steps contain the requests to be sent from the client and the expected responses from specified clients.
#[derive(Serialize, Deserialize, Debug)]
struct TestCaseData {
    pub name: String,
    pub topo: Option<String>,
    pub description: Option<String>,
    pub steps: Vec<TestStepData>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TestStepData {
    pub requests: Vec<MessageClientPair>,
    pub responses: Vec<MessageClientPair>,
}

#[derive(Serialize, Deserialize, Debug)]
struct MessageClientPair {
    pub client: String,
    pub message: SwbusMessage,
}

/// The topology definition including servers and clients.
#[derive(Deserialize, Debug)]
struct TopoData {
    pub servers: HashMap<String, SwbusdConfig>,
    pub clients: HashMap<String, SwbusClientConfig>,
}
#[derive(Deserialize, Debug)]
struct SwbusdConfig {
    /// the endpoint of the swbusd
    pub endpoint: String,
    /// the routes and peers configuration
    pub routes: Vec<RouteConfig>,
    pub peers: Vec<PeerConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SwbusClientConfig {
    /// the swbusd where the client is connected
    pub swbusd: String,
    /// the service path of the client
    pub client_sp: String,
}

/// Start a swbusd server with the given name and node address. The route_config_serde contains the routes and peers configuration.
fn start_server(name: &str, node_addr: &str, route_config: RoutesConfig) {
    let mut runtime = SwbusCoreRuntime::new(node_addr.to_string());
    let server_task = tokio::spawn(async move {
        runtime.start(route_config).await.unwrap();
    });
    TOPO.lock().unwrap().server_jobs.push(server_task);
    println!("Server {} started at {}", name, node_addr);
}

/// Start a client with the given name and swbusd address. The client_sp is the service path of the client.
async fn start_client(name: &str, node_addr: &str, client_sp: ServicePath) {
    let (receive_queue_tx, receive_queue_rx) = mpsc::channel::<SwbusMessage>(2);
    let start = Instant::now();
    let addr = format!("http://{}", node_addr);

    while start.elapsed() < Duration::from_secs(10) {
        match SwbusCoreClient::connect(addr.clone(), client_sp.clone(), receive_queue_tx.clone()).await {
            Ok((_, send_queue_tx, _)) => {
                TOPO.lock()
                    .unwrap()
                    .client_receivers
                    .insert(name.to_string(), receive_queue_rx);
                TOPO.lock()
                    .unwrap()
                    .client_senders
                    .insert(name.to_string(), send_queue_tx);
                println!("Client {} connected to {}", name, node_addr);
                return;
            }
            Err(e) => {
                eprintln!("Failed to connect to the server: {:?}", e);
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    panic!("Failed to connect to the server");
}

/// Bring up a topo with the given name if it is not already up. The topo data is read from tests/data/topos.json.
/// The topo data is a map of topo names to the server (swbusd) and client configurations.
/// The server configurations are a map of server names to the server configuration.
/// The server configuration contains the endpoint of the server and the routes and peers configuration.
/// The client configurations are a map of client names to the client configuration.
/// The client configuration contains the server (swbusd) name where the client is connected and the service path of the client.
pub async fn bring_up_topo(topo_name: &str) {
    if TOPO.lock().unwrap().name == topo_name {
        return;
    }
    let file = File::open("tests/data/topos.json").unwrap();
    let reader = BufReader::new(file);

    // Parse the topo data
    let topos: HashMap<String, TopoData> = serde_json::from_reader(reader).expect("failed to parse topos.json");

    let topo = topos
        .get(topo_name)
        .expect(&format!("Failed to find topo {}", topo_name));
    for (name, server) in &topo.servers {
        let routes_config = RoutesConfig {
            routes: server.routes.clone(),
            peers: server.peers.clone(),
        };
        start_server(name, &server.endpoint, routes_config);
    }
    for (name, client) in &topo.clients {
        let server = topo
            .servers
            .get(&client.swbusd)
            .expect(&format!("Failed to find topo swbusd {}", client.swbusd));
        start_client(
            name,
            &server.endpoint,
            ServicePath::from_string(&client.client_sp).unwrap(),
        )
        .await;
    }
    TOPO.lock().unwrap().name = topo_name.to_string();
    println!("Topo {} is up", topo_name);
}

/// Run the tests with the given test json file and test case name. If the test case name is provided,
/// only that test case will be run.
pub async fn run_tests(test_json_file: &str, test_case_name: Option<&str>) {
    let json_content = fs::read_to_string(test_json_file).unwrap();
    // Parse the test data
    let mut testcases: Vec<TestCaseData> = serde_json::from_str(&json_content).expect("failed to parse test data");
    let to_generate = env::var("GENERATE_TEST_DATA").is_ok();
    for test in &mut testcases {
        if let Some(tc) = test_case_name {
            if test.name != tc {
                continue;
            }
        }
        if test.topo.is_some() && test.topo.as_ref().unwrap() != &TOPO.lock().unwrap().name {
            println!(
                "Skipping test {} due to mismatched topo: test.topo={}, running-topo={}",
                test.name,
                test.topo.as_ref().unwrap(),
                TOPO.lock().unwrap().name
            );
            continue;
        }
        println!("Running test: {}", test.name);
        for (i, step) in test.steps.iter_mut().enumerate() {
            println!("  ---  Step {}  ---", i);
            for req in &step.requests {
                let topo = TOPO.lock().unwrap();
                let sender = topo.client_senders.get(&req.client).unwrap();
                match sender.send(req.message.clone()).await {
                    Ok(_) => {
                        println!("Sent message from client {}", req.client);
                    }
                    Err(e) => {
                        eprintln!("Failed to send message from client {}: {:?}", req.client, e);
                    }
                }
            }

            if to_generate {
                let responses = record_received_messages(RECEIVE_TIMEOUT).await;
                step.responses = responses;
                println!("  ---  Recorded {} messages  ---", &step.responses.len());
            } else {
                receive_and_compare(&step.responses, RECEIVE_TIMEOUT).await;
            }
        }
    }
    if to_generate {
        let json = serde_json::to_string_pretty(&testcases).unwrap();
        fs::write(test_json_file, json).unwrap();
    }
}

/// Wait for the responses. Currently we don't support multiple responses from the same client
/// if the responses are not in order.
async fn receive_and_compare(expected_responses: &Vec<MessageClientPair>, timeout: u32) {
    for resp in expected_responses.into_iter() {
        let mut topo = TOPO.lock().unwrap();
        let receiver = topo.client_receivers.get_mut(&resp.client).unwrap();
        match time::timeout(Duration::from_secs(timeout as u64), receiver.recv()).await {
            Ok(Some(msg)) => {
                // by serializing then deserializing, we reset undeterminstic fields to default to compare
                let json_string = serde_json::to_string(&msg).unwrap();
                let trimmed_msg: SwbusMessage = serde_json::from_str(&json_string).unwrap();
                assert_eq!(trimmed_msg, resp.message);
            }
            Ok(None) => {
                panic!("channel broken");
            }
            Err(_) => {
                panic!("timeout waiting for response: {:?}", resp);
            }
        }
    }
}

async fn record_received_messages(timeout: u32) -> Vec<MessageClientPair> {
    let mut topo = TOPO.lock().unwrap();
    let start = Instant::now();
    let mut responses = Vec::new();
    while start.elapsed() < Duration::from_secs(timeout as u64) {
        for (name, receiver) in topo.client_receivers.iter_mut() {
            match receiver.try_recv() {
                Ok(msg) => {
                    responses.push(MessageClientPair {
                        client: name.clone(),
                        message: msg,
                    });
                }
                Err(_) => continue,
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    responses
}

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::BufReader;
use std::net::SocketAddr;
use swbus_config::*;
use swbus_core::mux::service::SwbusServiceHost;
use swbus_edge::core_client::SwbusCoreClient;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant};

// 3 seconds receive timeout
pub const RECEIVE_TIMEOUT: u32 = 3;

/// The Topo struct contains the server jobs and clients' TX and RX of its message queues.
pub struct TopoRuntime {
    /// test folder name
    pub topo_file: String,
    /// The server jobs are the tokio tasks that run the swbusd servers.
    pub server_jobs: Vec<JoinHandle<()>>,
    /// The client_receivers are the message queues of the clients to receive messages.
    pub client_receivers: HashMap<String, mpsc::Receiver<SwbusMessage>>,
    /// The client_senders are the message queues of the clients to send messages.
    pub client_senders: HashMap<String, mpsc::Sender<SwbusMessage>>,
}

/// The test case data including the name, description, and test steps.
/// The test steps contain the requests to be sent from the client and the expected responses from specified clients.
#[derive(Serialize, Deserialize, Debug)]
struct TestCaseData {
    pub name: String,
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
    pub servers: HashMap<String, SwbusConfig>,
    pub clients: HashMap<String, SwbusClientConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SwbusClientConfig {
    /// the swbusd where the client is connected
    pub swbusd: String,
    /// the service path of the client
    pub client_sp: String,
}

impl TopoRuntime {
    pub fn new(topo_file: &str) -> Self {
        TopoRuntime {
            topo_file: topo_file.to_string(),
            server_jobs: Vec::new(),
            client_receivers: HashMap::new(),
            client_senders: HashMap::new(),
        }
    }

    /// Bring up a topo with the given name if it is not already up. The topo data is read from tests/data/topos.json.
    /// The topo data is a map of topo names to the server (swbusd) and client configurations.
    /// The server configurations are a map of server names to the server configuration.
    /// The server configuration contains the endpoint of the server and the routes and peers configuration.
    /// The client configurations are a map of client names to the client configuration.
    /// The client configuration contains the server (swbusd) name where the client is connected and the service path of the client.
    pub async fn bring_up(&mut self) {
        let file = File::open(&self.topo_file).unwrap();
        let reader = BufReader::new(file);

        // Parse the topo data
        let topo_cfg: TopoData = serde_json::from_reader(reader).expect("failed to parse topo.json");

        for (name, server) in topo_cfg.servers.clone() {
            self.start_server(&name, &server).await;
        }

        for (name, client) in &topo_cfg.clients {
            let server = topo_cfg
                .servers
                .get(&client.swbusd)
                .unwrap_or_else(|| panic!("Failed to find topo swbusd {}", client.swbusd));
            self.start_client(
                name,
                &server.endpoints.first().unwrap(),
                ServicePath::from_string(&client.client_sp).unwrap(),
            )
            .await;
        }

        println!("Topo {} is up", self.topo_file);
    }

    async fn start_server(&mut self, name: &str, route_config: &SwbusConfig) {
        let service_host = SwbusServiceHost::new(route_config.endpoints.clone());
        let config_clone = route_config.clone();
        let server_task = tokio::spawn(async move {
            service_host.start(config_clone).await.unwrap();
        });

        self.server_jobs.push(server_task);

        println!("Server {} started at {:?}", name, route_config.endpoints);
    }

    async fn start_client(&mut self, name: &str, node_addr: &SocketAddr, client_sp: ServicePath) {
        let (receive_queue_tx, receive_queue_rx) = mpsc::channel::<SwbusMessage>(2);
        let start = Instant::now();
        let addr = format!("http://{node_addr}");

        while start.elapsed() < Duration::from_secs(10) {
            println!("Trying to connect to the server at {}", node_addr);
            match SwbusCoreClient::connect(
                addr.clone(),
                client_sp.clone(),
                ConnectionType::InNode,
                receive_queue_tx.clone(),
            )
            .await
            {
                Ok((_, send_queue_tx)) => {
                    self.client_receivers.insert(name.to_string(), receive_queue_rx);
                    self.client_senders.insert(name.to_string(), send_queue_tx);
                    println!("Client {} connected to {}", name, node_addr);
                    return;
                }
                Err(e) => {
                    println!("Failed to connect to the server: {:?}", e);
                    //std::thread::sleep(std::time::Duration::from_secs(1));
                    time::sleep(Duration::from_secs(1)).await;
                    println!("awake from sleep");
                }
            }
        }
        panic!("Failed to connect to the server");
    }
}

/// Run the tests with the given test json file and test case name. If the test case name is provided,
/// only that test case will be run.
pub async fn run_tests(topo: &mut TopoRuntime, test_json_file: &str, test_case_name: Option<&str>) {
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

        println!("Running test: {}", test.name);
        for (i, step) in test.steps.iter_mut().enumerate() {
            println!("  ---  Step {}  ---", i);
            for req in &step.requests {
                let sender = topo
                    .client_senders
                    .get(&req.client)
                    .unwrap_or_else(|| panic!("Failed to find client sender {} in topo file", &req.client));

                match sender.send(req.message.clone()).await {
                    Ok(_) => {
                        println!("Sent message from client {}", req.client);
                    }
                    Err(e) => {
                        println!("Failed to send message from client {}: {:?}", req.client, e);
                    }
                }
            }

            if to_generate {
                let responses = record_received_messages(topo, RECEIVE_TIMEOUT).await;
                step.responses = responses;
                println!("  ---  Recorded {} messages  ---", &step.responses.len());
            } else {
                receive_and_compare(topo, &step.responses, RECEIVE_TIMEOUT).await;
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
async fn receive_and_compare(topo: &mut TopoRuntime, expected_responses: &[MessageClientPair], timeout: u32) {
    for resp in expected_responses.iter() {
        let receiver = topo
            .client_receivers
            .get_mut(&resp.client)
            .unwrap_or_else(|| panic!("Failed to find client receiver {} in topo file", &resp.client));

        match time::timeout(Duration::from_secs(timeout as u64), receiver.recv()).await {
            Ok(Some(msg)) => {
                let normalized_msg = swbus_proto::swbus::normalize_msg(&msg);

                assert_eq!(
                    normalized_msg, resp.message,
                    "Received(left) message does not match the expected(right)"
                );
            }
            Ok(None) => {
                panic!("channel broken");
            }
            Err(_) => {
                panic!("timeout waiting for response: {resp:?}");
            }
        }
    }
}

async fn record_received_messages(topo: &mut TopoRuntime, timeout: u32) -> Vec<MessageClientPair> {
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

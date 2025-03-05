# Introduction
A test infrastructure is implemented by common/test_executor.rs. It automates test topology bringup, sending swbus message and comparing received swbus messages to assert test pass or failure. Here are the high level functionalities of the infra.
- Test topology bringup. It takes topology definition in a specified JSON file and brings up the topology, which includes multiple swbusd instances and swbus clients.
- Execute tests in the topology. Test is defined in JSON file, which specifies the requests (swbus message) sent from clients and expected responses received by clients, which can be same or different from the clients sending the requests. 
- Record received messages to help creating test JSON file to be used in future runs.

# Topology Definition
Topology is defined in a JSON file. The topology has a few servers (swbusd e.g.) and clients.

1. Add a Description
Provide a short summary of the topology under the "description" field.

"description": "Brief explanation of the topology structure and its connections"

2. Define the Servers
Each server represents a node in the network and should have:

- A unique name (e.g., "swbusd1", "swbusd2")
- An endpoint specifying the IP address and port
- A routes array that defines "my-route" for the swbusd
- A peers array listing neighboring swbusd it connects to
```
Example:

"servers": {
    "server-name": {
        "endpoint": "IP:PORT",
        "routes": [
            {
                "key": "region.cluster.node-id",
                "scope": "InCluster"
            }
        ],
        "peers": [
            {
                "id": "region.cluster.peer-id",
                "endpoint": "IP:PORT",
                "conn_type": "InCluster"
            }
        ]
    }
}
```

3. Define the Clients
Each client connects to a specific server and interacts with a server directly
client-sp defines a service path for the client
```
"clients": {
    "client-name": {
        "swbusd": "server-name",
        "client_sp": "region.cluster.node-id/service-name/instance"
    }
}
```

# Test Data Definition
Test data is also defined in JSON file. It contains multiple test cases, which will be run in sequence. 
- Each test case has a name, description. 
- Each test case can include multiple test steps. In Each test step, multiple requests (swbus message) will be sent from the corresponding clients in parallel. Expected responses will be compared to received messages collected from the corresponding clients to assert test pass or fail.

Below guide explains how to create a structured test data entry in JSON format. 

1. Define the Test Case Name
Each test case should have a unique identifier under the name field, representing its purpose.
"name": "test_case_name"

2. Provide a Description
Describe what the test case is verifying.
"description": "Brief explanation of the test scenario"

3. Define Test Steps
Each test consists of one or more steps, where a client sends a request, and an expected response is verified.
"steps": [ { ... } ]

4. Define Requests
Each step contains a list of requests that are sent by a client. A request consists of:
- client: The client initiating the request.
- message: The swbus message to be sent

5. Define Expected Responses
Each step also contains expected responses that validate the outcome. The response structure mirrors the request, with:
- client: The client receiving the response.
- message: The swbus response message

# Record Response
Instead of hand-crafting a test data with complete info, we can define a test data without response and let test run to collect received messages and fill in response in test data. This can be used to create initial test data or when system changes behavior, it can update the test data with new responses. To do that,
1. Create a test data with empty response, "responses": [], in JSON.
2. Run test (make or cargo test) with env GENERATE_TEST_DATA=1.

# Run tests with trace enabled
In order to troubleshoot a test run with full traces from swbusd, use env ENABLE_TRACE=1 to run make or cargo test. trace output will be printed to stdout or stderr.

# Limitations
We can't have multiple tokio tests in the same test rust file. rust executes tests in the same file in parallel. If different test brings up different topology, there might be conflict between them. For example, multiple swbusd uses the same GRPC port. It also increases difficulties to trouble shoot because traces from different test cases will mix together.
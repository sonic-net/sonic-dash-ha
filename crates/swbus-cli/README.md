# Introduction
swbus-cli is a diagnostic tool to check connectivities between swbusd and to dump runtime information. 

swbus-cli is running in dash-ha container and connects to the local swbusd. It reads DPU table from config-db. With DEV environment, which provides its DPU slot id, it can find the local swbusd's endpoint (npu_ipv4/npu_ipv6) from the entry in DPU table.

In development environment, swbus-cli can run without config-db by providing a configuration file of the local swbusd. 
Here is the usage of the command.
```
Usage: swbus-cli [OPTIONS] <COMMAND>

Commands:
  ping  
  show  
  help  Print this message or the help of the given subcommand(s)

Options:
  -d, --debug                      Enable debug output
  -c, --config-file <CONFIG_FILE>  Path to swbusd config file. Only used for local testing
  -h, --help                       Print help
```
Below are the sub commands and their usage.

## ping
The command is used to test connectivity to a remote swbusd, which is identified by its service path.
```
Usage: swbus-cli ping [OPTIONS] <DEST>

Arguments:
  <DEST>  The destination service path to ping

Options:
  -c, --count <COUNT>        The number of pings to send. Default is unlimited [default: 4294967295]
  -t, --timeout <TIMEOUT>    Timeout in seconds for each ping [default: 1]
  -i, --interval <INTERVAL>  Interval in seconds between pings [default: 1]
  -h, --help                 Print help
```

Here is an example.
```
sonic-dash-ha$ ./target/debug/swbus-cli -c crates/swbusd/sample/swbusd1.cfg ping -c 5 region-a.cluster-a.10.0.0.2-dpu0
Starting edge runtime with URI: http://127.0.0.1:50001
PING region-a.cluster-a.10.0.0.2-dpu0
Connected to the server
Response received: ping_seq=0, ttl=62, time=10.578ms
Response received: ping_seq=1, ttl=62, time=6.083ms
Response received: ping_seq=2, ttl=62, time=5.657ms
Response received: ping_seq=3, ttl=62, time=5.913ms
Response received: ping_seq=4, ttl=62, time=5.893ms
```

## trace-route
The command is similar to trace-route command in Linux. The request will be routed to the destination. Every hop along the path will send a response back, which allows us to see the path the request going through.

```
Usage: swbus-cli trace-route [OPTIONS] <DEST>

Arguments:
  <DEST>  The destination service path of the request

Options:
  -t, --timeout <TIMEOUT>  Timeout in seconds for each request [default: 1]
  -c, --max-hop <MAX_HOP>  Max hop count [default: 10]
  -h, --help               Print help
```

Here is an example of the command.
```
sonic-dash-ha$ target/debug/swbus-cli -c crates/swbusd/sample/swbusd1.cfg trace-route region-a.cluster-a.10.0.0.2-dpu0
traceroute to region-a.cluster-a.10.0.0.2-dpu0, 10 hops max
Starting edge runtime with URI: http://127.0.0.1:50001
Connected to the server
1  region-a.cluster-a.10.0.0.1-dpu0  5.336ms  1 hops
2  region-a.cluster-a.10.0.0.2-dpu0  7.757ms  2 hops
```

## show swbusd route
The command displays route table in the local swbusd
```
Usage: swbus-cli show route

Options:
  -h, --help  Print help
```
Here is an example.
```
sonic-dash-ha$ ./target/debug/swbus-cli -c crates/swbusd/sample/swbusd1.cfg show swbusd route
Starting edge runtime with URI: http://127.0.0.1:50001
Connected to the server
+----------------------------------------+-----------+-----------------------------+---------------------+----------------------------------------+
| service_path                           | hop_count | nh_id                       | nh_scope            | nh_service_path                        |
+----------------------------------------+-----------+-----------------------------+---------------------+----------------------------------------+
| region-a.cluster-a.10.0.0.2-dpu0       | 1         | swbs-to://127.0.0.1:50002   | ROUTE_SCOPE_CLUSTER | region-a.cluster-a.10.0.0.2-dpu0       |
+----------------------------------------+-----------+-----------------------------+---------------------+----------------------------------------+
| region-a.cluster-a.10.0.0.1-dpu0/cli/0 | 1         | swbs-from://127.0.0.1:43806 | ROUTE_SCOPE_LOCAL   | region-a.cluster-a.10.0.0.1-dpu0/cli/0 |
+----------------------------------------+-----------+-----------------------------+---------------------+----------------------------------------+
```

## show hamgrd actor
The command displays actor state in hamgrd

```
sonic-dash-ha$ target/debug/swbus-cli -c crates/swbusd/sample/swbusd1.cfg show hamgrd actor --help
Usage: swbus-cli show hamgrd actor <ACTOR_PATH>

Arguments:
  <ACTOR_PATH>  The service path of the actor relative to the swbusd e.g. "/hamgrd/0/actor/actor1"

Options:
  -h, --help  Print help
```

Here is an example of the command.

```
sonic-dash-ha$ target/debug/swbus-cli -c crates/swbusd/sample/swbusd1.cfg show hamgrd actor '/hamgrd/0/actor/actor1'
Starting edge runtime with URI: http://127.0.0.1:50001
Connected to the server
┌─────┬────────────────────────────────────────────────────────────┐
│                          Incoming State                          │
├─────┼────────────────────────────────────────────────────────────┤
│ key │ details                                                    │
├─────┼────────────────────────────────────────────────────────────┤
│     │  attribute         | value                                 │
│     │ -------------------+-------------------------------------- │
│     │  source            | test.test.test/test/test/test/client  │
│     │ -------------------+-------------------------------------- │
│     │  request-id        | 1744734629495128160                   │
│     │ -------------------+-------------------------------------- │
│     │  version           | 2001                                  │
│     │ -------------------+-------------------------------------- │
│     │  message/key       |                                       │
│     │ -------------------+-------------------------------------- │
│     │  message/value     | {                                     │
│     │                    |   "Get": {                            │
│     │                    |     "key": "count"                    │
│     │                    |   }                                   │
│     │                    | }                                     │
│     │ -------------------+-------------------------------------- │
│     │  created-time      | 2025:04:15 12:30:29                   │
│     │ -------------------+-------------------------------------- │
│     │  last-updated-time | 2025:04:15 12:30:29                   │
│     │ -------------------+-------------------------------------- │
│     │  response          | Ok                                    │
│     │ -------------------+-------------------------------------- │
│     │  acked             | true                                  │
├─────┼────────────────────────────────────────────────────────────┤
│ abc │  attribute         | value                                 │
│     │ -------------------+-------------------------------------- │
│     │  source            | test.test.test/test/test/test/client  │
│     │ -------------------+-------------------------------------- │
│     │  request-id        | 1744734629495128161                   │
│     │ -------------------+-------------------------------------- │
│     │  version           | 2002                                  │
│     │ -------------------+-------------------------------------- │
│     │  message/key       |                                       │
│     │ -------------------+-------------------------------------- │
│     │  message/value     | {                                     │
│     │                    |   "Get": {                            │
│     │                    |     "key": "count"                    │
│     │                    |   }                                   │
│     │                    | }                                     │
│     │ -------------------+-------------------------------------- │
│     │  created-time      | 2025:04:15 12:30:55                   │
│     │ -------------------+-------------------------------------- │
│     │  last-updated-time | 2025:04:15 12:31:30                   │
│     │ -------------------+-------------------------------------- │
│     │  response          | Ok                                    │
│     │ -------------------+-------------------------------------- │
│     │  acked             | true                                  │
└─────┴────────────────────────────────────────────────────────────┘
┌──────┬───────────────────────────────────────────┬─────────────────────┬─────────────────────┐
│                                        Internal State                                        │
├──────┼───────────────────────────────────────────┼─────────────────────┼─────────────────────┤
│ key  │ table_meta                                │ fvs                 │ backup_fvs          │
├──────┼───────────────────────────────────────────┼─────────────────────┼─────────────────────┤
│ data │  attribute         | value                │  attribute | value  │  attribute | value  │
│      │ -------------------+--------------------- │ -----------+------- │ -----------+------- │
│      │  table             | kv-actor-data        │  count     | 1000   │  count     | 999    │
│      │ -------------------+--------------------- │                     │                     │
│      │  key               | kv-actor-data        │                     │                     │
│      │ -------------------+--------------------- │                     │                     │
│      │  mutated           | false                │                     │                     │
│      │ -------------------+--------------------- │                     │                     │
│      │  last-updated-time | 2025:04:15 12:30:29  │                     │                     │
└──────┴───────────────────────────────────────────┴─────────────────────┴─────────────────────┘
┌─────────────────────────────┬─────────────────────────────────────────────────────────────────────────────┐
│                                        Outgoing Sent Message State                                        │
├─────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ key                         │ details                                                                     │
├─────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ switch1_dpu0                │  attribute         | value                                                  │
│                             │ -------------------+--------------------------------------------            │
│                             │  response_source   | region-a.cluster-a.127.0.0.1-dpu0/hamgrd/0             │
│                             │ -------------------+--------------------------------------------            │
│                             │  id                | 1749223802800098272                                    │
│                             │ -------------------+--------------------------------------------            │
│                             │  version           | 4                                                      │
│                             │ -------------------+--------------------------------------------            │
│                             │  message/key       | switch1_dpu0                                           │
│                             │ -------------------+--------------------------------------------            │
│                             │  message/value     | {                                                      │
│                             │                    |   "field_values": {                                    │
│                             │                    |     "local_addr": "18.0.202.1",                        │
│                             │                    |     "multihop": "true",                                │
│                             │                    |     "multiplier": "3",                                 │
│                             │                    |     "rx_interval": "1000",                             │
│                             │                    |     "shutdown": "false",                               │
│                             │                    |     "tx_interval": "1000",                             │
│                             │                    |     "type": "passive"                                  │
│                             │                    |   },                                                   │
│                             │                    |   "key": "default|default|10.1.0.2",                   │
│                             │                    |   "operation": "Set"                                   │
│                             │                    | }                                                      │
│                             │ -------------------+--------------------------------------------            │
│                             │  created-time      | 2025:06:06 11:30:02                                    │
│                             │ -------------------+--------------------------------------------            │
│                             │  last-updated-time | 2025:06:06 11:30:02                                    │
│                             │ -------------------+--------------------------------------------            │
│                             │  last-sent-time    | 2025:06:06 11:30:02                                    │
│                             │ -------------------+--------------------------------------------            │
│                             │  response          | NoRoute (Route not found)                              │
│                             │ -------------------+--------------------------------------------            │
│                             │  acked             | false                                                  │
├─────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ DPUStateUpdate|switch1_dpu0 │  attribute         | value                                                  │
│                             │ -------------------+------------------------------------------------------- │
│                             │  response_source   | region-a.cluster-a.127.0.0.1-dpu0/hamgrd/0/vdpu/vdpu0  │
│                             │ -------------------+------------------------------------------------------- │
│                             │  id                | 1749223802800098276                                    │
│                             │ -------------------+------------------------------------------------------- │
│                             │  version           | 3                                                      │
│                             │ -------------------+------------------------------------------------------- │
│                             │  message/key       | DPUStateUpdate|switch1_dpu0                            │
│                             │ -------------------+------------------------------------------------------- │
│                             │  message/value     | {                                                      │
│                             │                    |   "dpu_id": 0,                                         │
│                             │                    |   "dpu_name": "switch1_dpu0",                          │
│                             │                    |   "is_managed": true,                                  │
│                             │                    |   "midplane_ipv4": "169.254.0.1",                      │
│                             │                    |   "npu_ipv4": "127.0.0.1",                             │
│                             │                    |   "orchagent_zmq_port": 5555,                          │
│                             │                    |   "pa_ipv4": "18.0.202.1",                             │
│                             │                    |   "remote_dpu": false,                                 │
│                             │                    |   "state": "up",                                       │
│                             │                    |   "swbus_port": 23606,                                 │
│                             │                    |   "up": true,                                          │
│                             │                    |   "vdpu_id": "vdpu0",                                  │
│                             │                    |   "vip_ipv4": "3.2.1.0"                                │
│                             │                    | }                                                      │
│                             │ -------------------+------------------------------------------------------- │
│                             │  created-time      | 2025:06:06 11:30:02                                    │
│                             │ -------------------+------------------------------------------------------- │
│                             │  last-updated-time | 2025:06:06 11:30:02                                    │
│                             │ -------------------+------------------------------------------------------- │
│                             │  last-sent-time    | 2025:06:06 11:30:02                                    │
│                             │ -------------------+------------------------------------------------------- │
│                             │  response          | Ok                                                     │
│                             │ -------------------+------------------------------------------------------- │
│                             │  acked             | true                                                   │
└─────────────────────────────┴─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                Outgoing Queued Message State                                                │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ message                                                                                                                     │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│  attribute            | value                                                                                               │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  actor-message/key    | switch1_dpu0                                                                                        │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  actor-message/value  | {                                                                                                   │
│                       |   "field_values": {                                                                                 │
│                       |     "local_addr": "18.0.202.1",                                                                     │
│                       |     "multihop": "true",                                                                             │
│                       |     "multiplier": "3",                                                                              │
│                       |     "rx_interval": "1000",                                                                          │
│                       |     "shutdown": "false",                                                                            │
│                       |     "tx_interval": "1000",                                                                          │
│                       |     "type": "passive"                                                                               │
│                       |   },                                                                                                │
│                       |   "key": "default|default|127.0.0.1",                                                               │
│                       |   "operation": "Set"                                                                                │
│                       | }                                                                                                   │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  swbus-message/header | {                                                                                                   │
│                       |   "version": 1,                                                                                     │
│                       |   "flag": 0,                                                                                        │
│                       |   "ttl": 64,                                                                                        │
│                       |   "source": "region-a.cluster-a.127.0.0.1-dpu0/hamgrd/0/dpu/switch1_dpu0",                          │
│                       |   "destination": "region-a.cluster-a.127.0.0.1-dpu0/hamgrd/0/swss-common-bridge/BFD_SESSION_TABLE"  │
│                       | }                                                                                                   │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  time-elapsed         | 5                                                                                                   │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│  attribute            | value                                                                                               │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  actor-message/key    | switch1_dpu0                                                                                        │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  actor-message/value  | {                                                                                                   │
│                       |   "field_values": {                                                                                 │
│                       |     "local_addr": "18.0.202.1",                                                                     │
│                       |     "multihop": "true",                                                                             │
│                       |     "multiplier": "3",                                                                              │
│                       |     "rx_interval": "1000",                                                                          │
│                       |     "shutdown": "false",                                                                            │
│                       |     "tx_interval": "1000",                                                                          │
│                       |     "type": "passive"                                                                               │
│                       |   },                                                                                                │
│                       |   "key": "default|default|10.1.0.3",                                                                │
│                       |   "operation": "Set"                                                                                │
│                       | }                                                                                                   │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  swbus-message/header | {                                                                                                   │
│                       |   "version": 1,                                                                                     │
│                       |   "flag": 0,                                                                                        │
│                       |   "ttl": 64,                                                                                        │
│                       |   "source": "region-a.cluster-a.127.0.0.1-dpu0/hamgrd/0/dpu/switch1_dpu0",                          │
│                       |   "destination": "region-a.cluster-a.127.0.0.1-dpu0/hamgrd/0/swss-common-bridge/BFD_SESSION_TABLE"  │
│                       | }                                                                                                   │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  time-elapsed         | 5                                                                                                   │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│  attribute            | value                                                                                               │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  actor-message/key    | switch1_dpu0                                                                                        │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  actor-message/value  | {                                                                                                   │
│                       |   "field_values": {                                                                                 │
│                       |     "local_addr": "18.0.202.1",                                                                     │
│                       |     "multihop": "true",                                                                             │
│                       |     "multiplier": "3",                                                                              │
│                       |     "rx_interval": "1000",                                                                          │
│                       |     "shutdown": "false",                                                                            │
│                       |     "tx_interval": "1000",                                                                          │
│                       |     "type": "passive"                                                                               │
│                       |   },                                                                                                │
│                       |   "key": "default|default|10.1.0.2",                                                                │
│                       |   "operation": "Set"                                                                                │
│                       | }                                                                                                   │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  swbus-message/header | {                                                                                                   │
│                       |   "version": 1,                                                                                     │
│                       |   "flag": 0,                                                                                        │
│                       |   "ttl": 64,                                                                                        │
│                       |   "source": "region-a.cluster-a.127.0.0.1-dpu0/hamgrd/0/dpu/switch1_dpu0",                          │
│                       |   "destination": "region-a.cluster-a.127.0.0.1-dpu0/hamgrd/0/swss-common-bridge/BFD_SESSION_TABLE"  │
│                       | }                                                                                                   │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  time-elapsed         | 5                                                                                                   │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│  attribute            | value                                                                                               │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  actor-message/key    | switch1_dpu0                                                                                        │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  actor-message/value  | {                                                                                                   │
│                       |   "field_values": {                                                                                 │
│                       |     "local_addr": "18.0.202.1",                                                                     │
│                       |     "multihop": "true",                                                                             │
│                       |     "multiplier": "3",                                                                              │
│                       |     "rx_interval": "1000",                                                                          │
│                       |     "shutdown": "false",                                                                            │
│                       |     "tx_interval": "1000",                                                                          │
│                       |     "type": "passive"                                                                               │
│                       |   },                                                                                                │
│                       |   "key": "default|default|10.1.0.2",                                                                │
│                       |   "operation": "Set"                                                                                │
│                       | }                                                                                                   │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  swbus-message/header | {                                                                                                   │
│                       |   "version": 1,                                                                                     │
│                       |   "flag": 0,                                                                                        │
│                       |   "ttl": 64,                                                                                        │
│                       |   "source": "region-a.cluster-a.127.0.0.1-dpu0/hamgrd/0/dpu/switch1_dpu0",                          │
│                       |   "destination": "region-a.cluster-a.127.0.0.1-dpu0/hamgrd/0/swss-common-bridge/BFD_SESSION_TABLE"  │
│                       | }                                                                                                   │
│ ----------------------+---------------------------------------------------------------------------------------------------- │
│  time-elapsed         | 5                                                                                                   │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```
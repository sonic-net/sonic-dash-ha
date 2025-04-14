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

## show route
The command displays route table in the local swbusd
```
Usage: swbus-cli show route

Options:
  -h, --help  Print help
```
Here is an example.
```
sonic-dash-ha$ ./target/debug/swbus-cli -c crates/swbusd/sample/swbusd1.cfg show route
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

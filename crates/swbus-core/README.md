# SONiC - SWitch BUS (SWBus) Core

## Overview

SWBus is a high-performance and scalable message channel for SONiC internal services. It is designed to provide an easy-to-use interface for the intra/inter-switch communication for the internal services.

SWBus is built on top of the [gRPC](https://grpc.io/) framework and uses [Protocol Buffers](https://developers.google.com/protocol-buffers) for message serialization.

## Architecture

On high level, the architecture of the switch bus core is shown as below.

```mermaid
classDiagram
    class ServiceHost

    class SwbusConnStore
    class SwbusConn
    class SwbusConnInfo
    class SwbusConnWorker
    class SwbusConnProxy

    class SwbusMultiplexer
    class SwbusNextHop

    ServiceHost "1" *-- "1" SwbusConnStore
    ServiceHost "1" *-- "1" SwbusMultiplexer

    SwbusConnStore "1" *-- "0..n" SwbusConn
    SwbusConn "1" *-- "1" SwbusConnWorker
    SwbusConn --> SwbusConnInfo

    SwbusMultiplexer "1" *-- "0..n" SwbusNextHop
    SwbusNextHop --> SwbusConnInfo
    SwbusNextHop "1" *-- "1" SwbusConnProxy
    SwbusConnProxy .. SwbusConnWorker : Queue message to worker\nvia mpsc channel
    SwbusConnWorker --> SwbusMultiplexer : Forward message to Mux\nfor message forwarding
```

### Message forwarding flow

```mermaid
sequenceDiagram
```

## Getting Started

To get started, please refer to the [DASH HA (High Availability) README](../README.md).


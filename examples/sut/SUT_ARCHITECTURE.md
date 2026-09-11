# SUT Example Architecture

The SUT (System Under Test) Example is a reference implementation of a stream processing architecture using AWS Lambda and the `aws-lambda-stream-kotlin` library. It demonstrates how to build event-driven systems with centralized event distribution, long-term storage, fault monitoring, and regional health checks.

## System Context

The SUT system interacts with two primary actors:
- **User**: Manages shipments through the Shipment BFF.
- **Canary**: An automated process that triggers regional health checks to ensure the system is operating correctly.

![System Context](./images/SystemContext.svg)

## Containers

The system is composed of several services (containers), each responsible for a specific domain:

- **Event Hub**: The backbone of the system, handling central event distribution and streaming.
- **Control Service**: Manages control-related events and state.
- **Shipment BFF**: A Backend-for-Frontend service for shipment management.
- **Event Lake**: Archives all system events for long-term storage and analysis.
- **Event Fault Monitor**: Monitors event processing faults and sends notifications.
- **Regional Health Check**: A self-contained loop that monitors the health of the system within a region.

![Containers](./images/Containers.svg)

---

## Service Details

### Event Hub
The Event Hub facilitates communication between services using a publish-subscribe model.

- **Event Bus (AWS EventBridge)**: Receives events from various services and routes them.
- **Event Stream (AWS Kinesis)**: Provides a high-volume stream for downstream consumers to process events in order.

![Event Hub Components](./images/EventHubComponents.svg)

### Control Service
The Control Service manages the state of control events.

- **Events Table (DynamoDB)**: Stores the current state.
- **Control Trigger (Lambda)**: Reacts to table changes and publishes events to the central Event Bus.
- **Control Listener (Lambda)**: Consumes events from the Kinesis stream to update the state in the Events Table.

![Control Service Components](./images/ControlServiceComponents.svg)

**Data Flow**:
1. Changes in the `Events Table` trigger the `Control Trigger`.
2. `Control Trigger` publishes events to the `Event Bus`.
3. `Event Bus` forwards events to the `Event Stream`.
4. `Control Listener` consumes events from the `Event Stream` and updates the `Events Table`.

### Shipment BFF
The Shipment BFF provides a REST API for managing shipments.

- **Shipment API (Lambda)**: The entry point for user requests.
- **Shipments Table (DynamoDB)**: Stores shipment data.
- **Shipment Trigger (Lambda)**: Publishes shipment-related events to the Event Bus.
- **Shipment Listener (Lambda)**: Updates shipment data based on events from the Kinesis stream.

![Shipment BFF Components](./images/ShipmentBffComponents.svg)

**Data Flow**:
1. `User` interacts with `Shipment API`.
2. `Shipment API` reads/writes to `Shipments Table`.
3. `Shipments Table` changes trigger `Shipment Trigger`.
4. `Shipment Trigger` publishes events to the `Event Bus`.
5. `Shipment Listener` updates the table based on events received from the `Event Stream`.

### Event Lake
The Event Lake ensures that all events are archived for audit and replay purposes.

- **Event Lake Firehose (Kinesis Firehose)**: Ingests events from EventBridge.
- **Event Lake Bucket (S3)**: Stores the archived events.

![Event Lake Components](./images/EventLakeComponents.svg)

### Event Fault Monitor
The Fault Monitor tracks processing failures across the system.

- **Fault Firehose (Kinesis Firehose)**: Ingests fault events.
- **Fault Transform (Lambda)**: Filters and transforms fault data.
- **Fault Bucket (S3)**: Stores raw fault logs.
- **Fault Topic (SNS)**: Sends alerts for processing faults.

![Event Fault Monitor Components](./images/FaultMonitorComponents.svg)

### Regional Health Check
The Health Check service implements a "tracer loop" to verify the entire regional infrastructure.

- **Health API**: Triggered by the Canary.
- **Health Table**: Tracks the status of health check iterations.
- **Tracer Loop**: The health check flows through DynamoDB, S3, SNS, SQS, and finally back to the Health Table via the local Event Bus and Kinesis stream.

![Regional Health Check Components](./images/HealthCheckComponents.svg)

**Health Check Data Flow**:
1. `Health API` initiates a check in the `Health Table`.
2. `Health DB Trigger` writes an artifact to the `Health Bucket`.
3. `Health Bucket` notifies the `Health Topic` via S3 Event Notifications.
4. `Health Topic` forwards the notification to the `Health Queue`.
5. `Health S3 Trigger` processes the queue message and publishes a health event to the `Health EventBus`.
6. `Health EventBus` forwards to the `Health Kinesis Stream`.
7. `Health Kinesis Trigger` consumes the event and completes the check in the `Health Table`.

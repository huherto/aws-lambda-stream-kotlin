# SUT Example Architecture: Design Motivations

The SUT (Serialized Unit Tracking) Example is a reference implementation of a stream-processing architecture using the `aws-lambda-stream-kotlin` library. It demonstrates a system for tracking shipments or other entities using serial numbers. 

## Architectural Philosophy

The system is built on the principle of **Autonomous Subsystems**. Each subsystem owns its data, its logic, and its entry/exit points. This design addresses several distributed systems challenges:

- **Fault Isolation**: A failure in the Shipment BFF does not impact the Control Service's ability to process background tasks.
- **Independent Scalability**: High-traffic components (like the Shipment API) can be scaled independently of background processors.
- **Decoupled Evolution**: Subsystems communicate via stable event contracts, allowing internal implementations to change without affecting the rest of the system.

---

## Key Design Choices & Motivations

### 1. Event Hub: Hybrid Broker-Stream Model
**Components**: AWS EventBridge (Broker) + Amazon Kinesis (Stream)

*   **Motivation**: 
    - **EventBridge** provides flexible, rule-based routing and filtering. It acts as the central "post office" for the subsystem, allowing any service to subscribe to events without the producer knowing about them.
    - **Kinesis** provides high-throughput, ordered processing. While EventBridge is great for routing, Kinesis ensures that related events (e.g., all updates for a single shipment) are processed in the correct order and allows consumers to "replay" the stream from a specific point in time if needed.

### 2. Transactional Outbox Pattern
**Components**: DynamoDB + Lambda Trigger + EventBridge

*   **Motivation**: In a distributed system, updating a database and publishing an event are two separate operations. If one succeeds and the other fails, the system becomes inconsistent.
*   **Design Choice**: The SUT uses DynamoDB Streams (via the `Trigger` Lambda) to implement the **Transactional Outbox** pattern. Every write to the `Events Table` or `Shipments Table` automatically triggers an event publication. This guarantees **Atomic Consistency**: if the data is saved, the event *will* be published.

### 3. Events Microstore
**Components**: DynamoDB (within Control Service and Shipment BFF)

*   **Motivation**: Services often need to correlate multiple events (e.g., matching a "Shipment Created" event with a "Payment Confirmed" event). Querying a central event store or another service's database for this information creates tight coupling and performance bottlenecks.
*   **Design Choice**: The **Events Microstore** pattern maintains a local, queryable copy of only the events relevant to that service. It uses a specific schema (Partition Key = Entity ID, Sort Key = Event ID/Type) to allow high-performance correlation and evaluation close to the processing logic.

### 4. Event Lake
**Components**: Kinesis Firehose + Amazon S3

*   **Motivation**: Operational databases (DynamoDB) are optimized for current state, not historical analysis. Storing years of events in DynamoDB is expensive and inefficient for analytics.
*   **Design Choice**: The **Event Lake** provides long-term, immutable, and low-cost storage. It decouples the **System of Record** (current state) from the **System of Evidence** (audit trail). It is essential for compliance, business intelligence, and disaster recovery (re-populating a new database from scratch).

### 5. Fault Monitoring & Resubmission
**Components**: Kinesis Firehose + S3 + SNS + `resubmit-events` tool

*   **Motivation**: In stream processing, a single "poison pill" event (malformed data or a bug) can block the entire pipeline. Standard Lambda retries might fail indefinitely, leading to data loss or stuck streams.
*   **Design Choice**: Instead of blocking, the framework captures a **Snapshot of the Unit of Work** (original record + error details) and moves it to a dedicated Fault Bucket. This allows the pipeline to continue while providing developers with the exact context needed to fix the bug and **resubmit** the failed event later.

### 6. Active Health Check (The Tracer Loop)
**Components**: API -> DynamoDB -> S3 -> SNS -> SQS -> EventBridge -> Kinesis

*   **Motivation**: Passive monitoring (CPU, Memory, 5XX errors) only tells you if a service is "up." It doesn't tell you if the complex integration between services is actually *working*.
*   **Design Choice**: The **Tracer Loop** is a synthetic transaction that exercises the entire regional infrastructure. By flowing a test event through multiple AWS services and verifying its arrival at the end, the system proves that IAM roles, connectivity, and configurations are correctly set up across the whole stack.

### 7. Envelope Encryption
**Components**: AWS KMS + `EnvelopeEncryptionMetadata`

*   **Motivation**: Events in the Event Lake or Fault Bucket may contain sensitive information (PII). Relying solely on S3 bucket permissions is often insufficient for strict compliance requirements.
*   **Design Choice**: The architecture supports **Envelope Encryption**. Each event payload is encrypted with a unique data key, which is itself encrypted using a KMS Master Key. This ensures that even if the storage layer is accessed, the data remains protected and can only be decrypted by services with explicit KMS permissions.

---

## System Components & Services

The SUT architecture is composed of several specialized services, each playing a critical role in the end-to-end event-driven workflow.

> **Note**: The following diagrams were generated using the [Structurizr](https://structurizr.com/) model defined in `examples/sut/structurizr/workspace.dsl`.

### 1. High-Level Views

#### System Context
Demonstrates the high-level actors and their interaction with the autonomous system.
![System Context](./images/SystemContext.svg)

#### Container View
Shows the boundaries between subsystems and the central role of the Event Hub.
![Containers](./images/Containers.svg)

### 2. Event Hub
The **Event Hub** acts as the central nervous system for the subsystem, facilitating communication between all other services.

![Event Hub](./images/EventHubComponents.svg)

*   **Event Bus (AWS EventBridge)**: The primary entry point for all events. It uses rule-based routing to deliver events to multiple targets, such as the Event Stream, Event Lake, and Fault Monitor.
*   **Event Stream (Amazon Kinesis)**: A durable, ordered log of all events. It allows downstream services (like the Control Service and Shipment BFF) to process events at their own pace while maintaining strict ordering for related records.

### 3. Control Service
The **Control Service** is responsible for managing business process state and orchestrating complex workflows.

![Control Service](./images/ControlServiceComponents.svg)

*   **Events Table (DynamoDB)**: A local **Events Microstore** that holds a sliding window of events for correlation and evaluation.
*   **Control Listener (Lambda)**: Subscribes to the Event Stream and populates the Events Table with relevant records.
*   **Control Trigger (Lambda)**: Reacts to changes in the Events Table (via DynamoDB Streams) to evaluate business rules and publish higher-order events back to the Event Hub.

### 4. Shipment BFF
The **Shipment BFF** (Backend for Frontend) provides the external interface for shipment management.

![Shipment BFF](./images/ShipmentBffComponents.svg)

*   **Shipment API (Lambda)**: A RESTful interface that allows users to create and track shipments.
*   **Shipments Table (DynamoDB)**: Stores the current state of shipments. It also functions as an **Outbox**, where every write triggers an event publication.
*   **Shipment Trigger (Lambda)**: Monitors the Shipments Table and publishes change events to the Event Hub.
*   **Shipment Listener (Lambda)**: Consumes events from the Event Stream to update the materialized view of shipments in the Shipments Table.

### 5. Event Lake
The **Event Lake** provides a permanent, immutable record of all domain activity.

![Event Lake](./images/EventLakeComponents.svg)

*   **Event Lake Firehose (Kinesis Firehose)**: Buffers and delivers events from the Event Bus to long-term storage.
*   **Event Lake Bucket (S3)**: The physical storage for the event archive, organized by date for efficient querying via tools like Amazon Athena.

### 6. Event Fault Monitor
The **Event Fault Monitor** ensures that no event is lost due to processing failures.

![Fault Monitor](./images/FaultMonitorComponents.svg)

*   **Fault Firehose (Kinesis Firehose)**: Captures technical `fault` events emitted by the framework when a pipeline fails.
*   **Fault Transform (Lambda)**: Optionally enriches or filters fault events before they are stored.
*   **Fault Bucket (S3)**: Stores detailed snapshots of failed Units of Work, including the original record and the error stack trace.
*   **Fault Topic (SNS) & Queue (SQS)**: Provide alerting and temporary buffering for fault notifications.

### 7. Regional Health Check
The **Regional Health Check** implements the **Tracer Loop** to verify that the entire AWS infrastructure is correctly configured and operational.

![Regional Health Check](./images/HealthCheckComponents.svg)

*   **Health API (Lambda)**: Triggered by an external canary to initiate a health check iteration.
*   **Health Table (DynamoDB)**: Tracks the state of the tracer transaction and records latency metrics.
*   **Health DB Trigger (Lambda)**: The first step in the loop; it writes a small file to S3.
*   **Health Bucket (S3)**: Triggers an S3 Event Notification when the tracer file is written.
*   **Health Topic (SNS) & Queue (SQS)**: Transport the notification across the regional boundary to ensure connectivity.
*   **Health S3 Trigger (Lambda)**: Processes the SQS message and publishes a health event to the local Health EventBus.
*   **Health EventBus & Stream**: Local versions of the Event Hub used to test the full event routing path.
*   **Health Kinesis Trigger (Lambda)**: The final step in the loop; it consumes the health event and updates the Health Table to mark the iteration as successful.


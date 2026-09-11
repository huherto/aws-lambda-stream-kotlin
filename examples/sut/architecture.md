# SUT Architecture

> **Note**: This documentation is part of the Structurizr model and reflects the system's design as defined in the C4 diagrams.

## System Overview

The SUT (Serialized Unit Tracking) system implements a robust, event-driven architecture designed for high reliability and observability. It tracks serialized units (such as shipments) through their lifecycle, ensuring that every state change is recorded, archived, and monitored.

### Architecture Diagrams

#### System Context
![System Context](images/SystemContext.svg)

#### Container Diagram
![Containers](images/Containers.svg)

### Service Descriptions

#### Event Hub
**Role**: The central nervous system of the architecture. It combines **AWS EventBridge** for flexible, rule-based event routing and **AWS Kinesis** for high-throughput, ordered event streaming. **Insights & Benefits**: By using both EventBridge and Kinesis, the system gains the best of both worlds: EventBridge simplifies event distribution to multiple consumers, while Kinesis allows consumers to process events in order and at their own pace, providing backpressure handling.
![Event Hub Components](images/EventHubComponents.svg)

#### Control Service
**Role**: Orchestrates the overall system flow by managing global control state. **Insights & Benefits**: It uses a **Transactional Outbox pattern** implemented via DynamoDB Streams. Changes to the control state are captured by the `Control Trigger` and published to the `Event Hub`, ensuring that the system state and the event stream are always in sync.
![Control Service Components](images/ControlServiceComponents.svg)

#### Shipment BFF (Backend for Frontend)
**Role**: Provides a dedicated interface for shipment-related operations, decoupling the frontend from the core domain logic. **Insights & Benefits**: It maintains its own read model in a DynamoDB `Shipments Table`, which is updated asynchronously via the `Shipment Listener`. This allows the API to serve requests with low latency, even if the underlying event processing has some delay.
![Shipment BFF Components](images/ShipmentBffComponents.svg)

#### Event Lake
**Role**: An immutable archive of every event that has ever passed through the system. **Insights & Benefits**: Built with **Amazon Kinesis Data Firehose** and **Amazon S3**, it provides a low-cost, durable storage solution for auditing, compliance, and "time-travel" debugging where past events can be replayed to fix issues or populate new services.
![Event Lake Components](images/EventLakeComponents.svg)

#### Event Fault Monitor
**Role**: A specialized monitoring service that catches and alerts on processing failures. **Insights & Benefits**: It extracts fault information from the event stream and uses an **SNS FIFO Topic** to ensure that notifications are delivered at most once (deduplication), preventing alert fatigue during system-wide outages.
![Fault Monitor Components](images/FaultMonitorComponents.svg)

#### Regional Health Check (Tracer Loop)
**Role**: A proactive monitoring tool that continuously validates the end-to-end health of the regional infrastructure. **Insights & Benefits**: It functions as a **Synthetic Canary** that initiates a "tracer" event. This event must successfully traverse through DynamoDB, S3, SNS, SQS, EventBridge, and Kinesis before returning to the Health Table. If the loop is broken, it signals a failure in one of the underlying AWS services or the system's configuration.
![Health Check Components](images/HealthCheckComponents.svg)

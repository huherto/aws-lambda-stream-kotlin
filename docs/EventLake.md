# Event Lake

## Table of Contents

- [Purpose](#purpose)
- [How it works](#how-it-works)
    - [Architecture](#architecture)
- [Event Lake vs. Fault Monitoring](#event-lake-vs-fault-monitoring)
- [Benefits](#benefits)

## Purpose

An **Event Lake** provides long-term, immutable storage for all events flowing through a subsystem. While an [Events Microstore](EventsMicrostore.md) is optimized for short-term operational needs (correlation, evaluation, recovery), the Event Lake serves as the permanent record of everything that has happened within the system.

The Event Lake is typically used for:

- **Auditing and Compliance**: Maintaining a full history of system state changes for regulatory requirements.
- **Analytics and Business Intelligence**: Providing a data source for large-scale analysis, reporting, and machine learning.
- **Disaster Recovery**: Reconstructing system state by replaying events from a specific point in time.
- **Fault Investigation**: Analyzing historical events to understand the root cause of complex failures.
- **Replay**: Feeding events back into pipelines to fix bugs in processing logic or to populate new services with historical data.

## How it works

The Event Lake is implemented using **Amazon S3** as the storage layer and **Amazon Kinesis Data Firehose** as the delivery mechanism.

### Architecture

1.  **Event Source**: Events are published to the subsystem's **Event Hub** (Amazon EventBridge).
2.  **Filtering**: An EventBridge **Rule** is configured to match events that should be archived. Typically, this includes all domain events but might exclude technical events like `fault` events if they are handled separately.
3.  **Delivery**: The rule targets a **Kinesis Data Firehose Delivery Stream**.
4.  **Buffering and Compression**: Firehose buffers the events based on size (e.g., 50MB) or time (e.g., 60 seconds) and optionally compresses them (e.g., GZIP) to optimize S3 storage.
5.  **Persistence**: Firehose writes the buffered events to an **S3 Bucket**, typically using a date-based prefix structure (e.g., `YYYY/MM/DD/HH/`) for efficient partitioning.

## Event Lake vs. Fault Monitoring

It is important to distinguish the **Event Lake** from the **Fault Monitoring Service**. 

While the Event Lake archives all successful domain events, it typically **excludes** technical `fault` events. Fault events are instead captured and persisted by a dedicated [Fault Monitoring Service](AutonomousServicePatterns.md#fault-monitoring-services).

This separation ensures that:
- The Event Lake remains a clean record of domain activity.
- Fault events, which contain large snapshots of the `UnitOfWork` for debugging and recovery, are stored in a purpose-built bucket for [Fault Re-submission](FaultResubmission.md).
- The `resubmit-events` tool targets the dedicated fault bucket, not the general-purpose Event Lake.

## Benefits

- **Durability**: Leverages S3's high durability.
- **Scalability**: Firehose and S3 automatically scale to handle varying event volumes.
- **Cost-Effective**: S3 offers low-cost storage for large volumes of data, especially when using lifecycle policies to transition older data to cheaper storage classes.
- **Separation of Concerns**: Decouples long-term archiving from operational event processing.

# Framework Features

A comprehensive list of features provided by the AWS Lambda Stream framework for Kotlin.

## Reliability
- **Fault Handling**: Automatic capture and persistence of failed events (faults) to S3 or DynamoDB, including full context for later investigation and recovery.
- **Retries**: Configurable retry strategies (e.g., exponential backoff) for AWS service connectors like DynamoDB, EventBridge, and S3.
- **Claim Check Pattern**: Integrated support for handling large event payloads by storing them in S3 and passing a reference through the stream.
- **Dead-Letter Handling**: Seamless integration with AWS Lambda's failure mechanisms, augmented by custom framework-level fault events.

## Observability
- **EMF Metrics**: Automatic generation of CloudWatch metrics using the Embedded Metric Format (EMF) for pipeline throughput, latency, and utilization.
- **Unit of Work Snapshots**: Captures a full diagnostic snapshot of the processing context (requests, responses, and metadata) during failures.
- **Distributed Tracing Support**: Facilitates correlation across serverless boundaries using standard event tags and correlation IDs.

## Architectural Patterns (Pipelines)
- **CDC Pipeline**: Purpose-built pipeline for processing Change Data Capture (CDC) events from DynamoDB Streams or Kinesis.
- **Correlate Pipeline**: Groups related events over time into a consistent partition within the Events Microstore.
- **Evaluate Pipeline**: Pattern for querying the Microstore to make business decisions based on event history.
- **Materialize Pipeline**: Projects event streams into read models, databases, or external systems.
- **Event Filtering**: DSL for filtering events based on type, source, or custom logic.

## Storage & Persistence
- **Events Microstore**: A lightweight, service-owned DynamoDB table for event correlation, evaluation, and recovery.
- **S3 Claim Check Store**: Standardized implementation for storing and retrieving large event payloads.
- **Flexible Serialization**: Support for pluggable serialization strategies (Jackson, kotlinx.serialization) for both domain events and internal framework DTOs.

## AWS Integration
High-level abstractions (Connectors and Sinks) for interacting with core AWS services:
- **Amazon Kinesis**
- **Amazon DynamoDB** (including Streams and Batch operations)
- **Amazon S3**
- **Amazon EventBridge**
- **Amazon SQS**

## Developer Experience
- **Kotlin-First Design**: Built from the ground up to leverage Kotlin Coroutines and Flow for efficient, non-blocking event processing.
- **Modular Design**: Extensible `UnitOfWork` allows adding custom metadata and service-specific extensions without bloating the core.

## Tooling
- **Replay Tools**: Utilities to replay events from S3 or the Microstore back into processing pipelines.
- **Resubmit Tools**: Utilities to resubmit failed operations (faults) once the root cause is resolved.

# Fault Re-submission

## Purpose

Fault re-submission provides a reliable mechanism for handling non-retryable failures in a stream processing pipeline. When a processing error occurs that cannot be resolved by standard AWS Lambda retries (e.g., business logic errors or malformed data), the framework captures the failure state and allows it to be re-processed at a later time after the underlying issue has been addressed.

## High-Level Overview

1.  **Fault Capture**: The `FaultManager` interceptor wraps pipeline operations.
2.  **State Snapshot**: If a non-retryable exception is caught, the framework creates a `FaultEvent`. This event contains a snapshot of the `UnitOfWork` including the original raw record (e.g., Kinesis record or DynamoDB stream record).
3.  **Fault Persistence**: The `FaultEvent` is published to the configured event sink (typically an EventBridge event bus). From there, it is usually routed to S3 for long-term storage and auditing.
4.  **Resubmission**: An external tool (`resubmit-events`) scans the stored fault events in S3, extracts the original payloads, and re-invokes the target Lambda function with a synthetic batch of these records.

## Data Flow

```text
Pipeline Processing
      |
      v
Error Caught by FaultManager
      |
      v
FaultEvent Created (with UOW Snapshot)
      |
      v
EventBridge / SQS / S3
      |
      v
Resubmit Tool (cli)
      |
      v
Lambda Invocation (Synthetic Batch)
```

## Fault Event Structure

A `FaultEvent` includes critical metadata and the original processing context:

- `tags`: Includes `functionname` and `pipeline` ID.
- `err`: A snapshot of the exception (name, message, and optional stack trace).
- `uow`: A `UnitOfWorkSnapshot` containing the original `record` payload.

Example snippet of a serialized `FaultEvent`:

```json
{
  "type": "fault",
  "tags": {
    "functionname": "my-service-processor",
    "pipeline": "order-processing"
  },
  "err": {
    "name": "ValidationException",
    "message": "Invalid order status"
  },
  "uow": {
    "record": {
      "kind": "kinesis",
      "payload": {
        "kinesis": {
          "data": "...",
          "partitionKey": "123"
        }
      }
    }
  }
}
```

## The Resubmit Tool

The `resubmit-events` tool is a CLI utility used to replay failed records. It automatically identifies the target Lambda function for each event using the `functionname` tag within the `FaultEvent`.

### Configuration

The tool can be configured via environment variables or a `.faultsrc` (or `.faultsrc.json`) file in the project root.

| Variable | Description | Default |
|----------|-------------|---------|
| `BUCKET_NAME` | S3 bucket where fault events are stored. | - |
| `AWS_REGION` | AWS region of the S3 bucket and Lambda. | - |
| `PREFIX` | S3 prefix to scan for fault events. | `YYYY/MM/DD/` |
| `FUNCTION_NAME` | The name of the Lambda function to invoke. | - |
| `DRY_RUN` | If `true`, the tool logs intentions without invoking Lambda. | `false` |

### Execution

To run the resubmit tool:

```bash
# Using environment variables
export BUCKET_NAME=my-faults-bucket
export FUNCTION_NAME=my-service-processor
./gradlew :tools:resubmit-events:run
```

## Operational Guidelines

- **Fix First**: Always identify and resolve the root cause of the fault before resubmitting.
- **Dry Run**: Use `DRY_RUN=true` to verify the number of events that will be resubmitted.
- **Rate Limiting**: The tool includes built-in rate limiting to avoid overwhelming the target Lambda or downstream dependencies.
- **Idempotency**: Ensure your Lambda processing logic is idempotent, as resubmitted events may have been partially processed before the original failure.

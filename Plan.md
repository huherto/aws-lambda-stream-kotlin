
# Serialization Strategy and Fault Snapshot Plan

## Goal

Introduce a serialization architecture that allows the framework user to choose the serialization framework, while also making fault events and resubmission data reliable, durable, and easy to verify.

Supported goals:

- Allow selectable serialization implementations:
  - Jackson
  - kotlinx.serialization
  - Moshi
  - future custom serializers
- Avoid serializing live runtime objects directly.
- Replace direct serialization of complex `UnitOfWork` graphs with stable snapshot DTOs.
- Make `FaultEvent` safely serializable.
- Preserve Kinesis and DynamoDB trigger records in a replayable format.
- Make resubmission tools consume the new snapshot shape.
- No backward compatibility is required.

---

## Design Principles

1. **Do not serialize runtime objects directly**

   `UnitOfWork` may contain AWS SDK requests/responses, Lambda records, exceptions, pipelines, batches, S3 responses, and other hard-to-serialize objects.

   Instead, convert runtime objects into stable DTO snapshots before serialization.

2. **Separate domain event serialization from framework diagnostic serialization**

   Domain events use `EventCodec`.

   Framework diagnostics and fault events use snapshot DTOs plus a configurable `SerializationStrategy`.

3. **Persist replayable records, not pretty records**

   Kinesis and DynamoDB records must be stored in a shape that can be sent back to a Lambda as:

   ```json
   {
     "Records": []
   }
   ```

4. **Prefer stable JSON contracts**

   Fault events persisted to S3/EventBridge should use predictable field names and simple DTOs.

5. **Serializer choice should be a framework configuration concern**

   Core framework logic should not be hard-wired to Jackson, kotlinx.serialization, or Moshi.

---

## Target Architecture
```
text
Runtime UnitOfWork
        |
        v
UnitOfWorkSnapshotter
        |
        v
UnitOfWorkSnapshot
        |
        v
FaultEvent
        |
        v
SerializationStrategy
        |
        v
JSON
```
For normal events:
```
text
Event
  |
  v
EventCodec
  |
  v
JSON
```
For replay:
```
text
Stored FaultEvent JSON
        |
        v
uow.record.payload or uow.batch[*].record.payload
        |
        v
{ "Records": [...] }
        |
        v
Lambda invoke payload
```
---

## New Core Concepts

### SerializationStrategy

A framework-level serializer abstraction.
```
kotlin
interface SerializationStrategy {
    fun serialize(value: Any?): String

    fun <T : Any> deserialize(
        payload: String,
        targetType: Class<T>,
    ): T
}
```
Initial implementations:

- `JacksonSerializationStrategy`
- `KotlinxSerializationStrategy`
- `MoshiSerializationStrategy`, optional/later

---

### EventCodec

Keep `EventCodec` as the domain-event boundary.

It should remain responsible for:

- decoding incoming event payloads into framework `Event` objects
- encoding domain events for publishing

Add reusable implementations:

- `JacksonEventCodec<T : Event>`
- `KotlinxEventCodec<T : Event>`
- optional `MoshiEventCodec<T : Event>`

---

### UnitOfWorkSnapshot

A serializable DTO that represents only durable, useful, safe data from a runtime `UnitOfWork`.

Proposed model:
```
kotlin
data class UnitOfWorkSnapshot(
    val pipeline: PipelineSnapshot? = null,
    val record: ReplayRecordSnapshot? = null,
    val event: EventSummarySnapshot? = null,
    val key: String? = null,
    val sequenceNumber: String? = null,
    val shardId: String? = null,
    val timestamp: String? = null,
    val meta: Map<String, String?>? = null,
    val triggers: List<EventSummarySnapshot>? = null,
    val correlated: List<EventSummarySnapshot>? = null,
    val batch: List<UnitOfWorkSnapshot>? = null,
    val aws: List<AwsOperationSnapshot>? = null,
    val s3: S3Snapshot? = null,
)
```
---

### ReplayRecordSnapshot

Represents a replayable trigger record.
```
kotlin
data class ReplayRecordSnapshot(
    val kind: String,
    val payload: Any,
    val diagnostic: Any? = null,
)
```
Expected `kind` values:

- `kinesis`
- `dynamodb`
- `sqs`
- `eventbridge`
- `unknown`

The `payload` should be the object/JSON that can be placed directly inside:
```
json
{
  "Records": [
    {}
  ]
}
```
---

### RecordSnapshotter

Converts runtime Lambda records into replayable snapshots.
```
kotlin
interface RecordSnapshotter {
    fun supports(record: Any): Boolean

    fun snapshot(record: Any): ReplayRecordSnapshot
}
```
Initial implementations:

- `KinesisRecordSnapshotter`
- `DynamoDbRecordSnapshotter`

Later:

- `SqsRecordSnapshotter`
- `EventBridgeRecordSnapshotter`
- custom user-provided snapshotters

---

### UnitOfWorkSnapshotter

Converts a runtime `UnitOfWork` into a stable `UnitOfWorkSnapshot`.
```
kotlin
interface UnitOfWorkSnapshotter {
    fun snapshot(uow: UnitOfWork): UnitOfWorkSnapshot
}
```
Default implementation:
```
kotlin
class DefaultUnitOfWorkSnapshotter(
    private val recordSnapshotters: List<RecordSnapshotter>,
)
```
---

### FaultEvent

`FaultEvent` should no longer persist the raw runtime `UnitOfWork` or `FaultException`.

Target structure:
```
kotlin
class FaultEvent : BaseEvent() {
    var err: Error? = null
    var uow: UnitOfWorkSnapshot? = null

    @Transient
    var runtimeUow: UnitOfWork? = null

    @Transient
    var faultException: FaultException? = null

    override fun eventType(): String = FAULT_EVENT_TYPE
}
```
Since backward compatibility is not required, the existing raw `uow: UnitOfWork?` field can be replaced by `uow: UnitOfWorkSnapshot?`.

---

## Target Fault Event JSON Shape

Example for a Kinesis-originated failure:
```
json
{
  "id": "fault-123",
  "timestamp": 123456789,
  "partitionKey": "pk-1",
  "tags": {
    "functionname": "target-lambda",
    "pipeline": "target-pipeline"
  },
  "err": {
    "name": "IllegalStateException",
    "message": "Something failed"
  },
  "uow": {
    "pipeline": {
      "id": "target-pipeline"
    },
    "key": "pk-1",
    "sequenceNumber": "123456",
    "shardId": "shardId-000000000000",
    "record": {
      "kind": "kinesis",
      "payload": {
        "eventID": "shardId-000000000000:1",
        "eventName": "aws:kinesis:record",
        "eventSource": "aws:kinesis",
        "awsRegion": "us-east-1",
        "kinesis": {
          "partitionKey": "pk-1",
          "sequenceNumber": "123456",
          "data": "base64-payload"
        }
      }
    },
    "event": {
      "id": "original-event-id",
      "type": "shipment-created",
      "partitionKey": "pk-1"
    }
  }
}
```
Example for DynamoDB:
```
json
{
  "id": "fault-456",
  "timestamp": 123456789,
  "tags": {
    "functionname": "target-lambda",
    "pipeline": "target-pipeline"
  },
  "err": {
    "name": "IllegalStateException",
    "message": "Something failed"
  },
  "uow": {
    "record": {
      "kind": "dynamodb",
      "payload": {
        "eventID": "1",
        "eventName": "INSERT",
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodb",
        "awsRegion": "us-east-1",
        "dynamodb": {
          "keys": {
            "id": {
              "S": "abc"
            }
          },
          "newImage": {
            "id": {
              "S": "abc"
            }
          },
          "sequenceNumber": "123",
          "sizeBytes": 42,
          "streamViewType": "NEW_AND_OLD_IMAGES"
        }
      }
    }
  }
}
```
---

# Implementation Steps

## Step 1: Add Serialization Strategy Abstraction

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/SerializationStrategy.kt
```
Add:
```
kotlin
interface SerializationStrategy {
    fun serialize(value: Any?): String

    fun <T : Any> deserialize(
        payload: String,
        targetType: Class<T>,
    ): T
}
```
Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/JacksonSerializationStrategy.kt
```
Add Jackson implementation.

### Verification

Add unit tests:
```
text
core/src/test/kotlin/io/github/huherto/awsLambdaStream/serialization/JacksonSerializationStrategyTest.kt
```
Verify:

- serializes simple data class
- deserializes simple data class
- omits nulls if that remains desired
- does not fail on empty beans if that remains desired

### Done When

- Tests pass.
- No framework code depends on a hard-coded global mapper for new functionality.

---

## Step 2: Add Event Codec Implementations

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/JacksonEventCodec.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/KotlinxEventCodec.kt
```
`JacksonEventCodec` should accept:
```
kotlin
class JacksonEventCodec<T : Event>(
    private val eventClass: Class<T>,
    private val serialization: SerializationStrategy,
) : EventCodec
```
`KotlinxEventCodec` should accept an explicit serializer:
```
kotlin
class KotlinxEventCodec<T : Event>(
    private val serializer: KSerializer<T>,
    private val json: Json,
) : EventCodec
```
### Verification

Add tests for:

- Jackson event encode/decode
- kotlinx event encode/decode
- custom serialization configuration

### Done When

- Domain event serialization works through codec implementations.
- Existing custom event codecs can be replaced or simplified.

---

## Step 3: Define Snapshot DTOs

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/FaultSnapshots.kt
```
Add DTOs:
```
kotlin
data class ErrorSnapshot(...)
data class UnitOfWorkSnapshot(...)
data class ReplayRecordSnapshot(...)
data class PipelineSnapshot(...)
data class EventSummarySnapshot(...)
data class AwsOperationSnapshot(...)
data class S3Snapshot(...)
```
Recommended fields:
```
kotlin
data class ErrorSnapshot(
    val name: String? = null,
    val message: String? = null,
    val stackTrace: List<String>? = null,
)
```

```
kotlin
data class PipelineSnapshot(
    val id: String? = null,
)
```

```
kotlin
data class EventSummarySnapshot(
    val id: String? = null,
    val type: String? = null,
    val timestamp: Long? = null,
    val partitionKey: String? = null,
    val tags: Map<String, String>? = null,
)
```
### Verification

Add tests that serialize the snapshot DTOs with Jackson.

Verify the generated JSON is stable and clean.

### Done When

- Snapshot DTOs serialize without custom runtime objects.
- Snapshot JSON does not contain SDK internals, exception internals, or pipeline object internals.

---

## Step 4: Add Record Snapshotters

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/RecordSnapshotter.kt
```
Add:
```
kotlin
interface RecordSnapshotter {
    fun supports(record: Any): Boolean

    fun snapshot(record: Any): ReplayRecordSnapshot
}
```
Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/KinesisRecordSnapshotter.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/DynamoDbRecordSnapshotter.kt
```
### Kinesis requirements

The Kinesis snapshot must preserve enough fields to recreate:
```
json
{
  "Records": [
    {
      "eventID": "...",
      "eventName": "...",
      "eventSource": "...",
      "eventSourceARN": "...",
      "awsRegion": "...",
      "kinesis": {
        "partitionKey": "...",
        "sequenceNumber": "...",
        "data": "..."
      }
    }
  ]
}
```
### DynamoDB requirements

The DynamoDB snapshot must preserve Lambda DynamoDB stream JSON shape, especially AttributeValues:
```
json
{
  "S": "value"
}
```

```
json
{
  "N": "123"
}
```

```
json
{
  "BOOL": true
}
```

```
json
{
  "M": {}
}
```

```
json
{
  "L": []
}
```
Do not flatten DynamoDB AttributeValues in replay payloads.

### Verification

Add tests:
```
text
core/src/test/kotlin/io/github/huherto/awsLambdaStream/faults/KinesisRecordSnapshotterTest.kt
core/src/test/kotlin/io/github/huherto/awsLambdaStream/faults/DynamoDbRecordSnapshotterTest.kt
```
Verify:

- `kind` is correct.
- `payload` can be wrapped in `{ "Records": [payload] }`.
- Kinesis data remains base64-compatible.
- DynamoDB AttributeValue shape is preserved.

### Done When

- Kinesis and DynamoDB records snapshot into replayable payloads.
- Tests verify the exact JSON shape needed for Lambda reinvocation.

---

## Step 5: Add UnitOfWorkSnapshotter

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/UnitOfWorkSnapshotter.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/DefaultUnitOfWorkSnapshotter.kt
```
The default snapshotter should include:

- pipeline summary
- replay record snapshot
- event summary
- key
- sequence number
- shard id
- timestamp
- meta
- triggers summary
- correlated summary
- recursively snapshotted batch
- S3 summaries where useful

It should exclude:

- full AWS SDK request/response objects
- raw exceptions
- full runtime pipeline objects
- response streams
- binary data unless base64 encoded
- credentials or sensitive metadata

### Verification

Add tests:
```
text
core/src/test/kotlin/io/github/huherto/awsLambdaStream/faults/DefaultUnitOfWorkSnapshotterTest.kt
```
Verify:

- simple `UnitOfWork` snapshots correctly
- Kinesis record snapshots correctly
- DynamoDB record snapshots correctly
- batch snapshots recursively
- unknown record type produces `kind = "unknown"` or a safe diagnostic string
- snapshot serialization does not throw

### Done When

- Runtime `UnitOfWork` can be converted to safe snapshot JSON.
- No direct serialization of raw `UnitOfWork` is needed for fault events.

---

## Step 6: Change FaultEvent to Store Snapshot

### Changes

Update `FaultEvent` so persisted fields are:
```
kotlin
var err: ErrorSnapshot? = null
var uow: UnitOfWorkSnapshot? = null
```
Runtime-only fields should be transient:
```
kotlin
@Transient
var runtimeUow: UnitOfWork? = null

@Transient
var faultException: FaultException? = null
```
If keeping nested `FaultEvent.Error`, either:

- replace it with `ErrorSnapshot`, or
- keep the existing class but ensure it is plain and serializable.

Recommended: use `ErrorSnapshot`.

### Verification

Update tests that inspect fault events.

Verify:

- `fault.uow` is a `UnitOfWorkSnapshot`
- `fault.runtimeUow` can still be inspected in memory if needed
- serialized fault event does not include `runtimeUow`
- serialized fault event does not include `faultException`

### Done When

- Fault events persist only snapshot data.
- No serialized fault JSON contains raw `UnitOfWork`, raw exceptions, or SDK internals.

---

## Step 7: Add Fault Event Factory or Builder

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/FaultEventFactory.kt
```
Responsibilities:

- build `FaultEvent`
- extract `ErrorSnapshot`
- create `UnitOfWorkSnapshot`
- copy useful tags
- set runtime-only fields for in-memory inspection, if desired

Example API:
```
kotlin
class FaultEventFactory(
    private val unitOfWorkSnapshotter: UnitOfWorkSnapshotter,
) {
    fun createFaultEvent(
        uow: UnitOfWork?,
        error: Throwable,
    ): FaultEvent
}
```
### Verification

Add tests:
```
text
core/src/test/kotlin/io/github/huherto/awsLambdaStream/faults/FaultEventFactoryTest.kt
```
Verify:

- fault ID is assigned if that is factory responsibility
- error name and message are captured
- stack trace policy works
- runtime `UnitOfWork` is not persisted
- snapshot is attached

### Done When

- Fault creation is centralized.
- Fault serialization behavior is consistent.

---

## Step 8: Update Fault Manager to Use Factory

### Changes

Update fault handling code to use `FaultEventFactory`.

The flow should become:
```
text
Throwable + UnitOfWork
        |
        v
FaultEventFactory
        |
        v
FaultEvent with UnitOfWorkSnapshot
        |
        v
EventPublisher
```
### Verification

Update existing fault manager tests.

Verify:

- faults are still collected
- faults are still flushed
- published fault events contain snapshots
- Kinesis-originated failures include replayable records
- DynamoDB-originated failures include replayable records

### Done When

- Fault manager no longer directly creates raw fault events with raw `UnitOfWork`.
- Fault events are safe to serialize through the selected strategy.

---

## Step 9: Update Event Publishing Serialization

### Changes

Ensure event publishing uses:
```
kotlin
EventCodec
```
or:
```
kotlin
SerializationStrategy
```
consistently.

Recommended distinction:

- use `EventCodec` for domain event payloads
- use `SerializationStrategy` for framework-created DTOs and fault snapshots

If `FaultEvent` is still an `Event`, its `encoded()` implementation should use the configured serialization strategy or the configured event codec.

### Verification

Add or update tests for:

- publishing normal domain event
- publishing fault event
- serialized EventBridge detail contains snapshot shape

### Done When

- Event publishing does not depend on a hard-coded global Jackson mapper for new behavior.
- Fault events serialize through the same configured strategy.

---

## Step 10: Update ResubmitFaults to Use New Snapshot Shape

### Changes

Update resubmission logic to extract records from:
```
json
detail.uow.record.payload
```
or:
```
json
detail.uow.batch[*].record.payload
```
Instead of directly using:
```
json
detail.uow.record
```
Target behavior:
```
kotlin
val records = if (batch != null) {
    batch.mapNotNull { item ->
        item.jsonObject["record"]
            ?.jsonObject
            ?.get("payload")
    }
} else {
    listOfNotNull(
        eventUow["record"]
            ?.jsonObject
            ?.get("payload")
    )
}
```
Also use `record.kind` for metrics, validation, or future filtering.

### Verification

Update resubmit tests.

Verify:

- single Kinesis fault record resubmits
- batched Kinesis fault records resubmit
- single DynamoDB fault record resubmits
- batched DynamoDB fault records resubmit
- generated Lambda payload is exactly:
```
json
{
  "Records": []
}
```
with original replay payloads inside.

### Done When

- Resubmit tool only uses snapshot payloads.
- Resubmit tool does not rely on raw `UnitOfWork` JSON.

---

## Step 11: Update Integration Tests

### Changes

Update integration tests that assert fault event shape.

Expected assertions:

- stored EventBridge event has `detail.type == "fault"`
- `detail.uow` exists
- `detail.uow.record.kind` exists
- `detail.uow.record.payload` exists
- or `detail.uow.batch[0].record.payload` exists
- `detail.uow` does not contain SDK request/response internals
- `detail.faultException` does not exist
- `detail.runtimeUow` does not exist

### Verification

Run integration tests for:

- Kinesis poison event
- DynamoDB trigger failure
- fault monitor flow
- resubmit dry run

### Done When

- Stored fault events are proven resubmittable.
- Integration tests verify the new durable JSON contract.

---

## Step 12: Add Optional Moshi Support

### Changes

Only after the core strategy is stable, add Moshi support.

Options:

1. Add to core dependency list.
2. Preferably create a separate module later:
```
text
serialization-moshi
```
Add:
```
kotlin
class MoshiSerializationStrategy : SerializationStrategy
```
Optional:
```
kotlin
class MoshiEventCodec<T : Event> : EventCodec
```
### Verification

Add tests:

- simple DTO serialization
- event serialization
- fault snapshot serialization

### Done When

- Moshi works without changing fault snapshot or event publishing architecture.

---

## Step 13: Clean Up Old JSON Utilities

### Changes

Review existing JSON helper functions.

Decide which remain:

- general-purpose utility functions
- logging utilities
- compatibility helpers

Move hard-coded Jackson helpers behind `JacksonSerializationStrategy`.

Avoid using global `asJson()` for new framework serialization paths.

### Verification

Search for usages of:
```
kotlin
asJson()
```
Classify each usage:

- keep for debugging
- replace with `SerializationStrategy`
- replace with `EventCodec`
- remove

### Done When

- Core publishing and fault persistence no longer depend on `asJson()`.
- `asJson()` is either removed or kept only as a debug convenience.

---

# Verification Checklist

## Unit Tests

Run:
```
bash
./gradlew :core:test
```
Required passing areas:

- serialization strategy tests
- event codec tests
- snapshot DTO tests
- Kinesis record snapshot tests
- DynamoDB record snapshot tests
- UnitOfWork snapshot tests
- FaultEvent factory tests
- ResubmitFaults tests

---

## Integration Tests

Run the existing integration test workflow.

Verify:

- normal event flow still works
- Kinesis failure creates resubmittable fault event
- DynamoDB failure creates resubmittable fault event
- fault monitor stores event in S3
- resubmit dry run creates Lambda invoke payloads
- no serialized fault event contains raw exception internals
- no serialized fault event contains raw AWS SDK response internals

---

## Manual JSON Inspection

Inspect a stored fault event from S3/EventBridge.

Verify top-level shape:
```
json
{
  "detail": {
    "type": "fault",
    "err": {},
    "uow": {}
  }
}
```
Verify replay record shape:
```
json
{
  "record": {
    "kind": "kinesis",
    "payload": {}
  }
}
```
or:
```
json
{
  "record": {
    "kind": "dynamodb",
    "payload": {}
  }
}
```
Verify forbidden fields are absent:
```
text
faultException
runtimeUow
batchGetRequest
batchGetResponse
putResponse
queryResponse
scanRequest
updateResponse
getResponse
putResponse
listResponse
```
---

# Suggested Order of Work

1. Add `SerializationStrategy`.
2. Add Jackson strategy and tests.
3. Add snapshot DTOs.
4. Add Kinesis record snapshotter.
5. Add DynamoDB record snapshotter.
6. Add `UnitOfWorkSnapshotter`.
7. Change `FaultEvent` to use `UnitOfWorkSnapshot`.
8. Add `FaultEventFactory`.
9. Update fault manager.
10. Update resubmit tool.
11. Update integration tests.
12. Add kotlinx/Jackson event codec implementations.
13. Add optional Moshi support.
14. Clean up old JSON helpers.

---

# Risks and Mitigations

## Risk: Kinesis/DynamoDB record payloads are not exactly replayable

Mitigation:

- Add tests that wrap snapshot payloads in `{ "Records": [...] }`.
- Deserialize them back into AWS Lambda event classes where practical.
- Use integration dry-run Lambda invoke tests.

---

## Risk: Snapshot DTO becomes too large

Mitigation:

- Store only replay fields and useful diagnostics.
- Summarize SDK requests/responses.
- Add size tests or warnings for large records.

---

## Risk: Sensitive data leaks into fault events

Mitigation:

- Avoid serializing full SDK request/response objects.
- Add a redaction hook later:
```
kotlin
interface SnapshotRedactor {
    fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot
}
```
---

## Risk: Serializer behavior differs across Jackson/kotlinx/Moshi

Mitigation:

- Snapshot DTOs should use simple data classes.
- Avoid `Any` where possible.
- For replay payloads, consider using a framework-neutral JSON representation later if needed.

Possible future improvement:
```
kotlin
data class ReplayRecordSnapshot(
    val kind: String,
    val payload: JsonObject,
    val diagnostic: JsonObject? = null,
)
```
---

# Definition of Done

This change is complete when:

- Framework users can select a serialization strategy.
- Domain events can be encoded/decoded through configurable `EventCodec`s.
- Fault events no longer serialize raw `UnitOfWork`.
- Fault events contain stable `UnitOfWorkSnapshot` data.
- Kinesis records are stored in replayable form.
- DynamoDB records are stored in replayable form.
- Resubmit tooling reads `uow.record.payload` or `uow.batch[*].record.payload`.
- Existing unit and integration tests pass after being updated to the new shape.
- Stored fault event JSON is safe, stable, readable, and resubmittable.



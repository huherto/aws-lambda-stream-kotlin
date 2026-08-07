```markdown
# Serialization Strategy and Fault Snapshot Plan

## Goal

Introduce a serialization architecture that allows the framework user to choose the serialization framework, while also making fault events and resubmission data reliable, durable, and easy to verify.

Supported goals:

- Allow selectable serialization implementations:
  - Jackson
  - kotlinx.serialization
  - Moshi
  - future custom serializers
- Allow serializer auto-detection when selection is unambiguous.
- Provide explicit serializer configuration when auto-detection is ambiguous.
- Avoid serializing live runtime objects directly.
- Replace direct serialization of complex `UnitOfWork` graphs with stable snapshot DTOs.
- Make `FaultEvent` safely serializable as a framework diagnostic DTO.
- Preserve Kinesis and DynamoDB trigger records in a replayable format.
- Use dedicated Kinesis and DynamoDB replay DTOs to build replay payloads.
- Make resubmission tools consume the new snapshot shape.
- Deprecate `Event.encoded()` and move serialization to `EventCodec` or `SerializationStrategy`.
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

4. **Use typed replay DTOs before writing replay JSON**

   Kinesis and DynamoDB replay payloads should be constructed from dedicated DTOs.

   This gives type-safe snapshotting logic while keeping the persisted replay contract stable.

5. **Prefer stable JSON contracts**

   Fault events persisted to S3/EventBridge should use predictable field names and simple DTOs.

6. **Serializer choice should be a framework configuration concern**

   Core framework logic should not be hard-wired to Jackson, kotlinx.serialization, or Moshi.

7. **Avoid persisted `Any` fields**

   Persisted snapshot DTOs should avoid arbitrary runtime `Any` fields because different serializers handle them differently.

8. **Replay payload is the replay contract**

   `record.payload` should contain the Lambda record JSON that can be inserted directly into:

   ```json
   {
     "Records": []
   }
   ```

   Any pretty, summarized, normalized, or redacted view should go into diagnostics instead.

9. **Redaction must be available before persistence**

   Fault events are durable and may contain sensitive business payloads. A redaction hook should exist from the first implementation.

---

## Target Architecture

For fault events:
```
text
Runtime UnitOfWork
        |
        v
RecordSnapshotter
        |
        v
Dedicated replay DTO
        |
        v
JsonObject replay payload
        |
        v
ReplayRecordSnapshot
        |
        v
UnitOfWorkSnapshotter
        |
        v
UnitOfWorkSnapshot
        |
        v
FaultEvent diagnostic DTO
        |
        v
SerializationStrategy
        |
        v
JSON
```
For normal domain events:
```
text
Domain Event
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
detail.uow.record.payload
or detail.uow.batch[*].record.payload
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

A framework-level serializer abstraction used for framework-owned DTOs, including fault events and snapshots.
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

Serializer selection should be configurable and may be auto-detected from the runtime classpath.

Selection precedence:

1. Explicit framework configuration
2. Environment/config property
3. Classpath auto-detection
4. Clear failure if no strategy can be selected

Recommended config model:
```
kotlin
enum class SerializationStrategyKind {
    AUTO,
    JACKSON,
    KOTLINX,
    MOSHI,
}
```

```
kotlin
data class SerializationConfig(
    val strategy: SerializationStrategyKind = SerializationStrategyKind.AUTO,
)
```
Recommended environment/config property names:
```
text
AWS_LAMBDA_STREAM_SERIALIZATION=jackson
AWS_LAMBDA_STREAM_SERIALIZATION=kotlinx
AWS_LAMBDA_STREAM_SERIALIZATION=moshi
AWS_LAMBDA_STREAM_SERIALIZATION=auto
```
or:
```
text
SERIALIZATION_STRATEGY=jackson
```
`AUTO` behavior:

- If exactly one supported serializer is available, use it.
- If none are available, fail with a clear error.
- If multiple are available, fail with a clear error and require explicit configuration.

Rationale:

A project may have multiple JSON libraries on the classpath for unrelated reasons. Failing on ambiguous auto-detection is safer than silently choosing one.

---

### EventCodec

`EventCodec` is the domain-event serialization boundary.

It is responsible for:

- decoding incoming event payloads into framework `Event` objects
- encoding domain events for publishing

Framework diagnostic objects, including `FaultEvent`, should not be serialized through `EventCodec`. They should be serialized through `SerializationStrategy`.

Add reusable implementations:

- `JacksonEventCodec<T : Event>`
- `KotlinxEventCodec<T : Event>`
- optional `MoshiEventCodec<T : Event>`

`Event.encoded()` should be deprecated and replaced in internal framework code by `EventCodec` or `SerializationStrategy`.

Recommended deprecation:
```
kotlin
@Deprecated(
    message = "Use EventCodec or the configured framework publisher instead.",
)
fun encoded(): String
```
Recommended distinction:
```
text
Domain Event
    |
    v
EventCodec
    |
    v
Published event JSON
```

```
text
Framework diagnostic DTO, such as FaultEvent
    |
    v
SerializationStrategy
    |
    v
Published diagnostic JSON
```
---

### Dedicated Replay DTOs

Replay payloads should be built from dedicated DTOs, not arbitrary `Any` values.

This gives:

- type-safe snapshotting logic
- stable Lambda replay shape
- serializer-independent persisted JSON
- explicit support for DynamoDB AttributeValue shape

#### KinesisReplayRecord
```
kotlin
@Serializable
data class KinesisReplayRecord(
    val eventID: String? = null,
    val eventName: String? = null,
    val eventSource: String? = "aws:kinesis",
    val eventSourceARN: String? = null,
    val awsRegion: String? = null,
    val kinesis: KinesisReplayData,
)
```

```
kotlin
@Serializable
data class KinesisReplayData(
    val partitionKey: String? = null,
    val sequenceNumber: String? = null,
    val data: String? = null,
    val approximateArrivalTimestamp: Double? = null,
    val kinesisSchemaVersion: String? = null,
)
```
`kinesis.data` must remain a base64 string compatible with the Lambda Kinesis event shape.

#### DynamoDbReplayRecord
```
kotlin
@Serializable
data class DynamoDbReplayRecord(
    val eventID: String? = null,
    val eventName: String? = null,
    val eventVersion: String? = null,
    val eventSource: String? = "aws:dynamodb",
    val eventSourceARN: String? = null,
    val awsRegion: String? = null,
    val dynamodb: DynamoDbStreamReplayData,
)
```

```
kotlin
@Serializable
data class DynamoDbStreamReplayData(
    val approximateCreationDateTime: Double? = null,
    val keys: Map<String, DynamoDbAttributeValueSnapshot>? = null,
    val newImage: Map<String, DynamoDbAttributeValueSnapshot>? = null,
    val oldImage: Map<String, DynamoDbAttributeValueSnapshot>? = null,
    val sequenceNumber: String? = null,
    val sizeBytes: Long? = null,
    val streamViewType: String? = null,
)
```
#### DynamoDbAttributeValueSnapshot

DynamoDB AttributeValues must preserve Lambda stream JSON shape.
```
kotlin
@Serializable
data class DynamoDbAttributeValueSnapshot(
    val S: String? = null,
    val N: String? = null,
    val B: String? = null,
    val BOOL: Boolean? = null,
    val NULL: Boolean? = null,
    val M: Map<String, DynamoDbAttributeValueSnapshot>? = null,
    val L: List<DynamoDbAttributeValueSnapshot>? = null,
    val SS: List<String>? = null,
    val NS: List<String>? = null,
    val BS: List<String>? = null,
)
```
Examples:
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
  "M": {
    "nested": {
      "BOOL": true
    }
  }
}
```
DynamoDB AttributeValues in replay payloads must not be flattened.

---

### ReplayRecordSnapshot

Represents a replayable trigger record.
```
kotlin
data class ReplayRecordSnapshot(
    val kind: String,
    val payload: JsonObject,
    val diagnostic: RecordDiagnosticSnapshot? = null,
)
```
Expected `kind` values:

- `kinesis`
- `dynamodb`
- `sqs`
- `eventbridge`
- `unknown`

The `payload` must be the Lambda event record JSON that can be placed directly inside:
```
json
{
  "Records": [
    {}
  ]
}
```
The `payload` field is the replay contract. It must not be flattened, prettified into a different structure, or replaced with a diagnostic representation.

Any normalized, summarized, or redacted display data should go in `diagnostic`.

Snapshotter pipeline:
```
text
runtime AWS Lambda record
    |
    v
dedicated replay DTO
    |
    v
JsonObject payload
    |
    v
ReplayRecordSnapshot(kind, payload)
```
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
Supporting DTOs:
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

```
kotlin
data class RecordDiagnosticSnapshot(
    val summary: String? = null,
    val fields: Map<String, String?>? = null,
)
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

Snapshotters should:

1. Accept runtime Lambda record objects.
2. Convert them to dedicated replay DTOs.
3. Encode those DTOs into `JsonObject`.
4. Return `ReplayRecordSnapshot(kind, payload)`.

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

---

### FaultSnapshotOptions

Controls bounded diagnostic capture.
```
kotlin
data class FaultSnapshotOptions(
    val includeStackTrace: Boolean = true,
    val maxStackTraceFrames: Int = 50,
    val includeCauseChain: Boolean = true,
    val maxCauseDepth: Int = 5,
    val maxDiagnosticStringLength: Int = 10_000,
)
```
Policies:

- Stack trace capture should be bounded.
- Cause-chain capture should be bounded.
- Diagnostic strings should be bounded.
- Replay payloads should not be truncated silently because truncating replay payloads can make them non-replayable.

---

### SnapshotRedactor

Fault events are durable and may contain sensitive business data. Redaction should be available from the first implementation.
```
kotlin
interface SnapshotRedactor {
    fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot
}
```
Default:
```
kotlin
object NoOpSnapshotRedactor : SnapshotRedactor {
    override fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot = snapshot
}
```
The framework should document that redacting `record.payload` may make the fault no longer faithfully replayable.

Recommended policy:

- Redact diagnostic fields freely.
- Redact replay payload only when the user explicitly accepts that replay may be affected.

---

### FaultEvent

`FaultEvent` should become a framework diagnostic DTO. It does not need to implement `Event`.

`FaultEvent` should no longer persist raw runtime `UnitOfWork` or `FaultException`.

Target structure:
```
kotlin
data class FaultEvent(
    val id: String? = null,
    val type: String = FAULT_EVENT_TYPE,
    val timestamp: Long? = null,
    val partitionKey: String? = null,
    val tags: Map<String, String>? = null,
    val err: ErrorSnapshot? = null,
    val uow: UnitOfWorkSnapshot? = null,

    @kotlinx.serialization.Transient
    @com.fasterxml.jackson.annotation.JsonIgnore
    val runtimeUow: UnitOfWork? = null,

    @kotlinx.serialization.Transient
    @com.fasterxml.jackson.annotation.JsonIgnore
    val faultException: FaultException? = null,
)
```
If changing from mutable properties to constructor properties causes too much churn, keep a mutable class shape during the transition:
```
kotlin
class FaultEvent {
    var id: String? = null
    var type: String = FAULT_EVENT_TYPE
    var timestamp: Long? = null
    var partitionKey: String? = null
    var tags: Map<String, String>? = null
    var err: ErrorSnapshot? = null
    var uow: UnitOfWorkSnapshot? = null

    @kotlinx.serialization.Transient
    @com.fasterxml.jackson.annotation.JsonIgnore
    var runtimeUow: UnitOfWork? = null

    @kotlinx.serialization.Transient
    @com.fasterxml.jackson.annotation.JsonIgnore
    var faultException: FaultException? = null
}
```
Fault events should be serialized through `SerializationStrategy`, not through `Event.encoded()` and not through domain `EventCodec`.

Since backward compatibility is not required, the existing raw `uow: UnitOfWork?` field can be replaced by `uow: UnitOfWorkSnapshot?`.

Runtime-only fields must be ignored by all supported serializers.

---

### FaultEventFactory

Centralizes fault event creation.

Responsibilities:

- build `FaultEvent`
- extract `ErrorSnapshot`
- apply `FaultSnapshotOptions`
- create `UnitOfWorkSnapshot`
- apply `SnapshotRedactor`
- copy useful tags
- set runtime-only fields for in-memory inspection, if desired

Example API:
```
kotlin
class FaultEventFactory(
    private val unitOfWorkSnapshotter: UnitOfWorkSnapshotter,
    private val redactor: SnapshotRedactor = NoOpSnapshotRedactor,
    private val options: FaultSnapshotOptions = FaultSnapshotOptions(),
) {
    fun createFaultEvent(
        uow: UnitOfWork?,
        error: Throwable,
    ): FaultEvent
}
```
---

## Target Fault Event JSON Shape

Example for a Kinesis-originated failure:
```
json
{
  "id": "fault-123",
  "type": "fault",
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
  "type": "fault",
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

## Step 1: Add Serialization Strategy Abstraction and Resolver

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/SerializationStrategy.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/SerializationConfig.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/SerializationStrategyResolver.kt
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
Add:
```
kotlin
enum class SerializationStrategyKind {
    AUTO,
    JACKSON,
    KOTLINX,
    MOSHI,
}
```
Add:
```
kotlin
data class SerializationConfig(
    val strategy: SerializationStrategyKind = SerializationStrategyKind.AUTO,
)
```
Add resolver behavior:

- explicit config wins
- environment/config value is used when framework config is absent
- `AUTO` detects available supported serializers from the classpath
- `AUTO` succeeds only when exactly one supported serializer is available
- `AUTO` fails when no supported serializer is available
- `AUTO` fails when multiple supported serializers are available

Recommended environment/config values:
```
text
AWS_LAMBDA_STREAM_SERIALIZATION=jackson
AWS_LAMBDA_STREAM_SERIALIZATION=kotlinx
AWS_LAMBDA_STREAM_SERIALIZATION=moshi
AWS_LAMBDA_STREAM_SERIALIZATION=auto
```
### Verification

Add unit tests:
```
text
core/src/test/kotlin/io/github/huherto/awsLambdaStream/serialization/SerializationStrategyResolverTest.kt
```
Verify:

- explicit Jackson config selects Jackson
- explicit kotlinx config selects kotlinx
- explicit Moshi config fails if Moshi is not present
- `AUTO` selects the only available strategy
- `AUTO` fails when no strategy is available
- `AUTO` fails when multiple strategies are available
- error messages tell the user how to configure the strategy

### Done When

- Framework code can resolve a configured serialization strategy.
- Ambiguous serializer selection never silently chooses an implementation.

---

## Step 2: Add Initial Serialization Strategy Implementations

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/JacksonSerializationStrategy.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/serialization/KotlinxSerializationStrategy.kt
```
Add Jackson and kotlinx implementations.

Moshi can remain optional/later.

### Verification

Add tests:
```
text
core/src/test/kotlin/io/github/huherto/awsLambdaStream/serialization/JacksonSerializationStrategyTest.kt
core/src/test/kotlin/io/github/huherto/awsLambdaStream/serialization/KotlinxSerializationStrategyTest.kt
```
Verify:

- serializes simple data class
- deserializes simple data class where supported
- omits nulls if that remains desired
- does not fail on empty beans if that remains desired
- runtime-only fields are omitted
- snapshot DTO JSON is semantically equivalent between Jackson and kotlinx where practical

### Done When

- Jackson strategy works.
- kotlinx strategy works.
- Framework-owned DTOs can be serialized without relying on a hard-coded global mapper.

---

## Step 3: Deprecate Event.encoded()

### Changes

Deprecate `Event.encoded()`.
```
kotlin
@Deprecated(
    message = "Use EventCodec or the configured framework publisher instead.",
)
fun encoded(): String
```
Update internal framework paths so new behavior uses:

- `EventCodec` for domain events
- `SerializationStrategy` for framework diagnostic DTOs and fault snapshots

Do not use `Event.encoded()` for new fault persistence behavior.

### Verification

Search for usages of:
```
kotlin
encoded()
```
Classify each usage:

- legacy test
- domain event serialization to be replaced by `EventCodec`
- framework diagnostic serialization to be replaced by `SerializationStrategy`
- debug-only usage

### Done When

- `Event.encoded()` is deprecated.
- New fault serialization paths do not call `encoded()`.

---

## Step 4: Define Typed Replay DTOs and Snapshot DTOs

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/replay/KinesisReplayRecord.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/replay/DynamoDbReplayRecord.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/replay/DynamoDbAttributeValueSnapshot.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/FaultSnapshots.kt
```
Add replay DTOs:
```
kotlin
@Serializable
data class KinesisReplayRecord(...)
```

```
kotlin
@Serializable
data class DynamoDbReplayRecord(...)
```

```
kotlin
@Serializable
data class DynamoDbAttributeValueSnapshot(...)
```
Add snapshot DTOs:
```
kotlin
data class ErrorSnapshot(...)
data class UnitOfWorkSnapshot(...)
data class ReplayRecordSnapshot(...)
data class PipelineSnapshot(...)
data class EventSummarySnapshot(...)
data class AwsOperationSnapshot(...)
data class S3Snapshot(...)
data class RecordDiagnosticSnapshot(...)
```
Recommended `ReplayRecordSnapshot` shape:
```
kotlin
data class ReplayRecordSnapshot(
    val kind: String,
    val payload: JsonObject,
    val diagnostic: RecordDiagnosticSnapshot? = null,
)
```
### Verification

Add tests that serialize the DTOs with Jackson and kotlinx where practical.

Verify:

- generated JSON is stable and clean
- replay DTOs serialize to valid Lambda event record shapes
- Kinesis data remains a base64 string
- DynamoDB AttributeValues preserve `{ "S": "..." }`, `{ "N": "..." }`, `{ "BOOL": true }`, `{ "M": {} }`, and `{ "L": [] }`
- snapshot JSON does not contain SDK internals, exception internals, or pipeline object internals

### Done When

- Snapshot DTOs serialize without custom runtime objects.
- Replay payloads are constructed from typed DTOs.
- `ReplayRecordSnapshot.payload` is a replayable `JsonObject`.

---

## Step 5: Add Fault Snapshot Options and Redaction

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/FaultSnapshotOptions.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/SnapshotRedactor.kt
```
Add:
```
kotlin
data class FaultSnapshotOptions(
    val includeStackTrace: Boolean = true,
    val maxStackTraceFrames: Int = 50,
    val includeCauseChain: Boolean = true,
    val maxCauseDepth: Int = 5,
    val maxDiagnosticStringLength: Int = 10_000,
)
```
Add:
```
kotlin
interface SnapshotRedactor {
    fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot
}
```
Add:
```
kotlin
object NoOpSnapshotRedactor : SnapshotRedactor {
    override fun redact(snapshot: UnitOfWorkSnapshot): UnitOfWorkSnapshot = snapshot
}
```
### Verification

Add tests for:

- stack trace disabled
- stack trace frame limit
- cause-chain depth limit
- diagnostic string length limit
- no-op redactor
- custom redactor

### Done When

- Snapshot diagnostic capture is bounded.
- Users have a redaction hook before fault data is persisted.

---

## Step 6: Add Record Snapshotters

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/RecordSnapshotter.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/KinesisRecordSnapshotter.kt
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/DynamoDbRecordSnapshotter.kt
```
Add:
```
kotlin
interface RecordSnapshotter {
    fun supports(record: Any): Boolean

    fun snapshot(record: Any): ReplayRecordSnapshot
}
```
Snapshotters should:

1. Accept runtime Lambda record objects.
2. Convert them to dedicated replay DTOs.
3. Encode those DTOs into `JsonObject`.
4. Return `ReplayRecordSnapshot(kind, payload)`.

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
`kinesis.data` must remain a base64-compatible string.

### DynamoDB requirements

The DynamoDB snapshot must preserve Lambda DynamoDB stream JSON shape, especially AttributeValues.

Do not flatten DynamoDB AttributeValues in replay payloads.

### Verification

Add tests:
```
text
core/src/test/kotlin/io/github/huherto/awsLambdaStream/faults/KinesisRecordSnapshotterTest.kt
core/src/test/kotlin/io/github/huherto/awsLambdaStream/faults/DynamoDbRecordSnapshotterTest.kt
```
Verify:

- `kind` is correct
- `payload` can be wrapped in `{ "Records": [payload] }`
- Kinesis data remains base64-compatible
- DynamoDB AttributeValue shape is preserved
- wrapped payload can be deserialized into AWS Lambda event classes where practical
- golden JSON fixtures match the expected replay contract

### Done When

- Kinesis and DynamoDB records snapshot into replayable payloads.
- Tests verify the exact JSON shape needed for Lambda reinvocation.

---

## Step 7: Add UnitOfWorkSnapshotter

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

## Step 8: Change FaultEvent to Framework Diagnostic DTO

### Changes

Update `FaultEvent` so persisted fields are:
```
kotlin
var id: String? = null
var type: String = FAULT_EVENT_TYPE
var timestamp: Long? = null
var partitionKey: String? = null
var tags: Map<String, String>? = null
var err: ErrorSnapshot? = null
var uow: UnitOfWorkSnapshot? = null
```
Runtime-only fields should be transient for all supported serializers:
```
kotlin
@kotlinx.serialization.Transient
@com.fasterxml.jackson.annotation.JsonIgnore
var runtimeUow: UnitOfWork? = null

@kotlinx.serialization.Transient
@com.fasterxml.jackson.annotation.JsonIgnore
var faultException: FaultException? = null
```
`FaultEvent` does not need to implement `Event`.

Recommended behavior:

- remove inheritance from `BaseEvent`
- remove `encoded()`
- serialize fault events through `SerializationStrategy`
- keep field names stable in persisted JSON

If removing `Event` immediately causes too much internal churn, use a short transition where publishing supports both domain events and framework diagnostic events.

### Verification

Update tests that inspect fault events.

Verify:

- `fault.uow` is a `UnitOfWorkSnapshot`
- `fault.runtimeUow` can still be inspected in memory if needed
- serialized fault event does not include `runtimeUow`
- serialized fault event does not include `faultException`
- serialized fault event does not include raw `UnitOfWork`
- `FaultEvent` serialization does not call `Event.encoded()`

### Done When

- Fault events persist only snapshot data.
- Fault events are framework diagnostic DTOs.
- No serialized fault JSON contains raw `UnitOfWork`, raw exceptions, or SDK internals.

---

## Step 9: Add Fault Event Factory

### Changes

Create:
```
text
core/src/main/kotlin/io/github/huherto/awsLambdaStream/faults/FaultEventFactory.kt
```
Responsibilities:

- build `FaultEvent`
- extract `ErrorSnapshot`
- apply `FaultSnapshotOptions`
- create `UnitOfWorkSnapshot`
- apply `SnapshotRedactor`
- copy useful tags
- set runtime-only fields for in-memory inspection, if desired

Example API:
```
kotlin
class FaultEventFactory(
    private val unitOfWorkSnapshotter: UnitOfWorkSnapshotter,
    private val redactor: SnapshotRedactor = NoOpSnapshotRedactor,
    private val options: FaultSnapshotOptions = FaultSnapshotOptions(),
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
- cause-chain policy works
- runtime `UnitOfWork` is not persisted
- snapshot is attached
- redactor is applied

### Done When

- Fault creation is centralized.
- Fault serialization behavior is consistent.
- Redaction and diagnostic capture options are applied before persistence.

---

## Step 10: Update Fault Manager to Use Factory

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
Fault publishing path
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

## Step 11: Update Publishing Serialization Boundaries

### Changes

Ensure publishing uses the correct serialization boundary:

- use `EventCodec` for domain event payloads
- use `SerializationStrategy` for framework-created DTOs and fault snapshots

Fault events should not depend on `Event.encoded()`.

If existing publishing APIs only accept `Event`, add or refactor a path for framework diagnostic DTOs.

Recommended distinction:
```
text
Domain Event
    |
    v
EventCodec
    |
    v
EventBridge detail JSON
```

```
text
FaultEvent diagnostic DTO
    |
    v
SerializationStrategy
    |
    v
EventBridge detail JSON
```
### Verification

Add or update tests for:

- publishing normal domain event
- publishing fault event
- serialized EventBridge detail contains snapshot shape
- fault event publishing does not call `encoded()`
- domain event publishing can be configured with an `EventCodec`

### Done When

- Event publishing does not depend on a hard-coded global Jackson mapper for new behavior.
- Fault events serialize through the configured `SerializationStrategy`.
- Domain events serialize through configured `EventCodec`.

---

## Step 12: Update ResubmitFaults to Use New Snapshot Shape

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
Also read `record.kind`:
```
kotlin
val kinds = if (batch != null) {
    batch.mapNotNull { item ->
        item.jsonObject["record"]
            ?.jsonObject
            ?.get("kind")
            ?.jsonPrimitive
            ?.contentOrNull
    }.toSet()
} else {
    setOfNotNull(
        eventUow["record"]
            ?.jsonObject
            ?.get("kind")
            ?.jsonPrimitive
            ?.contentOrNull
    )
}
```
Validation behavior:

- no records: error
- missing payload: error
- missing kind: warn or error
- mixed known kinds: error unless explicitly allowed
- `unknown` kind: skip or error unless explicitly allowed

Optional future CLI/config options:
```
kotlin
val allowUnknownKind: Boolean = false
val allowMixedKinds: Boolean = false
```
Generated Lambda invoke payload must be exactly:
```
json
{
  "Records": []
}
```
with original replay payloads inside.

### Verification

Update resubmit tests.

Verify:

- single Kinesis fault record resubmits
- batched Kinesis fault records resubmit
- single DynamoDB fault record resubmits
- batched DynamoDB fault records resubmit
- mixed kinds are rejected unless explicitly allowed
- missing payload is rejected
- generated Lambda payload is exactly `{ "Records": [...] }`

### Done When

- Resubmit tool only uses snapshot payloads.
- Resubmit tool does not rely on raw `UnitOfWork` JSON.
- Resubmit tool validates record kind.

---

## Step 13: Update Integration Tests

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

### Golden JSON Contract Tests

Add golden JSON fixtures for durable fault event contracts:
```
text
core/src/test/resources/faults/kinesis-fault-event.json
core/src/test/resources/faults/dynamodb-fault-event.json
core/src/test/resources/faults/batched-kinesis-fault-event.json
core/src/test/resources/faults/batched-dynamodb-fault-event.json
```
Verify:

- serialized fault events match the expected durable contract
- replay extraction produces exact `{ "Records": [...] }`
- forbidden fields are absent
- DynamoDB AttributeValue shape is preserved
- Kinesis data remains base64 string

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

## Step 14: Add Event Codec Implementations

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
- Internal framework code no longer depends on `Event.encoded()`.

---

## Step 15: Add Optional Moshi Support

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

## Step 16: Clean Up Old JSON Utilities

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

- serialization strategy resolver tests
- Jackson serialization strategy tests
- kotlinx serialization strategy tests
- event codec tests
- replay DTO tests
- snapshot DTO tests
- Kinesis record snapshot tests
- DynamoDB record snapshot tests
- UnitOfWork snapshot tests
- fault snapshot options tests
- snapshot redactor tests
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
2. Add `SerializationConfig`.
3. Add `SerializationStrategyResolver`.
4. Add Jackson serialization strategy.
5. Add kotlinx serialization strategy.
6. Deprecate `Event.encoded()`.
7. Add typed replay DTOs:
    - `KinesisReplayRecord`
    - `KinesisReplayData`
    - `DynamoDbReplayRecord`
    - `DynamoDbStreamReplayData`
    - `DynamoDbAttributeValueSnapshot`
8. Add snapshot DTOs:
    - `ErrorSnapshot`
    - `UnitOfWorkSnapshot`
    - `ReplayRecordSnapshot`
    - `PipelineSnapshot`
    - `EventSummarySnapshot`
    - `AwsOperationSnapshot`
    - `S3Snapshot`
    - `RecordDiagnosticSnapshot`
9. Add `FaultSnapshotOptions`.
10. Add `SnapshotRedactor`.
11. Add Kinesis record snapshotter.
12. Add DynamoDB record snapshotter.
13. Add `UnitOfWorkSnapshotter`.
14. Change `FaultEvent` into a framework diagnostic DTO using `UnitOfWorkSnapshot`.
15. Add `FaultEventFactory`.
16. Update fault manager.
17. Update fault publishing to use `SerializationStrategy`.
18. Update resubmit tool to read `uow.record.payload` and `uow.batch[*].record.payload`.
19. Add record-kind validation to resubmit tool.
20. Add golden JSON contract tests.
21. Update integration tests.
22. Add `JacksonEventCodec` and `KotlinxEventCodec`.
23. Update domain event publishing to use `EventCodec`.
24. Clean up old JSON helpers.
25. Add optional Moshi support later, preferably as a separate module.

---

# Risks and Mitigations

## Risk: Kinesis/DynamoDB record payloads are not exactly replayable

Mitigation:

- Add tests that wrap snapshot payloads in `{ "Records": [...] }`.
- Deserialize them back into AWS Lambda event classes where practical.
- Use integration dry-run Lambda invoke tests.
- Add golden JSON fixtures for expected replay shapes.

---

## Risk: Snapshot DTO becomes too large

Mitigation:

- Store only replay fields and useful diagnostics.
- Summarize SDK requests/responses.
- Add size tests or warnings for large records.
- Do not truncate replay payloads silently.

---

## Risk: Sensitive data leaks into fault events

Mitigation:

- Avoid serializing full SDK request/response objects.
- Add `SnapshotRedactor` from the beginning.
- Redact diagnostic fields freely.
- Redact replay payload only when the user explicitly accepts that replay may be affected.

---

## Risk: Serializer behavior differs across Jackson/kotlinx/Moshi

Mitigation:

- Use simple DTOs for framework snapshots.
- Avoid persisted `Any` fields.
- Use dedicated DTOs for Kinesis and DynamoDB replay records.
- Convert dedicated replay DTOs into `JsonObject` before assigning to `ReplayRecordSnapshot.payload`.
- Avoid polymorphic payload fields in persisted fault JSON.
- Add semantic JSON comparison tests across supported serializers.
- Add golden JSON contract tests for durable replay payloads.

Important rule:
```
kotlin
data class ReplayRecordSnapshot(
    val kind: String,
    val payload: JsonObject,
    val diagnostic: RecordDiagnosticSnapshot? = null,
)
```
`payload` is the replay contract. It should contain the exact Lambda record shape needed inside `{ "Records": [...] }`.

---

## Risk: Classpath serializer auto-detection selects an unexpected implementation

Mitigation:

- Explicit framework config always wins.
- Environment/config value wins over auto-detection.
- `AUTO` succeeds only when exactly one supported serializer is present.
- `AUTO` fails with a clear error when multiple serializers are present.
- Error message should list detected serializers and explain how to configure one explicitly.

---

## Risk: Redaction makes replay payloads unusable

Mitigation:

- Clearly distinguish replay payload from diagnostic data.
- Redact diagnostic data freely.
- Redact `record.payload` only when the user explicitly accepts that replay may be affected.
- Document that `record.payload` is the replay contract.
- Add tests that replay payloads remain valid when no redactor is configured.

---

# Definition of Done

This change is complete when:

- Framework users can select a serialization strategy explicitly.
- The framework can auto-detect a serializer only when selection is unambiguous.
- Ambiguous serializer auto-detection fails with a clear error.
- Domain events can be encoded/decoded through configurable `EventCodec`s.
- `Event.encoded()` is deprecated and not used by new fault persistence paths.
- `FaultEvent` is a framework diagnostic DTO and does not need to implement `Event`.
- Fault events no longer serialize raw `UnitOfWork`.
- Fault events contain stable `UnitOfWorkSnapshot` data.
- Runtime-only fields are ignored by both Jackson and kotlinx.serialization.
- Kinesis records are stored in replayable form.
- DynamoDB records are stored in replayable form.
- Replay payloads are built from dedicated Kinesis and DynamoDB DTOs.
- `ReplayRecordSnapshot.payload` is a `JsonObject` containing exact Lambda record shape.
- DynamoDB AttributeValues are not flattened.
- Resubmit tooling reads `uow.record.payload` or `uow.batch[*].record.payload`.
- Resubmit tooling validates `record.kind`.
- Golden JSON fixtures exist for Kinesis, DynamoDB, and batched fault events.
- Existing unit and integration tests pass after being updated to the new shape.
- Stored fault event JSON is safe, stable, readable, and resubmittable.
```

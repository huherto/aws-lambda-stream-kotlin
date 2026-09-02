# Kotlin Developers Guide

AWS Lambda Stream for the JVM is a Kotlin-first framework, leveraging modern language features to build reliable, serverless, event-driven applications on AWS.

This framework leverages **Kotlin Coroutines and Flow** to provide a type-safe, non-blocking, and highly composable developer experience for building AWS Lambda functions that process event streams.

## Why use this?

*   **Type-Safe Pipelines**: Catch errors at compile time, not runtime.
*   **Non-Blocking I/O**: Efficiently handle AWS service calls (DynamoDB, S3, EventBridge) using coroutines.
*   **Built-in Reliability**: Idempotency, retries, and fault-handling are first-class citizens.
*   **Composable**: Small, focused pipeline "flavors" (CDC, Correlate, Materialize) that you can chain together.
*   **Observability**: Automatic EMF metrics and detailed "Unit of Work" snapshots for debugging.

## Quick Start

### 1. Define your Event

Implementing the `Event` interface ensures standard metadata (ID, timestamp, correlation tags) is propagated automatically.

```kotlin
@Serializable
@SerialName("ORDER_PLACED")
data class OrderPlacedEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    val orderId: String
) : Event {
    override fun eventType() = "ORDER_PLACED"
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, 
        tags: Map<String, String>?, raw: RawRecord?, 
        eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?
    ) = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}
```

### 2. Configure your Pipeline

Choose a "flavor" that fits your use case. For example, use `CollectPipeline` to store events in a Microstore.

```kotlin
val collectPipeline = CollectPipeline(
    pipelineId = "order-collection",
    eventsMicrostore = eventsMicrostore,
    eventFilter = EventFilters.classes(OrderPlacedEvent::class)
)
```

### 3. Assemble and Run in Lambda

Adapters turn AWS Lambda events into a Kotlin `Flow`. The `PipelineAssembler` runs them through your configured pipelines.

```kotlin
class OrderHandler(
    private val container: MyContainer = MyContainer.build()
) : RequestHandler<KinesisEvent, Void?> {

    override fun handleRequest(input: KinesisEvent, context: Context): Void? = runBlocking {
        val flow = container.kinesisAdapter.fromKinesis(input)
        
        container.assembler
            .assemble(flow)
            .collect { uow -> 
                println("Processed ${uow.event?.id}")
            }
        null
    }
}
```

## Core Concepts for Developers

### Unit of Work (UOW)
Everything in the framework revolves around the `UnitOfWork`. It wraps your event and provides context, such as:
*   **Snapshots**: Useful for debugging failures.
*   **Metrics**: Automatic EMF reporting.
*   **Extensions**: Attach custom state to a processing unit.

### Adapters (`from`)
Zero-boilerplate integration with AWS event sources:
*   `KinesisAdapter`, `SqsAdapter`, `SnsAdapter`, `S3Adapter`, `DynamodbAdapter`.

### Pipeline Flavors
*   **CDC**: React to DynamoDB table changes.
*   **Correlate**: Group events by partition key for consistent processing.
*   **Evaluate**: Apply business logic and emit new events.
*   **Materialize**: Sink events to S3, DynamoDB, or EventBridge.

## Testing
Test your logic without mocks or complex infrastructure using `EventsMicrostoreInMemory` and `TestContext`.

```kotlin
@Test
fun `should process order`() = runTest {
    val uow = UnitOfWork(event = OrderPlacedEvent(orderId = "123"))
    val result = pipeline.process(flowOf(uow)).toList()
    result.size shouldBe 1
}
```

## Documentation for Developers

* [Framework Features](Features.md)
* [Understanding Events and Event Types](Events.md)
* [Benefits of Kotlin CoRoutines and the Flow Framework](KotlinCoRoutinesAndFlow.md)
* [Implementing Events](EventImplementation.md)

## Examples

Check out the [Shipment Unit Tracking (SUT)](../examples/sut) example for a complete Kotlin-based application.

---

*Looking for the architectural deep-dive? Check out the [Software Architects Guide](SoftwareArchitects.md).*

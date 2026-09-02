# Java Developers Guide

While written in Kotlin, AWS Lambda Stream for the JVM is designed to be Java-friendly. It provides Java-compatible APIs and abstractions to allow Java developers to build reliable, serverless, event-driven applications on AWS.

## Why use this in Java?

*   **JVM Interoperability**: Leverage a robust Kotlin framework directly from your Java projects.
*   **Reliability for Java**: First-class support for idempotency, retries, and fault-handling.
*   **Modern APIs**: Clean, builder-based configuration for pipelines.
*   **Seamless Integration**: Use standard Java 17+ features like Records and Sealed Classes for your event models.

## Quick Start

### 1. Define your Event

Use Java Records for a concise and immutable event definition. Use Jackson annotations for serialization.

```java
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public record OrderPlacedEvent(
    String id,
    Long timestamp,
    String partitionKey,
    Map<String, String> tags,
    RawRecord raw,
    EnvelopeEncryptionMetadata eem,
    List<EventReference> triggers,
    String orderId
) implements Event {
    @Override public String eventType() { return "ORDER_PLACED"; }

    @Override
    public Event copyEvent(String id, Long timestamp, String partitionKey, 
                          Map<String, String> tags, RawRecord raw, 
                          EnvelopeEncryptionMetadata eem, List<EventReference> triggers) {
        return new OrderPlacedEvent(id, timestamp, partitionKey, tags, raw, eem, triggers, orderId);
    }
}
```

### 2. Configure your Pipeline

Use the `PipelineAssembler` builder to configure your processing logic.

```java
public class OrderProcessingContainer {
    public final PipelineAssembler assembler;

    public OrderProcessingContainer() {
        CdcPipeline cdcPipeline = CdcPipeline.builder()
                .id("order-cdc")
                .toEventJava(this::toEvent)
                .build();

        this.assembler = PipelineAssembler.builder()
                .addPipeline(cdcPipeline)
                .build();
    }

    private Event toEvent(UnitOfWork uow) {
        // Map DynamoDB RecordPair to your Event
        return new OrderPlacedEvent(...);
    }
}
```

### 3. Assemble and Run in Lambda

The `PipelineRunner` provides a Java-friendly way to execute your pipelines within a standard `RequestHandler`.

```java
public class OrderHandler implements RequestHandler<DynamodbEvent, Void> {
    private final OrderProcessingContainer container = new OrderProcessingContainer();

    @Override
    public Void handleRequest(DynamodbEvent input, Context context) {
        new PipelineRunner<DynamodbEvent>(container.assembler)
                .headFlow(container.dynamoDbAdapter::fromDynamoDB)
                .transformer(Handlers::collectMetrics)
                .run(input);
        return null;
    }
}
```

## Core Concepts for Java Developers

### Unit of Work (UOW)
The `UnitOfWork` is a standard Java-compatible data class. You can access the event and context using standard getters:
*   `uow.getEvent()`: Access the event being processed.
*   `uow.getPipeline()`: Access pipeline metadata.
*   `uow.getExtensions()`: Access custom state.

### Java-Friendly Flavors
Pipeline "flavors" like `CdcPipeline` or `EvaluatePipeline` provide `toEventJava` and other methods that accept standard Java `Function` or `BiFunction` instead of Kotlin lambdas.

### Handlers and PipelineRunner
*   `PipelineRunner`: Orchestrates the execution of the flow in a blocking manner suitable for Lambda.
*   `Handlers`: Provides static utility methods like `collectMetrics` that can be used in your pipeline runner.

## Testing
You can test your pipeline logic using standard JUnit 5.

```java
@Test
void shouldProcessOrder() {
    OrderPlacedEvent event = new OrderPlacedEvent(..., "order-123");
    UnitOfWork uow = new UnitOfWork(null, null, event);
    
    // Test logic here
}
```

## Documentation for Developers

* [Framework Features](Features.md)
* [Understanding Events and Event Types](Events.md)
* [Implementing Events](EventImplementationJava.md)

## Examples

The [URL Shortener](../examples/urlshortener) example demonstrates how to use the framework in a Java-based project.

---

*Looking for the architectural deep-dive? Check out the [Software Architects Guide](SoftwareArchitects.md).*

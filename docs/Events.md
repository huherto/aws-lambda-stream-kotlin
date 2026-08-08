# Events
Events are the primary mechanism for communication between services and subsystems. They are used to trigger actions, coordinate business processes, and facilitate communication between services and subsystems.

## Table of Contents

- [Domain Events](#domain-events)
    - [Internal Domain Events](#internal-domain-events)
    - [External Domain Events](#external-domain-events)
- [Change Events](#change-events)
- [Implementation Details](#implementation-details)
    - [1. Immutability](#1-immutability)
    - [2. The `Event` Interface and `BaseEvent`](#2-the-event-interface-and-baseevent)
    - [3. Core Properties](#3-core-properties)
    - [4. Required Methods](#4-required-methods)
    - [5. Serialization](#5-serialization)
    - [6. The `copyEvent` Mechanism](#6-the-copyevent-mechanism)
    - [7. Recommended Pattern](#7-recommended-pattern)

## Domain Events

Domain Events represent significant business occurrences within the system. They capture facts about what has happened
in the domain and are named in the past tense to reflect completed actions (e.g., "OrderPlaced", "PaymentProcessed", "
CustomerRegistered").

Domain Events are:

- Immutable records of business facts
- Published when significant state changes occur
- Used to communicate business-level changes between services and subsystems
- The foundation for event-driven architecture and event sourcing patterns

### Internal Domain Events

Internal Domain Events represent significant business occurrences that are relevant within a single subsystem. They are
used for communication between services within the same subsystem boundary and are not exposed to external subsystems.

Internal Domain Events are:

- Scoped to a single subsystem
- Used to coordinate business processes within subsystem boundaries
- May have subsystem-specific payload structures
- Can evolve more freely as they don't cross subsystem boundaries

### External Domain Events

External Domain Events represent significant business occurrences that are relevant across subsystem boundaries. They
form the public contract between subsystems and require careful versioning and stability guarantees.

External Domain Events are:

- Published across subsystem boundaries
- Subject to strict versioning and backward compatibility requirements
- Define the public contract between subsystems
- Require coordination when changes are needed to prevent breaking consumers

## Change Events

Change Events represent technical changes to data or system state. They are lower-level notifications about
modifications to entities or resources within the system (e.g., "CustomerUpdated", "InventoryModified").

Change Events are:

- Technical notifications about state modifications
- Often generated automatically by data stores or persistence layers
- Used to trigger reactive updates, maintain read models, or synchronize caches
- Distinguished from Domain Events by their technical rather than business-focused nature

The distinction between Domain Events and Change Events helps maintain clean separation between business logic and
technical implementation concerns.

## Implementation Details

In the `aws-lambda-stream-kotlin` library, events must follow specific technical requirements to ensure predictable processing and compatibility with the framework's pipelines.

### 1. Immutability
Events must be immutable. All properties should be declared using `val`. This ensures that events cannot be mutated accidentally during pipeline processing, leading to safer and more predictable side effects.

### 2. The `Event` Interface and `BaseEvent`
All events must implement the `io.github.huherto.awsLambdaStream.Event` interface. It is highly recommended to extend the `BaseEvent` abstract class, which provides a default implementation for the `copyEvent` method using reflection.

### 3. Core Properties
The `Event` interface requires the following metadata properties:
*   `id: String?`: A unique identifier for the event.
*   `timestamp: Long?`: The time the event occurred (milliseconds since epoch).
*   `partitionKey: String?`: Used for ordering (e.g., Kinesis shard key).
*   `tags: Map<String, String>?`: Metadata tags for filtering or categorization.
*   `raw: Any?`: The raw payload or reference (e.g., a `ClaimCheck`).
*   `eem: Any?`: Envelope Encryption Metadata.
*   `triggers: List<EventReference>?`: References to preceding events in the causal chain.

### 4. Required Methods
*   **`eventType()`**: Must return a unique `String` identifying the event type. Used for routing and filtering.
*   **`copyEvent(...)`**: Creates a new instance with updated metadata. `BaseEvent` provides a reflection-based implementation for this.
*   **`encoded()`**: (Deprecated) Returns a JSON string representation of the event.

### 5. Serialization
Events use `kotlinx.serialization` for transport:
*   Use `@Serializable` on your event classes.
*   Use `@SerialName("TYPE_NAME")` to define a stable name for polymorphic serialization.
*   Use `@Contextual` for generic properties like `raw` or `eem`.

### 6. The `copyEvent` Mechanism
The `copyEvent` method allows the framework to update standard metadata on any `Event` without knowing its concrete type.

```kotlin
fun copyEvent(
    id: String? = this.id,
    timestamp: Long? = this.timestamp,
    partitionKey: String? = this.partitionKey,
    tags: Map<String, String>? = this.tags,
    raw: Any? = this.raw,
    eem: Any? = this.eem,
    triggers: List<EventReference>? = this.triggers
): Event
```

*   **`myEvent.copy(...)`**: Use Kotlin's generated data class `copy` for domain-specific fields.
*   **`event.copyEvent(...)`**: Use this to update metadata fields defined in the `Event` interface.

### 7. Recommended Pattern
The preferred way to implement events is using a `data class` extending `BaseEvent`:

```kotlin
@Serializable
@SerialName("ORDER_PLACED")
data class OrderPlacedEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    @Contextual override val raw: Any? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    val orderId: String
) : BaseEvent() {
    override fun eventType() = "ORDER_PLACED"

    @Deprecated("Legacy")
    override fun encoded(): String = Json.encodeToString(this)
}
```

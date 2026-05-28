# Claim Check Pattern

The **Claim Check** pattern is a messaging pattern used when an event or message contains a payload that is too large, expensive, sensitive, or inefficient to move through the messaging system directly.

Instead of sending the full payload through every queue, stream, or event bus, the system stores the full payload in an external storage location and sends a smaller message containing a reference to it. That reference is the “claim check.”

The name comes from the physical-world idea of checking a coat or bag: you leave the item in a storage area and receive a ticket. Later, someone can use that ticket to retrieve the original item.

---

## Why Claim Check Exists

Event-driven systems often move data through infrastructure such as:

- Amazon SQS
- Amazon SNS
- Amazon EventBridge
- Amazon Kinesis
- Lambda invocation payloads
- HTTP APIs
- internal event pipelines

These systems usually work best with compact messages. Many of them also have strict payload size limits.

For example, an event may contain:

- a large document
- a full order snapshot
- an image or binary payload
- a verbose audit record
- a batch of domain data
- a third-party API response
- a payload that should not be duplicated across multiple services

Passing that entire payload through every message channel can create several problems:

1. **Message size limits**  
   The payload may exceed the maximum size supported by the messaging service.

2. **Higher cost**  
   Larger messages cost more to transmit, store, retry, and log.

3. **Slower processing**  
   Every consumer must receive and deserialize the full payload, even if it only needs metadata.

4. **Data duplication**  
   The same large payload may be copied into many queues, logs, dead-letter queues, and retry stores.

5. **Security and governance concerns**  
   Sensitive data may be spread across systems where it is harder to control lifecycle, access, or retention.

The Claim Check pattern solves these issues by separating **payload transport** from **payload storage**.

---

## The Basic Idea

In a Claim Check flow, the system does not send the full payload directly.

Instead, it performs three steps:

1. **Store the full payload externally**
2. **Send a lightweight message containing a reference**
3. **Retrieve the full payload later when needed**

A simplified claim-check message might look like this:

```json
{
  "id": "event-123",
  "type": "ShipmentCreated",
  "timestamp": 1716820000000,
  "s3": {
    "bucket": "my-claim-check-bucket",
    "key": "us-east-1/claimchecks/2026/05/28/14/event-123"
  }
}
```


The message still contains enough information to route, identify, and process the event, but the large event body is stored somewhere else, such as Amazon S3.

---

## How Claim Check Works

A Claim Check implementation usually has two sides:

1. **Store / issue the claim check**
2. **Redeem the claim check**

---

## 1. Storing the Payload

When an event enters the pipeline, the Claim Check store receives the original event.

The store writes the complete encoded event payload to external storage. In an AWS-based system, Amazon S3 is a natural fit because it is durable, scalable, inexpensive, and supports fine-grained access control.

After the payload is stored, the original event is replaced with a smaller event that contains a pointer to the stored object.

Conceptually:

```plain text
Original event
    |
    | store full payload in S3
    v
Claim-check event containing bucket + key
```


The original payload might be:

```json
{
  "id": "event-123",
  "type": "ShipmentCreated",
  "partitionKey": "shipment-456",
  "timestamp": 1716820000000,
  "tags": {
    "source": "shipping"
  },
  "payload": {
    "large": "domain data goes here"
  }
}
```


After applying Claim Check, the event sent downstream becomes:

```json
{
  "id": "event-123",
  "type": "ShipmentCreated",
  "partitionKey": "shipment-456",
  "timestamp": 1716820000000,
  "tags": {
    "source": "shipping"
  },
  "s3": {
    "bucket": "my-claim-check-bucket",
    "key": "us-east-1/claimchecks/2026/05/28/14/event-123"
  }
}
```


The downstream message is much smaller, but it still preserves important event metadata.

---

## 2. Redeeming the Claim Check

A consumer that needs the original payload can redeem the claim check.

Redeeming means:

1. Read the claim-check reference from the event.
2. Fetch the stored object from external storage.
3. Decode the stored payload back into the original event.
4. Continue processing as if the original event had arrived directly.

Conceptually:

```plain text
Claim-check event
    |
    | read bucket + key
    | fetch payload from S3
    v
Original event restored
```


Not every consumer must redeem the claim check. Some consumers may only need the event ID, type, partition key, timestamp, or tags. Those consumers can process the lightweight message directly.

This is one of the major advantages of the pattern: consumers only pay the cost of retrieving the full payload when they actually need it.

---

## Typical Flow

A complete Claim Check flow looks like this:

```plain text
Producer
  |
  | emits large event
  v
Claim Check Store
  |
  | writes full event to S3
  | replaces event with reference
  v
Message channel
  |
  | sends lightweight event
  v
Consumer
  |
  | optionally redeems claim check
  v
Original payload available for processing
```


---

## Storage Key Design

A good Claim Check implementation should create predictable, unique, and operationally useful storage keys.

A common S3 key structure is:

```plain text
AWS_REGION/claimchecks/YYYY/MM/DD/HH/eventId
```


For example:

```plain text
us-east-1/claimchecks/2026/05/28/14/event-123
```


This structure has several benefits:

- **Region awareness**  
  The key makes it clear where the event originated.

- **Time-based organization**  
  Objects are grouped by year, month, day, and hour.

- **Operational debugging**  
  Engineers can quickly locate claim-check objects for a specific time window.

- **Lifecycle management**  
  Time-based prefixes work well with S3 lifecycle policies.

- **Event uniqueness**  
  Including the event ID helps avoid collisions and makes objects traceable.

---

## Batches and Claim Check

In event pipelines, messages are often processed in batches.

A Claim Check implementation should treat each event in a batch independently. That means each item in the batch is stored separately and replaced with its own claim-check reference.

Conceptually:

```plain text
Batch:
  Event A
  Event B
  Event C

After Claim Check:
  Reference to Event A in S3
  Reference to Event B in S3
  Reference to Event C in S3
```


This is important because each event may have a different ID, type, partition key, timestamp, or payload. Storing them independently keeps the model simple and allows individual retry, tracing, and recovery behavior.

---

## When Claim Check Should Be Used

Use Claim Check when events are:

- large enough to approach service payload limits
- expensive to copy through multiple queues or streams
- needed by only some consumers
- better managed in object storage
- subject to retention, encryption, or lifecycle requirements
- likely to appear in retries, dead-letter queues, or logs
- carrying data that should not be duplicated unnecessarily

Common examples include:

- shipment documents
- invoices
- generated reports
- images
- large JSON payloads
- third-party webhook bodies
- event snapshots
- audit records
- machine learning inputs or outputs

---

## When Claim Check May Not Be Needed

Claim Check adds an extra storage and retrieval step, so it is not always necessary.

Avoid it when:

- events are already small
- every consumer always needs the full payload immediately
- the additional S3 read/write latency is unacceptable
- the payload must be processed atomically with the message transport
- the system does not need external payload retention
- operational simplicity matters more than message size reduction

For small domain events, sending the full event directly is often simpler and more appropriate.

---

## Benefits

### Smaller Messages

The most obvious benefit is that downstream infrastructure receives a compact message instead of a large payload.

This reduces pressure on queues, streams, logs, retries, and dead-letter handling.

---

### Avoiding Payload Size Limits

Many AWS services have payload size limits. Claim Check allows applications to move large logical events through systems that only support smaller physical messages.

The messaging layer carries the reference; S3 carries the payload.

---

### Lower Cost

Large payloads can increase cost across several dimensions:

- queue storage
- network transfer
- stream throughput
- retries
- logging
- dead-letter queue storage
- Lambda execution time

Claim Check reduces repeated movement of the full payload.

---

### Better Consumer Efficiency

Some consumers only need metadata.

For example, a service may only need to know that a `ShipmentCreated` event occurred for a specific shipment ID. It may not need the full shipment document.

With Claim Check, that consumer can process the lightweight message without retrieving the full payload.

---

### Improved Data Governance

Storing the full payload in S3 allows teams to use S3-native controls:

- encryption
- access policies
- bucket policies
- object lifecycle rules
- retention policies
- audit logging
- object tagging
- replication

This can be easier to govern than allowing full payloads to spread across many messaging systems.

---

## Trade-Offs

Claim Check is useful, but it introduces trade-offs.

### Additional Latency

Writing to and reading from S3 adds I/O latency.

For many event-driven workloads this is acceptable, but for very low-latency use cases it may be a concern.

---

### Additional Failure Modes

The system now depends on both the messaging service and the external storage service.

Possible failures include:

- failed S3 writes
- failed S3 reads
- missing objects
- permission errors
- object lifecycle expiration
- corrupted or incompatible payloads

A reliable implementation should handle these cases explicitly.

---

### Lifecycle Coordination

If claim-check objects expire too soon, consumers may receive references to objects that no longer exist.

Retention policies should be designed around the maximum expected processing, retry, and replay windows.

---

### Security Requirements

The claim-check message contains the location of the payload. Access to the payload still needs to be protected through storage permissions.

A claim-check reference should not be treated as authorization by itself. Consumers should only be able to redeem claim checks if they have permission to read the underlying object.

---

## Reliability Considerations

A good Claim Check implementation should address the following questions.

### What happens if storage fails?

If the full payload cannot be written to S3, the system should not emit the claim-check event as if everything succeeded. Otherwise, downstream consumers would receive a reference to an object that does not exist.

The store operation should be treated as part of the event pipeline’s reliability boundary.

---

### What happens if redemption fails?

If a consumer cannot redeem the claim check, the event should usually be retried or moved to a fault-handling path.

Reasons may include:

- temporary S3 outage
- missing permissions
- object not found
- malformed stored payload
- incompatible event schema

---

### How long should payloads be retained?

The retention period should be longer than the maximum time an event may remain useful.

Consider:

- queue retention
- stream retention
- replay windows
- dead-letter queue retention
- disaster recovery requirements
- audit requirements
- downstream processing delays

If messages can be replayed for 14 days, claim-check objects should generally live at least that long, and often longer.

---

### Should claim-check objects be immutable?

In most event-driven systems, claim-check payloads should be treated as immutable.

An event represents something that happened. Once stored, the payload should not be changed in place, because downstream consumers may rely on its original meaning.

---

## Best Practices

### Preserve Event Metadata

The lightweight claim-check event should preserve important metadata such as:

- event ID
- event type
- partition key
- timestamp
- tags or tracing information

This allows consumers, routers, logs, and monitoring tools to work without always redeeming the full payload.

---

### Use Deterministic and Traceable Keys

Storage keys should make objects easy to find during debugging.

Including region, date, hour, and event ID gives a good balance between uniqueness and operability.

---

### Encrypt Stored Payloads

For AWS S3, use encryption at rest. Depending on requirements, this may be:

- SSE-S3
- SSE-KMS
- client-side encryption

For sensitive domains, SSE-KMS with controlled key policies is often preferred.

---

### Apply Least-Privilege Access

Only producers should be able to write claim-check objects.

Only consumers that need the full payload should be able to read them.

Not every service that receives the lightweight event should automatically have access to the stored payload.

---

### Align Lifecycle Policies with Replay Needs

Do not expire objects before all consumers, retries, and replay processes are finished with them.

A common mistake is setting an S3 lifecycle rule that deletes claim-check objects too aggressively.

---

### Monitor Both Store and Redeem Operations

Useful metrics include:

- number of payloads stored
- S3 write failures
- S3 read failures
- redemption latency
- object-not-found errors
- payload size distribution
- claim-check usage by event type

These metrics help detect broken producers, delayed consumers, permissions problems, and lifecycle misconfiguration.

---

## Example Use Case

Imagine a shipment tracking system that publishes a `ShipmentUpdated` event.

The full event includes:

- shipment metadata
- package line items
- route history
- carrier responses
- customs information
- audit details

Most consumers only need to know that the shipment changed. A notification service might only need the shipment ID and event type. An analytics service, however, may need the full payload.

Using Claim Check:

1. The producer emits the full `ShipmentUpdated` event.
2. The event pipeline stores the complete payload in S3.
3. The message sent downstream contains the event metadata and S3 reference.
4. The notification service processes the lightweight event directly.
5. The analytics service redeems the claim check and loads the full event.

This avoids forcing every consumer to receive and process the entire shipment snapshot.

---

## Summary

The Claim Check pattern keeps event messages small by storing large payloads externally and passing a reference through the messaging system.

It is especially useful in serverless and event-driven architectures where message size, cost, retry behavior, and payload governance matter.

The pattern works by:

1. Storing the full event payload in durable external storage.
2. Replacing the message body with a lightweight reference.
3. Allowing consumers to redeem the reference when they need the original payload.

Used well, Claim Check improves scalability, reduces message overhead, and gives teams better control over large or sensitive event payloads.
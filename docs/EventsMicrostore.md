
# Events Microstore

## Table of Contents

- [What is an Events Microstore?](#what-is-an-events-microstore)
- [How does it work?](#how-does-it-work)
    - [Collection](#collection)
    - [Correlation](#correlation)
    - [Evaluation](#evaluation)

## What is an Events Microstore?

An Event Microstore is a small, purpose-built event store owned by a service or pipeline. It keeps a queryable copy of the events that the service needs in order to process, correlate, evaluate, or recover event-driven workflows.

Unlike a central event lake or long-term archive, an Event Microstore is optimized for operational use. It stores events with access patterns that support the local processing model, such as finding all events related to an entity, correlation key, partition key, or pipeline execution.

In this framework, the Event Microstore acts as a lightweight persistence layer for events flowing through serverless pipelines. It allows later pipeline stages to look up previously collected or correlated events without replaying an entire stream or depending on another service's internal database.

The Event Microstore is useful for:

- **Correlation** — grouping events that belong to the same business entity or workflow.
- **Evaluation** — querying related events to decide whether a new event should trigger additional processing.
- **Recovery** — retaining enough event history to support retries, reprocessing, and fault investigation.
- **Decoupling** — allowing pipelines to query event history without coupling directly to upstream services.
- **Auditability** — preserving the raw or transformed event payload alongside metadata used for routing and lookup.

The store is intentionally narrow in scope. It is not the system of record for business entities, and it is not a general-purpose reporting database. Its primary responsibility is to support reliable event processing close to the pipelines that consume and produce events.

# How does it work?

Given that an event arrives, coming from a Kinesis stream, SQS queue, or other event source.
```
{
    id: "ev-001",
    timestamp: 1775658343,
    partitionKey: "thing-005",
    tags: { 
        awsregion: "us-east-1", 
    }
    eventType: 'thing-created',
    entity: {
        id: "thing-005"
    }
}
```

## Collection

When the event is collected. It is stored in the events microstore implemented using dynamodb.

| Property       | Value         | Description                                                                     |
|----------------|---------------|---------------------------------------------------------------------------------|
| pk             | ev-001        | The partition key, stores the event id.                                         |
| sk             | EVENT         | The sort key stores the constant "EVENT" to indicate that this is an event.     |
| discriminator  | EVENT         | Used to distinguish between different types of events.                          |
| timestamp      | 1775658343    | The time the event was collected.                                               |
| awsregion      | us-east-1     | The aws region where the event was collected.                                   |
| sequenceNumber | ...0121231223 | A monotonically increasing number. Usually comes as part of the Kinesis Record. |
| ttl            | 1775690000    | The time to live for the event.                                                 |
| expire         | TRUE          | A boolean flag indicating whether the event should be expired.                  |
| suffix         | null          | Not used for EVENT events.                                                      |
| data           | thing-005     | A reference to the entity correlated to the event.                              |
| pipelineId     | col1          | The id of the pipeline that collected the event.                                |
| event          | {...}         | The raw event data.                                                             |

## Correlation
This event is consumed by the correlate pipeline generating new 'CORREL' events which is inserted in the events microstore.

| Property       | Value         | Description                                                                                         |
|----------------|---------------|-----------------------------------------------------------------------------------------------------|
| pk             | thing-005     | The partition key, stores the correlation key (in this case the entity id from the original event). |
| sk             | ev-001        | The sort key, stores the event id that was correlated to this partition.                            |
| discriminator  | CORREL        | Set to "CORREL" to indicate that this is a correlation record rather than an original event.        |
| timestamp      | 1775658343    | The time the original event was collected (inherited from the source event).                        |
| awsregion      | us-east-1     | The aws region where the event was collected.                                                       |
| sequenceNumber | ...0121231223 | A monotonically increasing number. Usually comes as part of the Kinesis Record.                     |
| ttl            | 1775690000    | The time to live for the correlation record.                                                        |
| expire         | TRUE          | A boolean flag indicating whether the correlation record should be expired.                         |
| suffix         | ""            | Can be used when events are correlated to other entities.                                           |
| data           | null          | Null for correlation records (no secondary reference needed).                                       |
| pipelineId     | corr1         | The id of the correlation pipeline that created this correlation record.                            |
| event          | {...}         | The raw event data (same as the original event).                                                    |

## Evaluation
Both, the collected event and the correlated event are processed by the evaluate pipeline.

After an event is inserted in the Events Microstore, the evaluate pipeline queries the events microstore.

In the case of a 'CORREL' event, it finds all correlated events that share the same partition key.
```
# pseudo sql
select * from events where pk = 'thing-005'  
```

In the case of 'EVENT' event, it finds all the events with the same data field. (It also can use an optional index)
```
# pseudo sql
select * from events where data = 'thing-005'
```




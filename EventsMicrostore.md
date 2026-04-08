
This document explains how the events microstore works.

given this event arrives.
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

When the event is collected. It is stored in the events microstore implemented using dynamodb.

| pk     | sk    | discriminator | timestamp  | awsregion | sequenceNumber | ttl        | expire | suffix | data      | pipelineId | event |
|--------|-------|---------------|------------|-----------|----------------|------------|--------|--------|-----------|------------|-------|
| ev-001 | EVENT | EVENT         | 1775658343 | us-east-1 | ...0121231223  | 1775690000 | TRUE   | null   | thing-005 | col1       | {...} |

This event is consumed by the correlate pipeline generating new 'CORREL' events which is inserted in the events microstore.

| pk        | sk     | discriminator | timestamp  | awsregion | sequenceNumber | ttl        | expire | suffix | data | pipelineId | event |
|-----------|--------|---------------|------------|-----------|----------------|------------|--------|--------|------|------------|-------|
| thing-005 | ev-001 | CORREL        | 1775658343 | us-east-1 | ...0121232334  | 1775690000 | TRUE   | ""     | null | corr1      | {...} |

Both, the collected event and the correlated event are processed by the evaluate pipeline.

The evaluate pipeline queries the events microstore.

In the case of a CORREL event, it finds all correlated events that share the same partition key.
```
# pseudo sql
select * from events where pk = 'thing-005'  
```

In the case of EVENT event, it finds all the events with the same data field. It can use an optional index.
```
# pseudo sql
select * from events where data = 'thing-005'
```




package org.myorg.urls;

import io.kopipes.core.EnvelopeEncryptionMetadata;
import io.kopipes.core.Event;
import io.kopipes.core.EventReference;
import io.kopipes.core.RawRecord;
import java.util.List;
import java.util.Map;

public record UrlCreatedEvent(
    String id,
    Long timestamp,
    String partitionKey,
    Map<String, String> tags,
    RawRecord raw,
    EnvelopeEncryptionMetadata eem,
    List<EventReference> triggers,
    Url entity
) implements UrlEvent {

    @Override public String toString() { return JacksonEventCodec.INSTANCE.encode(this); }

    @Override public String getId() { return id; }
    @Override public Long getTimestamp() { return timestamp; }
    @Override public String getPartitionKey() { return partitionKey; }
    @Override public Map<String, String> getTags() { return tags; }
    @Override public RawRecord getRaw() { return raw; }
    @Override public EnvelopeEncryptionMetadata getEem() { return eem; }
    @Override public List<EventReference> getTriggers() { return triggers; }
    @Override public Url entity() { return entity; }

    @Override
    public Event copyEvent(
        String id,
        Long timestamp,
        String partitionKey,
        Map<String, String> tags,
        RawRecord raw,
        EnvelopeEncryptionMetadata eem,
        List<EventReference> triggers
    ) {
        return new UrlCreatedEvent(id, timestamp, partitionKey, tags, raw, eem, triggers, entity);
    }
}

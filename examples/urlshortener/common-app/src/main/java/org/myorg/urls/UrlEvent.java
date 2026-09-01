package org.myorg.urls;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.huherto.awsLambdaStream.Event;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = UrlCreatedEvent.class, name = "UrlCreatedEvent"),
    @JsonSubTypes.Type(value = UrlDeletedEvent.class, name = "UrlDeletedEvent"),
    @JsonSubTypes.Type(value = UrlChangedEvent.class, name = "UrlChangedEvent"),
    @JsonSubTypes.Type(value = UrlAccessedEvent.class, name = "UrlAccessedEvent")
})
public sealed interface UrlEvent extends Event
    permits UrlCreatedEvent, UrlDeletedEvent, UrlChangedEvent, UrlAccessedEvent {

    Url entity();

    @Override
    default String eventType() {
        return this.getClass().getSimpleName();
    }
}

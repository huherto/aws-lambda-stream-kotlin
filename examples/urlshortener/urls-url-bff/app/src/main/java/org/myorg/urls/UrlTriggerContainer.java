package org.myorg.urls;

import io.kopipes.aws.connectors.DefaultEventBridgeClientFactory;
import io.kopipes.aws.flavors.CdcPipeline;
import io.kopipes.aws.from.DynamodbAdapter;
import io.kopipes.aws.from.RecordImage;
import io.kopipes.aws.from.RecordPair;
import io.kopipes.aws.sinks.EventBridgePublisher;
import io.kopipes.core.Event;
import io.kopipes.core.PipelineAssembler;
import io.kopipes.core.UnitOfWork;
import io.kopipes.core.sinks.EventPublisher;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class UrlTriggerContainer {
    public final EventPublisher eventPublisher;
    public final PipelineAssembler assembler;
    public final DynamodbAdapter dynamoDbAdapter;

    public UrlTriggerContainer(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
        CdcPipeline cdcPipeline = CdcPipeline
                .builder()
                .id("cdc1")
                .toEventJava(this::toEvent)
                .build();

        this.assembler = PipelineAssembler
                .builder()
                .addPipeline(cdcPipeline)
                .build();
        this.dynamoDbAdapter = new DynamodbAdapter();
    }

    public static UrlTriggerContainer build() {
        String busName = System.getenv("BUS_NAME");
        if (busName == null) busName = "urls-dev-bus";

        EventPublisher eventPublisher = new EventBridgePublisher(
            busName,
            "urls-url-bff",
            256 * 1024,
            10,
            8,
            null,
            true,
            new DefaultEventBridgeClientFactory(),
            null
        );
        return new UrlTriggerContainer(eventPublisher);
    }

    public Event toEvent(UnitOfWork uow) {
        if (uow.getEvent() == null || !(uow.getEvent().getRaw() instanceof RecordPair raw)) {
            return null;
        }

        RecordImage newImage = raw.getNew();
        if (newImage == null) {
            // Deleted case
            RecordImage oldImage = raw.getOld();
            if (oldImage != null) {
                Url url = recordImageToUrl(oldImage);
                return new UrlDeletedEvent(
                    UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    url.shortUrl(),
                    Map.of(),
                    null, null, List.of(),
                    url
                );
            }
            return null;
        }

        Url url = recordImageToUrl(newImage);

        if (raw.getOld() == null) {
            return new UrlCreatedEvent(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                url.shortUrl(),
                Map.of(),
                null, null, List.of(),
                url
            );
        } else {
            return new UrlChangedEvent(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                url.shortUrl(),
                Map.of(),
                null, null, List.of(),
                url
            );
        }
    }

    private static Url recordImageToUrl(RecordImage image) {
        String shortUrl = image.getS("pk");
        String longUrl = image.getS("longUrl");
        Long accessCount = image.getLong("accessCount");
        return new Url(shortUrl, longUrl, accessCount != null ? accessCount.intValue() : 0);
    }
}

package org.myorg.urls;

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient;
import io.github.huherto.awsLambdaStream.Event;
import io.github.huherto.awsLambdaStream.PipelineAssembler;
import io.github.huherto.awsLambdaStream.UnitOfWork;
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory;
import io.github.huherto.awsLambdaStream.connectors.DynamoDbClientFactory;
import io.github.huherto.awsLambdaStream.filters.EventFilter;
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter;
import io.github.huherto.awsLambdaStream.java.Handlers;
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory;
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore;
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl;

import java.util.List;

public class ControlTriggerContainer {
    public final PipelineAssembler assembler;
    public final DynamodbAdapter dynamoDbAdapter;
    public final UrlDao urlDao;

    public ControlTriggerContainer(EventsMicrostore eventsMicrostore, UrlDao urlDao) {
        this.urlDao = urlDao;
        this.assembler = Handlers.assemblerBuilder()
                .addPipeline(Handlers.correlatePipeline("correlate", eventsMicrostore, JacksonEventCodec.INSTANCE, uow -> uow.getEvent().getPartitionKey()))
                .addPipeline(Handlers.evaluatePipeline("evaluate", new EventPublisherInMemory(), eventsMicrostore, JacksonEventCodec.INSTANCE,
                        EventFilter.Any.INSTANCE,
                        this::shouldProcess,
                        this::processEvent
                ))
                .build();
        this.dynamoDbAdapter = new DynamodbAdapter();
    }

    private Boolean shouldProcess(UnitOfWork uow) {
        return uow.getEvent() instanceof UrlAccessedEvent;
    }

    private List<Event> processEvent(UnitOfWork uow) {
        UrlAccessedEvent event = (UrlAccessedEvent) uow.getEvent();
        urlDao.incrementAccessCount(event.entity().shortUrl());
        return List.of();
    }

    public static ControlTriggerContainer build() {
        String eventsTableName = System.getenv("EVENTS_TABLE_NAME");
        if (eventsTableName == null) eventsTableName = "urls-dev-events";
        String urlsTableName = System.getenv("URLS_TABLE_NAME");
        if (urlsTableName == null) urlsTableName = "urls-dev-table";

        DynamoDbClientFactory factory = new DefaultDynamoDbClientFactory();
        DynamoDbClient client = factory.getClient("urls-control-service");

        EventsMicrostore eventsMicrostore = new EventsMicrostoreImpl(factory);
        UrlDao urlDao = new UrlDao(client, urlsTableName);

        return new ControlTriggerContainer(eventsMicrostore, urlDao);
    }
}

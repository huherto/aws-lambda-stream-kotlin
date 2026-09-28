package org.myorg.urls;

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient;
import io.kopipes.aws.Event;
import io.kopipes.aws.GlobalRegistry;
import io.kopipes.aws.PipelineAssembler;
import io.kopipes.aws.UnitOfWork;
import io.kopipes.aws.connectors.DefaultDynamoDbClientFactory;
import io.kopipes.aws.connectors.DynamoDbClientFactory;
import io.kopipes.aws.flavors.CorrelatePipeline;
import io.kopipes.aws.flavors.EvaluatePipeline;
import io.kopipes.aws.from.DynamodbAdapter;
import io.kopipes.aws.sinks.EventsMicrostore;
import io.kopipes.aws.sinks.EventsMicrostoreImpl;

import java.util.List;
import java.util.Objects;

public class ControlTriggerContainer {
    public final PipelineAssembler assembler;
    public final DynamodbAdapter dynamoDbAdapter;
    public final UrlDao urlDao;

    public ControlTriggerContainer(EventsMicrostore eventsMicrostore, UrlDao urlDao) {
        this.urlDao = urlDao;
        CorrelatePipeline correlate = CorrelatePipeline
                .builder()
                .id("correlate")
                .eventsMicrostore(eventsMicrostore)
                .eventCodec(JacksonEventCodec.INSTANCE)
                .correlationKeySupplierJava(this::correlationKey)
                .build();

        EvaluatePipeline evaluate = EvaluatePipeline
                .builder()
                .id("evaluate")
                .eventsMicrostore(eventsMicrostore)
                .eventCodec(JacksonEventCodec.INSTANCE)
                .expression(this::shouldProcess)
                .emit(this::processEvent)

                .build();


        this.assembler = PipelineAssembler.builder()
                .addPipeline(correlate)
                .addPipeline(evaluate)
                .build();
        this.dynamoDbAdapter = new DynamodbAdapter();
    }

    private String correlationKey(UnitOfWork uow) {
        Event event = Objects.requireNonNull(uow.getEvent(), "event is required");
        return Objects.requireNonNull(event.getPartitionKey(), "event.partitionKey is required");
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

        String urlsTableName = GlobalRegistry.envConfig().entityTableName();
        DynamoDbClientFactory factory = new DefaultDynamoDbClientFactory();
        DynamoDbClient client = factory.getClient("urls-control-service");

        EventsMicrostore eventsMicrostore = new EventsMicrostoreImpl(factory);
        UrlDao urlDao = new UrlDao(client, urlsTableName);

        return new ControlTriggerContainer(eventsMicrostore, urlDao);
    }
}

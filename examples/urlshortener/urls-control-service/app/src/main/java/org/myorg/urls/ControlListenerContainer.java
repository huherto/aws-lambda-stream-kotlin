package org.myorg.urls;

import io.kopipes.aws.GlobalRegistry;
import io.kopipes.aws.PipelineAssembler;
import io.kopipes.aws.connectors.DefaultDynamoDbClientFactory;
import io.kopipes.aws.connectors.DynamoDbClientFactory;
import io.kopipes.aws.flavors.CollectPipeline;
import io.kopipes.aws.from.KinesisAdapter;
import io.kopipes.aws.sinks.EventsMicrostore;
import io.kopipes.aws.sinks.EventsMicrostoreImpl;

public class ControlListenerContainer {
    public final EventsMicrostore eventsMicrostore;
    public final PipelineAssembler assembler;
    public final KinesisAdapter kinesisAdapter;

    public ControlListenerContainer(EventsMicrostore eventsMicrostore) {
        this.eventsMicrostore = eventsMicrostore;
        CollectPipeline collectPipeline = CollectPipeline
                .builder()
                .id("coll1")
                .eventsMicrostore(eventsMicrostore)
                .build();
        this.assembler = PipelineAssembler
                .builder()
                .addPipeline(collectPipeline)
                .build();
        this.kinesisAdapter = new KinesisAdapter(JacksonEventCodec.INSTANCE);
    }

    public static ControlListenerContainer build() {
        GlobalRegistry.envConfig().eventTableName();

        String tableName = System.getenv("EVENTS_TABLE_NAME");
        if (tableName == null) tableName = "urls-dev-events";

        DynamoDbClientFactory factory = new DefaultDynamoDbClientFactory();
        EventsMicrostore eventsMicrostore = new EventsMicrostoreImpl(factory);

        return new ControlListenerContainer(eventsMicrostore);
    }
}

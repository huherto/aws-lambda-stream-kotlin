package org.myorg.urls;

import io.github.huherto.awsLambdaStream.GlobalRegistry;
import io.github.huherto.awsLambdaStream.PipelineAssembler;
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory;
import io.github.huherto.awsLambdaStream.connectors.DynamoDbClientFactory;
import io.github.huherto.awsLambdaStream.flavors.CollectPipeline;
import io.github.huherto.awsLambdaStream.from.KinesisAdapter;
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore;
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl;

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

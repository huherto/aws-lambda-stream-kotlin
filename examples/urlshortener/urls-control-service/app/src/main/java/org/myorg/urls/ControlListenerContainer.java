package org.myorg.urls;

import io.github.huherto.awsLambdaStream.PipelineAssembler;
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory;
import io.github.huherto.awsLambdaStream.connectors.DynamoDbClientFactory;
import io.github.huherto.awsLambdaStream.from.KinesisAdapter;
import io.github.huherto.awsLambdaStream.java.Handlers;
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore;
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl;

public class ControlListenerContainer {
    public final EventsMicrostore eventsMicrostore;
    public final PipelineAssembler assembler;
    public final KinesisAdapter kinesisAdapter;

    public ControlListenerContainer(EventsMicrostore eventsMicrostore) {
        this.eventsMicrostore = eventsMicrostore;
        this.assembler = Handlers.assemblerBuilder()
                .addPipeline(Handlers.collectPipeline("listener1", eventsMicrostore))
                .build();
        this.kinesisAdapter = new KinesisAdapter(JacksonEventCodec.INSTANCE);
    }

    public static ControlListenerContainer build() {
        String tableName = System.getenv("EVENTS_TABLE_NAME");
        if (tableName == null) tableName = "urls-dev-events";

        DynamoDbClientFactory factory = new DefaultDynamoDbClientFactory();
        EventsMicrostore eventsMicrostore = new EventsMicrostoreImpl(factory);

        return new ControlListenerContainer(eventsMicrostore);
    }
}

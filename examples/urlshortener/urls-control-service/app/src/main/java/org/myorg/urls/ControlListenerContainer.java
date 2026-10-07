package org.myorg.urls;

import io.kopipes.aws.connectors.DefaultDynamoDbClientFactory;
import io.kopipes.aws.connectors.DynamoDbClientFactory;
import io.kopipes.aws.from.KinesisAdapter;
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore;
import io.kopipes.core.PipelineAssembler;
import io.kopipes.core.flavors.CollectPipeline;
import io.kopipes.core.sinks.EventsMicrostore;

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
        DynamoDbClientFactory factory = new DefaultDynamoDbClientFactory();
        EventsMicrostore eventsMicrostore = new DynamoDbEventsMicrostore(factory);

        return new ControlListenerContainer(eventsMicrostore);
    }
}

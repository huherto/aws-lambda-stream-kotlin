package org.myorg.sut

import io.kopipes.aws.connectors.DefaultDynamoDbClientFactory
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.aws.sinks.EventsMicrostoreImpl
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.filters.EventFilter
import io.kopipes.core.flavors.CollectPipeline
import io.kopipes.core.sinks.EventsMicrostore

class ListenerContainer(
    val eventsMicrostore: EventsMicrostore,
    val collectPipeline: CollectPipeline = CollectPipeline.builder()
        .id("collect-pipeline")
        .eventFilter(EventFilter.Any)
        .eventsMicrostore(eventsMicrostore)
        .build(),
    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .addPipeline(collectPipeline)
        .build(),
    val kinesisAdapter: KinesisAdapter = KinesisAdapter(
        eventCodec = TrackedUnitEventCodec,
    )
) {
    companion object {
        fun build(): ListenerContainer {
            val eventsMicrostore = EventsMicrostoreImpl(
                DefaultDynamoDbClientFactory(),
            )
            return ListenerContainer(eventsMicrostore = eventsMicrostore)
        }
    }
}

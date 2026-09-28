package org.myorg.sut

import io.kopipes.aws.PipelineAssembler
import io.kopipes.aws.connectors.DefaultDynamoDbClientFactory
import io.kopipes.aws.filters.EventFilters
import io.kopipes.aws.flavors.CollectPipeline
import io.kopipes.aws.flavors.Pipeline
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.aws.sinks.EventsMicrostore
import io.kopipes.aws.sinks.EventsMicrostoreImpl

class ListenerContainer(
    val eventsMicrostore: EventsMicrostore,
) {

    companion object {
        fun build() : ListenerContainer {
            val dynamoDbClientFactory = DynamoDBClientWrapperFactory(DefaultDynamoDbClientFactory())
            val eventsMicrostore = EventsMicrostoreImpl(
                dynamoDbClientFactory = dynamoDbClientFactory,
            )
            return ListenerContainer(
                eventsMicrostore = eventsMicrostore,
            )
        }
    }

    val kinesisAdapter: KinesisAdapter by lazy {
        KinesisAdapter(eventCodec = TrackedUnitEventCodec)
    }

    private val collectPipeline: Pipeline by lazy {
        CollectPipeline
            .builder()
            .id("coll1")
            .eventsMicrostore(eventsMicrostore)
            .eventFilter(EventFilters.classes(TrackedUnitEvent::class))
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(collectPipeline)
            .build()
    }

}
package org.myorg.sut

import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.CollectPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl

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
        CollectPipeline(
            pipelineId = "coll1",
            eventsMicrostore = eventsMicrostore,
            eventFilter = EventFilters.classes(TrackedUnitEvent::class)
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(collectPipeline)
            .build()
    }

}
package org.myorg.sut

import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.flavors.CdcPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.sinks.EventPublisher

class TriggerContainer(
    val eventPublisher: EventPublisher,
) {

    companion object {

        fun build() : TriggerContainer {
            return TriggerContainer(
                eventPublisher = GlobalRegistry.eventPublisher(),
            )
        }
    }

    private val cdcPipeline: Pipeline by lazy {
        CdcPipeline(
            id = "cdc1",
            eventPublisher = eventPublisher,
            toEvent = ::toEvent,
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(cdcPipeline)
            .build()
    }

    val dynamoDbAdapter : DynamodbAdapter by lazy {
        DynamodbAdapter()
    }

}
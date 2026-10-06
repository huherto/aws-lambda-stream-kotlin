package org.myorg.sut

import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.flavors.CdcPipeline
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.flavors.Pipeline
import io.kopipes.core.sinks.EventPublisher

class TriggerContainer(
    val eventPublisher: EventPublisher,
) {

    companion object {

        fun build() : TriggerContainer {
            return TriggerContainer(
                eventPublisher = AwsGlobalRegistry.eventPublisher(),
            )
        }
    }

    private val cdcPipeline: Pipeline by lazy {
        CdcPipeline.builder()
            .id("cdc1")
            .eventPublisher(eventPublisher)
            .toEvent(::toEvent)
            .build()
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

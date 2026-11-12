package org.myorg.sut

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.flavors.CdcPipeline
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.flavors.Pipeline
import io.kopipes.core.sinks.EventPublisher

class TriggerContainer(
    val context: AwsKopipesContext,
    val eventPublisher: EventPublisher = context.eventPublisher,
) {

    companion object {

        fun build() : TriggerContainer {
            return TriggerContainer(AwsKopipesContext())
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
            .context(context)
            .addPipeline(cdcPipeline)
            .build()
    }

    val dynamoDbAdapter : DynamodbAdapter by lazy {
        DynamodbAdapter(context)
    }

}

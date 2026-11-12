package org.myorg.sut

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.flavors.CdcPipeline
import io.kopipes.aws.from.S3Adapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.filters.EventFilter
import io.kopipes.core.flavors.Pipeline
import io.kopipes.core.sinks.EventPublisher

class S3TriggerContainer(
    val context: AwsKopipesContext,
    val eventPublisher: EventPublisher = context.eventPublisher,
) {

    companion object {

        fun build() : S3TriggerContainer {
            return S3TriggerContainer(AwsKopipesContext())
        }
    }

    private val cdcPipeline: Pipeline by lazy {
        CdcPipeline.builder()
            .id("cdc")
            .eventPublisher(eventPublisher)
            .eventFilter(EventFilter.Any)
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .context(context)
            .addPipeline(cdcPipeline)
            .build()
    }

    val s3Adapter : S3Adapter by lazy {
        S3Adapter(context, TracerEventCodec)
    }

}

package org.myorg.sut

import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.flavors.CdcPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.S3Adapter
import io.github.huherto.awsLambdaStream.sinks.EventPublisher

class S3TriggerContainer(
    val eventPublisher: EventPublisher,
) {

    companion object {

        fun build() : S3TriggerContainer {

            return S3TriggerContainer(
                eventPublisher = GlobalRegistry.eventPublisher(),
            )
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
            .addPipeline(cdcPipeline)
            .build()
    }

    val s3Adapter : S3Adapter by lazy {
        S3Adapter(
            eventCodec = TracerEventCodec,
        )
    }

}
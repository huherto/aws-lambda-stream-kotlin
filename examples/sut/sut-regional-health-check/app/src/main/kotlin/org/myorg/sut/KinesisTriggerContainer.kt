package org.myorg.sut

import io.kopipes.aws.flavors.UpdatePipeline
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.JsonEventCodec
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.flavors.Pipeline

class KinesisTriggerContainer () {

    companion object {

        fun build() : KinesisTriggerContainer {
            return KinesisTriggerContainer()
        }
    }

    private  val updatePipeline: Pipeline by lazy {
        UpdatePipeline.builder()
            .id("update")
            .eventCodec(JsonEventCodec) // NOOP, Should not be required.
            .toUpdateRequest(::toUpdateRequest)
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(updatePipeline)
            .build()
    }

    val kinesisAdapter: KinesisAdapter by lazy {
        KinesisAdapter(eventCodec = TracerEventCodec)
    }

}

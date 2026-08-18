package org.myorg.sut

import io.github.huherto.awsLambdaStream.JsonEventCodec
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.flavors.UpdatePipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter

class KinesisTriggerContainer () {

    companion object {

        fun build() : KinesisTriggerContainer {
            return KinesisTriggerContainer()
        }
    }

    private  val updatePipeline: Pipeline by lazy {
        UpdatePipeline(
            id = "update",
            eventCodec = JsonEventCodec, // NOOP, Should not be required.
            toUpdateRequest = ::toUpdateRequest
        )
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
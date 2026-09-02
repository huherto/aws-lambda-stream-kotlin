package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.MaterializePipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter

class ListenerContainer() {

    companion object {
        fun build() : ListenerContainer {
            return ListenerContainer()
        }
    }

    val kinesisAdapter: KinesisAdapter by lazy {
        KinesisAdapter(
            eventCodec = TrackedUnitEventCodec,
        )
    }

    suspend fun toUpdateRequest(uow: UnitOfWork): UpdateItemRequest? {

        val event = uow.event as? TrackedUnitEvent ?: return null
        val entity = event.entity ?: return null
        // No rules implemented yet.
        return null
    }

    private val materializePipeline: Pipeline by lazy {
        MaterializePipeline.builder()
            .id("m1")
            .eventFilter(EventFilters.classes(TrackedUnitEvent::class))
            .toUpdateRequest(::toUpdateRequest)
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(materializePipeline)
            .build()
    }

}
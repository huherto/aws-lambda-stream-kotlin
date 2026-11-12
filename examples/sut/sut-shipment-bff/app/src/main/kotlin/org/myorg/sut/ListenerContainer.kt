package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.flavors.MaterializePipeline
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.flavors.Pipeline

class ListenerContainer(val context: AwsKopipesContext) {

    companion object {
        fun build() : ListenerContainer {
            return ListenerContainer(AwsKopipesContext())
        }
    }

    val kinesisAdapter: KinesisAdapter by lazy {
        KinesisAdapter(context, TrackedUnitEventCodec)
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
            .context(context)
            .eventFilter(EventFilters.classes(TrackedUnitEvent::class))
            .toUpdateRequest(::toUpdateRequest)
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .context(context)
            .addPipeline(materializePipeline)
            .build()
    }

}

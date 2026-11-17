package org.myorg.sut

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.aws.sinks.DynamoDbEntityStore
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.flavors.MaterializePipeline
import io.kopipes.core.flavors.Pipeline
import io.kopipes.core.sinks.EntityStore

class ListenerContainer(val context: AwsKopipesContext) {

    companion object {
        fun build() : ListenerContainer {
            return ListenerContainer(AwsKopipesContext())
        }
    }

    val kinesisAdapter: KinesisAdapter by lazy {
        KinesisAdapter(context, TrackedUnitEventCodec)
    }

    suspend fun toUpdate(uow: UnitOfWork): EntityStore.Update? {

        val event = uow.event as? TrackedUnitEvent ?: return null
        val entity = event.entity ?: return null
        // No rules implemented yet.
        return null
    }

    private val materializePipeline: Pipeline by lazy {
        MaterializePipeline.builder()
            .id("m1")
            .entityStore(DynamoDbEntityStore(context))
            .eventFilter(EventFilters.classes(TrackedUnitEvent::class))
            .toUpdate(::toUpdate)
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

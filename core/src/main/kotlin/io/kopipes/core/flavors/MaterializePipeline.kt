package io.kopipes.core.flavors

import io.kopipes.core.PipelineBuilder
import io.kopipes.core.TableChangeEvent
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.EventFilter
import io.kopipes.core.filters.filterEvents
import io.kopipes.core.sinks.EntityStore
import io.kopipes.core.sinks.withEntityUpdate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

/**
 * Pipeline flavor that keeps a read model up to date: [toUpdate] turns each event into an update of one entity
 * (or `null` to skip it), and the [entityStore] applies it. For an event that affects many entities, use
 * [UpdatePipeline].
 *
 * Items this pipeline wrote itself are skipped ([sourceIsSelf]), so a pipeline fed by its own table's stream
 * doesn't react to its own writes.
 */
class MaterializePipeline(
    id: String,
    val entityStore: EntityStore,
    val toUpdate: suspend (UnitOfWork) -> EntityStore.Update?,
    val eventFilter: EventFilter = EventFilter.Any,
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val compact: (Flow<UnitOfWork>) -> Flow<UnitOfWork> = { it },
    val isSelf: ((UnitOfWork) -> Boolean)? = null,
) : Pipeline(id) {

    /** By default, a table item whose `pipelineId` is this pipeline's id. */
    fun sourceIsSelf(uow: UnitOfWork): Boolean {
        if (isSelf != null) return isSelf.invoke(uow)
        val event = uow.event as? TableChangeEvent ?: return false
        return event.getS("pipelineId") == id
    }

    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        logger.info { "MaterializePipeline.connect: id=$id" }

        with(fm) {
            val updates = fromFlow
                .filterNotFaulty { uow -> !sourceIsSelf(uow) }
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .let { flow -> compact(flow) }
                .mapNotFaulty { uow -> uow.withEntityUpdate(toUpdate(uow)) }

            return entityStore.update(updates)
                .onEach { uow -> printEndPipeline(uow) }
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<MaterializePipeline, Builder>() {
        private var entityStore: EntityStore? = null
        private var toUpdate: (suspend (UnitOfWork) -> EntityStore.Update?)? = null
        private var eventFilter: EventFilter = EventFilter.Any
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var compact: (Flow<UnitOfWork>) -> Flow<UnitOfWork> = { it }
        private var isSelf: ((UnitOfWork) -> Boolean)? = null

        fun entityStore(entityStore: EntityStore) = apply { this.entityStore = entityStore }
        fun toUpdate(toUpdate: suspend (UnitOfWork) -> EntityStore.Update?) = apply { this.toUpdate = toUpdate }
        fun toUpdate(toUpdate: java.util.function.Function<UnitOfWork, EntityStore.Update?>) = apply { this.toUpdate = { uow -> toUpdate.apply(uow) } }
        fun eventFilter(eventFilter: EventFilter) = apply { this.eventFilter = eventFilter }
        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun onContentType(onContentType: java.util.function.Predicate<UnitOfWork>) = apply { this.onContentType = { uow -> onContentType.test(uow) } }
        fun compact(compact: (Flow<UnitOfWork>) -> Flow<UnitOfWork>) = apply { this.compact = compact }
        fun compact(compact: java.util.function.Function<Flow<UnitOfWork>, Flow<UnitOfWork>>) = apply { this.compact = { flow -> compact.apply(flow) } }
        /** Which units this pipeline wrote itself, and skips; see [MaterializePipeline.sourceIsSelf]. */
        fun isSelf(predicate: (UnitOfWork) -> Boolean) = apply { this.isSelf = predicate }

        override fun build(): MaterializePipeline {
            return MaterializePipeline(
                id = id ?: throw IllegalArgumentException("id is required"),
                entityStore = entityStore ?: throw IllegalArgumentException("entityStore is required"),
                toUpdate = toUpdate ?: throw IllegalArgumentException("toUpdate is required"),
                eventFilter = eventFilter,
                onContentType = onContentType,
                compact = compact,
                isSelf = isSelf,
            )
        }
    }
}

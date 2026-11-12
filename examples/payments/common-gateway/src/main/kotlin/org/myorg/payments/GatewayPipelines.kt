package org.myorg.payments

import io.kopipes.core.Event
import io.kopipes.core.PipelineBuilder
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.EventFilter
import io.kopipes.core.filters.filterEvents
import io.kopipes.core.flavors.Pipeline
import io.kopipes.core.sinks.EventPublisher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.onEach

/**
 * Egress: acts on an event (calls the external system, or in simulation mode schedules its response) and
 * publishes nothing. An egress gateway never says "it worked": it acts, then hears back through its ingress.
 */
class ActionPipeline(
    id: String,
    private val eventFilter: EventFilter,
    private val onContentType: (UnitOfWork) -> Boolean,
    private val action: suspend (UnitOfWork) -> Unit,
) : Pipeline(id) {

    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        logger.info { "ActionPipeline.connect: id=$id" }
        with(fm) {
            return fromFlow
                .filterEvents(fm, eventFilter)
                .onEach { printStartPipeline(it) }
                .filterNotFaulty { onContentType(it) }
                .mapNotFaulty { uow -> action(uow); uow }
                .onEach { printEndPipeline(it) }
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<ActionPipeline, Builder>() {
        private var eventFilter: EventFilter = EventFilter.Any
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var action: (suspend (UnitOfWork) -> Unit)? = null

        fun eventFilter(eventFilter: EventFilter) = apply { this.eventFilter = eventFilter }
        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun action(action: suspend (UnitOfWork) -> Unit) = apply { this.action = action }

        override fun build() = ActionPipeline(
            id = id ?: error("id is required"),
            eventFilter = eventFilter,
            onContentType = onContentType,
            action = action ?: error("action is required"),
        )
    }
}

/**
 * Ingress: turns what the outside system said (a ledger row, a webhook, a file line) into internal events and
 * publishes them. Unlike the other flavors, the unit of work carries a raw record, not an event yet.
 */
class TranslatePipeline(
    id: String,
    private val toEvents: suspend (UnitOfWork) -> List<Event>,
    private val eventPublisher: EventPublisher,
) : Pipeline(id) {

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        logger.info { "TranslatePipeline.connect: id=$id" }
        val translated = fromFlow.flatMapConcat { uow ->
            fm.faulty(uow) { toEvents(it) }?.map { uow.copy(event = it) }?.asFlow() ?: emptyFlow()
        }
        return eventPublisher.publish(translated).onEach { with(fm) { printEndPipeline(it) } }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<TranslatePipeline, Builder>() {
        private var toEvents: (suspend (UnitOfWork) -> List<Event>)? = null
        private var eventPublisher: EventPublisher? = null

        fun toEvents(toEvents: suspend (UnitOfWork) -> List<Event>) = apply { this.toEvents = toEvents }
        fun eventPublisher(eventPublisher: EventPublisher) = apply { this.eventPublisher = eventPublisher }

        override fun build() = TranslatePipeline(
            id = id ?: error("id is required"),
            toEvents = toEvents ?: error("toEvents is required"),
            eventPublisher = eventPublisher ?: error("eventPublisher is required"),
        )
    }
}

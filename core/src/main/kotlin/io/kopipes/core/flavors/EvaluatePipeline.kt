package io.kopipes.core.flavors

import io.kopipes.core.*
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.EventFilter
import io.kopipes.core.filters.filterEvents
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventsMicrostore
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*

/** Pipeline flavor that evaluates collected and correlated events and publishes higher-order events. */
class EvaluatePipeline(
    id: String,
    val eventPublisher: EventPublisher? = null,
    val eventsMicrostore: EventsMicrostore,
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val eventFilter: EventFilter = EventFilter.Any,
    val correlationKeySuffix: String = "",
    val index: String? = null,
    val bufferCapacity: Int = Channel.BUFFERED,
    val eventCodec: EventCodec? = null,
    val expression: ((UnitOfWork) -> Boolean)? = null,
    val emit: ((UnitOfWork) -> List<Event>)? = null,
    val isEvaluateEvent: ((UnitOfWork) -> Boolean)? = null,
    val normalizer: ((UnitOfWork) -> UnitOfWork)? = null,
) : Pipeline(id) {

    fun forEvents(uow: UnitOfWork): Boolean {
        if (isEvaluateEvent != null) return isEvaluateEvent.invoke(uow)
        return uow.event != null
    }

    fun normalize(uow: UnitOfWork): UnitOfWork {
        if (normalizer != null) return normalizer.invoke(uow)
        return uow
    }

    fun onCorrelationKeySuffix(uow: UnitOfWork): Boolean {
        val uowSuffix = uow.meta?.get("suffix") ?: ""
        return correlationKeySuffix == uowSuffix
    }

    fun Flow<UnitOfWork>.queryCorrelated(): Flow<UnitOfWork> {
        // queryByPK already has a fault manager.
        return eventsMicrostore.queryByPk(this)
    }

    fun Flow<UnitOfWork>.complex(fm: FaultManager): Flow<UnitOfWork> {
        return if (expression == null) {
            this.map { uow ->
                uow.copy(
                    triggers = listOfNotNull(uow.event)
                )
            }
        } else {
            this
                .filter { uow -> fm.faulty(uow) { onCorrelationKeySuffix(uow) } == true }
                .queryCorrelated()
                .mapNotNull { uow ->
                    val result = fm.faulty(uow) { expression(uow) }
                    if (result == true) {
                        uow.copy(triggers = listOfNotNull(uow.event))
                    } else {
                        null
                    }
                }
        }
    }

    fun toHigherOrderEvents(uow: UnitOfWork): List<UnitOfWork> {
        val emit = this.emit ?: return emptyList()
        val triggeringEvent = uow.event ?: return emptyList()

        val eventId = uow.meta?.get("eventId")
        val partitionKey = uow.meta?.get("partitionKey")
        val trigger = uow.triggers?.lastOrNull()
        val aggregatedTags = aggregateTags(uow)
        val mappedTriggers = uow.triggers?.map {
            EventReference(it.id, it.eventType(), it.timestamp)
        }

        val resultEvents: List<Event> = emit(uow)

        return resultEvents.map { e ->
            val event = e.copyEvent(
                id = eventId,
                timestamp = trigger?.timestamp,
                partitionKey = partitionKey,
                tags = aggregatedTags,
                triggers = mappedTriggers,
                raw = e.raw ?: triggeringEvent.raw,
                eem = e.eem ?: triggeringEvent.eem
            )
            uow.copy(event = event)
        }
    }

    private fun aggregateTags(uow: UnitOfWork): MutableMap<String, String>? {
        // reduce + merge + omit(['region', 'source'])
        val aggregatedTags = uow.triggers
            ?.mapNotNull { it.tags }
            ?.fold(mutableMapOf<String, String>()) { acc, currentTags ->
                acc.apply { putAll(currentTags) }
            }?.apply {
                remove("region")
                remove("source")
            }
        return aggregatedTags
    }

    fun Flow<UnitOfWork>.publish(): Flow<UnitOfWork> {
        return eventPublisher?.publish(this) ?: this
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        logger.info { "Evaluate.connect: id=$id" }
        with(fm) {
            val flow = fromFlow
                .filterNotFaulty { uow -> forEvents(uow) }
                .mapNotFaulty { uow -> normalize(uow) }
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .complex(fm)
                .flatMapMerge { uow ->
                    faulty(uow) { toHigherOrderEvents(uow) }?.asFlow() ?: emptyFlow()
                }
                .buffer(bufferCapacity)
                .publish()
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<EvaluatePipeline, Builder>() {
        private var eventPublisher: EventPublisher? = null
        private var eventsMicrostore: EventsMicrostore? = null
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var eventFilter: EventFilter = EventFilter.Any
        private var correlationKeySuffix: String = ""
        private var index: String? = null
        private var bufferCapacity: Int = Channel.BUFFERED
        private var eventCodec: EventCodec? = null
        private var expression: ((UnitOfWork) -> Boolean)? = null
        private var emit: ((UnitOfWork) -> List<Event>)? = null
        private var isEvaluateEvent: ((UnitOfWork) -> Boolean)? = null
        private var normalizer: ((UnitOfWork) -> UnitOfWork)? = null

        fun eventPublisher(eventPublisher: EventPublisher) = apply { this.eventPublisher = eventPublisher }
        fun eventsMicrostore(eventsMicrostore: EventsMicrostore) = apply { this.eventsMicrostore = eventsMicrostore }
        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun onContentType(onContentType: java.util.function.Predicate<UnitOfWork>) = apply { this.onContentType = { uow -> onContentType.test(uow) } }
        fun eventFilter(eventFilter: EventFilter) = apply { this.eventFilter = eventFilter }
        fun correlationKeySuffix(correlationKeySuffix: String) = apply { this.correlationKeySuffix = correlationKeySuffix }
        fun index(index: String?) = apply { this.index = index }
        fun bufferCapacity(bufferCapacity: Int) = apply { this.bufferCapacity = bufferCapacity }
        fun eventCodec(eventCodec: EventCodec) = apply { this.eventCodec = eventCodec }
        fun expression(expression: (UnitOfWork) -> Boolean) = apply { this.expression = expression }
        fun expressionJava(expression: java.util.function.Predicate<UnitOfWork>) = apply { this.expression = { uow -> expression.test(uow) } }
        fun emit(emit: (UnitOfWork) -> List<Event>) = apply { this.emit = emit }
        fun emitJava(emit: java.util.function.Function<UnitOfWork, List<Event>>) = apply { this.emit = { uow -> emit.apply(uow) } }
        fun isEvaluateEvent(isEvaluateEvent: (UnitOfWork) -> Boolean) = apply { this.isEvaluateEvent = isEvaluateEvent }
        fun normalizer(normalizer: (UnitOfWork) -> UnitOfWork) = apply { this.normalizer = normalizer }

        override fun build(): EvaluatePipeline {
            val pipelineId = id ?: error("id is required")
            val microstore = eventsMicrostore ?: error("eventsMicrostore is required")
            return EvaluatePipeline(
                id = pipelineId,
                eventPublisher = eventPublisher,
                eventsMicrostore = microstore,
                onContentType = onContentType,
                eventFilter = eventFilter,
                correlationKeySuffix = correlationKeySuffix,
                index = index,
                bufferCapacity = bufferCapacity,
                eventCodec = eventCodec,
                expression = expression,
                emit = emit,
                isEvaluateEvent = isEvaluateEvent,
                normalizer = normalizer,
            )
        }
    }
}

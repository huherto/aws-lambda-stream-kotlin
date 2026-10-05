package io.kopipes.core.flavors

import io.kopipes.core.EventCodec
import io.kopipes.core.GlobalRegistry.envConfig
import io.kopipes.core.PipelineBuilder
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.EventFilter
import io.kopipes.core.filters.filterEvents
import io.kopipes.core.sinks.EventsMicrostore
import io.kopipes.core.sinks.withSaveOptions
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach

/** Pipeline flavor for correlating stream events. */
class CorrelatePipeline(
    id: String,
    val eventsMicrostore: EventsMicrostore,
    val correlationKeySupplier: (UnitOfWork) -> String?,
    val onContentType: (UnitOfWork) -> Boolean,
    val eventFilter: EventFilter,
    val ttlDays: Int?,
    val expire: Boolean,
    val eventCodec: EventCodec? = null,
    val isCollectedEvent: ((UnitOfWork) -> Boolean)? = null,
) : Pipeline(id) {

    fun forCollectedEvents(uow: UnitOfWork): Boolean {
        if (isCollectedEvent != null) return isCollectedEvent.invoke(uow)
        return uow.event != null
    }

    fun normalize(uow: UnitOfWork): UnitOfWork {
        return uow
    }

    fun Flow<UnitOfWork>.save(fm: FaultManager): Flow<UnitOfWork> {
        val flow = this.map { uow ->
            val key = correlationKeySupplier(uow)
            val event = uow.event
            val saveOptions = EventsMicrostore.SaveOptions(
                pk = key ?: event?.id ?: "unknown",
                sk = "CORREL",
                discriminator = "CORREL",
                timeStamp = event?.timestamp ?: System.currentTimeMillis(),
                includeRaw = false,
                expire = expire,
                ttl = ttlDays?.toLong() ?: envConfig().ttl()?.toLong() ?: 33L,
                suffix = "",
                data = event?.id,
                pipelineId = id
            )
            uow.withSaveOptions(saveOptions)
        }
        return eventsMicrostore.save(flow)
    }

    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return fromFlow
            .filterEvents(fm, eventFilter)
            .onEach { printStartPipeline(it) }
            .save(fm)
            .onEach { printEndPipeline(it) }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<CorrelatePipeline, Builder>() {
        private var eventsMicrostore: EventsMicrostore? = null
        private var correlationKeySupplier: (UnitOfWork) -> String? = { it.event?.partitionKey }
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var eventFilter: EventFilter = EventFilter.Any
        private var ttlDays: Int? = null
        private var expire: Boolean = false
        private var eventCodec: EventCodec? = null
        private var isCollectedEvent: ((UnitOfWork) -> Boolean)? = null

        fun eventsMicrostore(microstore: EventsMicrostore) = apply { this.eventsMicrostore = microstore }
        fun correlationKeySupplier(supplier: (UnitOfWork) -> String?) = apply { this.correlationKeySupplier = supplier }
        fun correlationKey(key: (UnitOfWork) -> String?) = apply { this.correlationKeySupplier = key }
        fun onContentType(predicate: (UnitOfWork) -> Boolean) = apply { this.onContentType = predicate }
        fun eventFilter(filter: EventFilter) = apply { this.eventFilter = filter }
        fun ttlDays(days: Int?) = apply { this.ttlDays = days }
        fun expire(expire: Boolean) = apply { this.expire = expire }
        fun eventCodec(codec: EventCodec) = apply { this.eventCodec = codec }
        fun isCollectedEvent(predicate: (UnitOfWork) -> Boolean) = apply { this.isCollectedEvent = predicate }

        override fun build(): CorrelatePipeline {
            val pipelineId = id ?: error("pipeline id is required")
            val microstore = eventsMicrostore ?: error("eventsMicrostore is required")
            return CorrelatePipeline(
                id = pipelineId,
                eventsMicrostore = microstore,
                correlationKeySupplier = correlationKeySupplier,
                onContentType = onContentType,
                eventFilter = eventFilter,
                ttlDays = ttlDays,
                expire = expire,
                eventCodec = eventCodec,
                isCollectedEvent = isCollectedEvent,
            )
        }
    }
}

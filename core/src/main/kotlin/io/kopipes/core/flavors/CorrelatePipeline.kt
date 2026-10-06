package io.kopipes.core.flavors

import io.kopipes.core.Event
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
import kotlinx.coroutines.flow.onEach

/** Pipeline flavor for correlating stream events. */
class CorrelatePipeline(
    id: String,
    val eventsMicrostore: EventsMicrostore,
    val correlationKeySupplier: (UnitOfWork) -> String? = { it.event?.partitionKey },
    val correlationKeySuffix: String = "",
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val eventFilter: EventFilter = EventFilter.Any,
    val ttlDays: Int? = null,
    val expire: Boolean = false,
    val eventCodec: EventCodec? = null,
    val isCollectedEvent: ((UnitOfWork) -> Boolean)? = null,
    val normalizer: ((UnitOfWork) -> UnitOfWork)? = null,
) : Pipeline(id) {

    fun forCollectedEvents(uow: UnitOfWork): Boolean {
        if (isCollectedEvent != null) return isCollectedEvent.invoke(uow)
        return uow.event != null
    }

    fun normalize(uow: UnitOfWork): UnitOfWork {
        if (normalizer != null) return normalizer.invoke(uow)
        return uow
    }

    private fun addCorrelationKey(uow: UnitOfWork): UnitOfWork {
        val correlationKey = correlationKeySupplier(uow)
        val key = if (correlationKey != null) correlationKey + correlationKeySuffix else null
        return uow.copy(key = key)
    }

    fun Flow<UnitOfWork>.save(fm: FaultManager): Flow<UnitOfWork> {
        val flow = with(fm) {
            this@save.mapNotFaulty { uow ->
                val key = uow.key ?: correlationKeySupplier(uow)?.let { it + correlationKeySuffix } ?: uow.event?.id ?: "unknown"
                val event: Event? = uow.event
                val eventId = event?.id ?: "unknown"
                val saveOptions = EventsMicrostore.SaveOptions(
                    pk = key,
                    sk = eventId,
                    discriminator = "CORREL",
                    timeStamp = event?.timestamp ?: System.currentTimeMillis(),
                    awsRegion = envConfig().awsRegion(),
                    sequenceNumber = uow.meta?.get("sequenceNumber")?.toString() ?: uow.sequenceNumber,
                    ttl = uow.meta?.get("ttl")?.toString()?.toLongOrNull() ?: ttlDays?.toLong() ?: envConfig().ttl()?.toLong() ?: 33L,
                    includeRaw = false,
                    expire = expire,
                    suffix = correlationKeySuffix,
                    data = eventId,
                    pipelineId = id,
                )
                uow.withSaveOptions(saveOptions)
            }
        }
        return eventsMicrostore.save(flow)
    }

    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        logger.info { "CorrelatePipeline.connect: id=$id" }
        with(fm) {
            return fromFlow
                .filterNotFaulty { uow -> forCollectedEvents(uow) }
                .mapNotFaulty { uow -> normalize(uow) }
                .filterEvents(fm, eventFilter)
                .onEach { printStartPipeline(it) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .mapNotFaulty { uow -> addCorrelationKey(uow) }
                .save(fm)
                .onEach { printEndPipeline(it) }
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<CorrelatePipeline, Builder>() {
        private var eventsMicrostore: EventsMicrostore? = null
        private var correlationKeySupplier: (UnitOfWork) -> String? = { it.event?.partitionKey }
        private var correlationKeySuffix: String = ""
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var eventFilter: EventFilter = EventFilter.Any
        private var ttlDays: Int? = null
        private var expire: Boolean = false
        private var eventCodec: EventCodec? = null
        private var isCollectedEvent: ((UnitOfWork) -> Boolean)? = null
        private var normalizer: ((UnitOfWork) -> UnitOfWork)? = null

        fun eventsMicrostore(microstore: EventsMicrostore) = apply { this.eventsMicrostore = microstore }
        fun correlationKeySupplier(supplier: (UnitOfWork) -> String?) = apply { this.correlationKeySupplier = supplier }
        fun correlationKey(key: (UnitOfWork) -> String?) = apply { this.correlationKeySupplier = key }
        fun correlationKeySuffix(suffix: String) = apply { this.correlationKeySuffix = suffix }
        fun onContentType(predicate: (UnitOfWork) -> Boolean) = apply { this.onContentType = predicate }
        fun eventFilter(filter: EventFilter) = apply { this.eventFilter = filter }
        fun ttlDays(days: Int?) = apply { this.ttlDays = days }
        fun expire(expire: Boolean) = apply { this.expire = expire }
        fun eventCodec(codec: EventCodec) = apply { this.eventCodec = codec }
        fun isCollectedEvent(predicate: (UnitOfWork) -> Boolean) = apply { this.isCollectedEvent = predicate }
        fun normalizer(normalizer: (UnitOfWork) -> UnitOfWork) = apply { this.normalizer = normalizer }

        override fun build(): CorrelatePipeline {
            val pipelineId = id ?: error("pipeline id is required")
            val microstore = eventsMicrostore ?: error("eventsMicrostore is required")
            return CorrelatePipeline(
                id = pipelineId,
                eventsMicrostore = microstore,
                correlationKeySupplier = correlationKeySupplier,
                correlationKeySuffix = correlationKeySuffix,
                onContentType = onContentType,
                eventFilter = eventFilter,
                ttlDays = ttlDays,
                expire = expire,
                eventCodec = eventCodec,
                isCollectedEvent = isCollectedEvent,
                normalizer = normalizer,
            )
        }
    }
}

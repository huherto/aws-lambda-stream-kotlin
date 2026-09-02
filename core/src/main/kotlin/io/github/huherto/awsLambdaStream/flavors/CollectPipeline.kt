package io.github.huherto.awsLambdaStream.flavors

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.PipelineBuilder
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.extensions.withSaveOptions
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach

/** Pipeline flavor that collects incoming events and persists them into an [EventsMicrostore]. */
class CollectPipeline(
    pipelineId: String,
    private val onContentType: (UnitOfWork) -> Boolean,
    private val eventFilter: EventFilter,
    private val correlationKey: (UnitOfWork) -> String?,
    private val ttlDays: Int?,
    private val includeRaw: Boolean,
    private val expire: Boolean,
    private val eventsMicrostore: EventsMicrostore,
) : Pipeline(pipelineId) {

    internal fun Flow<UnitOfWork>.save(): Flow<UnitOfWork> {

        val awsRegion = envConfig().awsRegion()
        val flow = this.map { uow ->
            val event: Event = uow.event ?: return@map uow
            val eventId = event.id ?: return@map uow

            val saveOptions = EventsMicrostore.SaveOptions(
                pk = eventId,
                sk = "EVENT",
                discriminator = "EVENT",
                timeStamp = event.timestamp,
                awsRegion = awsRegion,
                sequenceNumber = uow.sequenceNumber,
                ttl = ttlRule(uow),
                expire = expire,
                data = uow.key,
                includeRaw = includeRaw,
                suffix = "",
                pipelineId = id,
            )
            uow.withSaveOptions(saveOptions)
        }
        // The eventsMicrostore already has its own fault manager.
        return eventsMicrostore.save(flow)
    }

    fun daysInSecs(days: Int): Long {
        return days * 24 * 60 * 60L
    }

    private fun ttlRule(uow: UnitOfWork): Long {
        val ttl = this.ttlDays ?: envConfig().ttl() ?: 33
        return uow.event?.timestamp?.let { it / 1000 + daysInSecs(ttl) } ?: 0
    }

    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        logger.info { "CollectPipeline.connect: id=$id" }
        with(fm) {
            val flow = fromFlow
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .mapNotFaulty { uow -> uow.copy(key = correlationKey(uow)) }
                .save()
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<CollectPipeline, Builder>() {
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var eventFilter: EventFilter = EventFilter.Any
        private var correlationKey: (UnitOfWork) -> String? = { uow -> uow.event?.partitionKey }
        private var ttlDays: Int? = null
        private var includeRaw: Boolean = true
        private var expire: Boolean = false
        private var eventsMicrostore: EventsMicrostore? = null

        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun onContentType(onContentType: java.util.function.Predicate<UnitOfWork>) = apply { this.onContentType = { uow -> onContentType.test(uow) } }
        fun eventFilter(eventFilter: EventFilter) = apply { this.eventFilter = eventFilter }
        fun correlationKey(correlationKey: (UnitOfWork) -> String?) = apply { this.correlationKey = correlationKey }
        fun correlationKey(correlationKey: java.util.function.Function<UnitOfWork, String?>) = apply { this.correlationKey = { uow -> correlationKey.apply(uow) } }
        fun ttlDays(ttlDays: Int) = apply { this.ttlDays = ttlDays }
        fun includeRaw(includeRaw: Boolean) = apply { this.includeRaw = includeRaw }
        fun expire(expire: Boolean) = apply { this.expire = expire }
        fun eventsMicrostore(eventsMicrostore: EventsMicrostore) = apply { this.eventsMicrostore = eventsMicrostore }

        override fun build(): CollectPipeline {
            return CollectPipeline(
                pipelineId = id ?: throw IllegalArgumentException("id is required"),
                onContentType = onContentType,
                eventFilter = eventFilter,
                correlationKey = correlationKey,
                ttlDays = ttlDays,
                includeRaw = includeRaw,
                expire = expire,
                eventsMicrostore = eventsMicrostore ?: throw IllegalArgumentException("eventsMicrostore is required")
            )
        }
    }
}
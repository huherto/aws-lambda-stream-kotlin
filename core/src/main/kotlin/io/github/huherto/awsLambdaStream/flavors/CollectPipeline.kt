package io.github.huherto.awsLambdaStream.flavors

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach

/**
 * Pipeline flavor that collects incoming events and persists them into an [EventsMicrostore].
 *
 * A `CollectPipeline` receives a stream of [UnitOfWork] instances, optionally filters them by
 * [eventFilter] and [onContentType], derives a correlation/data key, and stores each event using
 * the event id as the microstore partition key.
 *
 * The saved records use:
 * - `pk`: the event id
 * - `sk`: `"EVENT"`
 * - `discriminator`: `"EVENT"`
 * - `data`: the value returned by [correlationKey]
 *
 * @param pipelineId Unique identifier for this pipeline.
 * @param envConfig Environment-backed configuration used for AWS region and default TTL lookup.
 * @param onContentType Predicate used to accept or reject a [UnitOfWork] after event filtering.
 * @param eventFilter Event-level filter applied before the pipeline-specific processing starts.
 * @param correlationKey Function used to derive the value saved as microstore `data`.
 * Defaults to the event partition key.
 * @param ttlDays Optional TTL duration in days. When `null`, the value from [envConfig] is used,
 * falling back to 33 days.
 * @param includeRaw Whether the raw event payload should be included in the stored record.
 * @param expire Whether the microstore record should be written as expirable.
 * @param eventsMicrostore Sink responsible for persisting collected event records.
 */
class CollectPipeline(
    pipelineId: String,
    envConfig: EnvironmentConfig,
    private val onContentType: (UnitOfWork) -> Boolean = {  true },
    private val eventFilter: EventFilter = EventFilter.Any,
    private val correlationKey: (UnitOfWork) -> String? = { uow -> uow.event?.partitionKey },
    private val ttlDays: Int? = null,
    private val includeRaw: Boolean = true,
    private val expire: Boolean = false,
    private val eventsMicrostore: EventsMicrostore,
) : Pipeline(pipelineId, envConfig) {

    /**
     * Builds save options for each valid event and delegates persistence to [eventsMicrostore].
     *
     * Unit-of-work items without an event or without an event id pass through unchanged.
     * The microstore itself is responsible for handling save failures.
     */
    internal fun Flow<UnitOfWork>.save(): Flow<UnitOfWork> {

        val awsRegion = envConfig.awsRegion()
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
            uow.copy(saveOptions = saveOptions)
        }
        // The eventsMicrostore already has its own fault manager.
        return eventsMicrostore.save(flow)
    }

    /**
     * Converts a day count to seconds.
     *
     * @param days Number of days to convert.
     * @return Equivalent duration in seconds.
     */
    fun daysInSecs(days: Int): Long {
        return days * 24 * 60 * 60L
    }

    /**
     * Calculates the Unix epoch TTL value, in seconds, for a collected record.
     *
     * The TTL is based on the event timestamp plus the configured number of days. If the
     * unit of work has no event timestamp, `0` is returned.
     */
    private fun ttlRule(uow: UnitOfWork): Long {
        val ttl = this.ttlDays ?: envConfig.ttl() ?: 33
        return uow.event?.timestamp?.let { it / 1000 + daysInSecs(ttl) } ?: 0
    }

    /**
     * Connects this pipeline to an upstream [Flow].
     *
     * Processing order:
     * 1. Filter events with [eventFilter].
     * 2. Log pipeline start.
     * 3. Apply [onContentType].
     * 4. Derive and attach the correlation key.
     * 5. Persist the event through [save].
     * 6. Log pipeline completion.
     *
     * @param fm Fault manager used by filtering and mapping stages.
     * @param fromFlow Upstream flow of units of work.
     * @return A flow that emits the processed units of work.
     */
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
}
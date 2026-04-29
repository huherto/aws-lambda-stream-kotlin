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

class CollectPipeline constructor(
    pipelineId: String,
    private val envConfig: EnvironmentConfig,
    private val onContentType: (UnitOfWork) -> Boolean = {  true },
    private val eventFilter: EventFilter = EventFilter.Any,
    private val correlationKey: (UnitOfWork) -> String? = { uow -> uow.event?.partitionKey },
    private val ttlDays: Int? = null,
    private val includeRaw: Boolean = true,
    private val expire: Boolean = false,
    private val eventsMicrostore: EventsMicrostore,
) : Pipeline(pipelineId) {

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

    fun daysInSecs(days: Int): Long {
        return days * 24 * 60 * 60L
    }

    private fun ttlRule(uow: UnitOfWork): Long {
        val ttl = this.ttlDays ?: envConfig.ttl() ?: 33
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
}
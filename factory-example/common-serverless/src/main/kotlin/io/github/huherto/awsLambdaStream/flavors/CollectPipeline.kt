package io.github.huherto.awsLambdaStream.flavors

import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import kotlinx.coroutines.flow.*
import kotlin.reflect.KClass

class CollectPipeline constructor(
    private val  pipelineId: String,
    private val envConfig: EnvironmentConfig,
    private val onContentType: (UnitOfWork) -> Boolean = {  true },
    private val onEventClass: List<KClass<out Event>> = listOf(Event::class),
    private val correlationKey: (UnitOfWork) -> String? = { uow -> uow.event?.partitionKey },
    private val ttlDays: Int? = null,
    private val includeRaw: Boolean = true,
    private val expire: Boolean? = null,
    private var eventsMicrostore: EventsMicrostore,
) : Pipeline(pipelineId) {

    internal fun Flow<UnitOfWork>.save(): Flow<UnitOfWork> {

        val flow = this.map { uow ->
            val event: Event? = uow.event
            val saveOptions = EventsMicrostore.SaveOptions(
                pk = event?.id,
                sk = "EVENT",
                discriminator = "EVENT",
                timeStamp = event?.timestamp.toString(),
                awsRegion = envConfig.awsRegion(),
                sequenceNumber = uow.meta?.get("sequenceNumber"),
                ttl = ttlRule(uow),
                expire = expire,
                data = uow.key,
                includeRaw = includeRaw,
                pipelineId = id,
            )
            uow.copy(saveOptions = saveOptions)
        }
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
                .filterNotNull()
                .onEach { printStepPipeline("stepA", it) }
                .filterEventTypes(this, *onEventClass.toTypedArray())
                .onEach { uow -> printStartPipeline(uow) }
                .filter { uow -> faulty(uow) { onContentType(uow) } == true }
                .onEach { uow -> printStepPipeline("stepB", uow) }
                .mapNotFaulty { uow -> uow.copy(key = correlationKey(uow)) }
                .onEach { uow -> printStepPipeline("stepC", uow) }
                .save()
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }

    }
}
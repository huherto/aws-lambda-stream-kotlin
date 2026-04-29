package io.github.huherto.awsLambdaStream.flavors

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach

const val CORREL = "CORREL"

class CorrelatePipeline(
    id: String,
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val eventFilter: EventFilter = EventFilter.Any,
    val correlationKey: ((UnitOfWork) -> String)? = null,
    val correlationKeySuffix: String = "",
    val envConfig: EnvironmentConfig,
    var eventsMicrostore: EventsMicrostore,
    val unmarshall: ((String) -> Event)? = null,
    val expire: Boolean = false,
) : Pipeline(id) {

    internal fun forCollectedEvents(uow: UnitOfWork) : Boolean {
        return when(uow.record) {
            is DynamodbEvent.DynamodbStreamRecord -> {
                uow.record.eventName == "INSERT"
                        && uow.record.dynamodb.keys["sk"]?.s == "EVENT"
                        && uow.event?.raw is RecordPair
            }
            else -> false
        }
    }
    internal fun defaultUnmarshall(eventAsString: String) : Event {
        if (unmarshall != null) return unmarshall(eventAsString)

        val jsonEvent: JsonEvent = try {
            JsonEvent(eventAsString)
        } catch (e: Exception) {
            logger.error {"Failed to parse event: $eventAsString, $e" }
            throw e
        }
        return jsonEvent
    }

    internal fun normalize(uow: UnitOfWork): UnitOfWork {
        val raw = uow.event?.raw as? RecordPair
        val rawNew = raw?.new ?: RecordImage(mapOf())
        val eventAsString = rawNew.getEvent()?: "{}"
        val eventAsObject = defaultUnmarshall(eventAsString)
        val record = uow.record as? DynamodbEvent.DynamodbStreamRecord

        return uow.copy(
            meta = mapOf(
                "sequenceNumber" to record?.dynamodb?.sequenceNumber,
                "ttl" to "" + rawNew.getTtl().toString(),
                "data" to "" + rawNew.getData(),
            ),
            event = eventAsObject
        )
    }

    private fun addCorrelationKey(uow: UnitOfWork) : UnitOfWork {
        require(correlationKey != null) { "correlationKey must be set" }

        // use a suffix when you need the same key for different sets of rules
        val key = correlationKey(uow) + correlationKeySuffix
        return uow.copy(key = key)
    }

    internal fun Flow<UnitOfWork>.save(): Flow<UnitOfWork> {
        val flow = this.map { uow ->
            val key = uow.key ?: return@map uow
            val event: Event = uow.event ?: return@map uow
            val eventId = event.id ?: return@map uow
            val saveOptions = EventsMicrostore.SaveOptions(
                pk = key,
                sk = eventId,
                discriminator = CORREL,
                timeStamp = uow.event.timestamp,
                awsRegion = envConfig.awsRegion(),
                sequenceNumber = uow.meta?.get("sequenceNumber"),
                ttl = uow.meta?.get("ttl")?.toLongOrNull(),
                expire = expire,
                suffix = correlationKeySuffix,
                pipelineId = id,
            )
            uow.copy(saveOptions = saveOptions)
        }
        // Save already has a fault manager.
        return eventsMicrostore.save(flow)
    }

    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        logger.info { "CorrelatePipeline.connect: id=$id" }
        with(fm) {
            val flow = fromFlow
                .filterNotFaulty{ uow ->  forCollectedEvents(uow) }
                .mapNotFaulty{  uow -> normalize(uow) }
                .filterEvents(this, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .mapNotFaulty { uow -> addCorrelationKey(uow)}
                .save()
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }
}
package io.github.huherto.awsLambdaStream.flavors

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.reflect.KClass

sealed interface EmitOption {
    data class Basic(val type: String) : EmitOption
    data class Custom(val emit: (UnitOfWork, Event) -> List<Event>) : EmitOption
}

class EvaluatePipeline (
    id: String,
    val envConfig: EnvironmentConfig,
    val eventPublisher: EventPublisher,
    val eventsMicrostore: EventsMicrostore,
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val onEventClass: List<KClass<out Event>> = listOf(Event::class),
    val correlationKeySuffix: String = "",
    val index: String? = null,
    val bufferCapacity: Int = Channel.Factory.BUFFERED,
    val unmarshall: ((String) -> Event)? = null,
    val expression: ((UnitOfWork) -> Boolean)? = null,
    val higherOrderEmit: EmitOption? = null,
) : Pipeline(id) {

    internal fun forEvents(uow: UnitOfWork) : Boolean {
        return when(uow.record) {
            is DynamodbEvent.DynamodbStreamRecord -> {
                (uow.record.eventName == "INSERT"
                        && uow.record.dynamodb?.keys["sk"]?.s == "EVENT")
                        || uow.record.dynamodb?.newImage?.get("discriminator")?.s == "CORREL"
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
        val isCorrel = rawNew.get("discriminator")?.s == "CORREL"
        val pk = rawNew.get("pk")?.s
        val data = rawNew.get("data")?.s
        val expire = rawNew.get("expire")?.b
        val correlationKey = if (isCorrel) pk else data
        val suffix = rawNew.get("suffix")?.s
        val queryParams = EventsMicrostore.QueryParams(
            pk = pk,
            isCorrelated =  isCorrel,
            data = data,
            index = index,
        )
        return uow.copy(
            meta = mapOf(
                "id" to uow.event?.id,
                //"sequenceNumber" to record?.dynamodb?.sequenceNumber,
                // "ttl" to "" + rawNew.getTtl().toString(),
                // "expire" to "" + expire,
                // "pk" to pk,
                // "data" to rawNew.getData(),
                //"correlationKey" to correlationKey,
                "suffix" to suffix,
                "correlation" to isCorrel.toString()
            ),
            queryParams = queryParams,
            event = eventAsObject
        )
    }

    internal fun onCorrelationKeySuffix(uow: UnitOfWork): Boolean {
        val uowSuffix = uow.meta?.get("suffix") ?: ""
        return correlationKeySuffix == uowSuffix
    }

    internal fun Flow<UnitOfWork>.queryCorrelated() : Flow<UnitOfWork> {
        return eventsMicrostore.queryByPk(this)
    }

    internal fun Flow<UnitOfWork>.complex(): Flow<UnitOfWork> {
        return if (expression == null) {
            this.map { uow ->
                uow.copy(
                    triggers = listOfNotNull(uow.event)
                )
            }
        } else {
            this
                .filter { uow -> onCorrelationKeySuffix(uow) }
                .queryCorrelated()
                .filter { uow ->
                    expression.invoke(uow)
                }
        }
    }

    // A concrete implementation of Event for the newly created higher-order template
    data class HigherOrderEvent(
        override var id: String? = null,
        override var timestamp: Long? = null,
        override var partitionKey: String? = null,
        override var tags: Map<String, String>? = null,
        override var raw: Any? = null,
        override var eem: Any? = null,
        var type: String? = null,
        var mappedTriggers: List<Map<String, Any?>>? = null,
        var baseEvent: Event? = null // Holds the properties of uow.event if basic is true
    ) : Event {
        override fun eventType(): String = type ?: baseEvent?.eventType() ?: "unknown"
        override fun encoded(): String = baseEvent?.encoded() ?: "{}"
    }

    internal fun toHigherOrderEvents(uow: UnitOfWork): List<UnitOfWork> {
        val basic = higherOrderEmit is EmitOption.Basic
        val trigger = uow.triggers?.lastOrNull()

        val aggregatedTags = aggregateTags(uow)

        val mappedTriggers = uow.triggers?.map {
            mapOf(
                "id" to it.id,
                "type" to it.eventType(),
                "timestamp" to it.timestamp
            )
        }

        val uowMetaId = uow.meta?.get("id") ?: ""
        val uowMetaCorrelationKey = uow.meta?.get("correlationKey") ?: ""
        val partitionKeyStr = uowMetaCorrelationKey.replace(".${correlationKeySuffix}", "")

        val template = HigherOrderEvent(
            id = "$uowMetaId.${id}", // plus a suffix if many
            type = (higherOrderEmit as? EmitOption.Basic)?.type,
            timestamp = trigger?.timestamp,
            partitionKey = partitionKeyStr,
            tags = aggregatedTags,
            mappedTriggers = mappedTriggers,
            baseEvent = if (basic) uow.event else null,
            raw = if (basic) uow.event?.raw else null,
            eem = if (basic) uow.event?.eem else null
        )

        val resultEvents: List<Event> = when (higherOrderEmit) {
            is EmitOption.Basic -> listOf(template)
            is EmitOption.Custom -> higherOrderEmit.emit(uow, template)
            null -> throw IllegalArgumentException("higherOrderEmit must be a String or a function")
        }

        // Maps results back to a UnitOfWork (replacing the current event with the emitted one)
        return resultEvents.map { emit ->
            uow.copy(event = emit)
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

    internal fun Flow<UnitOfWork>.publish() : Flow<UnitOfWork> {
        return eventPublisher.publish(this)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        logger.info { "CorrelatePipeline.connect: id=$id" }
        with(fm) {
            val flow = fromFlow
                .filter{ uow -> faulty(uow){ forEvents(uow) } == true }
                .mapNotFaulty{  uow -> normalize(uow) }
                .filterEventTypes(this, *onEventClass.toTypedArray())
                .onEach { uow -> printStartPipeline(uow) }
                .filter { uow -> faulty(uow) { onContentType(uow) } == true }
                .complex()
                .flatMapMerge { uow ->
                    faulty(uow) { toHigherOrderEvents(uow) }?.asFlow() ?: emptyFlow()
                }
                .buffer(bufferCapacity)
                .publish()
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }
}

package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.queries.queryAllDynamoDB
import io.github.huherto.awsLambdaStream.utils.CompactRule
import io.github.huherto.awsLambdaStream.utils.compact
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.reflect.KClass
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV

class EvaluatePipeline (
    id: String,
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val onEventClass: List<KClass<out Event>> = listOf(Event::class),
    val correlationKey: ((UnitOfWork) -> String)? = null,
    val correlationKeySuffix: String = "",
    val index: String? = null,
    val envConfig: EnvironmentConfig = EnvironmentConfig(),
    val bufferCapacity: Int = Channel.Factory.BUFFERED,
    var dynamoDbClient: DynamoDbClient? = null,
    val unmarshall: ((String) -> Event)? = null,
    val compactRule: CompactRule? = null,
    val expression: ((UnitOfWork) -> Boolean)? = null,
    val higherOrderEmit: Any? = null,
) : Pipeline(id) {

    internal fun forEvents(uow: UnitOfWork) : Boolean {
        return when(uow.record) {
            is DynamodbEvent.DynamodbStreamRecord -> {
                (uow.record.eventName == "INSERT"
                        && uow.record.dynamodb.keys["sk"]?.s == "EVENT")
                        || uow.record.dynamodb.newImage?.get("discriminator")?.s == "CORREL"
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
        val correlationKey = if (isCorrel) rawNew.get("pk")?.s else rawNew.get("data")?.s

        return uow.copy(
            meta = mapOf(
                "id" to uow.event?.id,
                "sequenceNumber" to record?.dynamodb?.sequenceNumber,
                "ttl" to "" + rawNew.getTtl().toString(),
                "expire" to "" + rawNew.get("expire")?.b,
                "pk" to rawNew.get("pk")?.s,
                "data" to rawNew.getData(),
                "correlationKey" to correlationKey,
                "suffix" to rawNew.get("suffix")?.s,
                "correlation" to isCorrel.toString()
            ),
            event = eventAsObject
        )
    }

    internal fun onCorrelationKeySuffix(uow: UnitOfWork): Boolean {
        val uowSuffix = uow.meta?.get("suffix")

        // evaluate rules with no suffix against correlations with no suffix
        if (correlationKeySuffix.isEmpty() && uowSuffix.isNullOrEmpty()) {
            return true
        }

        // do not evaluate rules with a suffix against correlations with no suffix
        if (correlationKeySuffix.isNotEmpty() && uowSuffix.isNullOrEmpty()) {
            return false
        }

        // evaluate rules with a suffix against correlations with the same suffix
        if (correlationKeySuffix.isNotEmpty() && uowSuffix == correlationKeySuffix) {
            return true
        }

        // do not evaluate rules with a suffix against correlations with a different suffix
        return false
    }

    internal fun toQueryRequest(uow: UnitOfWork) : UnitOfWork {
        val isCorrelation = uow.meta?.get("correlation").toBoolean()

        val request = QueryRequest {
            if (isCorrelation) {
                keyConditionExpression = "#pk = :pk"
                expressionAttributeNames = mapOf("#pk" to "pk")
                // Safely pulling the value from the meta-map and wrapping in AttributeValue.S
                expressionAttributeValues = mapOf(":pk" to SdkAV.S(uow.meta?.get("pk") ?: ""))
                consistentRead = true
            } else {
                indexName = index ?: "DataIndex"
                keyConditionExpression = "#data = :data"
                expressionAttributeNames = mapOf("#data" to "data")
                expressionAttributeValues = mapOf(":data" to SdkAV.S(uow.meta?.get("data") ?: ""))
            }
        }

        return uow.copy(queryRequest = request)
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
                .map { uow -> toQueryRequest(uow) }
                .queryAllDynamoDB(
                    dynamoDbClient ?: error("DynamoDB client must be configured to process expressions")
                )
                .map { uow ->
                    // In TS, queryAllDynamoDB assigns to 'correlated' via queryResponseField.
                    // In Kotlin, queryAllDynamoDB stores the result in `queryResponse` (List<Map<String, AttributeValue>>).
                    // We extract the JSON event string, unmarshall it, and assign the objects to `correlated`.
                    val correlatedEvents = uow.queryResponse?.mapNotNull { item ->
                        val eventString = (item["event"] as? SdkAV.S)?.value
                        eventString?.let { defaultUnmarshall(it) }
                    } ?: emptyList()

                    uow.copy(
                        correlated = correlatedEvents
                    )
                }
                .filter { uow ->
                    // In TS, the stream mapped the expression logic to a boolean field on uow, then filtered.
                    // In Kotlin, we can directly execute the expression block within standard filter operator.
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

    /**
     * Transforms a UnitOfWork into a list of UnitOfWorks with newly emitted higher order events.
     * Note: To use with 'faultyAsyncStream' in Kotlin Flow, you would typically use `flatMapConcat` or `mapNotNull`
     * wrapped in your `FaultManager.faulty()` handler.
     */
    internal fun toHigherOrderEvents(uow: UnitOfWork): List<UnitOfWork> {
        val basic = higherOrderEmit as? String != null
        val trigger = uow.triggers?.lastOrNull()

        // reduce + merge + omit(['region', 'source'])
        val aggregatedTags = uow.triggers
            ?.mapNotNull { it.tags }
            ?.fold(mutableMapOf<String, String>()) { acc, currentTags ->
                acc.apply { putAll(currentTags) }
            }?.apply {
                remove("region")
                remove("source")
            }

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
            type = if (basic) higherOrderEmit else null,
            timestamp = trigger?.timestamp,
            partitionKey = partitionKeyStr,
            tags = aggregatedTags,
            mappedTriggers = mappedTriggers,
            baseEvent = if (basic) uow.event else null,
            raw = if (basic) uow.event?.raw else null,
            eem = if (basic) uow.event?.eem else null
        )

        val resultEvents: List<Event> = if (basic) {
            listOf(template)
        } else {
            @Suppress("UNCHECKED_CAST")
            val emitFunction = higherOrderEmit as? (UnitOfWork, Event) -> List<Event>
                ?: throw IllegalArgumentException("higherOrderEmit must be a String or a function")

            val emitResult = emitFunction(uow, template)

            // Emulates castArray(result)
            when (emitResult) {
                is Iterable<*> -> emitResult.filterIsInstance<Event>()
                is Event -> listOf(emitResult)
                else -> emptyList()
            }
        }

        // Maps results back to a UnitOfWork (replacing the current event with the emitted one)
        return resultEvents.map { emit ->
            uow.copy(event = emit)
        }
    }

    fun publish(uow: UnitOfWork) : UnitOfWork {
        // Not implemented yet
        logger.info { "Publishing event: $uow.event" }
        return uow
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        logger.info { "CorrelatePipeline.connect: id=$id" }
        with(fm) {
            val flow = fromFlow
                .filterNotNull()
                .filter{ uow -> faulty(uow){ forEvents(uow) } == true }
                .mapNotFaulty{  uow -> normalize(uow) }
                .filterEventTypes(this, *onEventClass.toTypedArray())
                .onEach { uow -> printStartPipeline(uow) }
                .filter { uow -> faulty(uow) { onContentType(uow) } == true }
                .buffer(bufferCapacity)
                .compact(compactRule)
                .complex()
                .flatMapMerge { uow ->
                    faulty(uow) { toHigherOrderEvents(uow) }?.asFlow() ?: emptyFlow()
                }
                .buffer(bufferCapacity)
                .mapNotNull { uow -> faulty(uow) { publish(uow) } }
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }
}
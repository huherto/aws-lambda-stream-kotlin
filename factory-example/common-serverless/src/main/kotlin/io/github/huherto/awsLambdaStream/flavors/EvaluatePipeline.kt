package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.reflect.KClass
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV

class EvaluatePipeline constructor(
    id: String,
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val onEventClass: List<KClass<out Event>> = listOf(Event::class),
    val correlationKey: ((UnitOfWork) -> String)? = null,
    val correlationKeySuffix: String = "",
    val index: String? = null,
    val envConfig: EnvironmentConfig = EnvironmentConfig(),
    val bufferCapacity: Int = Channel.Factory.BUFFERED,
    var dynamoDbClient: DynamoDbClient? = null,
    val putRequest: ((UnitOfWork) -> UnitOfWork)? = null,
    val unmarshall: ((String) -> Event)? = null,
    val expire: Boolean = false,
    val expression: ((UnitOfWork) -> Boolean)? = null,
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

    private fun addCorrelationKey(uow: UnitOfWork) : UnitOfWork {
        require(correlationKey != null) { "correlationKey must be set" }

        // use a suffix when you need the same key for different sets of rules
        val key = correlationKey(uow) + correlationKeySuffix
        return uow.copy(key = key)
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
                // Safely pulling the value from the meta map and wrapping in AttributeValue.S
                expressionAttributeValues = mapOf(":pk" to SdkAV.S(uow.meta?.get("pk") ?: ""))
                consistentRead = true
            } else {
                indexName = index ?: "DataIndex"
                keyConditionExpression = "#data = :data"
                expressionAttributeNames = mapOf("#data" to "data")
                expressionAttributeValues = mapOf(":data" to SdkAV.S(uow.meta?.get("data") ?: ""))
            }
        }

        return uow.copy(toQueryRequest = request)
    }

    internal fun defaultPutRequest(uow: UnitOfWork) : UnitOfWork {

        if (putRequest != null) return putRequest(uow)

        val event: Event? = uow.event
        val timeStamp = event?.timestamp
        val awsRegion = envConfig.awsRegion()

        val itemValues = mapOf(
            "pk" to nullableS(uow.key),
            "sk" to nullableS(event?.id),
            "discriminator" to SdkAV.S("CORREL"), // ATION
            "timestamp" to SdkAV.N(timeStamp.toString()),
            "awsregion" to SdkAV.S(awsRegion),
            "sequenceNumber" to nullableS(uow.meta?.get("sequenceNumber")),
            "ttl" to nullableN(uow.meta?.get("ttl")),
            "expire" to nullableB(expire),
            "suffix" to SdkAV.S(correlationKeySuffix),
            "pipelineId" to nullableS(id),
            "event" to nullableS(event?.encoded()),
        )

        val putRequest = PutItemRequest.Companion {
            tableName = envConfig.tableName() ?: "events"
            item = itemValues
        }
        return uow.copy(putRequest = putRequest)
    }

    private val putDynamoDb: suspend (UnitOfWork) -> UnitOfWork = { uow ->
        if (dynamoDbClient == null) {
            dynamoDbClient = getDynamoDbClient(envConfig)
        }
        val putResponse = uow.putRequest?.let {
            dynamoDbClient?.putItem(uow.putRequest)
        }
        uow.copy(putResponse = putResponse)
    }

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
                .mapNotFaulty { uow -> addCorrelationKey(uow)}
                .mapNotFaulty{ uow -> defaultPutRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotNull { uow -> faulty(uow) { putDynamoDb(uow) } }
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }
}
package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.reflect.KClass

class CorrelatePipeline constructor(
    id: String,
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val onEventClass: List<KClass<out Event>> = listOf(Event::class),
    val correlationKey: ((UnitOfWork) -> String)? = null,
    val correlationKeySuffix: String = "",
    val envConfig: EnvironmentConfig = EnvironmentConfig(),
    val bufferCapacity: Int = Channel.Factory.BUFFERED,
    var dynamoDbClient: DynamoDbClient? = null,
    val putRequest: ((UnitOfWork) -> UnitOfWork)? = null,
    val unmarshall: ((String) -> Event)? = null,
) : Pipeline(id) {

    private fun nullableS(s: String?): AttributeValue {
        return s?.let { AttributeValue.S(it) } ?: AttributeValue.Null(true)
    }

    private fun nullableN(s: String?): AttributeValue {
        return s?.let { AttributeValue.N(it) } ?: AttributeValue.Null(true)
    }

    internal fun defaultPutRequest(uow: UnitOfWork) : UnitOfWork {

        if (putRequest != null) return putRequest(uow)

        val event: Event? = uow.event
        val timeStamp = event?.timestamp
        val awsRegion = envConfig.awsRegion()

        val itemValues = mapOf(
            "pk" to nullableS(uow.key),
            "sk" to nullableS(event?.id),
            "discriminator" to AttributeValue.S("CORREL"), // ATION
            "timestamp" to AttributeValue.N(timeStamp.toString()),
            "awsregion" to AttributeValue.S(awsRegion),
            "sequenceNumber" to nullableN(uow.meta?.get("sequenceNumber")),
            "ttl" to nullableN(uow.meta?.get("ttl")),
            "expire" to nullableS(uow.meta?.get("expire")),
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
            println("Failed to parse event: $eventAsString")
            println("Exception: $e")
            throw e
        }
        return jsonEvent
    }

    internal fun normalize(uow: UnitOfWork): UnitOfWork {
        val raw = uow.event?.raw as? RecordPair
        val rawNew = raw?.new ?: RecordImage(mapOf())
        val eventAsString = rawNew.getEvent()?: "{}"
        val eventAsObject = defaultUnmarshall(eventAsString)

        return uow.copy(
            meta = mapOf(
                "sequenceNumber" to rawNew.getSequenceNumber(),
                "ttl" to "" + rawNew.getTtl(),
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

    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        logger.info { "CorrelatePipeline.connect: id=$id" }
        val putRequest = this.putRequest?: ::defaultPutRequest
        with(fm) {
            val flow = fromFlow
                .filterNotNull()
                .filter{ uow -> faulty(uow){ forCollectedEvents(uow) } == true }
                .mapNotFaulty{  uow -> normalize(uow) }
                .filterEventTypes(this, *onEventClass.toTypedArray())
                .onEach { uow -> printStartPipeline(uow) }
                .filter { uow -> faulty(uow) { onContentType(uow) } == true }
                .mapNotFaulty { uow -> addCorrelationKey(uow)}
                .mapNotFaulty{ uow -> putRequest(uow) }
                .buffer(bufferCapacity)
                .onEach {  uow-> logger.info { "Before putDynamoDB uow=${uow}"} }
                .mapNotNull { uow -> faulty(uow) { putDynamoDb(uow) } }
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }
}
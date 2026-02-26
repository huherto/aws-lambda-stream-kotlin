package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue.N
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue.S
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import kotlin.reflect.KClass

abstract class Pipeline {

    protected val logger = KotlinLogging.logger {  }

    fun printStartPipeline(uom: UnitOfWork) {
        logger.debug { "start type: ${uom.event?.type}, eid: ${uom.event?.id}" }
    }

    fun printEndPipeline(uom: UnitOfWork) {
        val redacted = trimAndRedacted(uom)
        logger.debug { "end type: ${uom.event?.type}, eid: ${uom.event?.id}, uow: $redacted" }
    }

    fun trimAndRedacted(uom: UnitOfWork) {
        uom
    }

}

class CollectPipeline private constructor(builder: Builder) : Pipeline() {

    private val onContentType: (UnitOfWork) -> Boolean = builder.onContentType
    private val onEventClass: List<KClass<Event>> = builder.onEventClass
    private val correlationKey: (UnitOfWork) -> String? = builder.correlationKey
    private val ttlDays: Int? = builder.ttlDays
    private val includeRaw: Boolean = builder.includeRaw
    private val expire: String? = builder.expire
    private val envConfig: EnvironmentConfig = builder.envConfig
    private val bufferCapacity: Int = builder.bufferCapacity
    private var dynamoDbClient: DynamoDbClient? = builder.dynamoDbClient
    private var putRequest: (UnitOfWork) -> UnitOfWork = builder.putRequest?: ::defaultPutRequest

    class Builder {
        internal var onContentType: (UnitOfWork) -> Boolean = { true }
        internal var onEventClass: List<KClass<Event>> = listOf(Event::class)
        internal var correlationKey: (UnitOfWork) -> String? = { uom -> uom.event?.partitionKey }
        internal var ttlDays: Int? = null
        internal var includeRaw: Boolean = false
        internal var expire: String? = null
        internal var envConfig: EnvironmentConfig = EnvironmentConfig()
        internal var bufferCapacity: Int = Channel.BUFFERED
        internal var dynamoDbClient: DynamoDbClient? = null
        internal var putRequest: ((UnitOfWork) -> UnitOfWork)? = null

        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun onEventClass(onEventClass: List<KClass<Event>>) = apply { this.onEventClass = onEventClass }
        fun correlationKey(correlationKey: (UnitOfWork) -> String?) = apply { this.correlationKey = correlationKey }
        fun ttlDays(ttlDays: Int?) = apply { this.ttlDays = ttlDays }
        fun includeRaw(includeRaw: Boolean) = apply { this.includeRaw = includeRaw }
        fun expire(expire: String?) = apply { this.expire = expire }
        fun envConfig(envConfig: EnvironmentConfig) = apply { this.envConfig = envConfig }
        fun bufferCapacity(bufferCapacity: Int) = apply { this.bufferCapacity = bufferCapacity }
        fun dynamoDbClient(dynamoDbClient: DynamoDbClient?) = apply { this.dynamoDbClient = dynamoDbClient }
        fun putRequest(putRequest: (UnitOfWork) -> UnitOfWork) = apply { this.putRequest = putRequest }

        fun build(): CollectPipeline = CollectPipeline(this)
    }

    fun daysInSecs(days: Int): Long {
        return days * 24 * 60 * 60L
    }

    private fun ttlRule(uow: UnitOfWork): Long {
        val ttl = this.ttlDays ?: envConfig.ttl() ?: 33
        return uow.event?.timestamp?.let { it / 1000 + daysInSecs(ttl) } ?: 0
    }

    private fun nullableS(s: String?): AttributeValue {
        return s?.let { S(it) } ?: AttributeValue.Null(true)
    }

    private fun omitRaw(event: Event?): String {
        throw RuntimeException("Not implemented yet")
    }

    public fun defaultPutRequest(uow: UnitOfWork) : UnitOfWork {
        val event: Event? = uow.event
        val encodedEvent = if (includeRaw) event?.encoded() else omitRaw(event)
        val ttl = ttlRule(uow)
        val timeStamp = event?.timestamp
        val awsRegion = envConfig.awsRegion()

        val itemValues = mapOf(
            "pk" to nullableS(event?.id),
            "sk" to S("EVENT"),
            "discriminator" to S("EVENT"),
            "timestamp" to N(timeStamp.toString()),
            "awsregion" to S(awsRegion),
            "ttl" to N(ttl.toString()),
            "expire" to nullableS(expire),
            "data" to nullableS(uow.key),
            "event" to nullableS(encodedEvent)
        )

        val putRequest = PutItemRequest {
            tableName = envConfig.tableName()
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

    fun collect(fromFlow: Flow<UnitOfWork>) {
        val flow = fromFlow
            .filterEventTypes(*onEventClass.toTypedArray())
            .onEach { uow -> printStartPipeline(uow) }
            .filter { uow -> faulty(uow) { onContentType(uow) } }
            .map { uow -> faulty(uow) { uow.copy(key = correlationKey(uow)) } }
            .onEach { uow -> faulty(uow) { putRequest(uow) } }
            .buffer(bufferCapacity)
            .onEach { uow -> faulty(uow) { putDynamoDb(uow) } }
            .onEach { uow -> printEndPipeline(uow) }
    }
}
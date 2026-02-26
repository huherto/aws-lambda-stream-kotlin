package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue.N
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue.S
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest.Companion.invoke
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import mu.KotlinLogging
import java.time.Clock
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

class CollectPipeline(var eventsMicrostore: EventsMicrostore) : Pipeline() {

    private var onContentType: (UnitOfWork) -> Boolean = { true }

    private var onEventClass: List<KClass<Event>> = listOf(Event::class);

    private var correlationKey: (UnitOfWork) -> String? = {
            uom -> uom.event?.partitionKey
    }

    private var ttlDays : Int? = null

    private var includeRaw: Boolean = false

    private var expire: String? = null // Any value here means "true".

    private var envConfig : EnvironmentConfig = EnvironmentConfig()

    fun daysInSecs(days: Int) : Long {
        return days * 24 * 60 * 60L
    }

    private fun ttlRule(uow: UnitOfWork) : Long {
        val ttl = this.ttlDays?: envConfig.ttl()?: 33
        return uow.event?.timestamp?.let { it/1000 + daysInSecs(ttl) } ?: 0
    }

    private fun nullableS(s: String?) : AttributeValue {
        return s?.let { S(it) } ?: AttributeValue.Null(true)
    }

    private fun omitRaw(Event: Event?) : String {
        throw RuntimeException("Not implemented yet")
    }

    private var putRequest: (UnitOfWork) -> UnitOfWork = {
        uow ->
        val event : Event? = uow.event
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
        uow.copy(putRequest = putRequest)
    }

    suspend fun collect(fromFlow: Flow<UnitOfWork>) {

        val flow = fromFlow
            .filter { uom -> uom.event != null }
            .filterEventTypes(*onEventClass.toTypedArray())
            .onEach {  printStartPipeline(it) }
            .filter {
                faulty(it) {
                    onContentType(it)
                }
            }
            .map {
                faulty(it) {
                    it.copy(key = correlationKey(it))
                }
            }
            .onEach { uom -> putRequest(uom) }
            .onEach {  printEndPipeline(it) }

    }

}




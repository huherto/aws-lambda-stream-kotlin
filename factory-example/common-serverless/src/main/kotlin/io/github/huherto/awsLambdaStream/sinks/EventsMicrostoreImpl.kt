package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.mapNotNull
import mu.KotlinLogging

class EventsMicrostoreImpl constructor(
    private val envConfig: EnvironmentConfig,
    private val dynamoDbClient: DynamoDbClient,
    private val faultManager: FaultManager,
    private val bufferCapacity: Int = Channel.Factory.BUFFERED,
): EventsMicrostore {

    private val logger = KotlinLogging.logger {  }

    override fun save(flow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty{ uow -> putRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotNull { uow -> faulty(uow) { putDynamoDb(uow) } }
        }
    }

    fun nullableS(s: String?) : AttributeValue {
        return s?.let { AttributeValue.S(it) } ?: AttributeValue.Null(true)
    }

    private fun omitRaw(event: Event?): String {
        throw RuntimeException("Not implemented yet")
    }

    fun putRequest(uow: UnitOfWork) : UnitOfWork {
        val event: Event? = uow.event
        val savedOptions = uow.saveOptions ?: EventsMicrostore.SaveOptions()
        val encodedEvent = if (savedOptions.includeRaw) event?.encoded() else omitRaw(event)
        val ttl = savedOptions.ttlTimestampInSecs
        val expire = savedOptions.expire
        val timeStamp = event?.timestamp
        val awsRegion = envConfig.awsRegion()

        val itemValues = mapOf(
            "pk" to nullableS(event?.id),
            "sk" to AttributeValue.S("EVENT"),
            "discriminator" to AttributeValue.S("EVENT"),
            "timestamp" to AttributeValue.N(timeStamp.toString()),
            "awsregion" to AttributeValue.S(awsRegion),
            "ttl" to AttributeValue.N(ttl.toString()),
            "expire" to nullableS(expire),
            "data" to nullableS(uow.key),
            "event" to nullableS(encodedEvent)
        )

        val putRequest = PutItemRequest.Companion {
            tableName = envConfig.tableName() ?: "events"
            item = itemValues
        }
        return uow.copy(putRequest = putRequest)
    }

    private val putDynamoDb: suspend (UnitOfWork) -> UnitOfWork = { uow ->
        val putResponse = uow.putRequest?.let {
            dynamoDbClient.putItem(uow.putRequest)
        }
        uow.copy(putResponse = putResponse)
    }

}
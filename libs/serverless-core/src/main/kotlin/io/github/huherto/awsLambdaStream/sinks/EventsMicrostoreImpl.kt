package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.utils.nullableBool
import io.github.huherto.awsLambdaStream.utils.nullableN
import io.github.huherto.awsLambdaStream.utils.nullableS
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

    private fun omitRaw(event: Event?): String {
        throw RuntimeException("Not implemented yet")
    }

    internal fun putRequest(uow: UnitOfWork) : UnitOfWork {

        val event: Event? = uow.event

        if (uow.saveOptions == null) return uow.copy(putRequest = null)
        val itemValues = with(uow.saveOptions) {
            val encodedEvent = if (includeRaw) event?.encoded() else omitRaw(event)
            mapOf(
                "pk" to nullableS(pk),
                "sk" to nullableS(sk),
                "discriminator" to nullableS(discriminator),
                "timestamp" to nullableN(timeStamp),
                "awsregion" to nullableS(awsRegion),
                "sequenceNumber" to nullableS(sequenceNumber),
                "ttl" to nullableN(ttl?.toString()),
                "expire" to nullableBool(expire),
                "suffix" to nullableS(suffix),
                "data" to nullableS(data),
                "pipelineId" to nullableS(pipelineId),
                "event" to nullableS(encodedEvent),
            )
        }

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
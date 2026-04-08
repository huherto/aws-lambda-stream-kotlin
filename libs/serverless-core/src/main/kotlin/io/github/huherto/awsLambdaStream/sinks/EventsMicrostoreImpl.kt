package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.queries.queryAllDynamoDB
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

    override fun queryByPk(flow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty{ uow -> toQueryRequest(uow) }
                .buffer(bufferCapacity)
                .queryAllDynamoDB(dynamoDbClient)
                .mapNotNull { uow -> faulty(uow) { toCorrelated(uow) } }
        }
    }

    internal fun toQueryRequest(uow: UnitOfWork) : UnitOfWork {
        val pk = uow.meta?.get("pk")
        val isCorrelation = uow.meta?.get("correlation").toBoolean()
        if (!isCorrelation || pk.isNullOrEmpty()) {
            return uow
        }

        val request = QueryRequest {
            keyConditionExpression = "#pk = :pk"
            expressionAttributeNames = mapOf("#pk" to "pk")
            expressionAttributeValues = mapOf(":pk" to AttributeValue.S(pk))
            consistentRead = true
        }

        return uow.copy(queryRequest = request)
    }

    internal fun unmarshall(eventAsString: String) : Event {
        val jsonEvent: JsonEvent = try {
            JsonEvent(eventAsString)
        } catch (e: Exception) {
            logger.error {"Failed to parse event: $eventAsString, $e" }
            throw e
        }
        return jsonEvent
    }

    internal fun toCorrelated(uow: UnitOfWork): UnitOfWork {
        if (uow.queryResponse == null) return uow

        val correlatedEvents = uow.queryResponse.items?.mapNotNull { item ->
            val eventString = (item["event"] as? AttributeValue.S)?.value
            eventString?.let { unmarshall(it) }
        }
        return uow.copy(
            correlated = correlatedEvents
        )
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

    private val queryDynamoDb: suspend (UnitOfWork) -> UnitOfWork = { uow ->
        val queryResponse = uow.queryRequest?.let {
            dynamoDbClient.query(uow.queryRequest)
        }
        uow.copy(queryResponse = queryResponse)
    }

}
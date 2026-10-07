package io.kopipes.aws.sinks

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.awsEnvConfig
import io.kopipes.aws.connectors.DynamoDbClientFactory
import io.kopipes.aws.extensions.*
import io.kopipes.aws.utils.nullableBool
import io.kopipes.aws.utils.nullableN
import io.kopipes.aws.utils.nullableS
import io.kopipes.core.Event
import io.kopipes.core.JsonEvent
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.metrics.withStepMetrics
import io.kopipes.core.sinks.EventsMicrostore
import io.kopipes.core.sinks.queryParams
import io.kopipes.core.sinks.saveOptions
import io.kopipes.core.utils.omit
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import mu.KotlinLogging

/** DynamoDB-backed implementation of [EventsMicrostore]. */
open class DynamoDbEventsMicrostore @JvmOverloads constructor(
    private val dynamoDbClientFactory: DynamoDbClientFactory = AwsGlobalRegistry.dynamoDbClientFactory(),
    private val faultManager: FaultManager = AwsGlobalRegistry.faultManager(),
    private val bufferCapacity: Int = Channel.BUFFERED,
    private val tableName: String = awsEnvConfig().tableName() ?: "events",
) : EventsMicrostore {

    private val logger = KotlinLogging.logger { }

    init {
        logger.info { "DynamoDbEventsMicrostore initialized with tableName: $tableName" }
    }

    private fun omitRaw(event: Event?): String {
        if (event == null) return ""
        return omit(event, "raw")
    }

    internal fun toQueryRequest(uow: UnitOfWork): UnitOfWork {
        val queryParams = uow.queryParams ?: return uow
        val pk = queryParams.pk
        val isCorrelation = queryParams.correlation
        val data = queryParams.data

        val targetTable = this.tableName
        if (isCorrelation) {
            if (pk.isNullOrEmpty()) return uow
            val request = QueryRequest {
                tableName = targetTable
                keyConditionExpression = "#pk = :pk"
                expressionAttributeNames = mapOf("#pk" to "pk")
                expressionAttributeValues = mapOf(":pk" to AttributeValue.S(pk))
                consistentRead = true
            }
            return uow.withQueryRequest(request)
        } else {
            if (data.isNullOrEmpty()) return uow
            val request = QueryRequest {
                tableName = targetTable
                indexName = queryParams.index ?: "DataIndex"
                keyConditionExpression = "#data = :data"
                expressionAttributeNames = mapOf("#data" to "data")
                expressionAttributeValues = mapOf(":data" to AttributeValue.S(data))
            }
            return uow.withQueryRequest(request)
        }
    }

    internal fun unmarshall(eventAsString: String): Event {
        val jsonEvent: JsonEvent = try {
            JsonEvent(eventAsString)
        } catch (e: Exception) {
            logger.error { "Failed to parse event: $eventAsString, $e" }
            throw e
        }
        return jsonEvent
    }

    internal fun toCorrelated(uow: UnitOfWork): UnitOfWork {
        if (uow.queryResponse == null) return uow

        val correlatedEvents = uow.queryResponse?.items?.mapNotNull { item ->
            val eventString = (item["event"] as? AttributeValue.S)?.value
            eventString?.let { unmarshall(it) }
        }
        return uow.copy(
            correlated = correlatedEvents
        )
    }

    internal fun putRequest(uow: UnitOfWork): UnitOfWork {
        val event: Event? = uow.event

        val saveOptions = uow.saveOptions ?: return uow.withPutRequest(null)
        val itemValues = with(saveOptions) {
            val encodedEvent = if (includeRaw) event?.toString() else omitRaw(event)
            mapOf(
                "pk" to nullableS(pk),
                "sk" to nullableS(sk),
                "discriminator" to nullableS(discriminator),
                "timestamp" to nullableN(timeStamp?.toString()),
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

        val targetTable = this.tableName
        val putRequest = PutItemRequest {
            tableName = targetTable
            item = itemValues
        }
        return uow.withPutRequest(putRequest)
    }

    override fun save(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty { uow -> putRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotFaulty { uow ->
                    uow.withStepMetrics("save") { uowWithMetrics ->
                        putDynamoDb(uowWithMetrics)
                    }
                }
        }
    }

    override fun queryByPk(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty { uow -> toQueryRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotFaulty { uow ->
                    uow.withStepMetrics("query") { uowWithMetrics ->
                        queryDynamoDb(uowWithMetrics)
                    }
                }
                .mapNotFaulty { uow -> toCorrelated(uow) }
        }
    }

    private fun getClient(uow: UnitOfWork): DynamoDbClient {
        val pipelineId = uow.pipeline?.id ?: "unknown"
        return dynamoDbClientFactory.getClient(pipelineId)
    }

    private suspend fun putDynamoDb(uow: UnitOfWork): UnitOfWork {
        val client = getClient(uow)
        val putResponse = uow.putRequest?.let {
            client.putItem(it)
        }
        return uow.withPutResponse(putResponse)
    }

    private suspend fun queryDynamoDb(uow: UnitOfWork): UnitOfWork {
        val client = getClient(uow)
        val queryResponse = uow.queryRequest?.let {
            client.query(it)
        }
        return uow.withQueryResponse(queryResponse)
    }
}

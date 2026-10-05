package io.kopipes.aws.sinks

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import io.kopipes.aws.extensions.queryResponse
import io.kopipes.aws.extensions.withPutRequest
import io.kopipes.aws.extensions.withQueryRequest
import io.kopipes.aws.utils.nullableBool
import io.kopipes.aws.utils.nullableN
import io.kopipes.aws.utils.nullableS
import io.kopipes.core.Event
import io.kopipes.core.JsonEvent
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventsMicrostore
import io.kopipes.core.sinks.queryParams
import io.kopipes.core.sinks.saveOptions
import io.kopipes.core.utils.omit
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging

abstract class BaseEventsMicrostore(
    protected val faultManager: FaultManager,
    protected val bufferCapacity: Int = Channel.Factory.BUFFERED,
    protected val tableName: String,
) : EventsMicrostore {

    private val logger = KotlinLogging.logger { }

    init {
        logger.info { "BaseEventsMicrostore initialized with tableName: $tableName" }
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
                indexName = uow.queryParams?.index ?: "DataIndex"
                keyConditionExpression = "#data = :data"
                expressionAttributeNames = mapOf("#data" to "data")
                expressionAttributeValues = mapOf(":data" to AttributeValue.S(data))
                consistentRead = true
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
}

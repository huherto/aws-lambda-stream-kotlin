package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.JsonEvent
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.utils.nullableBool
import io.github.huherto.awsLambdaStream.utils.nullableN
import io.github.huherto.awsLambdaStream.utils.nullableS
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging

abstract class BaseEventsMicrostore(
    protected val faultManager: FaultManager,
    protected val bufferCapacity: Int = Channel.Factory.BUFFERED,
    protected val tableName: String = "events",
): EventsMicrostore {

    private val logger = KotlinLogging.logger {  }

    private fun omitRaw(event: Event?): String {
        throw RuntimeException("Not implemented yet")
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
            tableName = this@BaseEventsMicrostore.tableName
            item = itemValues
        }
        return uow.copy(putRequest = putRequest)
    }
}
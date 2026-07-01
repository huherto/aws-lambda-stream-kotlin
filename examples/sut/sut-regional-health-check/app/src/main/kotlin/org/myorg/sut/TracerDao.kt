package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemResponse
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.smithy.kotlin.runtime.content.ByteStream
import io.github.huherto.awsLambdaStream.sinks.DynamoDbUpdateValue
import io.github.huherto.awsLambdaStream.sinks.timestampCondition
import io.github.huherto.awsLambdaStream.sinks.updateExpression
import mu.KotlinLogging
import kotlin.math.roundToLong

const val DISCRIMINATOR = "trace"

data class Tracer (
    val awsRegion: String,
    val roundedTimestamp: Long,
    val timestamp: Long,
    val ttl: Long,
    val status: String,
)

class TracerDao(
    private val connector: Connector,
    private val awsRegion: String,
) {

    private val logger = KotlinLogging.logger {  }

    suspend fun check( unhealthyFlag: Boolean? = null): HealthCheckResponse {
        val roundedTimestamp = roundToNearestMinute(now())

        if (unhealthyFlag == true) {
            return HealthCheckResponse(
                statusCode = 503,
                timestamp = roundedTimestamp,
                region = awsRegion,
            )
        }

        val currentTracers = getTracers()
        val toBeSaved = Tracer(
            awsRegion = awsRegion,
            roundedTimestamp = roundedTimestamp,
            timestamp = now(),
            ttl = ttl(now(), 92),
            status = "STARTED")
        val save = save(toBeSaved)
        logger.info { "tracers=$currentTracers save=$toBeSaved" }

        val mostRecent = currentTracers.firstOrNull()
        val incomplete = mostRecent?.status == "STARTED"

        val sk = mostRecent?.roundedTimestamp ?: 0L
        val elapsed = (roundedTimestamp - sk).toDouble() / 1000.0 / 60.0

        // Is the most recent trace incomplete, or is it older than 1 minute?
        val unhealthyCheck = incomplete || elapsed > 1


        return HealthCheckResponse(
            statusCode = if (unhealthyCheck) 503 else 200,
            timestamp = roundedTimestamp,
            region = awsRegion,
            incomplete = incomplete,
            elapsed = elapsed,
            tracers = currentTracers,
            saveResponse = save.toString(),
        )
    }

    suspend fun getTracers(): List<Tracer> {
        return connector.get(awsRegion).map {
            Tracer(
                awsRegion = awsRegion,
                roundedTimestamp = it["sk"]?.asStringOrNull()?.toLongOrNull() ?: 0L,
                timestamp = it["timestamp"]?.asStringOrNull()?.toLongOrNull() ?: 0L,
                ttl = it["ttl"]?.asStringOrNull()?.toLongOrNull() ?: 0L,
                status = it["status"]?.asStringOrNull() ?: "UNKNOWN",
            )
        }
    }


    suspend fun save(tracer: Tracer) : UpdateItemResponse {
        return connector.update(
            key = mapOf(
                "pk" to AttributeValue.S(tracer.awsRegion),
                "sk" to AttributeValue.S(tracer.roundedTimestamp.toString()),
            ),
            inputParams = mapOf(
                "timestamp" to DynamoDbUpdateValue.DbSet(AttributeValue.N(tracer.timestamp.toString())),
                "status" to DynamoDbUpdateValue.DbSet(AttributeValue.S(tracer.status)),
                "discriminator" to DynamoDbUpdateValue.DbSet(AttributeValue.S(DISCRIMINATOR)),
                "latched" to DynamoDbUpdateValue.DbRemove,
                "ttl" to DynamoDbUpdateValue.DbSet(AttributeValue.N(tracer.ttl.toString())),
                "awsregion" to DynamoDbUpdateValue.DbSet(AttributeValue.S(tracer.awsRegion)),
            ),
        )
    }
}

data class HealthCheckResponse(
    val statusCode: Int,
    val timestamp: Long,
    val region: String?,
    val incomplete: Boolean? = null,
    val elapsed: Double? = null,
    val tracers: List<Tracer>? = null,
    val saveResponse: String? = null,
)

fun toUpdateRequest(uow: UnitOfWork): UpdateItemRequest {
    val timestamp = now()

    val expression = updateExpression(
        mapOf(
            "status" to DynamoDbUpdateValue.DbSet(AttributeValue.S("COMPLETED")),
            "discriminator" to DynamoDbUpdateValue.DbSet(AttributeValue.S(DISCRIMINATOR)),
            "timestamp" to DynamoDbUpdateValue.DbSet(AttributeValue.N(timestamp.toString())),
            "latency" to DynamoDbUpdateValue.DbSet(
                AttributeValue.N(((timestamp - uow.event.raw.new.timestamp).toDouble() / 1000.0).toString()),
            ),
            "latched" to DynamoDbUpdateValue.DbSet(AttributeValue.Bool(true)),
            "ttl" to DynamoDbUpdateValue.DbSet(AttributeValue.N(ttl(uow.event.timestamp, 92).toString())),
            "awsregion" to DynamoDbUpdateValue.DbSet(AttributeValue.S(System.getenv("AWS_REGION"))),
        ),
    )

    val condition = timestampCondition()

    return UpdateItemRequest {
        key = mapOf(
            "pk" to AttributeValue.S(System.getenv("AWS_REGION")),
            "sk" to AttributeValue.S(uow.event.raw.new.sk),
        )
        expressionAttributeNames = expression.expressionAttributeNames
        expressionAttributeValues = expression.expressionAttributeValues
        updateExpression = expression.updateExpression
        conditionExpression = condition["ConditionExpression"]
    }
}

fun toS3PutRequest(uow: UnitOfWork): PutObjectRequest =
    PutObjectRequest {
        key = "${uow.event.raw.new.pk}/${uow.event.raw.new.sk}"
        body = ByteStream.fromString(uow.event.toJson())
    }

fun now(): Long =
    System.currentTimeMillis()

fun roundToNearestMinute(timestamp: Long): Long {
    val minute = 60_000L
    return (timestamp.toDouble() / minute).roundToLong() * minute
}

fun ttl(timestamp: Long, minutes: Long): Long =
    (timestamp / 1000L) + (minutes * 60L)

private fun AttributeValue.asStringOrNull(): String? =
    when (this) {
        is AttributeValue.S -> value
        is AttributeValue.N -> value
        else -> null
    }

/**
 * Replace these DTOs with your real stream/event model if one already exists.
 */
data class UnitOfWork(
    val event: StreamEvent,
)

data class StreamEvent(
    val timestamp: Long,
    val raw: RawEvent,
) {
    fun toJson(): String =
        """{"timestamp":$timestamp,"raw":{"new":{"pk":"${raw.new.pk}","sk":"${raw.new.sk}","timestamp":${raw.new.timestamp}}}}"""
}

data class RawEvent(
    val new: NewImage,
)

data class NewImage(
    val pk: String,
    val sk: String,
    val timestamp: Long,
)
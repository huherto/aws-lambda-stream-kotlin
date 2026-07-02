package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemResponse
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.smithy.kotlin.runtime.content.ByteStream
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.asJson
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.sinks.DynamoDbUpdateValue
import io.github.huherto.awsLambdaStream.sinks.timestampCondition
import io.github.huherto.awsLambdaStream.sinks.updateExpression
import io.github.huherto.awsLambdaStream.utils.ttl
import mu.KotlinLogging
import kotlin.math.roundToLong
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.milliseconds

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
        val now = System.currentTimeMillis()
        val roundedTimestamp = roundToNearestMinute(now)

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
            timestamp = now,
            ttl = ttl(now, 92.days),
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

fun toUpdateRequest(uow: UnitOfWork): UpdateItemRequest? {

    val raw = uow.event?.raw as? RecordPair ?: return null
    val newRaw = raw.new ?: return null
    val timestamp = System.currentTimeMillis()
    val pk = newRaw.getS("pk") ?: return null
    val sk = newRaw.getS("sk") ?: return null
    val startTimestamp = newRaw.getS("timestamp")?.toLongOrNull() ?: return null
    val latency = (timestamp - startTimestamp).milliseconds.inWholeSeconds
    val ttl = ttl(timestamp, 92.days)

    val expression = updateExpression(
        mapOf(
            "status" to DynamoDbUpdateValue.DbSet(AttributeValue.S("COMPLETED")),
            "discriminator" to DynamoDbUpdateValue.DbSet(AttributeValue.S(DISCRIMINATOR)),
            "timestamp" to DynamoDbUpdateValue.DbSet(AttributeValue.N(timestamp.toString())),
            "latency" to DynamoDbUpdateValue.DbSet(
                AttributeValue.N(latency.toString()),
            ),
            "latched" to DynamoDbUpdateValue.DbSet(AttributeValue.Bool(true)),
            "ttl" to DynamoDbUpdateValue.DbSet(AttributeValue.N(ttl.toString())),
            "awsregion" to DynamoDbUpdateValue.DbSet(AttributeValue.S(System.getenv("AWS_REGION"))),
        ),
    )

    val condition = timestampCondition()

    return UpdateItemRequest {
        key = mapOf(
            "pk" to AttributeValue.S(pk),
            "sk" to AttributeValue.S(sk),
        )
        expressionAttributeNames = expression.expressionAttributeNames
        expressionAttributeValues = expression.expressionAttributeValues
        updateExpression = expression.updateExpression
        conditionExpression = condition["ConditionExpression"]
    }
}

private val logger = KotlinLogging.logger {  }

fun toS3PutRequest(uow: UnitOfWork): PutObjectRequest? {
    val raw = uow.event?.raw as? RecordPair ?: return null
    val newRaw = raw.new ?: return null
    logger.info("before newRaw=$newRaw")
    val pk = newRaw.getS("pk") ?: return null
    val sk = newRaw.getS("sk") ?: return null
    logger.info { "pk=$pk sk=$sk" }
    return PutObjectRequest {
        key = "${pk}/${sk}"
        body = ByteStream.fromString(uow.event.asJson())
    }
}

fun roundToNearestMinute(timestamp: Long): Long {
    val minute = 60_000L
    return (timestamp.toDouble() / minute).roundToLong() * minute
}

private fun AttributeValue.asStringOrNull(): String? =
    when (this) {
        is AttributeValue.S -> value
        is AttributeValue.N -> value
        else -> null
    }

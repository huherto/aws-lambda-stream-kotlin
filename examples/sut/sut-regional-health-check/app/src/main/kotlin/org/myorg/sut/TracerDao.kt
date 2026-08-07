package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemResponse
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.smithy.kotlin.runtime.content.ByteStream
import io.github.huherto.awsLambdaStream.BaseEvent
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.from.TableChangeEvent
import io.github.huherto.awsLambdaStream.sinks.DynamoDbUpdateValue
import io.github.huherto.awsLambdaStream.sinks.timestampCondition
import io.github.huherto.awsLambdaStream.sinks.updateExpression
import io.github.huherto.awsLambdaStream.utils.ttl
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import java.util.*
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.milliseconds

const val DISCRIMINATOR = "trace"

private val tracerEventJson = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
}

@Serializable
data class Tracer (
    val awsRegion: String,  // pk
    val roundedTimestamp: Long,  // sk
    val timestamp: Long,
    val ttl: Long,
    val status: String,
)

@Serializable
class TracerEvent(
    val tracer: Tracer,
) : BaseEvent() {

    val type: String = "tracer"

    override fun eventType(): String = "tracer"

    @Deprecated(
        message = "Use EventCodec or the configured framework publisher instead.",
    )
    override fun encoded(): String = TracerEventCodec.encode(this)
}

object TracerEventCodec : EventCodec {
    override fun decode(eventAsString: String): Event {
        return tracerEventJson.decodeFromString<TracerEvent>(eventAsString)
    }

    override fun encode(event: Event): String {
        require(event is TracerEvent) {
            "TracerEventCodec can only encode TracerEvent instances, but received ${event::class.qualifiedName}"
        }

        return tracerEventJson.encodeToString(event)
    }
}

class TracerDao(
    private val connector: Connector,
    private val awsRegion: String,
) {

    private val logger = KotlinLogging.logger {  }

    suspend fun check( unhealthyFlag: Boolean? = null): HealthCheckResponse {
        val now = System.currentTimeMillis()
        val truncatedTimestamp = truncateToMinute(now)

        if (unhealthyFlag == true) {
            return HealthCheckResponse(
                statusCode = 503,
                timestamp = truncatedTimestamp,
                region = awsRegion,
            )
        }

        val currentTracers = getTracers()
        val toBeSaved = Tracer(
            awsRegion = awsRegion,
            roundedTimestamp = truncatedTimestamp,
            timestamp = now,
            ttl = ttl(now, 92.days),
            status = "STARTED")
        val save = save(toBeSaved)
        logger.info { "tracers=$currentTracers save=$toBeSaved" }

        val mostRecent = currentTracers.firstOrNull()
        val incomplete = mostRecent?.status == "STARTED"

        val sk = mostRecent?.roundedTimestamp ?: 0L
        val elapsed = (truncatedTimestamp - sk).toDouble() / 1000.0 / 60.0

        // Is the most recent trace incomplete, or is it older than 1 minute?
        val unhealthyCheck = incomplete || elapsed > 1

        return HealthCheckResponse(
            statusCode = if (unhealthyCheck) 503 else 200,
            timestamp = truncatedTimestamp,
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

private val logger = KotlinLogging.logger {  }

fun toUpdateRequest(uow: UnitOfWork): UpdateItemRequest? {

    val timestamp = System.currentTimeMillis()

    val event = uow.event as? TracerEvent
    if (event == null) {
        logger.error { "Cannot build DynamoDb update request: event is not a TracerEvent. event=${uow.event}, classType=${uow.event?.let { it::class.qualifiedName }}" }
        return null
    }

    val pk = event.tracer.awsRegion
    val sk = event.tracer.roundedTimestamp.toString()
    val startTimestamp = event.tracer.timestamp
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
        tableName = System.getenv("ENTITY_TABLE_NAME")
        expressionAttributeNames = expression.expressionAttributeNames
        expressionAttributeValues = expression.expressionAttributeValues
        updateExpression = expression.updateExpression
        conditionExpression = condition["ConditionExpression"]
    }
}


fun toS3PutRequest(uow: UnitOfWork): PutObjectRequest? {
    val event = uow.event as? TableChangeEvent
    if (event == null) {
        logger.error { "Cannot build S3 put request: event is not a TableChangeEvent. event=${uow.event}" }
        return null
    }

    val raw = uow.event?.raw as? RecordPair
    if (raw == null) {
        logger.error { "Cannot build S3 put request: event raw is not RecordPair. event=${uow.event}" }
        return null
    }

    val newRaw = raw.new
    if (newRaw == null) {
        logger.error { "Cannot build S3 put request: raw.new is null. event=${uow.event}" }
        return null
    }

    val pk = newRaw.getS("pk")
    val sk = newRaw.getS("sk")

    if (pk == null || sk == null) {
        logger.error {
            "Cannot build S3 put request: pk or sk missing. pk=$pk, sk=$sk, event=${uow.event}"
        }
        return null
    }

    val timestamp = newRaw.getLong("timestamp") ?: 0

    val s3Key = "${pk}/${sk}"
    logger.info { "Writing tracer event to S3. s3Key=$s3Key" }

    val tracer = Tracer(
        awsRegion = pk,
        roundedTimestamp = sk.toLongOrNull() ?: 0,
        timestamp = timestamp,
        ttl = 0,
        status = "STARTED"
    )

    val tracerEvent = TracerEvent(tracer).apply {
        id = UUID.randomUUID().toString()
        partitionKey = tracer.awsRegion
    }

    val eventAsString = TracerEventCodec.encode(tracerEvent)

    return PutObjectRequest {
        key = "${pk}/${sk}"
        body = ByteStream.fromString(eventAsString)
    }
}

fun truncateToMinute(timestamp: Long): Long {
    val minute = 60_000L
    return timestamp / minute * minute
}

private fun AttributeValue.asStringOrNull(): String? =
    when (this) {
        is AttributeValue.S -> value
        is AttributeValue.N -> value
        else -> null
    }

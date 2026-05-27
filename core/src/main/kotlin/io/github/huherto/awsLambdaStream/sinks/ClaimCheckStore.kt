package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectResponse
import aws.smithy.kotlin.runtime.content.ByteStream
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.S3ClientFactory
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * S3-backed Claim Check store.
 *
 * Implements the claim-check pattern by storing the full event payload in S3 and replacing it with
 * a lightweight event that points to the stored object.
 *
 * S3 keys are formatted as:
 *
 * AWS_REGION/claimchecks/YYYY/MM/DD/HH/eventId
 */
class ClaimCheckStore(
    private val envConfig: EnvironmentConfig,
    private val s3ClientFactory: S3ClientFactory,
    private val faultManager: FaultManager,
    private val claimCheckBucketName: String? = System.getenv("CLAIMCHECK_BUCKET_NAME"),
    private val clock: Clock = Clock.systemUTC(),
    private val bufferCapacity: Int = Channel.BUFFERED,
) {
    data class ClaimCheck(
        val bucket: String,
        val key: String,
    )

    data class ClaimCheckEvent(
        override var id: String?,
        private val type: String,
        override var partitionKey: String?,
        override var timestamp: Long?,
        override var tags: Map<String, String>?,
        val s3: ClaimCheck,
        override var raw: Any? = null,
        override var eem: Any? = null,
        override var triggers: List<io.github.huherto.awsLambdaStream.EventReference>? = null,
    ) : BaseEvent() {
        override fun eventType(): String = type

        override fun encoded(): String {
            return """
                {
                  "id": ${id?.let { "\"$it\"" }},
                  "type": "$type",
                  "partitionKey": ${partitionKey?.let { "\"$it\"" }},
                  "timestamp": $timestamp,
                  "tags": $tags,
                  "s3": {
                    "bucket": "${s3.bucket}",
                    "key": "${s3.key}"
                  }
                }
            """.trimIndent()
        }
    }

    private val keyDateFormatter: DateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy/MM/dd/HH").withZone(ZoneOffset.UTC)

    fun formatKey(event: Event): String {
        val region = envConfig.awsRegion()
        val timestamp = keyDateFormatter.format(Instant.now(clock))
        val eventId = event.id ?: error("Cannot create claim-check key for event without id")

        return "$region/claimchecks/$timestamp/$eventId"
    }

    fun toClaimCheckEvent(
        event: Event,
        bucket: String,
    ): ClaimCheckEvent {
        return ClaimCheckEvent(
            id = event.id,
            type = event.eventType(),
            partitionKey = event.partitionKey,
            timestamp = event.timestamp,
            tags = event.tags,
            s3 = ClaimCheck(
                bucket = bucket,
                key = formatKey(event),
            ),
        )
    }

    fun toPutClaimCheckRequest(
        event: Event,
        bucket: String,
    ): PutObjectRequest {
        return PutObjectRequest {
            this.bucket = bucket
            this.key = formatKey(event)
            this.body = ByteStream.fromString(event.encoded())
        }
    }

    /**
     * Stores full event payloads in S3.
     *
     * If [claimCheckBucketName] is not configured, this returns the original flow unchanged.
     *
     * This is designed to run after batching. When a [UnitOfWork] contains [UnitOfWork.batch],
     * each item in the batch is stored independently and the batch is reassembled.
     */
    fun storeClaimCheck(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        val bucket = claimCheckBucketName

        if (bucket.isNullOrBlank()) {
            return flow
        }

        with(faultManager) {
            return flow
                .mapNotFaulty { uow -> storeBatchOrSingle(uow, bucket) }
                .buffer(bufferCapacity)
        }
    }

    private suspend fun storeBatchOrSingle(
        uow: UnitOfWork,
        bucket: String,
    ): UnitOfWork {
        val batch = uow.batch

        if (batch == null) {
            return storeSingle(uow, bucket)
        }

        return uow.copy(
            batch = batch.map { item ->
                storeSingle(item, bucket)
            },
        )
    }

    private suspend fun storeSingle(
        uow: UnitOfWork,
        bucket: String,
    ): UnitOfWork {
        val event = uow.event ?: return uow
        val putRequest = toPutClaimCheckRequest(event, bucket)
        val client = getClient(uow)

        val putResponse: PutObjectResponse = client.putObject(putRequest)

        return uow
            .copy(event = toClaimCheckEvent(event, bucket))
            .copyS3 {
                copy(
                    putRequest = putRequest,
                    putResponse = putResponse,
                )
            }
    }

    private fun getClient(uow: UnitOfWork) =
        s3ClientFactory.getClient(uow.pipeline?.id ?: "unknown")
}
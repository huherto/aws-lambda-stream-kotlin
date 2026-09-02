@file:OptIn(kotlin.time.ExperimentalTime::class)
package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectResponse
import aws.smithy.kotlin.runtime.content.ByteStream
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.S3ClientFactory
import io.github.huherto.awsLambdaStream.extensions.copyS3
import io.github.huherto.awsLambdaStream.faults.FaultManager
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.datetime.Clock
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.Serializable

/** S3-backed Claim Check store. */
class ClaimCheckStore(
    private val s3ClientFactory: S3ClientFactory,
    private val faultManager: FaultManager = GlobalRegistry.faultManager(),
    private val claimCheckBucketName: String? = java.lang.System.getenv("CLAIMCHECK_BUCKET_NAME"),
    private val clock: Clock = kotlinx.datetime.Clock.System,
    private val bufferCapacity: Int = Channel.BUFFERED,
) {
    /** Pointer to an event payload in S3. */
    @Serializable
    data class ClaimCheck(
        val bucket: String,
        val key: String,
    )

    /** Event containing a claim check pointer. */
    data class ClaimCheckEvent(
        override val id: String?,
        private val type: String,
        override val partitionKey: String?,
        override val timestamp: Long?,
        override val tags: Map<String, String>?,
        override val raw: RawRecord? = null,
        override val eem: EnvelopeEncryptionMetadata? = null,
        override val triggers: List<EventReference>? = null,
        val s3: ClaimCheck,
    ) : Event {

        override fun eventType(): String = type

        override fun copyEvent(
            id: String?,
            timestamp: Long?,
            partitionKey: String?,
            tags: Map<String, String>?,
            raw: RawRecord?,
            eem: EnvelopeEncryptionMetadata?,
            triggers: List<EventReference>?
        ): Event = copy(
            id = id,
            timestamp = timestamp,
            partitionKey = partitionKey,
            tags = tags,
            raw = raw,
            eem = eem,
            triggers = triggers
        )

        override fun toString(): String {
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

    fun formatKey(event: Event): String {
        val region = envConfig().awsRegion()
        val instant = clock.now()
        val dateTime = instant.toLocalDateTime(TimeZone.UTC)
        val year = dateTime.year
        val month = dateTime.monthNumber.toString().padStart(2, '0')
        val day = dateTime.dayOfMonth.toString().padStart(2, '0')
        val hour = dateTime.hour.toString().padStart(2, '0')

        val timestamp = "$year/$month/$day/$hour"
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
            this.body = ByteStream.fromString(event.toString())
        }
    }

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
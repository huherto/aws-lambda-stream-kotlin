package io.github.huherto.awsLambdaStream.from

import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.copyS3
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

class S3Adapter(
    private val faultManager: FaultManager,
    private val eventCodec: EventCodec,
    private val s3Connector: S3Connector,
) {
    private val json = Json {
        ignoreUnknownKeys = true
    }

    data class S3RecordEnvelope(
        val sqs: SQSEvent.SQSMessage? = null,
        val sns: SnsMessage? = null,
        val s3Event: S3Event? = null,
    )

    @Serializable
    data class SqsEvent(
        @SerialName("Records")
        val records: List<SqsRecord> = emptyList(),
    )

    @Serializable
    data class SqsRecord(
        val body: String,
    )

    @Serializable
    data class SnsMessage(
        @SerialName("Message")
        val message: String,
    )

    @Serializable
    data class S3Event(
        @SerialName("Records")
        val records: List<S3Record> = emptyList(),
    )

    @Serializable
    data class S3Record(
        val eventSource: String? = null,
        val awsRegion: String? = null,
        val responseElements: Map<String, String> = emptyMap(),
        val s3: S3Entity,
    )

    @Serializable
    data class S3Entity(
        val bucket: S3Bucket,
        val `object`: S3Object,
    )

    @Serializable
    data class S3Notification(
        val bucket: S3Bucket,
        val `object`: S3Object,
    )

    @Serializable
    data class S3Bucket(
        val name: String,
        val arn: String? = null,
    )

    @Serializable
    data class S3Object(
        val key: String,
        val size: Long? = null,
        val eTag: String? = null,
        val sequencer: String? = null,
    )

    @Serializable
    data class S3GetRequest(
        val bucket: String,
        val key: String,
    )

    @Serializable
    data class S3GetResponse(
        val body: ByteArray,
    )

    /**
     * Intended for intra-service messages, as opposed to consuming inter-service events.
     */
    fun fromS3(event: S3Event): Flow<UnitOfWork> =
        event.records.asFlow()
            .map { s3Record ->
            UnitOfWork(
                record = s3Record,
            )
        }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun fromSqsSnsS3(event: SQSEvent): Flow<UnitOfWork> {
        with(faultManager) {
            return event.records.asFlow()
                .mapNotNull { sqsMessage ->
                    UnitOfWork(record = S3RecordEnvelope(sqs = sqsMessage))
                }
                // sns
                .mapNotFaulty { uow ->
                    val s3RecordEnvelope = uow.record as S3RecordEnvelope
                    val sns = json.decodeFromString<SnsMessage>(s3RecordEnvelope.sqs!!.body)
                    uow.copy(
                        record = s3RecordEnvelope.copy(
                            sns = sns,
                        ),
                    )
                }
                // s3
                .mapNotFaulty { uow ->
                    val s3RecordEnvelope = uow.record as S3RecordEnvelope
                    val s3 = json.decodeFromString<S3Event>(s3RecordEnvelope.sns!!.message)
                    uow.copy(
                        record = uow.record.copy(
                            s3Event = s3,
                        ),
                    )
                }
                .flatMapConcat { uow ->
                    val s3RecordEnvelope = uow.record as S3RecordEnvelope
                    fromS3(s3RecordEnvelope.s3Event!!).map { uow ->
                        val s3Record = uow.record as S3Record
                        uow.copy(
                            record = s3Record.copy(
                                s3 = s3Record.s3
                            ),
                        )
                    }
                }
        }
    }

    fun fromS3Event(
        event: SQSEvent,
    ): Flow<UnitOfWork> {

        return with(faultManager) {
            fromSqsSnsS3(event)
                .map { uow ->
                    val s3Record = uow.record as S3Record

                    val request = GetObjectRequest {
                        bucket = s3Record.s3.bucket.name
                        key = s3Record.s3.`object`.key
                    }
                    uow.copyS3 {
                        copy(getRequest = request)
                    }
                }
                .mapNotFaulty { uow ->
                    val request = uow.s3.getRequest ?: return@mapNotFaulty uow
                    val responseText = s3Connector.getObjectAsText(request, uow)
                    uow.copyS3 {
                        copy(getResponseText = responseText)
                    }
                }
                .mapNotFaulty { uow ->
                    val eventAsString = uow.s3.getResponseText ?: return@mapNotFaulty uow
                    val event = eventCodec.decode(eventAsString)
                    uow.copy(
                        event = event,
                    )
                }
        }
    }

    /**
     * Test helper.
     *
     * Equivalent to the TypeScript `toS3Records`.
     *
     * https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html
     */
    fun toS3Records(notifications: List<S3Notification>): S3Event =
        S3Event(
            records = notifications.mapIndexed { index, notification ->
                S3Record(
                    eventSource = "aws:s3",
                    awsRegion = System.getenv("AWS_REGION") ?: "us-west-2",
                    responseElements = mapOf(
                        "x-amz-request-id" to "000000000000000$index",
                    ),
                    s3 = S3Entity(
                        bucket = notification.bucket,
                        `object` = notification.`object`,
                    ),
                )
            },
        )

    fun toSqsSnsS3Records(notifications: List<S3Notification>): SqsEvent =
        SqsEvent(
            records = listOf(
                SqsRecord(
                    body = json.encodeToString(
                        SnsMessage.serializer(),
                        SnsMessage(
                            message = json.encodeToString(
                                S3Event.serializer(),
                                S3Event(
                                    records = notifications.map { s3 ->
                                        S3Record(
                                            s3 = S3Entity(
                                                bucket = s3.bucket,
                                                `object` = s3.`object`,
                                            ),
                                        )
                                    },
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
}
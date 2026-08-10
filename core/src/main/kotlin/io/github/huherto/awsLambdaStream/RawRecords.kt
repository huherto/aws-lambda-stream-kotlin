package io.github.huherto.awsLambdaStream

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.serialization.aws.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * A DynamoDB stream record, kept whole.
 *
 * [new] and [old] are derived views over [record], so nothing is duplicated and nothing about
 * the original invocation payload is lost on the way to JSON.
 */
@Serializable
@SerialName(RAW_DYNAMODB)
data class DynamodbRaw(
    @Serializable(with = DynamodbStreamRecordSerializer::class)
    @field:JsonSerialize(using = DynamodbStreamRecordJacksonSerializer::class)
    @field:JsonDeserialize(using = DynamodbStreamRecordJacksonDeserializer::class)
    val record: DynamodbEvent.DynamodbStreamRecord,
) : RawRecord, RecordPair {

    @get:JsonIgnore
    override val new: RecordImage? by lazy { record.dynamodb?.newImage?.let(::RecordImage) }

    @get:JsonIgnore
    override val old: RecordImage? by lazy { record.dynamodb?.oldImage?.let(::RecordImage) }
}

/**
 * Before/after images without an originating stream record.
 */
@Serializable
@SerialName(RAW_IMAGES)
data class ImagesRaw(
    override val new: RecordImage? = null,
    override val old: RecordImage? = null,
) : RawRecord, RecordPair

/** A Kinesis stream record, kept whole. */
@Serializable
@SerialName(RAW_KINESIS)
data class KinesisRaw(
    @Serializable(with = KinesisEventRecordSerializer::class)
    @field:JsonSerialize(using = KinesisEventRecordJacksonSerializer::class)
    @field:JsonDeserialize(using = KinesisEventRecordJacksonDeserializer::class)
    val record: KinesisEvent.KinesisEventRecord,
) : RawRecord

/** An SQS message, kept whole. */
@Serializable
@SerialName(RAW_SQS)
data class SqsRaw(
    @Serializable(with = SQSMessageSerializer::class)
    @field:JsonSerialize(using = SQSMessageJacksonSerializer::class)
    @field:JsonDeserialize(using = SQSMessageJacksonDeserializer::class)
    val message: SQSEvent.SQSMessage,
) : RawRecord

/**
 * Claim-check pointer to an event payload parked in S3.
 *
 * See https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html
 */
@Serializable
@SerialName(RAW_CLAIM_CHECK)
data class ClaimCheck(
    val bucket: String,
    val key: String,
) : RawRecord

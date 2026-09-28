package io.kopipes.aws

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kopipes.aws.from.RecordImage
import io.kopipes.aws.from.RecordPair
import io.kopipes.aws.serialization.aws.DynamodbStreamRecordSerializer
import io.kopipes.aws.serialization.aws.KinesisEventRecordSerializer
import io.kopipes.aws.serialization.aws.SQSMessageSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** A DynamoDB stream record, kept whole. */
@Serializable
@SerialName(RAW_DYNAMODB)
data class DynamodbRaw(
    @Serializable(with = DynamodbStreamRecordSerializer::class)
    val record: DynamodbEvent.DynamodbStreamRecord,
) : RawRecord, RecordPair {

    override val new: RecordImage? by lazy { record.dynamodb?.newImage?.let(::RecordImage) }

    override val old: RecordImage? by lazy { record.dynamodb?.oldImage?.let(::RecordImage) }
}

/** Before/after images without an originating stream record. */
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
    val record: KinesisEvent.KinesisEventRecord,
) : RawRecord

/** An SQS message, kept whole. */
@Serializable
@SerialName(RAW_SQS)
data class SqsRaw(
    @Serializable(with = SQSMessageSerializer::class)
    val message: SQSEvent.SQSMessage,
) : RawRecord

/** Claim-check pointer to an event payload parked in S3. */
@Serializable
@SerialName(RAW_CLAIM_CHECK)
data class ClaimCheck(
    val bucket: String,
    val key: String,
) : RawRecord

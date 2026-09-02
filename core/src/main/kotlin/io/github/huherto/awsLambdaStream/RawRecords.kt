package io.github.huherto.awsLambdaStream

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.serialization.aws.DynamodbStreamRecordSerializer
import io.github.huherto.awsLambdaStream.serialization.aws.KinesisEventRecordSerializer
import io.github.huherto.awsLambdaStream.serialization.aws.SQSMessageSerializer
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

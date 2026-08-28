package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.nio.ByteBuffer
import java.util.*

@Serializable
data class KinesisEventSurrogate(
    @SerialName("Records")
    val records: List<KinesisEventRecordSurrogate>? = null
)

@Serializable
data class KinesisEventRecordSurrogate(
    val eventID: String? = null,
    val eventName: String? = null,
    val eventSource: String? = null,
    val eventSourceARN: String? = null,
    val invokeIdentityArn: String? = null,
    val awsRegion: String? = null,
    val eventVersion: String? = null,
    val kinesis: KinesisRecordSurrogate? = null,
)

@Serializable
data class KinesisRecordSurrogate(
    val sequenceNumber: String? = null,
    val partitionKey: String? = null,
    val kinesisSchemaVersion: String? = null,
    val encryptionType: String? = null,
    @Serializable(with = DateSerializer::class)
    val approximateArrivalTimestamp: Date? = null,
    @Serializable(with = ByteBufferSerializer::class)
    val data: ByteBuffer? = null,
)

fun KinesisEvent.toSurrogate() = KinesisEventSurrogate(
    records = records?.map { it.toSurrogate() }
)

fun KinesisEvent.KinesisEventRecord.toSurrogate() = KinesisEventRecordSurrogate(
    eventID = eventID,
    eventName = eventName,
    eventSource = eventSource,
    eventSourceARN = eventSourceARN,
    invokeIdentityArn = invokeIdentityArn,
    awsRegion = awsRegion,
    eventVersion = eventVersion,
    kinesis = kinesis?.toSurrogate(),
)

fun KinesisEvent.Record.toSurrogate() = KinesisRecordSurrogate(
    sequenceNumber = sequenceNumber,
    partitionKey = partitionKey,
    kinesisSchemaVersion = kinesisSchemaVersion,
    encryptionType = encryptionType,
    approximateArrivalTimestamp = approximateArrivalTimestamp,
    data = data,
)

fun KinesisEventSurrogate.toSdk() = KinesisEvent().apply {
    records = this@toSdk.records?.map { it.toSdk() }
}

fun KinesisEventRecordSurrogate.toSdk() = KinesisEvent.KinesisEventRecord().apply {
    eventID = this@toSdk.eventID
    eventName = this@toSdk.eventName
    eventSource = this@toSdk.eventSource
    eventSourceARN = this@toSdk.eventSourceARN
    invokeIdentityArn = this@toSdk.invokeIdentityArn
    awsRegion = this@toSdk.awsRegion
    eventVersion = this@toSdk.eventVersion
    kinesis = this@toSdk.kinesis?.toSdk()
}

fun KinesisRecordSurrogate.toSdk() = KinesisEvent.Record().apply {
    sequenceNumber = this@toSdk.sequenceNumber
    partitionKey = this@toSdk.partitionKey
    kinesisSchemaVersion = this@toSdk.kinesisSchemaVersion
    encryptionType = this@toSdk.encryptionType
    approximateArrivalTimestamp = this@toSdk.approximateArrivalTimestamp
    data = this@toSdk.data
}

object KinesisEventReplayJson {
    fun encode(event: KinesisEvent): String = AwsJson.encodeToString(event.toSurrogate())
    fun decode(json: String): KinesisEvent = AwsJson.decodeFromString<KinesisEventSurrogate>(json).toSdk()
}

object KinesisEventRecordReplayJson {
    fun encode(record: KinesisEvent.KinesisEventRecord): String = AwsJson.encodeToString(record.toSurrogate())
    fun decode(json: String): KinesisEvent.KinesisEventRecord = AwsJson.decodeFromString<KinesisEventRecordSurrogate>(json).toSdk()
}

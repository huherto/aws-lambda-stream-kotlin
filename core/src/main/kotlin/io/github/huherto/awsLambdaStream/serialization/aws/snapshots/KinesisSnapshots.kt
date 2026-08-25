package io.github.huherto.awsLambdaStream.serialization.aws.snapshots

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.nio.ByteBuffer
import java.util.*

@Serializable
data class KinesisEventSnapshot(
    @SerialName("Records")
    val records: List<KinesisEventRecordSnapshot>? = null
)

@Serializable
data class KinesisEventRecordSnapshot(
    val eventID: String? = null,
    val eventName: String? = null,
    val eventSource: String? = null,
    val eventSourceARN: String? = null,
    val invokeIdentityArn: String? = null,
    val awsRegion: String? = null,
    val eventVersion: String? = null,
    val kinesis: KinesisRecordSnapshot? = null,
)

@Serializable
data class KinesisRecordSnapshot(
    val sequenceNumber: String? = null,
    val partitionKey: String? = null,
    val kinesisSchemaVersion: String? = null,
    val encryptionType: String? = null,
    @Serializable(with = DateSerializer::class)
    val approximateArrivalTimestamp: Date? = null,
    @Serializable(with = ByteBufferSerializer::class)
    val data: ByteBuffer? = null,
)

fun KinesisEvent.toSnapshot() = KinesisEventSnapshot(
    records = records?.map { it.toSnapshot() }
)

fun KinesisEvent.KinesisEventRecord.toSnapshot() = KinesisEventRecordSnapshot(
    eventID = eventID,
    eventName = eventName,
    eventSource = eventSource,
    eventSourceARN = eventSourceARN,
    invokeIdentityArn = invokeIdentityArn,
    awsRegion = awsRegion,
    eventVersion = eventVersion,
    kinesis = kinesis?.toSnapshot(),
)

fun KinesisEvent.Record.toSnapshot() = KinesisRecordSnapshot(
    sequenceNumber = sequenceNumber,
    partitionKey = partitionKey,
    kinesisSchemaVersion = kinesisSchemaVersion,
    encryptionType = encryptionType,
    approximateArrivalTimestamp = approximateArrivalTimestamp,
    data = data,
)

fun KinesisEventSnapshot.toRecord() = KinesisEvent().apply {
    records = this@toRecord.records?.map { it.toRecord() }
}

fun KinesisEventRecordSnapshot.toRecord() = KinesisEvent.KinesisEventRecord().apply {
    eventID = this@toRecord.eventID
    eventName = this@toRecord.eventName
    eventSource = this@toRecord.eventSource
    eventSourceARN = this@toRecord.eventSourceARN
    invokeIdentityArn = this@toRecord.invokeIdentityArn
    awsRegion = this@toRecord.awsRegion
    eventVersion = this@toRecord.eventVersion
    kinesis = this@toRecord.kinesis?.toRecord()
}

fun KinesisRecordSnapshot.toRecord() = KinesisEvent.Record().apply {
    sequenceNumber = this@toRecord.sequenceNumber
    partitionKey = this@toRecord.partitionKey
    kinesisSchemaVersion = this@toRecord.kinesisSchemaVersion
    encryptionType = this@toRecord.encryptionType
    approximateArrivalTimestamp = this@toRecord.approximateArrivalTimestamp
    data = this@toRecord.data
}

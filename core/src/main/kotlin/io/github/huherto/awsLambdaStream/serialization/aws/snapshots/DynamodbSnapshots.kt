package io.github.huherto.awsLambdaStream.serialization.aws.snapshots

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamViewType
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.nio.ByteBuffer
import java.util.*

@Serializable
data class DynamodbStreamRecordSnapshot(
    val eventID: String? = null,
    val eventName: String? = null,
    val eventSource: String? = null,
    val eventVersion: String? = null,
    val awsRegion: String? = null,
    val eventSourceARN: String? = null,
    val dynamodb: StreamRecordSnapshot? = null,
)

@Serializable
data class StreamRecordSnapshot(
    @SerialName("ApproximateCreationDateTime")
    @Serializable(with = DateSerializer::class)
    val approximateCreationDateTime: Date? = null,
    @SerialName("Keys")
    val keys: Map<String, AttributeValueSnapshot>? = null,
    @SerialName("NewImage")
    val newImage: Map<String, AttributeValueSnapshot>? = null,
    @SerialName("OldImage")
    val oldImage: Map<String, AttributeValueSnapshot>? = null,
    @SerialName("SequenceNumber")
    val sequenceNumber: String? = null,
    @SerialName("SizeBytes")
    val sizeBytes: Long? = null,
    @SerialName("StreamViewType")
    val streamViewType: String? = null,
)

@Serializable
data class AttributeValueSnapshot(
    @SerialName("S")
    val s: String? = null,
    @SerialName("N")
    val n: String? = null,
    @SerialName("B")
    @Serializable(with = ByteBufferSerializer::class)
    val b: ByteBuffer? = null,
    @SerialName("SS")
    val ss: List<String>? = null,
    @SerialName("NS")
    val ns: List<String>? = null,
    @SerialName("BS")
    val bs: List<@Serializable(with = ByteBufferSerializer::class) ByteBuffer>? = null,
    @SerialName("M")
    val m: Map<String, AttributeValueSnapshot>? = null,
    @SerialName("L")
    val l: List<AttributeValueSnapshot>? = null,
    @SerialName("NULL")
    val nullValue: Boolean? = null,
    @SerialName("BOOL")
    val bool: Boolean? = null,
)

fun DynamodbEvent.DynamodbStreamRecord.toSnapshot() = DynamodbStreamRecordSnapshot(
    eventID = eventID,
    eventName = eventName,
    eventSource = eventSource,
    eventVersion = eventVersion,
    awsRegion = awsRegion,
    eventSourceARN = eventSourceARN,
    dynamodb = dynamodb?.toSnapshot(),
)

fun StreamRecord.toSnapshot() = StreamRecordSnapshot(
    approximateCreationDateTime = approximateCreationDateTime,
    keys = keys?.mapValues { it.value.toSnapshot() },
    newImage = newImage?.mapValues { it.value.toSnapshot() },
    oldImage = oldImage?.mapValues { it.value.toSnapshot() },
    sequenceNumber = sequenceNumber,
    sizeBytes = sizeBytes,
    streamViewType = streamViewType,
)

fun AttributeValue.toSnapshot(): AttributeValueSnapshot = AttributeValueSnapshot(
    s = s,
    n = n,
    b = b,
    ss = ss,
    ns = ns,
    bs = bs,
    m = m?.mapValues { it.value.toSnapshot() },
    l = l?.map { it.toSnapshot() },
    nullValue = getNULL(),
    bool = bool,
)

fun DynamodbStreamRecordSnapshot.toRecord() = DynamodbEvent.DynamodbStreamRecord().apply {
    eventID = this@toRecord.eventID
    eventName = this@toRecord.eventName
    eventSource = this@toRecord.eventSource
    eventVersion = this@toRecord.eventVersion
    awsRegion = this@toRecord.awsRegion
    eventSourceARN = this@toRecord.eventSourceARN
    dynamodb = this@toRecord.dynamodb?.toRecord()
}

fun StreamRecordSnapshot.toRecord() = StreamRecord().apply {
    approximateCreationDateTime = this@toRecord.approximateCreationDateTime
    keys = this@toRecord.keys?.mapValues { it.value.toRecord() }
    newImage = this@toRecord.newImage?.mapValues { it.value.toRecord() }
    oldImage = this@toRecord.oldImage?.mapValues { it.value.toRecord() }
    sequenceNumber = this@toRecord.sequenceNumber
    sizeBytes = this@toRecord.sizeBytes
    this@toRecord.streamViewType?.let { setStreamViewType(StreamViewType.fromValue(it)) }
}

fun AttributeValueSnapshot.toRecord(): AttributeValue = AttributeValue().apply {
    withS(this@toRecord.s)
    withN(this@toRecord.n)
    withB(this@toRecord.b)
    withSS(this@toRecord.ss)
    withNS(this@toRecord.ns)
    withBS(this@toRecord.bs)
    withM(this@toRecord.m?.mapValues { it.value.toRecord() })
    withL(this@toRecord.l?.map { it.toRecord() })
    withNULL(this@toRecord.nullValue)
    withBOOL(this@toRecord.bool)
}

package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamViewType
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.nio.ByteBuffer
import java.util.*

@Serializable
data class DynamodbStreamRecordSurrogate(
    val eventID: String? = null,
    val eventName: String? = null,
    val eventSource: String? = null,
    val eventVersion: String? = null,
    val awsRegion: String? = null,
    val eventSourceARN: String? = null,
    val dynamodb: StreamRecordSurrogate? = null,
)

@Serializable
data class StreamRecordSurrogate(
    @SerialName("ApproximateCreationDateTime")
    @Serializable(with = DateSerializer::class)
    val approximateCreationDateTime: Date? = null,
    @SerialName("Keys")
    val keys: Map<String, AttributeValueSurrogate>? = null,
    @SerialName("NewImage")
    val newImage: Map<String, AttributeValueSurrogate>? = null,
    @SerialName("OldImage")
    val oldImage: Map<String, AttributeValueSurrogate>? = null,
    @SerialName("SequenceNumber")
    val sequenceNumber: String? = null,
    @SerialName("SizeBytes")
    val sizeBytes: Long? = null,
    @SerialName("StreamViewType")
    val streamViewType: String? = null,
)

@Serializable
data class AttributeValueSurrogate(
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
    val m: Map<String, AttributeValueSurrogate>? = null,
    @SerialName("L")
    val l: List<AttributeValueSurrogate>? = null,
    @SerialName("NULL")
    val nullValue: Boolean? = null,
    @SerialName("BOOL")
    val bool: Boolean? = null,
)

fun DynamodbEvent.DynamodbStreamRecord.toSurrogate() = DynamodbStreamRecordSurrogate(
    eventID = eventID,
    eventName = eventName,
    eventSource = eventSource,
    eventVersion = eventVersion,
    awsRegion = awsRegion,
    eventSourceARN = eventSourceARN,
    dynamodb = dynamodb?.toSurrogate(),
)

fun StreamRecord.toSurrogate() = StreamRecordSurrogate(
    approximateCreationDateTime = approximateCreationDateTime,
    keys = keys?.mapValues { it.value.toSurrogate() },
    newImage = newImage?.mapValues { it.value.toSurrogate() },
    oldImage = oldImage?.mapValues { it.value.toSurrogate() },
    sequenceNumber = sequenceNumber,
    sizeBytes = sizeBytes,
    streamViewType = streamViewType,
)

fun AttributeValue.toSurrogate(): AttributeValueSurrogate = AttributeValueSurrogate(
    s = s,
    n = n,
    b = b,
    ss = ss,
    ns = ns,
    bs = bs,
    m = m?.mapValues { it.value.toSurrogate() },
    l = l?.map { it.toSurrogate() },
    nullValue = getNULL(),
    bool = bool,
)

fun DynamodbStreamRecordSurrogate.toSdk() = DynamodbEvent.DynamodbStreamRecord().apply {
    eventID = this@toSdk.eventID
    eventName = this@toSdk.eventName
    eventSource = this@toSdk.eventSource
    eventVersion = this@toSdk.eventVersion
    awsRegion = this@toSdk.awsRegion
    eventSourceARN = this@toSdk.eventSourceARN
    dynamodb = this@toSdk.dynamodb?.toSdk()
}

fun StreamRecordSurrogate.toSdk() = StreamRecord().apply {
    approximateCreationDateTime = this@toSdk.approximateCreationDateTime
    keys = this@toSdk.keys?.mapValues { it.value.toSdk() }
    newImage = this@toSdk.newImage?.mapValues { it.value.toSdk() }
    oldImage = this@toSdk.oldImage?.mapValues { it.value.toSdk() }
    sequenceNumber = this@toSdk.sequenceNumber
    sizeBytes = this@toSdk.sizeBytes
    this@toSdk.streamViewType?.let { setStreamViewType(StreamViewType.fromValue(it)) }
}

fun AttributeValueSurrogate.toSdk(): AttributeValue = AttributeValue().apply {
    withS(this@toSdk.s)
    withN(this@toSdk.n)
    withB(this@toSdk.b)
    withSS(this@toSdk.ss)
    withNS(this@toSdk.ns)
    withBS(this@toSdk.bs)
    withM(this@toSdk.m?.mapValues { it.value.toSdk() })
    withL(this@toSdk.l?.map { it.toSdk() })
    withNULL(this@toSdk.nullValue)
    withBOOL(this@toSdk.bool)
}

object DynamodbStreamRecordReplayJson {
    fun encode(record: DynamodbEvent.DynamodbStreamRecord): String = AwsJson.encodeToString(record.toSurrogate())
    fun decode(json: String): DynamodbEvent.DynamodbStreamRecord = AwsJson.decodeFromString<DynamodbStreamRecordSurrogate>(json).toSdk()
}

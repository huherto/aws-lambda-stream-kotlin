package io.github.huherto.awsLambdaStream.serialization.aws.snapshots

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.nio.ByteBuffer

@Serializable
data class SQSEventSnapshot(
    @SerialName("Records")
    val records: List<SQSMessageSnapshot>? = null
)

@Serializable
data class SQSMessageSnapshot(
    val messageId: String? = null,
    val receiptHandle: String? = null,
    val body: String? = null,
    val md5OfBody: String? = null,
    val md5OfMessageAttributes: String? = null,
    val eventSource: String? = null,
    val eventSourceARN: String? = null, // Note: Jackson/AWS uses eventSourceARN (all caps ARN)
    val awsRegion: String? = null,
    val attributes: Map<String, String>? = null,
    val messageAttributes: Map<String, MessageAttributeSnapshot>? = null,
)

@Serializable
data class MessageAttributeSnapshot(
    val stringValue: String? = null,
    @Serializable(with = ByteBufferSerializer::class)
    val binaryValue: ByteBuffer? = null,
    val stringListValues: List<String>? = null,
    val binaryListValues: List<@Serializable(with = ByteBufferSerializer::class) ByteBuffer>? = null,
    val dataType: String? = null,
)

fun SQSEvent.toSnapshot() = SQSEventSnapshot(
    records = records?.map { it.toSnapshot() }
)

fun SQSEvent.SQSMessage.toSnapshot() = SQSMessageSnapshot(
    messageId = messageId,
    receiptHandle = receiptHandle,
    body = body,
    md5OfBody = md5OfBody,
    md5OfMessageAttributes = md5OfMessageAttributes,
    eventSource = eventSource,
    eventSourceARN = eventSourceArn, // Map from eventSourceArn
    awsRegion = awsRegion,
    attributes = attributes,
    messageAttributes = messageAttributes?.mapValues { it.value.toSnapshot() },
)

fun SQSEvent.MessageAttribute.toSnapshot() = MessageAttributeSnapshot(
    stringValue = stringValue,
    binaryValue = binaryValue,
    stringListValues = stringListValues,
    binaryListValues = binaryListValues,
    dataType = dataType,
)

fun SQSEventSnapshot.toRecord() = SQSEvent().apply {
    records = this@toRecord.records?.map { it.toRecord() }
}

fun SQSMessageSnapshot.toRecord() = SQSEvent.SQSMessage().apply {
    messageId = this@toRecord.messageId
    receiptHandle = this@toRecord.receiptHandle
    body = this@toRecord.body
    md5OfBody = this@toRecord.md5OfBody
    md5OfMessageAttributes = this@toRecord.md5OfMessageAttributes
    eventSource = this@toRecord.eventSource
    eventSourceArn = this@toRecord.eventSourceARN
    awsRegion = this@toRecord.awsRegion
    attributes = this@toRecord.attributes
    messageAttributes = this@toRecord.messageAttributes?.mapValues { it.value.toRecord() }
}

fun MessageAttributeSnapshot.toRecord() = SQSEvent.MessageAttribute().apply {
    stringValue = this@toRecord.stringValue
    binaryValue = this@toRecord.binaryValue
    stringListValues = this@toRecord.stringListValues
    binaryListValues = this@toRecord.binaryListValues
    dataType = this@toRecord.dataType
}

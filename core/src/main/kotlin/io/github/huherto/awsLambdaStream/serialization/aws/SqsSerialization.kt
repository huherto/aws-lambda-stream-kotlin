package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.nio.ByteBuffer

@Serializable
data class SQSEventSurrogate(
    @SerialName("Records")
    val records: List<SQSMessageSurrogate>? = null
)

@Serializable
data class SQSMessageSurrogate(
    val messageId: String? = null,
    val receiptHandle: String? = null,
    val body: String? = null,
    val md5OfBody: String? = null,
    val md5OfMessageAttributes: String? = null,
    val eventSource: String? = null,
    val eventSourceARN: String? = null, // Note: Jackson/AWS uses eventSourceARN (all caps ARN)
    val awsRegion: String? = null,
    val attributes: Map<String, String>? = null,
    val messageAttributes: Map<String, MessageAttributeSurrogate>? = null,
)

@Serializable
data class MessageAttributeSurrogate(
    val stringValue: String? = null,
    @Serializable(with = ByteBufferSerializer::class)
    val binaryValue: ByteBuffer? = null,
    val stringListValues: List<String>? = null,
    val binaryListValues: List<@Serializable(with = ByteBufferSerializer::class) ByteBuffer>? = null,
    val dataType: String? = null,
)

fun SQSEvent.toSurrogate() = SQSEventSurrogate(
    records = records?.map { it.toSurrogate() }
)

fun SQSEvent.SQSMessage.toSurrogate() = SQSMessageSurrogate(
    messageId = messageId,
    receiptHandle = receiptHandle,
    body = body,
    md5OfBody = md5OfBody,
    md5OfMessageAttributes = md5OfMessageAttributes,
    eventSource = eventSource,
    eventSourceARN = eventSourceArn, // Map from eventSourceArn
    awsRegion = awsRegion,
    attributes = attributes,
    messageAttributes = messageAttributes?.mapValues { it.value.toSurrogate() },
)

fun SQSEvent.MessageAttribute.toSurrogate() = MessageAttributeSurrogate(
    stringValue = stringValue,
    binaryValue = binaryValue,
    stringListValues = stringListValues,
    binaryListValues = binaryListValues,
    dataType = dataType,
)

fun SQSEventSurrogate.toSdk() = SQSEvent().apply {
    records = this@toSdk.records?.map { it.toSdk() }
}

fun SQSMessageSurrogate.toSdk() = SQSEvent.SQSMessage().apply {
    messageId = this@toSdk.messageId
    receiptHandle = this@toSdk.receiptHandle
    body = this@toSdk.body
    md5OfBody = this@toSdk.md5OfBody
    md5OfMessageAttributes = this@toSdk.md5OfMessageAttributes
    eventSource = this@toSdk.eventSource
    eventSourceArn = this@toSdk.eventSourceARN
    awsRegion = this@toSdk.awsRegion
    attributes = this@toSdk.attributes
    messageAttributes = this@toSdk.messageAttributes?.mapValues { it.value.toSdk() }
}

fun MessageAttributeSurrogate.toSdk() = SQSEvent.MessageAttribute().apply {
    stringValue = this@toSdk.stringValue
    binaryValue = this@toSdk.binaryValue
    stringListValues = this@toSdk.stringListValues
    binaryListValues = this@toSdk.binaryListValues
    dataType = this@toSdk.dataType
}

object SQSEventReplayJson {
    fun encode(event: SQSEvent): String = AwsJson.encodeToString(event.toSurrogate())
    fun decode(json: String): SQSEvent = AwsJson.decodeFromString<SQSEventSurrogate>(json).toSdk()
}

object SQSMessageReplayJson {
    fun encode(message: SQSEvent.SQSMessage): String = AwsJson.encodeToString(message.toSurrogate())
    fun decode(json: String): SQSEvent.SQSMessage = AwsJson.decodeFromString<SQSMessageSurrogate>(json).toSdk()
}

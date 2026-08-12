package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

class SQSMessageReplayJsonTest {

    @Test
    fun `should round trip serialize and deserialize a single sqs message`() {
        val body = """{"id":"event-1","type":"example"}"""
        val original = sqsMessage(
            messageId = "059f36b4-87a3-44ab-83d2-661975830a7d",
            receiptHandle = "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a",
            body = body,
        )

        val json = SQSMessageReplayJson.encode(original)
        val decoded = SQSMessageReplayJson.decode(json)

        decoded.messageId shouldBe original.messageId
        decoded.receiptHandle shouldBe original.receiptHandle
        decoded.body shouldBe body
        decoded.md5OfBody shouldBe original.md5OfBody
        decoded.md5OfMessageAttributes shouldBe original.md5OfMessageAttributes
        decoded.eventSource shouldBe original.eventSource
        decoded.eventSourceArn shouldBe original.eventSourceArn
        decoded.awsRegion shouldBe original.awsRegion
        decoded.attributes shouldBe original.attributes
    }

    @Test
    fun `should round trip a message with a plain text body`() {
        val original = sqsMessage(
            messageId = "message-1",
            receiptHandle = "receipt-1",
            body = "plain text payload",
        )

        val decoded = SQSMessageReplayJson.decode(SQSMessageReplayJson.encode(original))

        decoded.body shouldBe "plain text payload"
    }

    @Test
    fun `should round trip sqs message attributes`() {
        val original = sqsMessage(
            messageId = "message-1",
            receiptHandle = "receipt-1",
            body = """{"id":"1"}""",
        ).apply {
            messageAttributes = mapOf(
                "stringAttribute" to SQSEvent.MessageAttribute().apply {
                    dataType = "String"
                    stringValue = "attribute-value"
                },
                "binaryAttribute" to SQSEvent.MessageAttribute().apply {
                    dataType = "Binary"
                    binaryValue = ByteBuffer.wrap("binary-payload".toByteArray())
                },
            )
        }

        val json = SQSMessageReplayJson.encode(original)
        val decoded = SQSMessageReplayJson.decode(json)

        decoded.messageAttributes.keys shouldBe setOf("stringAttribute", "binaryAttribute")

        decoded.messageAttributes.getValue("stringAttribute").dataType shouldBe "String"
        decoded.messageAttributes.getValue("stringAttribute").stringValue shouldBe "attribute-value"

        decoded.messageAttributes.getValue("binaryAttribute").dataType shouldBe "Binary"
        decoded.messageAttributes.getValue("binaryAttribute").binaryValue.asUtf8String() shouldBe "binary-payload"
    }

    @Test
    fun `should serialize a bare message without a Records wrapper`() {
        val original = sqsMessage(
            messageId = "message-1",
            receiptHandle = "receipt-1",
            body = """{"id":"1"}""",
        )

        val json = SQSMessageReplayJson.encode(original)

        json shouldContain """"messageId":"message-1""""
        json shouldContain """"eventSourceARN""""
        json.contains(""""Records"""") shouldBe false
    }

    @Test
    fun `should round trip a message read from an sqs event`() {
        val event = SQSEvent().apply {
            records = listOf(
                sqsMessage(
                    messageId = "message-1",
                    receiptHandle = "receipt-1",
                    body = """{"id":"1"}""",
                )
            )
        }

        val eventJson = SQSEventReplayJson.encode(event)
        val messageFromEvent = SQSEventReplayJson.decode(eventJson).records.single()

        val decoded = SQSMessageReplayJson.decode(SQSMessageReplayJson.encode(messageFromEvent))

        decoded shouldBe messageFromEvent
    }

    @Test
    fun `should decode a message lifted verbatim out of sqs event json`() {
        val event = SQSEvent().apply {
            records = listOf(
                sqsMessage(
                    messageId = "message-1",
                    receiptHandle = "receipt-1",
                    body = """{"id":"1"}""",
                )
            )
        }

        val messageJson = ObjectMapper()
            .readTree(SQSEventReplayJson.encode(event))
            .get("Records")
            .get(0)
            .toString()

        SQSMessageReplayJson.decode(messageJson) shouldBe event.records.single()
    }

    @Test
    fun `should round trip an empty message`() {
        val decoded = SQSMessageReplayJson.decode(SQSMessageReplayJson.encode(SQSEvent.SQSMessage()))

        decoded shouldBe SQSEvent.SQSMessage()
    }

    private fun sqsMessage(
        messageId: String,
        receiptHandle: String,
        body: String,
    ): SQSEvent.SQSMessage {
        return SQSEvent.SQSMessage().apply {
            this.messageId = messageId
            this.receiptHandle = receiptHandle
            this.body = body
            this.md5OfBody = "7b270e59b47ff90a553787216d55d91d"
            this.md5OfMessageAttributes = "d25a6aea97eb8f585bfa92d314504a92"
            this.eventSource = "aws:sqs"
            this.eventSourceArn = "arn:aws:sqs:us-east-1:123456789012:example-queue"
            this.awsRegion = "us-east-1"
            this.attributes = mapOf(
                "ApproximateReceiveCount" to "1",
                "SentTimestamp" to "1700000000000",
                "SenderId" to "123456789012",
                "ApproximateFirstReceiveTimestamp" to "1700000000001",
            )
        }
    }

    private fun ByteBuffer.asUtf8String(): String {
        val duplicate = duplicate()
        val bytes = ByteArray(duplicate.remaining())
        duplicate.get(bytes)
        return bytes.toString(Charsets.UTF_8)
    }
}

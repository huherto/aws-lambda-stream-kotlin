package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

class SQSEventReplayJsonTest {

    @Test
    fun `should round trip serialize and deserialize a single sqs message`() {
        val body = """{"id":"event-1","type":"example"}"""
        val original = SQSEvent().apply {
            records = listOf(
                sqsMessage(
                    messageId = "059f36b4-87a3-44ab-83d2-661975830a7d",
                    receiptHandle = "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a",
                    body = body,
                )
            )
        }

        val json = SQSEventReplayJson.encode(original)
        val decoded = SQSEventReplayJson.decode(json)

        decoded.records shouldHaveSize 1

        val decodedMessage = decoded.records.single()
        val originalMessage = original.records.single()

        decodedMessage.messageId shouldBe originalMessage.messageId
        decodedMessage.receiptHandle shouldBe originalMessage.receiptHandle
        decodedMessage.body shouldBe body
        decodedMessage.md5OfBody shouldBe originalMessage.md5OfBody
        decodedMessage.md5OfMessageAttributes shouldBe originalMessage.md5OfMessageAttributes
        decodedMessage.eventSource shouldBe originalMessage.eventSource
        decodedMessage.eventSourceArn shouldBe originalMessage.eventSourceArn
        decodedMessage.awsRegion shouldBe originalMessage.awsRegion
        decodedMessage.attributes shouldBe originalMessage.attributes
    }

    @Test
    fun `should round trip serialize and deserialize multiple sqs messages`() {
        val original = SQSEvent().apply {
            records = listOf(
                sqsMessage(
                    messageId = "message-1",
                    receiptHandle = "receipt-1",
                    body = """{"id":"1"}""",
                ),
                sqsMessage(
                    messageId = "message-2",
                    receiptHandle = "receipt-2",
                    body = """{"id":"2","nested":{"value":true}}""",
                ),
                sqsMessage(
                    messageId = "message-3",
                    receiptHandle = "receipt-3",
                    body = """plain text payload""",
                ),
            )
        }

        val json = SQSEventReplayJson.encode(original)
        val decoded = SQSEventReplayJson.decode(json)

        decoded.records shouldHaveSize 3

        decoded.records[0].messageId shouldBe "message-1"
        decoded.records[0].receiptHandle shouldBe "receipt-1"
        decoded.records[0].body shouldBe """{"id":"1"}"""

        decoded.records[1].messageId shouldBe "message-2"
        decoded.records[1].receiptHandle shouldBe "receipt-2"
        decoded.records[1].body shouldBe """{"id":"2","nested":{"value":true}}"""

        decoded.records[2].messageId shouldBe "message-3"
        decoded.records[2].receiptHandle shouldBe "receipt-3"
        decoded.records[2].body shouldBe "plain text payload"
    }

    @Test
    fun `should round trip sqs message attributes`() {
        val binaryValue = "binary-payload".toByteArray()
        val original = SQSEvent().apply {
            records = listOf(
                sqsMessage(
                    messageId = "message-1",
                    receiptHandle = "receipt-1",
                    body = """{"id":"1"}""",
                ).apply {
                    messageAttributes = mapOf(
                        "stringAttribute" to SQSEvent.MessageAttribute().apply {
                            dataType = "String"
                            stringValue = "attribute-value"
                        },
                        "numberAttribute" to SQSEvent.MessageAttribute().apply {
                            dataType = "Number"
                            stringValue = "42"
                        },
                        "binaryAttribute" to SQSEvent.MessageAttribute().apply {
                            dataType = "Binary"
                            this.binaryValue = ByteBuffer.wrap(binaryValue)
                        },
                    )
                }
            )
        }

        val json = SQSEventReplayJson.encode(original)
        val decoded = SQSEventReplayJson.decode(json)

        val decodedAttributes = decoded.records.single().messageAttributes
        decodedAttributes.keys shouldBe setOf("stringAttribute", "numberAttribute", "binaryAttribute")

        decodedAttributes.getValue("stringAttribute").dataType shouldBe "String"
        decodedAttributes.getValue("stringAttribute").stringValue shouldBe "attribute-value"

        decodedAttributes.getValue("numberAttribute").dataType shouldBe "Number"
        decodedAttributes.getValue("numberAttribute").stringValue shouldBe "42"

        decodedAttributes.getValue("binaryAttribute").dataType shouldBe "Binary"
        decodedAttributes.getValue("binaryAttribute").binaryValue.asUtf8String() shouldBe "binary-payload"
    }

    @Test
    fun `should serialize sqs records under the Records key with the aws event source arn spelling`() {
        val original = SQSEvent().apply {
            records = listOf(
                sqsMessage(
                    messageId = "message-1",
                    receiptHandle = "receipt-1",
                    body = """{"id":"1"}""",
                )
            )
        }

        val json = SQSEventReplayJson.encode(original)

        json shouldContain """"Records""""
        json shouldContain """"messageId":"message-1""""
        json shouldContain """"eventSourceARN""""
    }

    @Test
    fun `should round trip an empty sqs event`() {
        val original = SQSEvent().apply {
            records = emptyList()
        }

        val json = SQSEventReplayJson.encode(original)
        val decoded = SQSEventReplayJson.decode(json)

        decoded.records shouldHaveSize 0
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

package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.serialization.events.LambdaEventSerializers
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.*

class AwsSerializationCompatibilityTest {

    @Test
    fun `SQSEvent should be compatible with official AWS serializer`() {
        val event = SQSEvent().apply {
            records = listOf(sqsMessage("msg-1", "hello"))
        }

        // Custom -> AWS
        val customJson = SQSEventReplayJson.encode(event)
        val decodedByAws = deserialize(customJson, SQSEvent::class.java)
        assertSqsMessagesEqual(decodedByAws.records[0], event.records[0])

        // AWS -> Custom
        val awsJson = serialize(event, SQSEvent::class.java)
        val decodedByCustom = SQSEventReplayJson.decode(awsJson)
        assertSqsMessagesEqual(decodedByCustom.records[0], event.records[0])
    }

    @Test
    fun `SQSMessage should be compatible with official AWS serializer`() {
        val message = sqsMessage("msg-1", "hello")
        val event = SQSEvent().apply { records = listOf(message) }

        // Custom -> AWS (wrapped in event to use official serializer)
        val customMessageJson = SQSMessageReplayJson.encode(message)
        val customEventJson = """{"Records":[$customMessageJson]}"""
        val decodedByAws = deserialize(customEventJson, SQSEvent::class.java)
        assertSqsMessagesEqual(decodedByAws.records[0], message)

        // AWS -> Custom
        val awsEventJson = serialize(event, SQSEvent::class.java)
        val awsMessageJson = Json.parseToJsonElement(awsEventJson).jsonObject["Records"]!!.jsonArray[0].toString()
        val decodedByCustom = SQSMessageReplayJson.decode(awsMessageJson)
        assertSqsMessagesEqual(decodedByCustom, message)
    }

    @Test
    fun `KinesisEvent should be compatible with official AWS serializer`() {
        val event = KinesisEvent().apply {
            records = listOf(kinesisRecord("part-1", "hello kinesis"))
        }

        // Custom -> AWS
        val customJson = KinesisEventReplayJson.encode(event)
        val decodedByAws = deserialize(customJson, KinesisEvent::class.java)
        assertKinesisRecordsEqual(decodedByAws.records[0], event.records[0])

        // AWS -> Custom
        val awsJson = serialize(event, KinesisEvent::class.java)
        val decodedByCustom = KinesisEventReplayJson.decode(awsJson)
        assertKinesisRecordsEqual(decodedByCustom.records[0], event.records[0])
    }

    @Test
    fun `KinesisEventRecord should be compatible with official AWS serializer`() {
        val record = kinesisRecord("part-1", "hello kinesis")
        val event = KinesisEvent().apply { records = listOf(record) }

        // Custom -> AWS
        val customRecordJson = KinesisEventRecordReplayJson.encode(record)
        val customEventJson = """{"Records":[$customRecordJson]}"""
        val decodedByAws = deserialize(customEventJson, KinesisEvent::class.java)
        assertKinesisRecordsEqual(decodedByAws.records[0], record)

        // AWS -> Custom
        val awsEventJson = serialize(event, KinesisEvent::class.java)
        val awsRecordJson = Json.parseToJsonElement(awsEventJson).jsonObject["Records"]!!.jsonArray[0].toString()
        val decodedByCustom = KinesisEventRecordReplayJson.decode(awsRecordJson)
        assertKinesisRecordsEqual(decodedByCustom, record)
    }

    @Test
    fun `DynamodbStreamRecord should be compatible with official AWS serializer`() {
        val record = DynamodbStreamRecordReplayJsonTest.streamRecord()
        val event = DynamodbEvent().apply { records = listOf(record) }

        // Custom -> AWS
        val customRecordJson = DynamodbStreamRecordReplayJson.encode(record)
        val customEventJson = """{"Records":[$customRecordJson]}"""
        val decodedByAws = deserialize(customEventJson, DynamodbEvent::class.java)
        
        // Use shouldBe if DynamodbEvent.DynamodbStreamRecord implements equals correctly
        // and all fields are serialized.
        decodedByAws.records[0] shouldBe record

        // AWS -> Custom
        val awsEventJson = serialize(event, DynamodbEvent::class.java)
        val awsRecordJson = Json.parseToJsonElement(awsEventJson).jsonObject["Records"]!!.jsonArray[0].toString()
        val decodedByCustom = DynamodbStreamRecordReplayJson.decode(awsRecordJson)
        decodedByCustom shouldBe record
    }

    @Test
    fun `DynamodbStreamRecordSerializer should work correctly in kotlinx serialization`() {
        val record = DynamodbStreamRecordReplayJsonTest.streamRecord()
        val wrapper = DynamodbWrapper(record)
        val json = Json.encodeToString(wrapper)

        json shouldContain "\"record\":{"
        json shouldContain "\"eventID\":\"event-1\""

        val decoded = Json.decodeFromString<DynamodbWrapper>(json)
        decoded.record shouldBe record
    }

    @Test
    fun `KinesisEventRecordSerializer should work correctly in kotlinx serialization`() {
        val record = kinesisRecord("part-1", "hello")
        val wrapper = KinesisWrapper(record)
        val json = Json.encodeToString(wrapper)

        json shouldContain "\"record\":{"
        json shouldContain "\"partitionKey\":\"part-1\""

        val decoded = Json.decodeFromString<KinesisWrapper>(json)
        assertKinesisRecordsEqual(decoded.record, record)
    }

    @Test
    fun `SQSMessageSerializer should work correctly in kotlinx serialization`() {
        val message = sqsMessage("msg-1", "hello")
        val wrapper = SqsWrapper(message)
        val json = Json.encodeToString(wrapper)

        json shouldContain "\"record\":{"
        json shouldContain "\"messageId\":\"msg-1\""

        val decoded = Json.decodeFromString<SqsWrapper>(json)
        assertSqsMessagesEqual(decoded.record, message)
    }

    @Serializable
    data class DynamodbWrapper(
        @Serializable(with = DynamodbStreamRecordSerializer::class)
        val record: DynamodbEvent.DynamodbStreamRecord
    )

    @Serializable
    data class KinesisWrapper(
        @Serializable(with = KinesisEventRecordSerializer::class)
        val record: KinesisEvent.KinesisEventRecord
    )

    @Serializable
    data class SqsWrapper(
        @Serializable(with = SQSMessageSerializer::class)
        val record: SQSEvent.SQSMessage
    )

    // Helpers
    private fun <T> serialize(event: T, clazz: Class<T>): String {
        val serializer = LambdaEventSerializers.serializerFor(clazz, clazz.classLoader)
        val os = ByteArrayOutputStream()
        serializer.toJson(event, os)
        return os.toString()
    }

    private fun <T> deserialize(json: String, clazz: Class<T>): T {
        val serializer = LambdaEventSerializers.serializerFor(clazz, clazz.classLoader)
        return serializer.fromJson(json)
    }

    private fun sqsMessage(messageId: String, body: String) = SQSEvent.SQSMessage().apply {
        this.messageId = messageId
        this.body = body
        this.eventSource = "aws:sqs"
        this.awsRegion = "us-east-1"
        this.attributes = mapOf("ApproximateReceiveCount" to "1")
    }

    private fun kinesisRecord(partitionKey: String, data: String) = KinesisEvent.KinesisEventRecord().apply {
        kinesis = KinesisEvent.Record().apply {
            this.partitionKey = partitionKey
            this.data = ByteBuffer.wrap(data.toByteArray())
            this.approximateArrivalTimestamp = Date(1700000000000L)
            this.sequenceNumber = "seq-123"
        }
        eventSource = "aws:kinesis"
        awsRegion = "us-east-1"
        eventName = "aws:kinesis:record"
        eventSourceARN = "arn:aws:kinesis:us-east-1:123456789012:stream/example"
    }

    private fun assertSqsMessagesEqual(actual: SQSEvent.SQSMessage, expected: SQSEvent.SQSMessage) {
        actual.messageId shouldBe expected.messageId
        actual.body shouldBe expected.body
        actual.eventSource shouldBe expected.eventSource
        actual.awsRegion shouldBe expected.awsRegion
        actual.attributes shouldBe expected.attributes
    }

    private fun assertKinesisRecordsEqual(actual: KinesisEvent.KinesisEventRecord, expected: KinesisEvent.KinesisEventRecord) {
        actual.kinesis.partitionKey shouldBe expected.kinesis.partitionKey
        actual.kinesis.data shouldBe expected.kinesis.data
        actual.kinesis.sequenceNumber shouldBe expected.kinesis.sequenceNumber
        actual.kinesis.approximateArrivalTimestamp.time / 1000 shouldBe expected.kinesis.approximateArrivalTimestamp.time / 1000
        actual.eventSource shouldBe expected.eventSource
        actual.awsRegion shouldBe expected.awsRegion
        actual.eventName shouldBe expected.eventName
        actual.eventSourceARN shouldBe expected.eventSourceARN
    }
}

package io.github.huherto.awsLambdaStream

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.serialization.JacksonSerializationStrategy
import io.github.huherto.awsLambdaStream.serialization.aws.DynamodbStreamRecordReplayJsonTest
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.json.*
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.*

/**
 * The point of the sealed [RawRecord] hierarchy is that both serialization strategies dispatch on
 * the same `type` discriminator without `@Contextual` guesswork. These tests pin that down for
 * kotlinx and for Jackson, since the two resolve polymorphism through entirely different machinery.
 */
class RawRecordSerializationTest {

    private val mapper = JacksonSerializationStrategy.defaultMapper()

    @Test
    fun `kotlinx should round trip a dynamodb raw record`() {
        val original = DynamodbRaw(DynamodbStreamRecordReplayJsonTest.streamRecord())

        kotlinxRoundTrip(original) shouldBe original
    }

    @Test
    fun `jackson should round trip a dynamodb raw record`() {
        val original = DynamodbRaw(DynamodbStreamRecordReplayJsonTest.streamRecord())

        jacksonRoundTrip(original) shouldBe original
    }

    @Test
    fun `both strategies should tag a dynamodb raw record the same way`() {
        val original = DynamodbRaw(DynamodbStreamRecordReplayJsonTest.streamRecord())

        val fromKotlinx = Json.encodeToJsonElement(RawRecord.serializer(), original).jsonObject
        val fromJackson = Json.parseToJsonElement(mapper.writeValueAsString(original)).jsonObject

        fromKotlinx["type"] shouldBe JsonPrimitive(RAW_DYNAMODB)
        fromJackson["type"] shouldBe JsonPrimitive(RAW_DYNAMODB)
        fromJackson["record"] shouldBe fromKotlinx["record"]
    }

    @Test
    fun `a serialized dynamodb raw record should carry the whole lambda payload`() {
        val record = DynamodbStreamRecordReplayJsonTest.streamRecord()

        val json = Json.encodeToString(RawRecord.serializer(), DynamodbRaw(record))

        // Fields the old RecordPair projection dropped on the floor.
        json shouldContain """"eventID""""
        json shouldContain """"eventSourceARN""""
        json shouldContain """"SequenceNumber""""
        json shouldContain """"Keys""""
    }

    @Test
    fun `derived images should read through to the original record`() {
        val raw = DynamodbRaw(DynamodbStreamRecordReplayJsonTest.streamRecord())

        raw.new?.getPk() shouldBe "shipment-1"
        raw.old?.getPk() shouldBe "shipment-1"
        raw.new?.getLong("timestamp") shouldBe 1_700_000_000L
        raw.old?.getLong("timestamp") shouldBe 1_699_999_999L
        raw.new?.isDeleted() shouldBe false
    }

    @Test
    fun `derived images should not be written to json`() {
        val raw = DynamodbRaw(DynamodbStreamRecordReplayJsonTest.streamRecord())

        Json.encodeToJsonElement(RawRecord.serializer(), raw).jsonObject.keys shouldBe setOf("type", "record")
        Json.parseToJsonElement(mapper.writeValueAsString(raw)).jsonObject.keys shouldBe setOf("type", "record")
    }

    @Test
    fun `kotlinx should round trip images raw`() {
        val original = imagesRaw()

        kotlinxRoundTrip(original) shouldBe original
    }

    @Test
    fun `jackson should round trip images raw`() {
        val original = imagesRaw()

        jacksonRoundTrip(original) shouldBe original
    }

    @Test
    fun `images raw should serialize as canonical dynamodb json`() {
        val raw = ImagesRaw(new = RecordImage(mapOf("tags" to AttributeValue().withSS("a", "b"))))

        val json = Json.encodeToJsonElement(RawRecord.serializer(), raw).jsonObject

        json["new"].toString() shouldBe """{"tags":{"SS":["a","b"]}}"""
        // Sets survive the trip, which the flattened projection could not manage.
        (kotlinxRoundTrip(raw) as ImagesRaw).new?.get("tags")?.ss shouldBe listOf("a", "b")
    }

    @Test
    fun `kotlinx should round trip kinesis raw`() {
        val original = KinesisRaw(kinesisRecord())

        kotlinxRoundTrip(original) shouldBe original
    }

    @Test
    fun `jackson should round trip kinesis raw`() {
        val original = KinesisRaw(kinesisRecord())

        jacksonRoundTrip(original) shouldBe original
    }

    @Test
    fun `kotlinx should round trip sqs raw`() {
        val original = SqsRaw(sqsMessage())

        kotlinxRoundTrip(original) shouldBe original
    }

    @Test
    fun `jackson should round trip sqs raw`() {
        val original = SqsRaw(sqsMessage())

        jacksonRoundTrip(original) shouldBe original
    }

    @Test
    fun `kotlinx should round trip a claim check`() {
        val original = ClaimCheck(bucket = "bucket-1", key = "key-1")

        kotlinxRoundTrip(original) shouldBe original
    }

    @Test
    fun `jackson should round trip a claim check`() {
        val original = ClaimCheck(bucket = "bucket-1", key = "key-1")

        jacksonRoundTrip(original) shouldBe original
    }

    @Test
    fun `kotlinx should round trip arbitrary json`() {
        val original = JsonRaw(buildJsonObject { put("anything", "goes") })

        kotlinxRoundTrip(original) shouldBe original
    }

    @Test
    fun `jackson should round trip arbitrary json`() {
        val original = JsonRaw(buildJsonObject { put("anything", "goes") })

        jacksonRoundTrip(original) shouldBe original
    }

    @Test
    fun `toRawRecord should restore the variant a discriminator names`() {
        val original: RawRecord = DynamodbRaw(DynamodbStreamRecordReplayJsonTest.streamRecord())

        original.toJsonElement().toRawRecord() shouldBe original
    }

    @Test
    fun `toRawRecord should wrap undiscriminated json rather than fail`() {
        val bare = buildJsonObject { put("legacy", "payload") }

        bare.toRawRecord() shouldBe JsonRaw(bare)
        JsonPrimitive("a string").toRawRecord() shouldBe JsonRaw(JsonPrimitive("a string"))
    }

    @Test
    fun `toRawRecord should wrap json whose discriminator names no known variant`() {
        val unknown = buildJsonObject {
            put("type", "somethingElse")
            put("value", 1)
        }

        unknown.toRawRecord() shouldBe JsonRaw(unknown)
    }

    @Test
    fun `a dynamodb raw record should still be usable as a record pair`() {
        val raw: RawRecord = DynamodbRaw(DynamodbStreamRecordReplayJsonTest.streamRecord())

        raw.shouldBeInstanceOf<io.github.huherto.awsLambdaStream.from.RecordPair>()
    }

    private fun kotlinxRoundTrip(value: RawRecord): RawRecord =
        Json.decodeFromString(RawRecord.serializer(), Json.encodeToString(RawRecord.serializer(), value))

    private fun jacksonRoundTrip(value: RawRecord): RawRecord =
        mapper.readValue(mapper.writeValueAsString(value), RawRecord::class.java)

    private fun imagesRaw() = ImagesRaw(
        new = RecordImage(
            mapOf(
                "pk" to AttributeValue().withS("shipment-1"),
                "timestamp" to AttributeValue().withN("1700000000"),
            )
        ),
        old = RecordImage(mapOf("pk" to AttributeValue().withS("shipment-1"))),
    )

    private fun kinesisRecord() = KinesisEvent.KinesisEventRecord().apply {
        eventID = "shardId-000000000000:4959033827149025660855969253836157109592157598913658889"
        eventName = "aws:kinesis:record"
        eventSource = "aws:kinesis"
        eventSourceARN = "arn:aws:kinesis:us-east-1:123456789012:stream/example"
        awsRegion = "us-east-1"
        eventVersion = "1.0"
        kinesis = KinesisEvent.Record().apply {
            sequenceNumber = "sequence-1"
            partitionKey = "partition-1"
            kinesisSchemaVersion = "1.0"
            encryptionType = "NONE"
            approximateArrivalTimestamp = Date(1_700_000_000_000L)
            data = ByteBuffer.wrap("""{"id":"1"}""".toByteArray(StandardCharsets.UTF_8))
        }
    }

    private fun sqsMessage() = SQSEvent.SQSMessage().apply {
        messageId = "message-1"
        receiptHandle = "receipt-1"
        body = """{"id":"1"}"""
        md5OfBody = "md5-of-body"
        eventSource = "aws:sqs"
        eventSourceArn = "arn:aws:sqs:us-east-1:123456789012:example"
        awsRegion = "us-east-1"
        attributes = mapOf("SentTimestamp" to "1700000000000")
        messageAttributes = emptyMap()
    }
}

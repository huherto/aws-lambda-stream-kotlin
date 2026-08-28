package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.*

class KinesisSerializationTest {

    @Test
    fun `should round trip serialize and deserialize a single kinesis record`() {
        val payload = """{"id":"event-1","type":"example"}"""
        val original = KinesisEvent().apply {
            records = listOf(
                kinesisRecord(
                    eventId = "shardId-000000000000:49590338271490256608559692538361571095921575989136588898",
                    sequenceNumber = "49590338271490256608559692538361571095921575989136588898",
                    partitionKey = "partition-1",
                    payload = payload,
                )
            )
        }

        val json = KinesisEventReplayJson.encode(original)
        val decoded = KinesisEventReplayJson.decode(json)

        decoded.records shouldHaveSize 1

        val decodedRecord = decoded.records.single()
        val originalRecord = original.records.single()

        decodedRecord.eventID shouldBe originalRecord.eventID
        decodedRecord.eventName shouldBe originalRecord.eventName
        decodedRecord.eventSource shouldBe originalRecord.eventSource
        decodedRecord.eventSourceARN shouldBe originalRecord.eventSourceARN
        decodedRecord.awsRegion shouldBe originalRecord.awsRegion
        decodedRecord.eventVersion shouldBe originalRecord.eventVersion

        decodedRecord.kinesis.sequenceNumber shouldBe originalRecord.kinesis.sequenceNumber
        decodedRecord.kinesis.partitionKey shouldBe originalRecord.kinesis.partitionKey
        decodedRecord.kinesis.kinesisSchemaVersion shouldBe originalRecord.kinesis.kinesisSchemaVersion
        decodedRecord.kinesis.encryptionType shouldBe originalRecord.kinesis.encryptionType

        decodedRecord.kinesis.data.asUtf8String() shouldBe payload
    }

    @Test
    fun `should round trip serialize and deserialize multiple kinesis records`() {
        val original = KinesisEvent().apply {
            records = listOf(
                kinesisRecord(
                    eventId = "event-1",
                    sequenceNumber = "sequence-1",
                    partitionKey = "partition-1",
                    payload = """{"id":"1"}""",
                ),
                kinesisRecord(
                    eventId = "event-2",
                    sequenceNumber = "sequence-2",
                    partitionKey = "partition-2",
                    payload = """{"id":"2","nested":{"value":true}}""",
                ),
                kinesisRecord(
                    eventId = "event-3",
                    sequenceNumber = "sequence-3",
                    partitionKey = "partition-3",
                    payload = """plain text payload""",
                ),
            )
        }

        val json = KinesisEventReplayJson.encode(original)
        val decoded = KinesisEventReplayJson.decode(json)

        decoded.records shouldHaveSize 3

        decoded.records[0].eventID shouldBe "event-1"
        decoded.records[0].kinesis.sequenceNumber shouldBe "sequence-1"
        decoded.records[0].kinesis.partitionKey shouldBe "partition-1"
        decoded.records[0].kinesis.data.asUtf8String() shouldBe """{"id":"1"}"""

        decoded.records[1].eventID shouldBe "event-2"
        decoded.records[1].kinesis.sequenceNumber shouldBe "sequence-2"
        decoded.records[1].kinesis.partitionKey shouldBe "partition-2"
        decoded.records[1].kinesis.data.asUtf8String() shouldBe """{"id":"2","nested":{"value":true}}"""

        decoded.records[2].eventID shouldBe "event-3"
        decoded.records[2].kinesis.sequenceNumber shouldBe "sequence-3"
        decoded.records[2].kinesis.partitionKey shouldBe "partition-3"
        decoded.records[2].kinesis.data.asUtf8String() shouldBe "plain text payload"
    }

    @Test
    fun `should serialize kinesis data as base64 in replay json`() {
        val payload = """{"id":"event-with-base64-check"}"""
        val expectedBase64Payload = Base64.getEncoder()
            .encodeToString(payload.toByteArray())

        val original = KinesisEvent().apply {
            records = listOf(
                kinesisRecord(
                    eventId = "event-1",
                    sequenceNumber = "sequence-1",
                    partitionKey = "partition-1",
                    payload = payload,
                )
            )
        }

        val json = KinesisEventReplayJson.encode(original)

        json shouldContain """"Records""""
        json shouldContain """"kinesis""""
        json shouldContain """"data""""
        json shouldContain expectedBase64Payload
    }

    @Test
    fun `should round trip an empty kinesis event`() {
        val original = KinesisEvent().apply {
            records = emptyList()
        }

        val json = KinesisEventReplayJson.encode(original)
        val decoded = KinesisEventReplayJson.decode(json)

        decoded.records shouldHaveSize 0
    }

    @Test
    fun `should round trip serialize and deserialize a bare kinesis event record`() {
        val payload = """{"id":"event-1","type":"example"}"""
        val original = kinesisRecord(
            eventId = "shardId-000000000000:49590338271490256608559692538361571095921575989136588898",
            sequenceNumber = "49590338271490256608559692538361571095921575989136588898",
            partitionKey = "partition-1",
            payload = payload,
        )

        val json = KinesisEventRecordReplayJson.encode(original)
        val decoded = KinesisEventRecordReplayJson.decode(json)

        decoded.eventID shouldBe original.eventID
        decoded.eventName shouldBe original.eventName
        decoded.eventSource shouldBe original.eventSource
        decoded.eventSourceARN shouldBe original.eventSourceARN
        decoded.invokeIdentityArn shouldBe original.invokeIdentityArn
        decoded.awsRegion shouldBe original.awsRegion
        decoded.eventVersion shouldBe original.eventVersion

        decoded.kinesis.sequenceNumber shouldBe original.kinesis.sequenceNumber
        decoded.kinesis.partitionKey shouldBe original.kinesis.partitionKey
        decoded.kinesis.kinesisSchemaVersion shouldBe original.kinesis.kinesisSchemaVersion
        decoded.kinesis.encryptionType shouldBe original.kinesis.encryptionType
        decoded.kinesis.approximateArrivalTimestamp shouldBe original.kinesis.approximateArrivalTimestamp

        decoded.kinesis.data.asUtf8String() shouldBe payload
    }

    @Test
    fun `should serialize kinesis data as base64 without a Records wrapper`() {
        val payload = """{"id":"event-with-base64-check"}"""
        val expectedBase64Payload = Base64.getEncoder()
            .encodeToString(payload.toByteArray())

        val original = kinesisRecord(
            eventId = "event-1",
            sequenceNumber = "sequence-1",
            partitionKey = "partition-1",
            payload = payload,
        )

        val json = KinesisEventRecordReplayJson.encode(original)

        json shouldContain """"kinesis""""
        json shouldContain """"data""""
        json shouldContain expectedBase64Payload
        json.contains(""""Records"""") shouldBe false
    }

    @Test
    fun `should decode a record lifted verbatim out of kinesis event json`() {
        val payload = """{"id":"1"}"""
        val event = KinesisEvent().apply {
            records = listOf(
                kinesisRecord(
                    eventId = "event-1",
                    sequenceNumber = "sequence-1",
                    partitionKey = "partition-1",
                    payload = payload,
                )
            )
        }

        val recordJson = Json.parseToJsonElement(KinesisEventReplayJson.encode(event))
            .jsonObject["Records"]!!
            .jsonArray[0]
            .toString()

        val decoded = KinesisEventRecordReplayJson.decode(recordJson)

        decoded shouldBe event.records.single()
        decoded.kinesis.data.asUtf8String() shouldBe payload
    }

    private fun kinesisRecord(
        eventId: String,
        sequenceNumber: String,
        partitionKey: String,
        payload: String,
    ): KinesisEvent.KinesisEventRecord {
        return KinesisEvent.KinesisEventRecord().apply {
            eventID = eventId
            eventName = "aws:kinesis:record"
            eventSource = "aws:kinesis"
            eventSourceARN = "arn:aws:kinesis:us-east-1:123456789012:stream/example-stream"
            invokeIdentityArn = "arn:aws:iam::123456789012:role/example-role"
            awsRegion = "us-east-1"
            eventVersion = "1.0"

            kinesis = KinesisEvent.Record().apply {
                this.sequenceNumber = sequenceNumber
                this.partitionKey = partitionKey
                this.kinesisSchemaVersion = "1.0"
                this.encryptionType = "NONE"
                this.approximateArrivalTimestamp = Date(1_700_000_000_000L)
                this.data = ByteBuffer.wrap(payload.toByteArray())
            }
        }
    }

    private fun ByteBuffer.asUtf8String(): String {
        val duplicate = duplicate()
        val bytes = ByteArray(duplicate.remaining())
        duplicate.get(bytes)
        return bytes.toString(Charsets.UTF_8)
    }
}

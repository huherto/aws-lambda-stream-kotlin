package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.*

class KinesisEventRecordReplayJsonTest {

    @Test
    fun `should round trip serialize and deserialize a single kinesis event record`() {
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
    fun `should round trip a record with a plain text payload`() {
        val original = kinesisRecord(
            eventId = "event-1",
            sequenceNumber = "sequence-1",
            partitionKey = "partition-1",
            payload = "plain text payload",
        )

        val decoded = KinesisEventRecordReplayJson.decode(KinesisEventRecordReplayJson.encode(original))

        decoded.kinesis.data.asUtf8String() shouldBe "plain text payload"
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
    fun `should round trip a record read from a kinesis event`() {
        val event = KinesisEvent().apply {
            records = listOf(
                kinesisRecord(
                    eventId = "event-1",
                    sequenceNumber = "sequence-1",
                    partitionKey = "partition-1",
                    payload = """{"id":"1"}""",
                )
            )
        }

        val recordFromEvent = KinesisEventReplayJson.decode(KinesisEventReplayJson.encode(event)).records.single()

        val decoded = KinesisEventRecordReplayJson.decode(KinesisEventRecordReplayJson.encode(recordFromEvent))

        decoded shouldBe recordFromEvent
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

        val recordJson = ObjectMapper()
            .readTree(KinesisEventReplayJson.encode(event))
            .get("Records")
            .get(0)
            .toString()

        val decoded = KinesisEventRecordReplayJson.decode(recordJson)

        decoded shouldBe event.records.single()
        decoded.kinesis.data.asUtf8String() shouldBe payload
    }

    @Test
    fun `should round trip an empty record`() {
        val decoded = KinesisEventRecordReplayJson.decode(
            KinesisEventRecordReplayJson.encode(KinesisEvent.KinesisEventRecord())
        )

        decoded shouldBe KinesisEvent.KinesisEventRecord()
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

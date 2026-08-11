package io.github.huherto.awsLambdaStream.faults

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.serialization.snapshots.KinesisRecordSnapshotter
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.*

class KinesisRecordSnapshotterTest {

    private val snapshotter = KinesisRecordSnapshotter()

    @Test
    fun `snapshot should capture kinesis record fields correctly`() {
        // Arrange
        val payload = "{\"foo\":\"bar\"}"
        val payloadBytes = payload.toByteArray(StandardCharsets.UTF_8)
        val record = KinesisEvent.KinesisEventRecord().apply {
            eventID = "event-123"
            eventName = "aws:kinesis:record"
            eventSource = "aws:kinesis"
            awsRegion = "us-east-1"
            kinesis = KinesisEvent.Record().apply {
                partitionKey = "pk-123"
                sequenceNumber = "seq-456"
                data = ByteBuffer.wrap(payloadBytes)
            }
        }

        // Act
        val snapshot = snapshotter.snapshot(record)

        // Assert
        snapshot.kind shouldBe "kinesis"
        
        val payloadElement = snapshot.payload
        payloadElement["eventID"] shouldBe JsonPrimitive("event-123")
        val kinesis = payloadElement["kinesis"]?.let { it as kotlinx.serialization.json.JsonObject }
        kinesis?.get("partitionKey") shouldBe JsonPrimitive("pk-123")
        kinesis?.get("sequenceNumber") shouldBe JsonPrimitive("seq-456")
        kinesis?.get("data") shouldBe JsonPrimitive(Base64.getEncoder().encodeToString(payloadBytes))
    }

    @Test
    fun `supports should return true for KinesisEventRecord`() {
        snapshotter.supports(KinesisEvent.KinesisEventRecord()) shouldBe true
        snapshotter.supports("not a record") shouldBe false
    }
}

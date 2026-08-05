package io.github.huherto.awsLambdaStream.faults

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.faults.replay.KinesisReplayRecord
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromJsonElement
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
        
        val replayRecord = Json.decodeFromJsonElement<KinesisReplayRecord>(snapshot.payload)
        replayRecord.eventID shouldBe "event-123"
        replayRecord.kinesis.partitionKey shouldBe "pk-123"
        replayRecord.kinesis.sequenceNumber shouldBe "seq-456"
        replayRecord.kinesis.data shouldBe Base64.getEncoder().encodeToString(payloadBytes)
    }

    @Test
    fun `supports should return true for KinesisEventRecord`() {
        snapshotter.supports(KinesisEvent.KinesisEventRecord()) shouldBe true
        snapshotter.supports("not a record") shouldBe false
    }
}

package io.github.huherto.awsLambdaStream.faults

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.faults.replay.DynamoDbReplayRecord
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromJsonElement
import org.junit.jupiter.api.Test

class DynamoDbRecordSnapshotterTest {

    private val snapshotter = DynamoDbRecordSnapshotter()

    @Test
    fun `snapshot should capture dynamodb record fields correctly`() {
        // Arrange
        val record = DynamodbEvent.DynamodbStreamRecord().apply {
            eventID = "event-456"
            eventName = "INSERT"
            eventSource = "aws:dynamodb"
            awsRegion = "us-east-1"
            dynamodb = StreamRecord().apply {
                keys = mapOf("id" to AttributeValue().apply { s = "id-123" })
                newImage = mapOf(
                    "id" to AttributeValue().apply { s = "id-123" },
                    "active" to AttributeValue().apply { setBOOL(true) },
                    "count" to AttributeValue().apply { n = "10" }
                )
                sequenceNumber = "seq-789"
                // sizeBytes = 123L
                // streamViewType = "NEW_AND_OLD_IMAGES"
            }
        }

        // Act
        val snapshot = snapshotter.snapshot(record)

        // Assert
        snapshot.kind shouldBe "dynamodb"

        val replayRecord = Json.decodeFromJsonElement<DynamoDbReplayRecord>(snapshot.payload)
        replayRecord.eventID shouldBe "event-456"
        replayRecord.dynamodb.keys?.get("id")?.S shouldBe "id-123"
        replayRecord.dynamodb.newImage?.get("active")?.BOOL shouldBe true
        replayRecord.dynamodb.newImage?.get("count")?.N shouldBe "10"
        replayRecord.dynamodb.sequenceNumber shouldBe "seq-789"
    }

    @Test
    fun `supports should return true for DynamodbStreamRecord`() {
        snapshotter.supports(DynamodbEvent.DynamodbStreamRecord()) shouldBe true
        snapshotter.supports("not a record") shouldBe false
    }
}

package io.github.huherto.awsLambdaStream.faults

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.serialization.snapshots.DynamoDbRecordSnapshotter
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
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
        println("DynamoDB Snapshot Payload: ${Json.encodeToString(snapshot.payload)}")

        // Assert
        snapshot.kind shouldBe "dynamodb"

        val payload = snapshot.payload
        payload["eventID"] shouldBe JsonPrimitive("event-456")
        val dynamodb = payload["dynamodb"]?.let { it as kotlinx.serialization.json.JsonObject }
        dynamodb?.get("Keys")?.let { it as kotlinx.serialization.json.JsonObject }?.get("id")?.let { it as kotlinx.serialization.json.JsonObject }?.get("S") shouldBe JsonPrimitive("id-123")
        dynamodb?.get("NewImage")?.let { it as kotlinx.serialization.json.JsonObject }?.get("active")?.let { it as kotlinx.serialization.json.JsonObject }?.get("BOOL") shouldBe JsonPrimitive(true)
        dynamodb?.get("NewImage")?.let { it as kotlinx.serialization.json.JsonObject }?.get("count")?.let { it as kotlinx.serialization.json.JsonObject }?.get("N") shouldBe JsonPrimitive("10")
        dynamodb?.get("SequenceNumber") shouldBe JsonPrimitive("seq-789")
    }

    @Test
    fun `supports should return true for DynamodbStreamRecord`() {
        snapshotter.supports(DynamodbEvent.DynamodbStreamRecord()) shouldBe true
        snapshotter.supports("not a record") shouldBe false
    }
}

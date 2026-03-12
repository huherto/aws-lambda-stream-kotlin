package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.FaultManager
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.*

class DynamodbAdapterTest {

    private val adapter = DynamodbAdapter()
    private val faultManager = FaultManager()

    // ============================================================================
    // Tests for calculateEventTypeSuffix
    // ============================================================================

    @Test
    fun `calculateEventTypeSuffix should map event names correctly`() {
        val insertRecord = createDynamodbRecord(eventName = "INSERT")
        val modifyRecord = createDynamodbRecord(eventName = "MODIFY")
        val removeRecord = createDynamodbRecord(eventName = "REMOVE")

        assertEquals("created", adapter.calculateEventTypeSuffix(insertRecord))
        assertEquals("updated", adapter.calculateEventTypeSuffix(modifyRecord))
        assertEquals("deleted", adapter.calculateEventTypeSuffix(removeRecord))
    }

    @Test
    fun `calculateEventTypeSuffix should return empty string for unknown event names`() {
        val unknownRecord = createDynamodbRecord(eventName = "UNKNOWN")

        assertEquals("", adapter.calculateEventTypeSuffix(unknownRecord))
    }

    @Test
    fun `calculateEventTypeSuffix should return deleted when new image has deleted flag true`() {
        val newImage = mapOf(
            "id" to AttributeValue().withS("123"),
            "deleted" to AttributeValue().withBOOL(true)
        )
        val record = createDynamodbRecord(eventName = "MODIFY", newImage = newImage)

        assertEquals("deleted", adapter.calculateEventTypeSuffix(record))
    }

    @Test
    fun `calculateEventTypeSuffix should return undeleted when old image has deleted flag true but new is false`() {
        val newImage = mapOf(
            "id" to AttributeValue().withS("123"),
            "deleted" to AttributeValue().withBOOL(false)
        )
        val oldImage = mapOf(
            "id" to AttributeValue().withS("123"),
            "deleted" to AttributeValue().withBOOL(true)
        )
        val record = createDynamodbRecord(eventName = "MODIFY", newImage = newImage, oldImage = oldImage)

        assertEquals("undeleted", adapter.calculateEventTypeSuffix(record))
    }

    @Test
    fun `calculateEventTypeSuffix should handle missing deleted flag gracefully`() {
        val newImage = mapOf(
            "id" to AttributeValue().withS("123"),
            "name" to AttributeValue().withS("test")
        )
        val record = createDynamodbRecord(eventName = "MODIFY", newImage = newImage)

        assertEquals("updated", adapter.calculateEventTypeSuffix(record))
    }

    @Test
    fun `calculateEventTypeSuffix should prefer deleted suffix over updated for MODIFY events`() {
        val newImage = mapOf(
            "id" to AttributeValue().withS("123"),
            "deleted" to AttributeValue().withBOOL(true)
        )
        val record = createDynamodbRecord(eventName = "MODIFY", newImage = newImage)

        assertEquals("deleted", adapter.calculateEventTypeSuffix(record))
    }

    // ============================================================================
    // Tests for buildEvent
    // ============================================================================

    @Test
    fun `buildEvent should set all required fields from dynamodb record`() {
        val eventId = "event-123"
        val region = "us-east-1"
        val pkValue = "partition-key-value"
        val timestamp = Date()
        val newImage = mapOf(
            "pk" to AttributeValue().withS(pkValue),
            "discriminator" to AttributeValue().withS("OrderEvent"),
            "timestamp" to AttributeValue().withN("1609459200")
        )

        val record = createDynamodbRecord(
            eventId = eventId,
            region = region,
            newImage = newImage,
            timestamp = timestamp,
            eventName = "INSERT",
            partitionKeyValue = pkValue
        )

        val event = adapter.buildEvent(record)

        assertEquals(eventId, event.id)
        assertEquals(region, event.tags?.get("region"))
        assertEquals(pkValue, event.partitionKey)
        assertNotNull(event.timestamp)
    }

    @Test
    fun `buildEvent should include raw data with new and old images`() {
        val newImage = mapOf(
            "pk" to AttributeValue().withS("key1"),
            "data" to AttributeValue().withS("new-value")
        )
        val oldImage = mapOf(
            "pk" to AttributeValue().withS("key1"),
            "data" to AttributeValue().withS("old-value")
        )

        val record = createDynamodbRecord(
            newImage = newImage,
            oldImage = oldImage,
            eventName = "MODIFY"
        )

        val event = adapter.buildEvent(record)

        @Suppress("UNCHECKED_CAST")
        val raw = event.raw as? Map<String, Any?>
        assertEquals(newImage, raw?.get("new"))
        assertEquals(oldImage, raw?.get("old"))
    }

    @Test
    fun `buildEvent should calculate correct event type`() {
        val newImage = mapOf(
            "pk" to AttributeValue().withS("key1"),
            "discriminator" to AttributeValue().withS("UserEvent")
        )

        val record = createDynamodbRecord(newImage = newImage, eventName = "INSERT")

        val event = adapter.buildEvent(record)

        assertEquals("UserEvent-created", event.type)
    }

    // ============================================================================
    // Tests for fromDynamoDB (integration)
    // ============================================================================

    @Test
    fun `fromDynamoDB should process valid dynamodb records`() = runBlocking {
        val newImage = mapOf(
            "pk" to AttributeValue().withS("key1"),
            "discriminator" to AttributeValue().withS("TestEvent")
        )

        val records = listOf(
            createDynamodbRecord(eventId = "event-1", newImage = newImage, eventName = "INSERT"),
            createDynamodbRecord(eventId = "event-2", newImage = newImage, eventName = "MODIFY")
        )

        val dynamodbEvent = DynamodbEvent().apply {
            this.records = records.map { record ->
                DynamodbEvent.DynamodbStreamRecord().apply {
                    eventID = record.eventID
                    eventName = record.eventName
                    awsRegion = record.awsRegion
                    dynamodb = record.dynamodb
                }
            }
        }

        val results = adapter.fromDynamoDB(faultManager, dynamodbEvent).toList()

        assertEquals(2, results.size)
        assertEquals("event-1", results[0].event?.id)
        assertEquals("event-2", results[1].event?.id)
        assertTrue(results.all { it.event is TableEvent })
    }

    @Test
    fun `fromDynamoDB should handle empty event records`() = runBlocking {
        val dynamodbEvent = DynamodbEvent().apply {
            records = emptyList()
        }

        val results = adapter.fromDynamoDB(faultManager, dynamodbEvent).toList()

        assertEquals(0, results.size)
    }

    @Test
    fun `fromDynamoDB should preserve record reference in unit of work`() = runBlocking {
        val newImage = mapOf(
            "pk" to AttributeValue().withS("key1"),
            "discriminator" to AttributeValue().withS("Event")
        )

        val dynamodbRecord = createDynamodbRecord(eventId = "test-id", newImage = newImage, eventName = "INSERT")
        val dynamodbEvent = DynamodbEvent().apply {
            records = listOf(
                DynamodbEvent.DynamodbStreamRecord().apply {
                    eventID = dynamodbRecord.eventID
                    eventName = dynamodbRecord.eventName
                    awsRegion = dynamodbRecord.awsRegion
                    dynamodb = dynamodbRecord.dynamodb
                }
            )
        }

        val results = adapter.fromDynamoDB(faultManager, dynamodbEvent).toList()

        assertEquals(1, results.size)
        assertNotNull(results[0].record)
        assertNotNull(results[0].event)
    }

    // ============================================================================
    // Helper functions
    // ============================================================================

    private fun createDynamodbRecord(
        eventId: String = UUID.randomUUID().toString(),
        eventName: String = "INSERT",
        region: String = "us-east-1",
        newImage: Map<String, AttributeValue>? = null,
        oldImage: Map<String, AttributeValue>? = null,
        timestamp: Date? = Date(),
        partitionKeyValue: String = "default-pk"
    ): DynamodbEvent.DynamodbStreamRecord {
        val streamRecord = StreamRecord()
            .withNewImage(newImage)
            .withOldImage(oldImage)
            .withKeys(
                mapOf("pk" to AttributeValue().withS(partitionKeyValue)
            ))
            .withApproximateCreationDateTime(timestamp)

        return DynamodbEvent.DynamodbStreamRecord().apply {
            this.eventID = eventId
            this.eventName = eventName
            this.awsRegion = region
            dynamodb = streamRecord
        }
    }
}
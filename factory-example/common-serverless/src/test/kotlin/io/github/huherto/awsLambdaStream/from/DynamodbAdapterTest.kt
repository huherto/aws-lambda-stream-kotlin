package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.asJson
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.Date

class DynamodbAdapterTest {

    @Test
    fun `should successfully map DynamoDB record to UnitOfWork flow`() = runBlocking {
        // Arrange
        val adapter = DynamodbAdapter()
        val creationDate = Date()
        val streamRecord = StreamRecord().apply {
            approximateCreationDateTime = creationDate
            keys = mapOf("pk" to AttributeValue().withS("test-pk-123"))
            newImage = mapOf(
                "discriminator" to AttributeValue().withS("TestEvent"),
                "timestamp" to AttributeValue().withN("1630000000")
            )
            oldImage = mapOf(
                "discriminator" to AttributeValue().withS("OldEvent")
            )
        }

        val record = DynamodbEvent.DynamodbStreamRecord().apply {
            eventID = "event-1"
            awsRegion = "us-east-1"
            dynamodb = streamRecord
        }

        val dynamodbEvent = DynamodbEvent().apply {
            records = listOf(record)
        }

        // Act
        val result = adapter.fromDynamoDB(dynamodbEvent).toList()

        // Assert
        assertEquals(1, result.size)
        val uow = result.first()

        assertEquals(record, uow.record)

        val tableEvent = uow.event as TableEvent
        assertEquals("event-1", tableEvent.id)
        assertEquals(creationDate.time * 1000, tableEvent.timestamp)
        assertEquals("test-pk-123", tableEvent.partitionKey)
        assertEquals("TestEvent-", tableEvent.type)
        assertEquals(mapOf("region" to "us-east-1"), tableEvent.tags)

        val rawMap = tableEvent.raw as Map<*, *>
        assertEquals(streamRecord.newImage, rawMap["new"])
        assertEquals(streamRecord.oldImage, rawMap["old"])

    }

    @Test
    fun `should map event type prefix using 'sk' when 'discriminator' is absent`() = runBlocking {
        // Arrange
        val adapter = DynamodbAdapter()
        val streamRecord = StreamRecord().apply {
            keys = emptyMap()
            newImage = mapOf(
                "sk" to AttributeValue().withS("SkEvent")
            )
        }
        val record = DynamodbEvent.DynamodbStreamRecord().apply { dynamodb = streamRecord }
        val dynamodbEvent = DynamodbEvent().apply { records = listOf(record) }

        // Act
        val result = adapter.fromDynamoDB(dynamodbEvent).toList()

        // Assert
        val tableEvent = result.first().event as TableEvent
        assertEquals("SkEvent-", tableEvent.type)
    }

    @Test
    fun `should fallback to oldImage for event type calculation if newImage is null`() = runBlocking {
        // Arrange
        val adapter = DynamodbAdapter()
        val streamRecord = StreamRecord().apply {
            keys = emptyMap()
            newImage = null
            oldImage = mapOf(
                "discriminator" to AttributeValue().withS("DeletedEventDiscriminator")
            )
        }
        val record = DynamodbEvent.DynamodbStreamRecord().apply { dynamodb = streamRecord }
        val dynamodbEvent = DynamodbEvent().apply { records = listOf(record) }

        // Act
        val result = adapter.fromDynamoDB(dynamodbEvent).toList()

        // Assert
        val tableEvent = result.first().event as TableEvent
        assertEquals("DeletedEventDiscriminator-", tableEvent.type)
    }

    @Test
    fun `should process multiple dynamodb records correctly`() = runBlocking {
        // Arrange
        val adapter = DynamodbAdapter()
        val record1 = DynamodbEvent.DynamodbStreamRecord().apply {
            eventID = "id-1"
            dynamodb = StreamRecord().apply { keys = emptyMap() }
        }
        val record2 = DynamodbEvent.DynamodbStreamRecord().apply {
            eventID = "id-2"
            dynamodb = StreamRecord().apply { keys = emptyMap() }
        }
        val dynamodbEvent = DynamodbEvent().apply { records = listOf(record1, record2) }

        // Act
        val result = adapter.fromDynamoDB(dynamodbEvent).toList()

        // Assert
        assertEquals(2, result.size)
        assertEquals("id-1", (result[0].event as TableEvent).id)
        assertEquals("id-2", (result[1].event as TableEvent).id)
    }

    @Test
    fun `should handle missing approximateCreationDateTime gracefully`() = runBlocking {
        // Arrange
        val adapter = DynamodbAdapter()
        val streamRecord = StreamRecord().apply {
            keys = emptyMap()
            approximateCreationDateTime = null // simulate missing timestamp
        }
        val record = DynamodbEvent.DynamodbStreamRecord().apply { dynamodb = streamRecord }
        val dynamodbEvent = DynamodbEvent().apply { records = listOf(record) }

        // Act
        val result = adapter.fromDynamoDB(dynamodbEvent).toList()

        // Assert
        val tableEvent = result.first().event as TableEvent
        assertNull(tableEvent.timestamp)
    }

    @Test
    fun `should return 'created' suffix for INSERT events`() {
        // Arrange
        val adapter = DynamodbAdapter()
        val record = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "INSERT"
            dynamodb = StreamRecord().apply {
                newImage = emptyMap()
                oldImage = emptyMap()
            }
        }

        // Act
        val result = adapter.calculateEventTypeSuffix(record)

        // Assert
        assertEquals("created", result)
    }

    @Test
    fun `should return 'updated' suffix for MODIFY events without deleted flag`() {
        // Arrange
        val adapter = DynamodbAdapter()
        val record = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "MODIFY"
            dynamodb = StreamRecord().apply {
                newImage = mapOf("field" to AttributeValue().withS("value"))
                oldImage = mapOf("field" to AttributeValue().withS("oldValue"))
            }
        }

        // Act
        val result = adapter.calculateEventTypeSuffix(record)

        // Assert
        assertEquals("updated", result)
    }

    @Test
    fun `should return 'deleted' suffix for REMOVE events`() {
        // Arrange
        val adapter = DynamodbAdapter()
        val record = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "REMOVE"
            dynamodb = StreamRecord().apply {
                newImage = null
                oldImage = emptyMap()
            }
        }

        // Act
        val result = adapter.calculateEventTypeSuffix(record)

        // Assert
        assertEquals("deleted", result)
    }

    @Test
    fun `should return 'deleted' suffix when newImage contains deleted flag set to true`() {
        // Arrange
        val adapter = DynamodbAdapter()
        val record = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "MODIFY"
            dynamodb = StreamRecord().apply {
                newImage = mapOf("deleted" to AttributeValue().withBOOL(true))
                oldImage = mapOf("deleted" to AttributeValue().withBOOL(false))
            }
        }

        // Act
        val result = adapter.calculateEventTypeSuffix(record)

        // Assert
        assertEquals("deleted", result)
    }

    @Test
    fun `should return 'undeleted' suffix when oldImage has deleted flag true and newImage has it false`() {
        // Arrange
        val adapter = DynamodbAdapter()
        val record = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "MODIFY"
            dynamodb = StreamRecord().apply {
                newImage = mapOf("deleted" to AttributeValue().withBOOL(false))
                oldImage = mapOf("deleted" to AttributeValue().withBOOL(true))
            }
        }

        // Act
        val result = adapter.calculateEventTypeSuffix(record)

        // Assert
        assertEquals("undeleted", result)
    }

    @Test
    fun `should return 'updated' suffix when deleted flag is false in both images`() {
        // Arrange
        val adapter = DynamodbAdapter()
        val record = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "MODIFY"
            dynamodb = StreamRecord().apply {
                newImage = mapOf("deleted" to AttributeValue().withBOOL(false))
                oldImage = mapOf("deleted" to AttributeValue().withBOOL(false))
            }
        }

        // Act
        val result = adapter.calculateEventTypeSuffix(record)

        // Assert
        assertEquals("updated", result)
    }

    @Test
    fun `should return empty string suffix for unknown event name`() {
        // Arrange
        val adapter = DynamodbAdapter()
        val record = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "UNKNOWN_EVENT"
            dynamodb = StreamRecord().apply {
                newImage = emptyMap()
                oldImage = emptyMap()
            }
        }

        // Act
        val result = adapter.calculateEventTypeSuffix(record)

        // Assert
        assertEquals("", result)
    }
}


package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.flavors.CorrelatePipeline
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlin.test.*
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV

class CorrelatePipelineTest {

    /**
     * Concrete Fake Event to avoid issues with Kotlin reflection (KClass) on anonymous objects
     * inside the filterEventTypes stage of the pipeline.
     */
    class FakeEvent(
        override var id: String? = "event-1",
        override var timestamp: Long? = 1600000000L,
        override var partitionKey: String? = null,
        override var tags: Map<String, String>? = null,
        override var raw: Any? = null,
        override var eem: Any? = null,
        private val encodedStr: String = "{}"
    ) : Event {
        override fun eventType() = "TestEvent"
        override fun encoded() = encodedStr
    }

    private fun createFakeEvent(
        rawObj: Any? = null,
        encodedStr: String = "{}",
        eventId: String? = "event-1",
        eventTimestamp: Long? = 1600000000L
    ): Event {
        return FakeEvent(
            id = eventId,
            timestamp = eventTimestamp,
            raw = rawObj,
            encodedStr = encodedStr
        )
    }

    // --- Unit Tests for Internal Functions ---

    @Test
    fun `forCollectedEvents should evaluate correctly based on record and event properties`() {
        // Arrange
        val pipeline = CorrelatePipeline("test-pipeline")
        
        val skEventAttr = com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue().apply { s = "EVENT" }
        val skOtherAttr = com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue().apply { s = "OTHER" }
        
        val streamRecordWithEvent = com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord().apply {
            keys = mapOf("sk" to skEventAttr)
        }
        val streamRecordWithOther = com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord().apply {
            keys = mapOf("sk" to skOtherAttr)
        }
        
        val validRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "INSERT"
            dynamodb = streamRecordWithEvent
        }
        
        val validUow = UnitOfWork(
            record = validRecord, 
            event = createFakeEvent(rawObj = RecordPair(null, null))
        )
        
        // Act & Assert
        assertTrue(
            pipeline.forCollectedEvents(validUow), 
            "Should return true for INSERT event with sk=EVENT and raw=RecordPair"
        )
        
        val wrongEventNameRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "MODIFY"
            dynamodb = streamRecordWithEvent
        }
        assertFalse(
            pipeline.forCollectedEvents(validUow.copy(record = wrongEventNameRecord)), 
            "Should return false when eventName is not INSERT"
        )
        
        val wrongSkRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "INSERT"
            dynamodb = streamRecordWithOther
        }
        assertFalse(
            pipeline.forCollectedEvents(validUow.copy(record = wrongSkRecord)), 
            "Should return false when sk is not EVENT"
        )
        
        assertFalse(
            pipeline.forCollectedEvents(validUow.copy(event = createFakeEvent(rawObj = "Not a RecordPair"))), 
            "Should return false when event.raw is not of type RecordPair"
        )
    }

    @Test
    fun `normalize should extract metadata and populate JsonEvent from RecordPair`() {
        // Arrange
        val pipeline = CorrelatePipeline("test-pipeline")

        val record = DynamodbEvent.DynamodbStreamRecord().apply {
            dynamodb = StreamRecord().apply {
                sequenceNumber = "seq-123"
            }
        }
        val populatedMap = mapOf<String, EventAV?>(
            "ttl" to EventAV().withN("999" ),
            "data" to EventAV().withS("test-data"),
            "event" to EventAV().withS( """{"key":"value"}"""),
        )
        val uowPopulated = UnitOfWork(
            record = record,
            event = createFakeEvent(rawObj = RecordPair(new = RecordImage(populatedMap), old = null)),
        )

        // Act & Assert
        val resultPopulated = pipeline.normalize(uowPopulated)
        assertEquals("seq-123", resultPopulated.meta?.get("sequenceNumber"))
        assertEquals("999", resultPopulated.meta?.get("ttl"))
        assertEquals("test-data", resultPopulated.meta?.get("data"))
        assertEquals("JsonEvent", resultPopulated.event!!::class.simpleName)
    }

    @Test
    fun `defaultPutRequest should generate PutItemRequest correctly or use custom putRequest if provided`() {
        // Arrange
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.awsRegion() } returns "us-east-1"
        val pipeline = CorrelatePipeline(id = "test-pipeline", envConfig = envConfig)
        val uow = UnitOfWork(
            key = "test-key",
            event = createFakeEvent(
                eventId = "event-1", 
                eventTimestamp = 1600000000L, 
                encodedStr = """{"id":"event-1"}"""
            ),
            meta = mapOf(
                "sequenceNumber" to "123",
                "ttl" to "456",
                "expire" to "789"
            )
        )

        // Act - Default behavior
        val result = pipeline.defaultPutRequest(uow)

        // Assert - Default behavior
        val request = result.putRequest
        assertNotNull(request, "PutRequest should not be null")
        assertEquals("CORREL", (request.item?.get("discriminator") as? SdkAV.S)?.value)
        assertEquals("test-key", (request.item?.get("pk") as? SdkAV.S)?.value)
        assertEquals("test-pipeline", (request.item?.get("pipelineId") as? SdkAV.S)?.value)

        // Arrange & Act & Assert - Custom putRequest delegate function
        val expectedUow = UnitOfWork()
        val customPipeline = CorrelatePipeline("test-pipeline", putRequest = { expectedUow })
        
        val customResult = customPipeline.defaultPutRequest(uow)
        assertSame(expectedUow, customResult, "Should return the result of the custom putRequest delegate when it is provided")
    }

    // --- Unit Tests for the Connect Function ---

    @Test
    fun `connect should successfully process a valid UnitOfWork`() = runBlocking {
        // Arrange
        val dynamoDbClientMock = mockk<DynamoDbClient>()
        coEvery { dynamoDbClientMock.putItem(any()) } returns PutItemResponse.invoke {}

        val envConfigMock = mockk<EnvironmentConfig>()
        every { envConfigMock.awsRegion() } returns "us-east-1"
        every { envConfigMock.tableName() } returns "test-table"

        val pipeline = CorrelatePipeline(
            id = "test-pipeline",
            correlationKey = { "test-correlation-key" },
            dynamoDbClient = dynamoDbClientMock,
            envConfig = envConfigMock,
            onEventClass = listOf(FakeEvent::class), // specifically matching our FakeEvent
            unmarshall = { eventAsString -> FakeEvent(encodedStr = eventAsString)}
        )

        val skEventAttr = com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue().apply { s = "EVENT" }
        val streamRecord = com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord().apply {
            keys = mapOf("sk" to skEventAttr)
        }
        val validRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "INSERT"
            dynamodb = streamRecord
        }

        val validUow = UnitOfWork(
            record = validRecord,
            event = createFakeEvent(rawObj = RecordPair(null, null))
        )

        val fm = FaultManager()

        // Act
        val resultFlow = pipeline.connect(fm, flowOf(validUow))
        val resultList = resultFlow.toList()

        // Assert
        assertEquals(1, resultList.size, "Should process exactly one UnitOfWork")
        
        val processedUow = resultList.first()
        assertEquals("test-correlation-key", processedUow.key, "Correlation key should be added by the pipeline")
        assertNotNull(processedUow.putRequest, "A put request should be assigned")
        assertNotNull(processedUow.putResponse, "A put response should be assigned after dynamo mock execution")
        assertEquals("FakeEvent", processedUow.event!!::class.simpleName, "Event should have been normalized to a FakeEvent")
    }

    @Test
    fun `connect should filter out UnitOfWork when forCollectedEvents returns false`() = runBlocking {
        // Arrange
        val pipeline = CorrelatePipeline(
            id = "test-pipeline",
            correlationKey = { "test-correlation-key" }
        )

        // Invalid record (wrong eventName)
        val invalidRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "MODIFY" // Modify won't pass `forCollectedEvents` which expects INSERT
        }

        val invalidUow = UnitOfWork(
            record = invalidRecord,
            event = createFakeEvent(rawObj = RecordPair(null, null))
        )

        val fm = FaultManager()

        // Act
        val resultFlow = pipeline.connect(fm, flowOf(invalidUow))
        val resultList = resultFlow.toList()

        // Assert
        assertTrue(resultList.isEmpty(), "Flow should be empty because UnitOfWork was filtered out by forCollectedEvents")
    }

    @Test
    fun `connect should filter out UnitOfWork when onContentType returns false`() = runBlocking {
        // Arrange
        val pipeline = CorrelatePipeline(
            id = "test-pipeline",
            correlationKey = { "test-correlation-key" },
            onContentType = { false } // This will cause the event to be filtered mid-pipeline
        )

        val skEventAttr = com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue().apply { s = "EVENT" }
        val streamRecord = com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord().apply {
            keys = mapOf("sk" to skEventAttr)
        }
        val validRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "INSERT"
            dynamodb = streamRecord
        }

        val validUow = UnitOfWork(
            record = validRecord,
            event = createFakeEvent(rawObj = RecordPair(null, null))
        )

        val fm = FaultManager()

        // Act
        val resultFlow = pipeline.connect(fm, flowOf(validUow))
        val resultList = resultFlow.toList()

        // Assert
        assertTrue(resultList.isEmpty(), "Flow should be empty because UnitOfWork was filtered out by onContentType")
    }
}
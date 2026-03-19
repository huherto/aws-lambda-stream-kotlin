package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.flavors.CorrelatePipeline
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
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
        
        val streamRecordWithEvent = StreamRecord().apply {
            keys = mapOf("sk" to skEventAttr)
        }
        val streamRecordWithOther = StreamRecord().apply {
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
        pipeline.forCollectedEvents(validUow).shouldBeTrue()
        
        val wrongEventNameRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "MODIFY"
            dynamodb = streamRecordWithEvent
        }
        pipeline.forCollectedEvents(validUow.copy(record = wrongEventNameRecord)).shouldBeFalse()
        
        val wrongSkRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "INSERT"
            dynamodb = streamRecordWithOther
        }
        pipeline.forCollectedEvents(validUow.copy(record = wrongSkRecord)).shouldBeFalse()
        
        pipeline.forCollectedEvents(validUow.copy(event = createFakeEvent(rawObj = "Not a RecordPair"))).shouldBeFalse()
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
        resultPopulated.meta?.get("sequenceNumber") shouldBe "seq-123"
        resultPopulated.meta?.get("ttl") shouldBe "999"
        resultPopulated.meta?.get("data") shouldBe "test-data"
        resultPopulated.event!!::class.simpleName shouldBe "JsonEvent"
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
        request.shouldNotBeNull()
        (request.item?.get("discriminator") as? SdkAV.S)?.value shouldBe "CORREL"
        (request.item?.get("pk") as? SdkAV.S)?.value shouldBe "test-key"
        (request.item?.get("pipelineId") as? SdkAV.S)?.value shouldBe "test-pipeline"

        // Arrange & Act & Assert - Custom putRequest delegate function
        val expectedUow = UnitOfWork()
        val customPipeline = CorrelatePipeline("test-pipeline", putRequest = { expectedUow })
        
        val customResult = customPipeline.defaultPutRequest(uow)
        customResult shouldBeSameInstanceAs expectedUow
    }

    // --- Unit Tests for the Connect Function ---

    @Test
    fun `connect should successfully process a valid UnitOfWork`() : Unit = runBlocking {
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

        val skEventAttr = EventAV().apply { s = "EVENT" }
        val streamRecord = StreamRecord().apply {
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
        resultList.size shouldBe 1
        
        val processedUow = resultList.first()
        processedUow.key shouldBe "test-correlation-key"
        processedUow.putRequest.shouldNotBeNull()
        processedUow.putResponse.shouldNotBeNull()
        processedUow.event!!::class.simpleName shouldBe "FakeEvent"
    }

    @Test
    fun `connect should filter out UnitOfWork when forCollectedEvents returns false`() : Unit = runBlocking {
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
        resultList.shouldBeEmpty()
    }

    @Test
    fun `connect should filter out UnitOfWork when onContentType returns false`() : Unit = runBlocking {
        // Arrange
        val pipeline = CorrelatePipeline(
            id = "test-pipeline",
            correlationKey = { "test-correlation-key" },
            onContentType = { false } // This will cause the event to be filtered mid-pipeline
        )

        val skEventAttr = EventAV().apply { s = "EVENT" }
        val streamRecord = StreamRecord().apply {
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
        resultList.shouldBeEmpty()
    }
}
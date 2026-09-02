package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.DynamoDbClientFactory
import io.github.huherto.awsLambdaStream.extensions.putRequest
import io.github.huherto.awsLambdaStream.extensions.putResponse
import io.github.huherto.awsLambdaStream.extensions.saveOptions
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreInMemory
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Clock
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV


/** Tests for CorrelatePipeline. */
class CorrelatePipelineTest {
    companion object {
        val TIMESTAMP = Clock.System.now().toEpochMilliseconds()
    }

    /** Fake event implementation for testing. */
    data class FakeEvent(
        override val id: String? = "event-1",
        override val timestamp: Long? = TIMESTAMP,
        override val partitionKey: String? = null,
        override val tags: Map<String, String>? = null,
        override val raw: RawRecord? = null,
        override val eem: EnvelopeEncryptionMetadata? = null,
        override val triggers: List<EventReference>? = null,
        val encodedStr: String = "{}"
    ) : Event {
        override fun eventType() = "TestEvent"
        override fun toString() = encodedStr
        override fun copyEvent(
            id: String?,
            timestamp: Long?,
            partitionKey: String?,
            tags: Map<String, String>?,
            raw: RawRecord?,
            eem: EnvelopeEncryptionMetadata?,
            triggers: List<EventReference>?
        ): Event = copy(
            id = id,
            timestamp = timestamp,
            partitionKey = partitionKey,
            tags = tags,
            raw = raw,
            eem = eem,
            triggers = triggers
        )
    }

    /** Fake event codec for testing. */
    class FakeEventCodec : EventCodec {
        override fun encode(event: Event): String {
            return (event as FakeEvent).encodedStr
        }

        override fun decode(eventAsString: String): Event {
            return FakeEvent(encodedStr = eventAsString)
        }
    }

    private fun createFakeEvent(
        rawObj: RawRecord? = null,
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

    private val envConfig : EnvironmentConfig by lazy {
        val spy = spyk<EnvironmentConfig>()
        coEvery { spy.awsRegion() } returns "us-east-1"
        coEvery { spy.tableName() } returns "test-table"
        spy
    }

    @BeforeEach
    fun beforeEach() {
        GlobalRegistry.setEnvConfig(envConfig)
    }

    fun createEventsMicrostore() : EventsMicrostoreInMemory {
        val faultManager = mockk<FaultManager>(relaxed = true)
        val eventsMicrostore = EventsMicrostoreInMemory(faultManager)
        return eventsMicrostore
    }

    // --- Unit Tests for Internal Functions ---

    @Test
    fun `forCollectedEvents should evaluate correctly based on record and event properties`() {
        // Arrange
        val pipeline = CorrelatePipeline(
            "test-pipeline",
            correlationKeySupplier = { "test-correlation-key" },
            eventCodec = FakeEventCodec(),
            eventsMicrostore = createEventsMicrostore()
        )
        
        val skEventAttr = AttributeValue().apply { s = "EVENT" }
        val skOtherAttr = AttributeValue().apply { s = "OTHER" }
        
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
        
        pipeline.forCollectedEvents(validUow.copy(event = createFakeEvent(rawObj = JsonRaw(JsonPrimitive("Not a RecordPair"))))).shouldBeFalse()
    }

    @Test
    fun `normalize should extract metadata and populate JsonEvent from RecordPair`() {
        // Arrange
        val pipeline = CorrelatePipeline(
            "test-pipeline",
            correlationKeySupplier = { "test-correlation-key" },
            eventCodec = FakeEventCodec(),
            eventsMicrostore = createEventsMicrostore()
        )

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
        resultPopulated.event!!::class.simpleName shouldBe "FakeEvent"
    }

    @Test
    fun `save should map UnitOfWork to include SaveOptions and call EventsMicrostore`() : Unit = runBlocking {
        // Arrange
        val pipeline = CorrelatePipeline(
            id = "test-pipeline",
            eventsMicrostore = createEventsMicrostore(),
            expire = true,
            correlationKeySupplier = { "test-correlation-key" },
            eventCodec = FakeEventCodec(),
            correlationKeySuffix = "-suffix"
        )

        val event = createFakeEvent(eventId = "test-event-id", eventTimestamp = 12345L)
        val uow = UnitOfWork(
            record = DynamodbEvent.DynamodbStreamRecord(),
            event = event
        ).copy(
            key = "test-key",
            meta = mapOf("sequenceNumber" to "seq-1", "ttl" to "999")
        )

        // Act
        val resultFlow = with(pipeline) { flowOf(uow).save() }
        val resultList = resultFlow.toList()

        // Assert
        resultList.size shouldBe 1
        val processedUow = resultList.first()

        val saveOptions = processedUow.saveOptions
        saveOptions.shouldNotBeNull()

        saveOptions.pk shouldBe "test-key"
        saveOptions.sk shouldBe "test-event-id"
        saveOptions.discriminator shouldBe "CORREL"
        saveOptions.timeStamp shouldBe 12345
        saveOptions.awsRegion shouldBe "us-east-1" // comes from the envConfig spy
        saveOptions.sequenceNumber shouldBe "seq-1"
        saveOptions.ttl shouldBe 999L
        saveOptions.expire shouldBe true
        saveOptions.suffix shouldBe "-suffix"
        saveOptions.pipelineId shouldBe "test-pipeline"
    }

    // --- Unit Tests for the Connect Function ---

    @Test
    fun `connect should successfully process a valid UnitOfWork`() : Unit = runBlocking {
        // Arrange
        val dynamoDbClientMock = mockk<DynamoDbClient>()
        val dynamoDbClientFactory = spyk<DynamoDbClientFactory>()
        every { dynamoDbClientFactory.getClient(any()) } returns dynamoDbClientMock
        coEvery { dynamoDbClientMock.putItem(any()) } returns PutItemResponse.invoke {}

        val faultManager = FaultManager(eventPublisher = EventPublisherInMemory())
        val pipeline = CorrelatePipeline(
            id = "test-pipeline",
            correlationKeySupplier = { "test-correlation-key" },
            eventFilter = EventFilters.classes(FakeEvent::class),
            eventCodec = FakeEventCodec(),
            eventsMicrostore = EventsMicrostoreImpl(
                dynamoDbClientFactory = dynamoDbClientFactory,
                faultManager = faultManager),
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

        // Act
        val resultFlow = pipeline.connect(faultManager, flowOf(validUow))
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
            correlationKeySupplier = { "test-correlation-key" },
            eventCodec = FakeEventCodec(),
            eventsMicrostore = createEventsMicrostore(),
        )

        // Invalid record (wrong eventName)
        val invalidRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "MODIFY" // Modify won't pass `forCollectedEvents` which expects INSERT
        }

        val invalidUow = UnitOfWork(
            record = invalidRecord,
            event = createFakeEvent(rawObj = RecordPair(null, null))
        )

        val fm = FaultManager(eventPublisher = EventPublisherInMemory())

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
            correlationKeySupplier = { "test-correlation-key" },
            eventCodec = FakeEventCodec(),
            onContentType = { false }, // This will cause the event to be filtered mid-pipeline
            eventsMicrostore = createEventsMicrostore(),
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

        val fm = FaultManager(eventPublisher = EventPublisherInMemory())

        // Act
        val resultFlow = pipeline.connect(fm, flowOf(validUow))
        val resultList = resultFlow.toList()

        // Assert
        resultList.shouldBeEmpty()
    }
}
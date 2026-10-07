package io.kopipes.aws.flavors

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.kopipes.aws.AwsEnvironmentConfig
import io.kopipes.aws.DynamodbRaw
import io.kopipes.aws.connectors.DynamoDbClientFactory
import io.kopipes.aws.extensions.putRequest
import io.kopipes.aws.extensions.putResponse
import io.kopipes.aws.from.DynamoDbChangeEvent
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.from.RecordImage
import io.kopipes.aws.from.RecordPair
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore
import io.kopipes.core.*
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.flavors.CorrelatePipeline
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kopipes.core.sinks.EventsMicrostoreInMemory
import io.kopipes.core.sinks.saveOptions
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Clock
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV

/** Tests for CorrelatePipeline in AWS context. */
class CorrelatePipelineTest {
    companion object {
        val TIMESTAMP = Clock.System.now().toEpochMilliseconds()
    }

    data class FakeEvent(
        override val id: String? = "event-1",
        override val timestamp: Long? = TIMESTAMP,
        override val partitionKey: String? = null,
        override val tags: Map<String, String>? = null,
        override val raw: RawRecord? = null,
        override val eem: EnvelopeEncryptionMetadata? = null,
        override val triggers: List<EventReference>? = null,
        val encodedStr: String = "{}",
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

    class FakeEventCodec : EventCodec {
        override fun encode(event: Event): String = (event as FakeEvent).encodedStr
        override fun decode(eventAsString: String): Event = FakeEvent(encodedStr = eventAsString)
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

    private val envConfig: AwsEnvironmentConfig by lazy {
        val spy = spyk(AwsEnvironmentConfig())
        every { spy.awsRegion() } returns "us-east-1"
        every { spy.tableName() } returns "test-table"
        spy
    }

    @BeforeEach
    fun beforeEach() {
        GlobalRegistry.setEnvConfig(envConfig)
    }

    private fun createEventsMicrostore(): EventsMicrostoreInMemory {
        return EventsMicrostoreInMemory()
    }

    @Test
    fun `save should map UnitOfWork to include SaveOptions and call EventsMicrostore`(): Unit = runBlocking {
        val pipeline = CorrelatePipeline.builder()
            .id("test-pipeline")
            .eventsMicrostore(createEventsMicrostore())
            .expire(true)
            .correlationKeySupplier { "test-correlation-key" }
            .eventCodec(FakeEventCodec())
            .build()

        val event = createFakeEvent(eventId = "test-event-id", eventTimestamp = 12345L)
        val uow = UnitOfWork(
            event = event,
        )

        val fm = FaultManager(EventPublisherInMemory())
        val resultFlow = with(pipeline) { flowOf(uow).save(fm) }
        val resultList = resultFlow.toList()

        resultList.size shouldBe 1
        val processedUow = resultList.first()

        val saveOptions = processedUow.saveOptions
        saveOptions.shouldNotBeNull()
        saveOptions.pk shouldBe "test-correlation-key"
        saveOptions.sk shouldBe "test-event-id"
        saveOptions.discriminator shouldBe "CORREL"
        saveOptions.data shouldBe "test-event-id"
        saveOptions.expire shouldBe true
        saveOptions.pipelineId shouldBe "test-pipeline"
    }

    @Test
    fun `DynamodbAdapter forCollectedEvents should accept INSERT records with sk EVENT`() {
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
            event = DynamoDbChangeEvent(raw = DynamodbRaw(validRecord))
        )

        DynamodbAdapter.forCollectedEvents(validUow).shouldBeTrue()
    }

    @Test
    fun `DynamodbAdapter forCollectedEvents should reject non-INSERT or non-EVENT records`() {
        val skNotEventAttr = EventAV().apply { s = "CORREL" }
        val streamRecord = StreamRecord().apply {
            keys = mapOf("sk" to skNotEventAttr)
        }
        val nonInsertRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "MODIFY"
            dynamodb = streamRecord
        }

        val invalidUow = UnitOfWork(
            record = nonInsertRecord,
            event = DynamoDbChangeEvent(raw = DynamodbRaw(nonInsertRecord))
        )

        DynamodbAdapter.forCollectedEvents(invalidUow).shouldBeFalse()
    }

    @Test
    fun `DynamodbAdapter normalize should extract metadata and decode event`() {
        val recordImageMap = mapOf(
            "event" to EventAV().apply { s = "{\"hello\":\"world\"}" },
            "ttl" to EventAV().apply { n = "12345" },
            "data" to EventAV().apply { s = "data-val" }
        )
        val streamRecord = StreamRecord().apply {
            sequenceNumber = "seq-999"
        }
        val dynamodbRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            dynamodb = streamRecord
        }

        val rawPair = RecordPair(new = RecordImage(recordImageMap), old = null)
        val uow = UnitOfWork(
            record = dynamodbRecord,
            event = DynamoDbChangeEvent(raw = rawPair)
        )

        val normalizer = DynamodbAdapter.normalize(FakeEventCodec())
        val normalizedUow = normalizer(uow)

        normalizedUow.event.shouldBeInstanceOf<FakeEvent>()
        (normalizedUow.event as FakeEvent).encodedStr shouldBe "{\"hello\":\"world\"}"
        normalizedUow.meta?.get("sequenceNumber") shouldBe "seq-999"
        normalizedUow.meta?.get("ttl") shouldBe "12345"
        normalizedUow.meta?.get("data") shouldBe "data-val"
    }

    @Test
    fun `connect should successfully process a valid UnitOfWork with DynamoDbEventsMicrostore`(): Unit = runBlocking {
        val dynamoDbClientMock = mockk<DynamoDbClient>()
        val dynamoDbClientFactory = spyk<DynamoDbClientFactory>()
        every { dynamoDbClientFactory.getClient(any()) } returns dynamoDbClientMock
        coEvery { dynamoDbClientMock.putItem(any()) } returns PutItemResponse.invoke {}

        val faultManager = FaultManager(eventPublisher = EventPublisherInMemory())
        val pipeline = CorrelatePipeline.builder()
            .id("test-pipeline")
            .correlationKeySupplier { "test-correlation-key" }
            .eventFilter(EventFilters.classes(FakeEvent::class))
            .eventCodec(FakeEventCodec())
            .eventsMicrostore(
                DynamoDbEventsMicrostore(
                    dynamoDbClientFactory = dynamoDbClientFactory,
                    faultManager = faultManager
                )
            )
            .build()

        val validUow = UnitOfWork(
            event = createFakeEvent()
        )

        val resultFlow = pipeline.connect(faultManager, flowOf(validUow))
        val resultList = resultFlow.toList()

        resultList.size shouldBe 1
        val processedUow = resultList.first()
        processedUow.putRequest.shouldNotBeNull()
        processedUow.putResponse.shouldNotBeNull()
    }

    @Test
    fun `connect should decode DynamoDB stream record and correlate end to end`(): Unit = runBlocking {
        val dynamoDbClientMock = mockk<DynamoDbClient>()
        val dynamoDbClientFactory = spyk<DynamoDbClientFactory>()
        every { dynamoDbClientFactory.getClient(any()) } returns dynamoDbClientMock
        coEvery { dynamoDbClientMock.putItem(any()) } returns PutItemResponse.invoke {}

        val faultManager = FaultManager(eventPublisher = EventPublisherInMemory())
        val pipeline = CorrelatePipeline.builder()
            .id("test-pipeline")
            .correlationKeySupplier { "correlated-pk" }
            .eventFilter(EventFilters.classes(FakeEvent::class))
            .eventCodec(FakeEventCodec())
            .isCollectedEvent(DynamodbAdapter::forCollectedEvents)
            .normalizer(DynamodbAdapter.normalize(FakeEventCodec()))
            .eventsMicrostore(
                DynamoDbEventsMicrostore(
                    dynamoDbClientFactory = dynamoDbClientFactory,
                    faultManager = faultManager
                )
            )
            .build()

        val recordImageMap = mapOf(
            "event" to EventAV().apply { s = "{}" },
            "ttl" to EventAV().apply { n = "12345" },
            "data" to EventAV().apply { s = "data-val" }
        )
        val skEventAttr = EventAV().apply { s = "EVENT" }
        val streamRecord = StreamRecord().apply {
            keys = mapOf("sk" to skEventAttr)
            sequenceNumber = "seq-123"
        }
        val dynamodbRecord = DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "INSERT"
            dynamodb = streamRecord
        }

        val rawPair = RecordPair(new = RecordImage(recordImageMap), old = null)
        val incomingUow = UnitOfWork(
            record = dynamodbRecord,
            event = DynamoDbChangeEvent(raw = rawPair)
        )

        val resultFlow = pipeline.connect(faultManager, flowOf(incomingUow))
        val resultList = resultFlow.toList()

        resultList.size shouldBe 1
        val processedUow = resultList.first()
        processedUow.event.shouldBeInstanceOf<FakeEvent>()
        processedUow.saveOptions?.pk shouldBe "correlated-pk"
        processedUow.saveOptions?.discriminator shouldBe "CORREL"
        processedUow.putRequest.shouldNotBeNull()
        processedUow.putResponse.shouldNotBeNull()
    }
}

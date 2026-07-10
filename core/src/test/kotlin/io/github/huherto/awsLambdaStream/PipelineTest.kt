package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResultEntry
import aws.smithy.kotlin.runtime.ServiceException
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.connectors.DynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.EventBridgeClientFactory
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.CollectPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Test failure scenarios:
 * - DynamoDb is down
 * - DynamoDb is flaky
 * - EventBridge is down
 * - EventBridge is flaky
 */
class PipelineTest {

    private lateinit var envConfig: EnvironmentConfig
    private lateinit var dynamoDbClientFactory: DynamoDbClientFactory
    private lateinit var eventBridgeClientFactory: EventBridgeClientFactory
    private lateinit var dynamoDbClient: DynamoDbClient
    private lateinit var eventBridgeClient: EventBridgeClient

    @BeforeEach
    fun setUp() {
        envConfig = spyk(EnvironmentConfig())
        dynamoDbClientFactory = mockk()
        eventBridgeClientFactory = mockk()
        dynamoDbClient = mockk()
        eventBridgeClient = mockk()

        every { envConfig.awsRegion() } returns "us-east-1"
        every { envConfig.region() } returns "us-east-1"
        every { envConfig.tableName() } returns "events"
        every { envConfig.eventTableName() } returns "events"
        every { envConfig.entityTableName() } returns null
        every { envConfig.ttl() } returns 33
        every { envConfig.busName() } returns "test-bus"
        every { envConfig.busSource() } returns "pipeline-test"
        every { envConfig.busEndPointId() } returns null
        every { envConfig.maxPublishRequestSize() } returns null
        every { envConfig.maxRequestSize() } returns null
        every { envConfig.publishBatchSize() } returns 10
        every { envConfig.batchSize() } returns 10
        every { envConfig.publishParallel() } returns 1
        every { envConfig.parallel() } returns 1
        every { envConfig.timeout() } returns 1_000
        every { envConfig.accountName() } returns "test-account"
        every { envConfig.stage() } returns "test"
        every { envConfig.service() } returns "pipeline-test"
        every { envConfig.streamRetryEnabled() } returns false

        every { dynamoDbClientFactory.getClient(any()) } returns dynamoDbClient
        every { eventBridgeClientFactory.getClient(any()) } returns eventBridgeClient

        coEvery { dynamoDbClient.putItem(any<PutItemRequest>()) } returns PutItemResponse {}
        coEvery { eventBridgeClient.putEvents(any<PutEventsRequest>()) } returns successfulPutEventsResponse()
    }

    @Test
    fun `pipeline works end to end when everything works well`() = runTest {
        val container = buildTestContainer()
        val event = kinesisEvent(validEventJson(id = "event-1", partitionKey = "my-event-unit-1"))

        val results = container.assembler
            .assemble(container.kinesisAdapter.fromKinesis(event), includeFaultHandler = true)
            .toList()

        results shouldHaveSize 1
        results.first().event.shouldNotBeNull()
        results.first().event?.id shouldBe "event-1"
        results.first().key shouldBe "my-event-unit-1"

        coVerify(exactly = 1) { dynamoDbClient.putItem(any<PutItemRequest>()) }
        coVerify(exactly = 0) { eventBridgeClient.putEvents(any<PutEventsRequest>()) }
    }

    @Test
    fun `pipeline publishes a fault when an event is malformed poison pill`() = runBlocking {
        val container = buildTestContainer()
        val event = kinesisEvent("""{"this-is":"not a valid tracked unit event"""")

        val results = container.assembler
            .assemble(container.kinesisAdapter.fromKinesis(event), includeFaultHandler = true)
            .toList()

        results shouldHaveSize 0

        coVerify(exactly = 0) { dynamoDbClient.putItem(any<PutItemRequest>()) }
        coVerify(exactly = 1) { eventBridgeClient.putEvents(any<PutEventsRequest>()) }
    }

    @Test
    fun `pipeline publishes a fault when DynamoDb is down`() = runBlocking {
        coEvery { dynamoDbClient.putItem(any<PutItemRequest>()) } throws ServiceException("DynamoDb is down")

        val container = buildTestContainer()
        val event = kinesisEvent(validEventJson(id = "event-2", partitionKey = "tracked-unit-2"))

        val results = container.assembler
            .assemble(container.kinesisAdapter.fromKinesis(event), includeFaultHandler = true)
            .toList()

        results shouldHaveSize 0

        coVerify(exactly = 1) { dynamoDbClient.putItem(any<PutItemRequest>()) }
        coVerify(exactly = 1) { eventBridgeClient.putEvents(any<PutEventsRequest>()) }
    }

    @Test
    fun `pipeline throws when event bridge is down while publishing a fault`() = runTest {
        coEvery { dynamoDbClient.putItem(any<PutItemRequest>()) } throws ServiceException("DynamoDb is down")
        coEvery { eventBridgeClient.putEvents(any<PutEventsRequest>()) } throws ServiceException("EventBridge is down")

        val container = buildTestContainer()
        val event = kinesisEvent(validEventJson(id = "event-3", partitionKey = "tracked-unit-3"))

        kotlin.test.assertFailsWith<ServiceException> {
            container.assembler
                .assemble(container.kinesisAdapter.fromKinesis(event), includeFaultHandler = true)
                .toList()
        }

        coVerify(exactly = 1) { dynamoDbClient.putItem(any<PutItemRequest>()) }
        coVerify(atLeast = 1) { eventBridgeClient.putEvents(any<PutEventsRequest>()) }
    }

    @Test
    fun `pipeline succeeds when DynamoDb is flaky but recovers`() = runTest {
        coEvery { dynamoDbClient.putItem(any<PutItemRequest>()) } throwsMany listOf(
            ServiceException("DynamoDb transient failure"),
        ) andThen PutItemResponse {}

        val container = buildTestContainer()
        val first = kinesisRecord(validEventJson(id = "event-4", partitionKey = "tracked-unit-4"), "seq-4")
        val second = kinesisRecord(validEventJson(id = "event-5", partitionKey = "tracked-unit-5"), "seq-5")
        val event = kinesisEvent(first, second)

        val results = container.assembler
            .assemble(container.kinesisAdapter.fromKinesis(event), includeFaultHandler = true)
            .toList()

        results shouldHaveSize 1
        results.first().event?.id shouldBe "event-5"

        coVerify(exactly = 2) { dynamoDbClient.putItem(any<PutItemRequest>()) }
        coVerify(exactly = 1) { eventBridgeClient.putEvents(any<PutEventsRequest>()) }
    }

    @Test
    fun `pipeline succeeds when DynamoDb is down and event bridge is flaky but recovers`() = runTest {
        coEvery { dynamoDbClient.putItem(any<PutItemRequest>()) } throws ServiceException("DynamoDb is down")
        coEvery { eventBridgeClient.putEvents(any<PutEventsRequest>()) } returnsMany listOf(
            PutEventsResponse {
                failedEntryCount = 1
                entries = listOf(
                    PutEventsResultEntry {
                        errorCode = "InternalFailure"
                        errorMessage = "EventBridge transient failure"
                    }
                )
            },
            successfulPutEventsResponse(),
        )

        val container = buildTestContainer()
        val event = kinesisEvent(validEventJson(id = "event-6", partitionKey = "my-think-6"))

        val results = container.assembler
            .assemble(container.kinesisAdapter.fromKinesis(event), includeFaultHandler = true)
            .toList()

        results shouldHaveSize 0

        coVerify(exactly = 1) { dynamoDbClient.putItem(any<PutItemRequest>()) }
        coVerify(exactly = 2) { eventBridgeClient.putEvents(any<PutEventsRequest>()) }
    }

    private fun buildTestContainer(): TestListenerContainer {
        val eventPublisher = EventBridgePublisher(
            envConfig = envConfig,
            clientFactory = eventBridgeClientFactory,
            parallel = 1,
            batchSize = 10,
        )
        val faultManager = FaultManager(
            envConfig = envConfig,
            eventPublisher = eventPublisher,
        )
        val eventsMicrostore = EventsMicrostoreImpl(
            envConfig = envConfig,
            dynamoDbClientFactory = dynamoDbClientFactory,
            faultManager = faultManager,
        )

        return TestListenerContainer(
            envConfig = envConfig,
            eventsMicrostore = eventsMicrostore,
            faultManager = faultManager,
        )
    }

    private class TestListenerContainer(
        val envConfig: EnvironmentConfig,
        val eventsMicrostore: EventsMicrostore,
        val faultManager: FaultManager,
    ) {
        val kinesisAdapter: KinesisAdapter by lazy {
            KinesisAdapter(
                faultManager = faultManager,
                eventCodec = MyEventCodec(),
            )
        }

        private val collectPipeline: Pipeline by lazy {
            CollectPipeline(
                pipelineId = "coll1",
                envConfig = envConfig,
                eventsMicrostore = eventsMicrostore,
                eventFilter = EventFilters.classes(MyEvent::class),
            )
        }

        val assembler: PipelineAssembler by lazy {
            PipelineAssembler
                .builder()
                .faultManager(faultManager)
                .addPipeline(collectPipeline)
                .build()
        }
    }

    private fun kinesisEvent(payload: String): KinesisEvent =
        kinesisEvent(kinesisRecord(payload, sequenceNumber = "seq-1"))

    private fun kinesisEvent(vararg records: KinesisEvent.KinesisEventRecord): KinesisEvent =
        KinesisEvent().apply {
            this.records = records.toList()
        }

    private fun kinesisRecord(payload: String, sequenceNumber: String): KinesisEvent.KinesisEventRecord =
        KinesisEvent.KinesisEventRecord().apply {
            eventID = sequenceNumber
            kinesis = KinesisEvent.Record().apply {
                data = ByteBuffer.wrap(payload.toByteArray(StandardCharsets.UTF_8))
                this.sequenceNumber = sequenceNumber
                partitionKey = "partition-$sequenceNumber"
            }
        }

    private fun validEventJson(
        id: String,
        partitionKey: String,
    ): String =
        """
            {
              "id": "${id}",
              "type": "MY_EVENT_A",
              "timestamp": 1700000000000,
              "partitionKey": "${partitionKey}",
              "foo": "foo-value",
              "bar": "bar-value"
            }
        """.trimIndent()

    private fun successfulPutEventsResponse(): PutEventsResponse =
        PutEventsResponse {
            failedEntryCount = 0
            entries = listOf(
                PutEventsResultEntry {
                    eventId = "published-event"
                }
            )
        }
}
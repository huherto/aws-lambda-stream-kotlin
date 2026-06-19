package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultEvent
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreInMemory
import io.github.huherto.awsLambdaStream.testsupport.TestContext
import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.maps.shouldNotBeEmpty
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.every
import io.mockk.spyk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import kotlin.io.encoding.Base64

typealias TestEvent = com.amazonaws.services.lambda.runtime.tests.annotations.Event

class ListenerTest {

    private lateinit var microstore: EventsMicrostoreInMemory
    private lateinit var container: ListenerContainer
    private lateinit var listener: Listener

    @BeforeEach
    fun beforeEach() {
        val envConfig = spyk( EnvironmentConfig())
        every { envConfig.awsRegion() } returns "eu-west-1"
        every { envConfig.tableName() } returns "events-table-name"
        val eventPublisher = EventPublisherInMemory()
        val faultManager = FaultManager(envConfig, eventPublisher, skipErrorLogging = true)
        microstore = EventsMicrostoreInMemory(faultManager)
        
        container = ListenerContainer(
            envConfig = envConfig,
            eventsMicrostore = microstore,
            faultManager = faultManager,
        )
        
        listener = Listener(container)
    }

    @Test
    fun `should handle empty or null Kinesis records successfully without storing anything`() {
        // Arrange
        val emptyEvent = KinesisEvent().apply { records = emptyList() }
        val nullRecordsEvent = KinesisEvent()
        val context = TestContext()

        // Act
        val resultEmpty = listener.handleRequest(emptyEvent, context)
        val resultNull = listener.handleRequest(nullRecordsEvent, context)

        // Assert
        resultEmpty.shouldBeNull()
        resultNull.shouldBeNull()
        microstore.saveUowMap().shouldBeEmpty()
    }

    @ParameterizedTest
    @TestEvent(value = "events/kinesis_basic.json", type = KinesisEvent::class)
    fun `should handle valid Kinesis events and save to microstore`(event: KinesisEvent) {
        // Arrange
        val context = TestContext()

        // Act
        val result = listener.handleRequest(event, context)

        // Assert
        result.shouldBeNull()

        // Assert that the event was processed and collected into the memory microstore
        val savedUows = microstore.saveUowMap()
        savedUows.shouldNotBeEmpty()

        val uow = savedUows.values.first()
        uow.event shouldNotBe null
        uow.event?.id shouldBe "test-event-123"

        val publisher = container.faultManager.publisher() as EventPublisherInMemory
        publisher shouldNotBe null
        publisher.events().size shouldBe 1
        val event = publisher.events().first()
        event shouldNotBe null
        val fault = event.shouldBeInstanceOf<FaultEvent>()
        val faultUow = fault.uow shouldNotBe null
        val record = faultUow?.record.shouldBeInstanceOf<KinesisEvent.KinesisEventRecord>()
        record shouldNotBe null
        record.eventID shouldBe "shardId-000000000000:1"
    }

    @ParameterizedTest
    @TestEvent(value = "events/kinesis_basic.json", type = KinesisEvent::class)
    fun `should transform kinesis events into a flow`(event: KinesisEvent) {
        // Arrange

        // Act
        val flow = container.kinesisAdapter
            .fromKinesis(event)

        runBlocking {
            val eventList = flow.toList()
            eventList.size shouldBe 1
            val uow = eventList[0]
            uow.event shouldNotBe null
            uow.event?.id shouldBe "test-event-123"
        }

        val faults = container.faultManager.getFaults()
        faults.size shouldBe 1
        val fault = faults[0]
        fault shouldNotBe null
        fault.err shouldNotBe null

        val faultUow = fault.uow
        faultUow shouldNotBe null
        faultUow?.record shouldNotBe null
        val fm = container.faultManager

        runBlocking {
            val flushedCount = fm.flushFaults()
            flushedCount shouldBe 1
        }

    }

    @Test
    fun `create an event converted to base64 and back`() {
        val event = ShipmentCreatedEvent().apply {
            id = "test-event-123"
            partitionKey = "pk-1"
            entity = TrackedUnit().apply {
                id = "tu=123"
                senderFullName = "Fulanito de tal"
            }
        }

        val eventString: String = event.encoded()
        val base64: String = Base64.encode(eventString.toByteArray())

        val payload: ByteArray = Base64.decode(base64)
        val jsonString = String(payload)

        assert(jsonString == eventString)
    }
}
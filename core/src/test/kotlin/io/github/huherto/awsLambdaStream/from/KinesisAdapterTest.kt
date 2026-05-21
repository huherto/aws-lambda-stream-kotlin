package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.spyk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class KinesisAdapterTest {

    private val envConfig = spyk(EnvironmentConfig())
    private val faultManager = FaultManager(
        envConfig = envConfig,
        eventPublisher = EventPublisherInMemory(),
        skipErrorLogging = true,
    )
    private val eventCodec = MyEventCodec()
    private val adapter = KinesisAdapter(faultManager, eventCodec)

    @Test
    fun `fromKinesis should return empty flow for null or empty records`() = runBlocking {
        // Arrange
        val events = listOf(
            KinesisEvent().apply { records = null },
            KinesisEvent().apply { records = emptyList() },
        )

        // Act
        val results = events.map { adapter.fromKinesis(it).toList() }

        // Assert
        results.forEach { it.shouldBeEmpty() }
    }

    @Test
    fun `fromKinesis should decode kinesis records and preserve record sequence number and event ids`() : Unit = runBlocking {
        // Arrange
        val eventWithoutId = MyEventA(foo = "foo-1", bar = "bar-1")
        val eventWithId = MyEventB(foo = "foo-2", bar = "bar-2").apply {
            id = "existing-event-id"
        }

        val firstRecord = createKinesisRecord(
            eventId = "kinesis-event-1",
            sequenceNumber = "sequence-1",
            payload = eventWithoutId.encoded(),
        )
        val secondRecord = createKinesisRecord(
            eventId = "kinesis-event-2",
            sequenceNumber = "sequence-2",
            payload = eventWithId.encoded(),
        )
        val kinesisEvent = KinesisEvent().apply {
            records = listOf(firstRecord, secondRecord)
        }

        // Act
        val results = adapter.fromKinesis(kinesisEvent).toList()

        // Assert
        results.shouldHaveSize(2)

        results[0].record shouldBe firstRecord
        results[0].event shouldBe eventWithoutId
        results[0].event?.id shouldBe "kinesis-event-1"
        results[0].sequenceNumber shouldBe "sequence-1"

        results[1].record shouldBe secondRecord
        results[1].event shouldBe eventWithId
        results[1].event?.id shouldBe "existing-event-id"
        results[1].sequenceNumber shouldBe "sequence-2"
    }

    @Test
    fun `fromKinesis should redirect decode failures to fault manager and continue processing remaining records`() : Unit = runBlocking {
        // Arrange
        val validEvent = MyEventA(foo = "valid-foo", bar = "valid-bar").apply {
            id = "valid-event-id"
        }

        val invalidRecord = createKinesisRecord(
            eventId = "invalid-kinesis-event",
            sequenceNumber = "sequence-invalid",
            payload = """{"type":"UNKNOWN_EVENT","foo":"invalid"}""",
        )
        val validRecord = createKinesisRecord(
            eventId = "valid-kinesis-event",
            sequenceNumber = "sequence-valid",
            payload = validEvent.encoded(),
        )
        val kinesisEvent = KinesisEvent().apply {
            records = listOf(invalidRecord, validRecord)
        }

        // Act
        val results = adapter.fromKinesis(kinesisEvent).toList()

        // Assert
        results.shouldHaveSize(1)
        results[0].record shouldBe validRecord
        results[0].event shouldBe validEvent
        results[0].sequenceNumber shouldBe "sequence-valid"

        faultManager.getFaults().shouldHaveSize(1)
        faultManager.getFaults()[0].failureException?.uow?.record shouldBe invalidRecord
    }

    private fun createKinesisRecord(
        eventId: String,
        sequenceNumber: String,
        payload: String,
    ): KinesisEvent.KinesisEventRecord {
        val payloadBytes = payload.toByteArray(StandardCharsets.UTF_8)
        return KinesisEvent.KinesisEventRecord().apply {
            this.eventID = eventId
            this.kinesis = KinesisEvent.Record().apply {
                this.withSequenceNumber(sequenceNumber)
                .withData(ByteBuffer.wrap(payloadBytes))
            }
        }
    }
}
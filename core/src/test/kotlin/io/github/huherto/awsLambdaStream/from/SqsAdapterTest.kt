package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.spyk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class SqsAdapterTest {

    private val envConfig = spyk(EnvironmentConfig())

    private fun faultManager(): FaultManager {
        return FaultManager(
            envConfig = envConfig,
            eventPublisher = EventPublisherInMemory(),
            skipErrorLogging = true,
        )
    }

    private fun adapter(
        faultManager: FaultManager = faultManager(),
        eventCodec: EventCodec = MyEventCodec(),
    ): SqsAdapter {
        return SqsAdapter(faultManager, eventCodec)
    }

    @Test
    fun `fromSqs should return empty flow for null or empty records`() = runBlocking {
        // Arrange
        val adapter = adapter()
        val events = listOf(
            SQSEvent().apply { records = null },
            SQSEvent().apply { records = emptyList() },
        )

        // Act
        val results = events.map { adapter.fromSqs(it).toList() }

        // Assert
        results.forEach { it.shouldBeEmpty() }
    }

    @Test
    fun `fromSqs should wrap records without decoding events`() : Unit = runBlocking {
        // Arrange
        val adapter = adapter()
        val firstRecord = createSqsMessage(
            messageId = "message-1",
            body = """{"type":"MY_EVENT_A","foo":"foo-1"}""",
        )
        val secondRecord = createSqsMessage(
            messageId = "message-2",
            body = """{"type":"MY_EVENT_B","foo":"foo-2"}""",
        )
        val sqsEvent = SQSEvent().apply {
            records = listOf(firstRecord, secondRecord)
        }

        // Act
        val results = adapter.fromSqs(sqsEvent).toList()

        // Assert
        results.shouldHaveSize(2)

        results[0].record shouldBe firstRecord
        results[0].event shouldBe null

        results[1].record shouldBe secondRecord
        results[1].event shouldBe null
    }

    @Test
    fun `fromSqsEvent should return empty flow for null or empty records`() : Unit = runBlocking {
        // Arrange
        val adapter = adapter()
        val events = listOf(
            SQSEvent().apply { records = null },
            SQSEvent().apply { records = emptyList() },
        )

        // Act
        val results = events.map { adapter.fromSqsEvent(it).toList() }

        // Assert
        results.forEach { it.shouldBeEmpty() }
    }

    @Test
    fun `fromSqsEvent should decode records and preserve existing event ids or use message id`() : Unit = runBlocking {
        // Arrange
        val adapter = adapter()

        val eventWithoutId = MyEventA(foo = "foo-1", bar = "bar-1")
        val eventWithId = MyEventB(foo = "foo-2", bar = "bar-2").apply {
            id = "existing-event-id"
        }

        val firstRecord = createSqsMessage(
            messageId = "message-1",
            body = eventWithoutId.encoded(),
        )
        val secondRecord = createSqsMessage(
            messageId = "message-2",
            body = eventWithId.encoded(),
        )
        val sqsEvent = SQSEvent().apply {
            records = listOf(firstRecord, secondRecord)
        }

        // Act
        val results = adapter.fromSqsEvent(sqsEvent).toList()

        // Assert
        results.shouldHaveSize(2)

        results[0].record shouldBe firstRecord
        results[0].event shouldBe eventWithoutId
        results[0].event?.id shouldBe "message-1"

        results[1].record shouldBe secondRecord
        results[1].event shouldBe eventWithId
        results[1].event?.id shouldBe "existing-event-id"
    }

    @Test
    fun `fromSqsEvent should redirect decode failures to fault manager and continue processing remaining records`(): Unit  = runBlocking {
        // Arrange
        val faultManager = faultManager()
        val adapter = adapter(faultManager = faultManager)

        val validEvent = MyEventA(foo = "valid-foo", bar = "valid-bar").apply {
            id = "valid-event-id"
        }

        val invalidRecord = createSqsMessage(
            messageId = "invalid-message",
            body = """{"type":"UNKNOWN_EVENT","foo":"invalid"}""",
        )
        val validRecord = createSqsMessage(
            messageId = "valid-message",
            body = validEvent.encoded(),
        )
        val sqsEvent = SQSEvent().apply {
            records = listOf(invalidRecord, validRecord)
        }

        // Act
        val results = adapter.fromSqsEvent(sqsEvent).toList()

        // Assert
        results.shouldHaveSize(1)
        results[0].record shouldBe validRecord
        results[0].event shouldBe validEvent

        faultManager.getFaults().shouldHaveSize(1)
        faultManager.getFaults()[0].failureException?.uow?.record shouldBe invalidRecord
    }

    @Test
    fun `fromSqsEvent should filter out events tagged to skip`() : Unit = runBlocking {
        // Arrange
        val adapter = adapter()

        val skippedEvent = MyEventA(foo = "skip-foo").apply {
            id = "skipped-event-id"
            tags = mapOf("skip" to "true")
        }
        val keptEvent = MyEventA(foo = "keep-foo").apply {
            id = "kept-event-id"
            tags = mapOf("skip" to "false")
        }
        val eventWithoutSkipTag = MyEventA(foo = "untagged-foo").apply {
            id = "untagged-event-id"
        }

        val skippedRecord = createSqsMessage(
            messageId = "skipped-message",
            body = skippedEvent.encoded(),
        )
        val keptRecord = createSqsMessage(
            messageId = "kept-message",
            body = keptEvent.encoded(),
        )
        val untaggedRecord = createSqsMessage(
            messageId = "untagged-message",
            body = eventWithoutSkipTag.encoded(),
        )
        val sqsEvent = SQSEvent().apply {
            records = listOf(skippedRecord, keptRecord, untaggedRecord)
        }

        // Act
        val results = adapter.fromSqsEvent(sqsEvent).toList()

        // Assert
        results.shouldHaveSize(2)
        results[0].record shouldBe keptRecord
        results[0].event shouldBe keptEvent
        results[1].record shouldBe untaggedRecord
        results[1].event shouldBe eventWithoutSkipTag
    }

    @Test
    fun `fromSqsEvent should isolate codec failures per record`() : Unit = runBlocking {
        // Arrange
        val faultManager = faultManager()
        val codec = object : EventCodec {
            override fun decode(eventAsString: String): Event {
                if (eventAsString == "bad-body") {
                    error("decode failed")
                }

                return MyEventA(foo = eventAsString)
            }

            override fun encode(event: Event): String {
                return event.encoded()
            }
        }
        val adapter = adapter(
            faultManager = faultManager,
            eventCodec = codec,
        )

        val firstRecord = createSqsMessage(
            messageId = "message-1",
            body = "first-body",
        )
        val faultyRecord = createSqsMessage(
            messageId = "message-2",
            body = "bad-body",
        )
        val thirdRecord = createSqsMessage(
            messageId = "message-3",
            body = "third-body",
        )
        val sqsEvent = SQSEvent().apply {
            records = listOf(firstRecord, faultyRecord, thirdRecord)
        }

        // Act
        val results = adapter.fromSqsEvent(sqsEvent).toList()

        // Assert
        results.shouldHaveSize(2)

        results[0].record shouldBe firstRecord
        results[0].event?.id shouldBe "message-1"

        results[1].record shouldBe thirdRecord
        results[1].event?.id shouldBe "message-3"

        faultManager.getFaults().shouldHaveSize(1)
        faultManager.getFaults()[0].failureException?.uow?.record shouldBe faultyRecord
        faultManager.getFaults()[0].failureException?.cause?.message shouldBe "decode failed"
    }

    private fun createSqsMessage(
        messageId: String,
        body: String,
    ): SQSEvent.SQSMessage {
        return SQSEvent.SQSMessage().apply {
            this.messageId = messageId
            this.body = body
        }
    }
}
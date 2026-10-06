package io.kopipes.core.flavors

import io.kopipes.core.*
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kopipes.core.sinks.EventsMicrostoreInMemory
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Clock
import org.junit.jupiter.api.Test

class EvaluatePipelineTest {

    data class TestEvent(
        override val id: String? = "event-1",
        override val timestamp: Long? = Clock.System.now().toEpochMilliseconds(),
        override val partitionKey: String? = "pk-1",
        override val tags: Map<String, String>? = null,
        override val raw: RawRecord? = null,
        override val eem: EnvelopeEncryptionMetadata? = null,
        override val triggers: List<EventReference>? = null,
        val type: String = "TestEvent",
    ) : Event {
        override fun eventType() = type
        override fun toString() = """{"id":"$id","type":"$type"}"""
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

    private fun createPipeline(
        id: String = "pipeline-1",
        eventPublisher: EventPublisherInMemory = EventPublisherInMemory(),
        eventsMicrostore: EventsMicrostoreInMemory = EventsMicrostoreInMemory(),
        expression: ((UnitOfWork) -> Boolean)? = null,
        emit: ((UnitOfWork) -> List<Event>)? = null,
        isEvaluateEvent: ((UnitOfWork) -> Boolean)? = null,
        normalizer: ((UnitOfWork) -> UnitOfWork)? = null,
        correlationKeySuffix: String = "",
    ): EvaluatePipeline {
        return EvaluatePipeline.builder()
            .id(id)
            .eventPublisher(eventPublisher)
            .eventsMicrostore(eventsMicrostore)
            .expression(expression ?: { true })
            .emit(emit ?: { uow -> listOf(TestEvent(id = "higher-order-${uow.event?.id}")) })
            .apply {
                if (isEvaluateEvent != null) isEvaluateEvent(isEvaluateEvent)
                if (normalizer != null) normalizer(normalizer)
                if (correlationKeySuffix.isNotEmpty()) correlationKeySuffix(correlationKeySuffix)
            }
            .build()
    }

    @Test
    fun `forEvents should use default check when isEvaluateEvent is null`() {
        val pipeline = createPipeline()
        pipeline.forEvents(UnitOfWork(event = TestEvent())).shouldBeTrue()
        pipeline.forEvents(UnitOfWork(event = null)).shouldBeFalse()
    }

    @Test
    fun `forEvents should use custom isEvaluateEvent predicate`() {
        val pipeline = createPipeline(
            isEvaluateEvent = { uow -> uow.meta?.get("custom") == "true" }
        )
        pipeline.forEvents(UnitOfWork(meta = mapOf("custom" to "true"))).shouldBeTrue()
        pipeline.forEvents(UnitOfWork(meta = mapOf("custom" to "false"))).shouldBeFalse()
    }

    @Test
    fun `normalize should return uow when normalizer is null`() {
        val pipeline = createPipeline()
        val uow = UnitOfWork(event = TestEvent(id = "test-1"))
        pipeline.normalize(uow) shouldBe uow
    }

    @Test
    fun `normalize should use custom normalizer`() {
        val pipeline = createPipeline(
            normalizer = { uow ->
                val original = uow.event as TestEvent
                uow.copy(event = original.copy(id = "normalized-${original.id}"))
            }
        )
        val uow = UnitOfWork(event = TestEvent(id = "test-1"))
        val result = pipeline.normalize(uow)
        result.event?.id shouldBe "normalized-test-1"
    }

    @Test
    fun `toHigherOrderEvents should aggregate tags omit region and source and set triggers`() {
        val pipeline = EvaluatePipeline.builder()
            .id("pipeline-1")
            .eventPublisher(EventPublisherInMemory())
            .eventsMicrostore(EventsMicrostoreInMemory())
            .emit { uow ->
                listOf(TestEvent(id = "emitted-1"))
            }
            .build()

        val triggerEvent1 = TestEvent(
            id = "trigger-1",
            timestamp = 1000L,
            tags = mapOf("region" to "us-east-1", "source" to "aws", "app" to "my-app", "k1" to "v1")
        )
        val triggerEvent2 = TestEvent(
            id = "trigger-2",
            timestamp = 2000L,
            tags = mapOf("region" to "us-west-2", "k2" to "v2")
        )

        val uow = UnitOfWork(
            event = triggerEvent2,
            meta = mapOf("eventId" to "uow-event-id", "partitionKey" to "part-key-1"),
            triggers = listOf(triggerEvent1, triggerEvent2)
        )

        val results = pipeline.toHigherOrderEvents(uow)
        results shouldHaveSize 1
        val resultEvent = results.first().event.shouldNotBeNull()
        resultEvent.id shouldBe "uow-event-id"
        resultEvent.partitionKey shouldBe "part-key-1"
        resultEvent.timestamp shouldBe 2000L
        resultEvent.tags shouldBe mapOf("app" to "my-app", "k1" to "v1", "k2" to "v2")
        resultEvent.tags?.containsKey("region") shouldBe false
        resultEvent.tags?.containsKey("source") shouldBe false
        resultEvent.triggers shouldBe listOf(
            EventReference("trigger-1", "TestEvent", 1000L),
            EventReference("trigger-2", "TestEvent", 2000L)
        )
    }

    @Test
    fun `connect should evaluate and publish higher order events`(): Unit = runBlocking {
        val publisher = EventPublisherInMemory()
        val microstore = EventsMicrostoreInMemory()
        val pipeline = EvaluatePipeline.builder()
            .id("pipeline-eval")
            .eventPublisher(publisher)
            .eventsMicrostore(microstore)
            .expression { true }
            .emit { uow ->
                listOf(TestEvent(id = "higher-order-${uow.event?.id}"))
            }
            .build()

        val event = TestEvent(id = "evt-1")
        val uow = UnitOfWork(
            event = event,
            meta = mapOf("eventId" to "higher-order-evt-1", "partitionKey" to "pk-1")
        )
        val fm = FaultManager(EventPublisherInMemory())

        val result = pipeline.connect(fm, flowOf(uow)).toList()
        result shouldHaveSize 1
        result.first().event?.id shouldBe "higher-order-evt-1"
        publisher.uows() shouldHaveSize 1
    }

    @Test
    fun `connect should emit nothing when expression is false`(): Unit = runBlocking {
        val publisher = EventPublisherInMemory()
        val microstore = EventsMicrostoreInMemory()
        val pipeline = EvaluatePipeline.builder()
            .id("pipeline-eval")
            .eventPublisher(publisher)
            .eventsMicrostore(microstore)
            .expression { false }
            .emit { uow ->
                listOf(TestEvent(id = "higher-order-${uow.event?.id}"))
            }
            .build()

        val event = TestEvent(id = "evt-1")
        val uow = UnitOfWork(event = event)
        val fm = FaultManager(EventPublisherInMemory())

        val result = pipeline.connect(fm, flowOf(uow)).toList()
        result.shouldBeEmpty()
        publisher.uows().shouldBeEmpty()
    }
}

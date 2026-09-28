package io.kopipes.core

import io.kopipes.aws.*
import io.kopipes.aws.connectors.RetryConfig
import io.kopipes.aws.connectors.RetryExecutor
import io.kopipes.aws.connectors.RetryStrategy
import io.kopipes.aws.faults.FaultManager
import io.kopipes.aws.filters.EventFilter
import io.kopipes.aws.filters.filterEvents
import io.kopipes.aws.flavors.Pipeline
import io.kopipes.aws.metrics.CalculateMetrics
import io.kopipes.aws.metrics.MetricStats
import io.kopipes.aws.serialization.Snapshottable
import io.kopipes.aws.serialization.snapshots.DefaultUnitOfWorkSnapshotter
import io.kopipes.aws.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

/**
 * Validates that core stream processing, pipelines, filters, retry, and snapshotting
 * can execute completely in memory with zero AWS SDK dependencies.
 */
class PureCorePipelineTest {

    data class SampleDomainEvent(
        override val id: String? = "evt-123",
        override val timestamp: Long? = 1700000000000L,
        override val partitionKey: String? = "pk-1",
        override val tags: Map<String, String>? = mapOf("source" to "pure-test"),
        override val raw: RawRecord? = JsonRaw(kotlinx.serialization.json.JsonPrimitive("raw-data")),
        override val eem: EnvelopeEncryptionMetadata? = null,
        override val triggers: List<EventReference>? = null,
        val payload: String = "test-payload"
    ) : Event {
        override fun eventType(): String = "SampleDomainEvent"
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

    class PureTransformPipeline(id: String) : Pipeline(id) {
        override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
            return with(fm) {
                fromFlow
                    .filterEvents(fm, EventFilter.ByName("SampleDomainEvent"))
                    .mapNotFaulty { uow ->
                        val event = uow.event as? SampleDomainEvent
                        uow.copy(key = "transformed-${event?.id}")
                    }
            }
        }
    }

    class CustomDiagnosticContext(val detail: String) : Snapshottable {
        override fun toSnapshot(): Any = mapOf("detail" to detail)
    }

    @Test
    fun `pure pipeline processes in-memory flow successfully`() = runBlocking {
        val eventPublisher = EventPublisherInMemory()
        val faultManager = FaultManager(eventPublisher = eventPublisher, skipErrorLogging = true)

        val pipeline = PureTransformPipeline("pure-pipe-1")
        val inputUow = UnitOfWork(
            pipeline = pipeline,
            event = SampleDomainEvent(id = "order-42")
        )

        val output = pipeline.connect(faultManager, flowOf(inputUow)).toList()

        output shouldHaveSize 1
        output[0].key shouldBe "transformed-order-42"
        faultManager.getFaults() shouldHaveSize 0
    }

    @Test
    fun `pipeline assembler runs multiple pure pipelines`() = runBlocking {
        val eventPublisher = EventPublisherInMemory()
        val faultManager = FaultManager(eventPublisher = eventPublisher, skipErrorLogging = true)

        val pipeline1 = PureTransformPipeline("p1")
        val pipeline2 = PureTransformPipeline("p2")

        val assembler = PipelineAssembler.builder()
            .faultManager(faultManager)
            .addPipeline(pipeline1)
            .addPipeline(pipeline2)
            .build()

        val input = flowOf(
            UnitOfWork(event = SampleDomainEvent(id = "e1")),
            UnitOfWork(event = SampleDomainEvent(id = "e2"))
        )

        val result = assembler.assemble(input).toList()
        result shouldHaveSize 4
    }

    @Test
    fun `fault manager captures errors in pure pipeline and publishes faults`() = runBlocking {
        val eventPublisher = EventPublisherInMemory()
        val faultManager = FaultManager(eventPublisher = eventPublisher, skipErrorLogging = true)

        val faultyPipeline = object : Pipeline("faulty-pipe") {
            override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
                return with(fm) {
                    fromFlow.mapNotFaulty { _ ->
                        throw IllegalStateException("Intentional pure failure")
                    }
                }
            }
        }

        val input = flowOf(UnitOfWork(event = SampleDomainEvent(id = "err-1")))
        val output = faultyPipeline.connect(faultManager, input).toList()

        output shouldHaveSize 0
        faultManager.getFaults() shouldHaveSize 1

        val flushedCount = faultManager.flushFaults()
        flushedCount shouldBe 1
        eventPublisher.faults() shouldHaveSize 1
    }

    @Test
    fun `unit of work snapshot captures pure event and snapshottable extensions`() {
        val uow = UnitOfWork(
            event = SampleDomainEvent(id = "snap-1"),
            key = "sample-key"
        ).withExtension(CustomDiagnosticContext("extra-info"))

        val snapshotter = DefaultUnitOfWorkSnapshotter(emptyList())
        val snapshot = snapshotter.snapshot(uow)

        snapshot.event?.id shouldBe "snap-1"
        snapshot.event?.type shouldBe "SampleDomainEvent"
        snapshot.key shouldBe "sample-key"
        snapshot.extensions shouldNotBe null
    }

    @Test
    fun `pure retry executor retries until successful`() = runBlocking {
        var attempts = 0
        val retryConfig = RetryConfig(maxRetries = 3, retryWait = 1.milliseconds)
        val strategy = object : RetryStrategy<String, String, String> {
            override fun shouldRetry(response: String): Boolean = response == "retry"
            override fun nextRequest(originalRequest: String, response: String): String = originalRequest
            override fun combineAttempts(attempts: List<String>, finalResponse: String): String = finalResponse
        }

        val retryExecutor = RetryExecutor(retryConfig, strategy) { req ->
            attempts++
            if (attempts < 3) "retry" else "success-$req"
        }

        val result = retryExecutor.execute("item-1")

        result shouldBe "success-item-1"
        attempts shouldBe 3
    }

    @Test
    fun `pure calculate metrics calculates summary statistics`() {
        val stats: MetricStats = CalculateMetrics.calculateStats(listOf(10.0, 20.0, 30.0))
        stats.count shouldBe 3
        stats.sum shouldBe 60.0
        stats.average shouldBe 20.0
        stats.min shouldBe 10.0
        stats.max shouldBe 30.0
    }
}

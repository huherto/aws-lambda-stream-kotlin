package io.kopipes.core.serialization

import io.kopipes.core.*
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.flavors.Pipeline
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.junit.jupiter.api.Test

/** Tests for agnostic UnitOfWork snapshotting. */
class UnitOfWorkSnapshotTest {

    data class SimpleEvent(
        override val id: String? = "test-id",
        override val timestamp: Long? = 123456789L,
        override val partitionKey: String? = "pk-1",
        override val tags: Map<String, String>? = mapOf("tag1" to "val1"),
        override val raw: RawRecord? = null,
        override val eem: EnvelopeEncryptionMetadata? = null,
        override val triggers: List<EventReference>? = null,
    ) : Event {
        override fun eventType(): String = "simple.event"
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

    class TestPipeline(id: String) : Pipeline(id) {
        override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> = emptyFlow()
    }

    @Test
    fun `should create snapshot of pipeline and event`() {
        val pipeline = TestPipeline("test-p")
        val event = SimpleEvent()
        val uow = UnitOfWork(pipeline = pipeline, event = event, key = "key-1")

        val snapshot = uow.toSnapshot()

        snapshot.pipeline.shouldNotBeNull().id shouldBe "test-p"
        val eventSnapshot = snapshot.event.shouldNotBeNull()
        eventSnapshot.id shouldBe "test-id"
        eventSnapshot.type shouldBe "simple.event"
        eventSnapshot.partitionKey shouldBe "pk-1"
        snapshot.key shouldBe "key-1"
    }

    @Test
    fun `should leave record and raw absent when the unit of work has neither`() {
        val snapshot = UnitOfWork().toSnapshot()

        snapshot.record shouldBe null
        snapshot.event shouldBe null
    }
}

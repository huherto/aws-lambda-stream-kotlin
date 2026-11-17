package io.kopipes.core.flavors

import io.kopipes.core.JsonEvent
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.sinks.EntityStore
import io.kopipes.core.sinks.EntityStoreInMemory
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kopipes.core.sinks.entityUpdated
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class MaterializePipelineTest {

    private fun event(id: String, type: String = "PackageCreated") =
        JsonEvent("""{"id":"$id","type":"$type","timestamp":1700000000000}""")

    private fun markMaterialized(uow: UnitOfWork) =
        EntityStore.Update(EntityStore.Key("package", uow.event!!.id!!), set = mapOf("materialized" to true))

    private fun builder(store: EntityStore) = MaterializePipeline.builder()
        .id("materialize-packages")
        .entityStore(store)

    private fun run(pipeline: MaterializePipeline, fm: FaultManager, vararg uows: UnitOfWork) = runBlocking {
        pipeline.connect(fm, flowOf(*uows)).toList()
    }

    private fun faultManager() = FaultManager(EventPublisherInMemory(), skipErrorLogging = true)

    @Test
    fun `materializes each event the compaction keeps`() {
        // Arrange
        val store = EntityStoreInMemory()
        val pipeline = builder(store)
            .compact { flow -> flow.filter { it.key == "keep" } }
            .toUpdate(::markMaterialized)
            .build()

        // Act
        val result = run(
            pipeline, faultManager(),
            UnitOfWork(event = event("p1"), key = "skip"),
            UnitOfWork(event = event("p2"), key = "keep"),
        )

        // Assert
        result.map { it.key } shouldContainExactly listOf("keep")
        result.single().entityUpdated shouldBe true
        store.getStored().keys shouldBe setOf(EntityStore.Key("package", "p2"))
        store.get(EntityStore.Key("package", "p2"))!!["materialized"] shouldBe true
    }

    @Test
    fun `drops units the event filter or content filter rejects, before building the update`() {
        // Arrange
        val store = EntityStoreInMemory()
        val calls = mutableListOf<String>()
        val pipeline = builder(store)
            .eventFilter(EventFilters.name("PackageCreated"))
            .onContentType { it.key == "keep" }
            .toUpdate { uow -> calls += uow.event!!.id!!; markMaterialized(uow) }
            .build()

        // Act
        val result = run(
            pipeline, faultManager(),
            UnitOfWork(event = event("p1"), key = "keep"),
            UnitOfWork(event = event("p2", type = "PackageCancelled"), key = "keep"),
            UnitOfWork(event = event("p3"), key = "drop"),
        )

        // Assert
        result shouldHaveSize 1
        calls shouldContainExactly listOf("p1")
    }

    @Test
    fun `records a fault and goes on when building an update fails`() {
        // Arrange
        val store = EntityStoreInMemory()
        val fm = faultManager()
        val failing = UnitOfWork(event = event("p1"), key = "fail")
        val pipeline = builder(store)
            .toUpdate { uow -> if (uow.key == "fail") error("cannot materialize") else markMaterialized(uow) }
            .build()

        // Act
        val result = run(pipeline, fm, failing, UnitOfWork(event = event("p2"), key = "pass"))

        // Assert
        result.map { it.key } shouldContainExactly listOf("pass")
        fm.getFaults().single().runtimeUow shouldBe failing
    }

    @Test
    fun `a null update writes nothing`() {
        // Arrange
        val store = EntityStoreInMemory()
        val pipeline = builder(store).toUpdate { null }.build()

        // Act
        run(pipeline, faultManager(), UnitOfWork(event = event("p1")))

        // Assert
        store.getStored() shouldBe emptyMap()
    }

    @Test
    fun `sourceIsSelf matches the items this pipeline wrote`() {
        // Arrange
        val pipeline = builder(EntityStoreInMemory()).toUpdate { null }.build()
        fun item(pipelineId: String?) = UpdatePipelineTest.EventsTableItem(mapOf("pk" to "p", "pipelineId" to pipelineId))

        // Act & Assert
        pipeline.sourceIsSelf(UnitOfWork(event = item("materialize-packages"))) shouldBe true
        pipeline.sourceIsSelf(UnitOfWork(event = item("other"))) shouldBe false
        pipeline.sourceIsSelf(UnitOfWork(event = item(null))) shouldBe false
        pipeline.sourceIsSelf(UnitOfWork(event = event("p1"))) shouldBe false
    }

    @Test
    fun `build requires an entity store and toUpdate`() {
        // Act & Assert
        shouldThrow<IllegalArgumentException> {
            MaterializePipeline.builder().id("m").toUpdate { null }.build()
        }.message shouldBe "entityStore is required"
        shouldThrow<IllegalArgumentException> {
            MaterializePipeline.builder().id("m").entityStore(EntityStoreInMemory()).build()
        }.message shouldBe "toUpdate is required"
    }
}

package org.myorg.payments

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import io.kopipes.core.FaultException
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class GatewayPipelinesTest {

    private val publisher = EventPublisherInMemory()

    private fun assembler(vararg pipelines: io.kopipes.core.flavors.Pipeline, fm: FaultManager = FaultManager(publisher, skipErrorLogging = true)) =
        PipelineAssembler.builder().faultManager(fm).apply { pipelines.forEach { addPipeline(it) } }.build()

    @Test
    fun `the action pipeline acts on the events it filters and publishes nothing`() = runTest {
        // Arrange
        val acted = mutableListOf<String?>()
        val pipeline = ActionPipeline.builder()
            .id("credit")
            .eventFilter(depositsTo(Party.CARD))
            .action { uow -> acted += uow.event?.id }
            .build()
        val events = listOf(Fixtures.depositRequested(), Fixtures.depositRequested(Fixtures.toBank), Fixtures.fundsWithdrawn())

        // Act
        assembler(pipeline).assemble(events.map { UnitOfWork(event = it) }.asFlow()).toList()

        // Assert
        acted shouldContainExactly listOf("p-7.next-step")
        publisher.uows().shouldHaveSize(0)
    }

    @Test
    fun `a failing action becomes a fault and the others carry on`() = runTest {
        // Arrange
        val pipeline = ActionPipeline.builder()
            .id("withdraw")
            .action { uow -> if (uow.event?.id == "boom") error("core checking is down") }
            .build()
        val events = listOf(Fixtures.withdrawalRequested().copy(id = "boom"), Fixtures.withdrawalRequested())

        // Act
        val out = assembler(pipeline).assemble(events.map { UnitOfWork(event = it) }.asFlow()).toList()

        // Assert
        out.map { it.event?.id } shouldContainExactly listOf("p-7.start")
        publisher.faults() shouldHaveSize 1
    }

    @Test
    fun `the translate pipeline publishes what it translates from raw records`() = runTest {
        // Arrange
        val pipeline = TranslatePipeline.builder()
            .id("ledger")
            .toEvents { uow -> listOf(Fixtures.fundsWithdrawn().copy(id = uow.record as String)) }
            .eventPublisher(publisher)
            .build()

        // Act
        assembler(pipeline).assemble(listOf(UnitOfWork(record = "row-1"), UnitOfWork(record = "row-2")).asFlow()).toList()

        // Assert
        publisher.uows().mapNotNull { it.event?.id } shouldContainExactly listOf("row-1", "row-2")
    }

    @Test
    fun `an answer before its request is retried by the source when stream retry is on, not faulted`() = runTest {
        // Arrange
        val fm = FaultManager(publisher, skipErrorLogging = true, isStreamRetryEnabled = true, isRetryable = { it is PaymentNotKnownYet })
        val pipeline = TranslatePipeline.builder()
            .id("ledger")
            .toEvents { throw PaymentNotKnownYet("p-7") }
            .eventPublisher(publisher)
            .build()

        // Act / Assert
        shouldThrow<FaultException> {
            assembler(pipeline, fm = fm).assemble(listOf(UnitOfWork(record = "row-1")).asFlow()).toList()
        }
        publisher.faults() shouldHaveSize 0
    }

    @Test
    fun `requestUpdate keeps the payment of a request event under its id`() {
        // Act
        val request = requestUpdate("requests", UnitOfWork(event = Fixtures.withdrawalRequested())).shouldNotBeNull()

        // Assert
        request.key shouldBe mapOf("pk" to AttributeValue.S("p-7"), "sk" to AttributeValue.S("PAYMENT"))
        val json = (request.expressionAttributeValues!![":p"] as AttributeValue.S).value
        paymentsJson.decodeFromString(Payment.serializer(), json) shouldBe Fixtures.payment
        requestUpdate("requests", UnitOfWork(event = Fixtures.accountChanged())).shouldBeNull()
    }

    @Test
    fun `depositsTo tells card deposits from bank deposits`() {
        val filter = depositsTo(Party.BANK)
        filter.matches(Fixtures.depositRequested(Fixtures.toBank)) shouldBe true
        filter.matches(Fixtures.depositRequested()) shouldBe false
        filter.matches(Fixtures.fundsDeposited(Fixtures.toBank)) shouldBe false
        EventFilters.any().matches(Fixtures.fundsDeposited()) shouldBe true
    }
}

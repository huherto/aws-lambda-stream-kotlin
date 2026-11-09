package org.myorg.payments

import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class CardProcessorEgressTest {

    @AfterEach
    fun tearDown() = AwsGlobalRegistry.reset()

    @Test
    fun `in simulation the credit schedules the processor's webhook a few seconds later`() = runTest {
        // Arrange
        val deliveries = mutableListOf<Pair<String, Int>>()
        val processor = SimulatedCardProcessor({ body, delay -> deliveries += body to delay }, delaySeconds = 5, clock = { 1_000_000L })

        // Act
        processor.credit(Fixtures.payment.copy(to = Party(Party.CARD, "K-3")))

        // Assert
        val (body, delay) = deliveries.single()
        delay shouldBe 5
        val webhook = cardJson.decodeFromString(CardWebhook.serializer(), body)
        webhook.type shouldBe CardWebhook.FAILED
        webhook.created shouldBe 1005
    }

    @Test
    fun `K-6 - the processor delivers the same webhook twice, a few seconds apart`() = runTest {
        // Arrange
        val deliveries = mutableListOf<Pair<String, Int>>()
        val processor = SimulatedCardProcessor({ body, delay -> deliveries += body to delay }, delaySeconds = 5, clock = { 1_000_000L })

        // Act
        processor.credit(Fixtures.payment.copy(to = Party(Party.CARD, Simulation.DUPLICATE_WEBHOOK_CARD)))

        // Assert
        deliveries.map { it.second } shouldBe listOf(5, 5 + Simulation.REDELIVERY_SECONDS)
        deliveries.map { it.first }.toSet().size shouldBe 1
        cardJson.decodeFromString(CardWebhook.serializer(), deliveries.first().first).type shouldBe CardWebhook.SUCCEEDED
    }

    @Test
    fun `only card deposits are credited`() = runTest {
        // Arrange
        GlobalRegistry.setFaultManager(FaultManager(EventPublisherInMemory(), skipErrorLogging = true))
        val credited = mutableListOf<String>()
        val container = EgressContainer { payment -> credited += payment.to.account }
        val events = listOf(Fixtures.depositRequested(), Fixtures.depositRequested(Fixtures.toBank), Fixtures.fundsDeposited())

        // Act
        container.assembler.assemble(events.map { UnitOfWork(event = it) }.asFlow()).toList()

        // Assert
        credited shouldContainExactly listOf("K-2")
    }
}

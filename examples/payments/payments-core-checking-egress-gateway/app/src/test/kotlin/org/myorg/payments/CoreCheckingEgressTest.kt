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
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class CoreCheckingEgressTest {

    private class Recorded : DelayedDelivery {
        val deliveries = mutableListOf<Pair<String, Int>>()
        override suspend fun deliverLater(body: String, delaySeconds: Int) {
            deliveries += body to delaySeconds
        }
    }

    @AfterEach
    fun tearDown() = AwsGlobalRegistry.reset()

    @Test
    fun `in simulation the debit is not a call but the ledger row core checking would write, later`() = runTest {
        // Arrange
        val delivery = Recorded()
        val coreChecking = SimulatedCoreChecking(delivery, delaySeconds = 2, clock = { 1000L })

        // Act
        coreChecking.debit(Fixtures.payment)

        // Assert
        val (body, delay) = delivery.deliveries.single()
        delay shouldBe 2
        Json.decodeFromString(LedgerEntry.serializer(), body) shouldBe
            LedgerEntry("p-7-PMT", "C-1", "-100.00", "PMT", "p-7", "POSTED", null, "3000")
    }

    @Test
    fun `withdrawals debit, refunds credit, everything else is ignored, and nothing is published`() = runTest {
        // Arrange
        val publisher = EventPublisherInMemory()
        GlobalRegistry.setFaultManager(FaultManager(publisher, skipErrorLogging = true))
        val calls = mutableListOf<String>()
        val container = EgressContainer(object : CoreChecking {
            override suspend fun debit(payment: Payment) { calls += "debit ${payment.id}" }
            override suspend fun credit(payment: Payment) { calls += "credit ${payment.id}" }
        })
        val events = listOf(Fixtures.withdrawalRequested(), Fixtures.depositRequested(), Fixtures.refundRequested())

        // Act
        container.assembler.assemble(events.map { UnitOfWork(event = it) }.asFlow()).toList()

        // Assert
        calls shouldContainExactly listOf("debit p-7", "credit p-7")
        publisher.uows().size shouldBe 0
    }
}

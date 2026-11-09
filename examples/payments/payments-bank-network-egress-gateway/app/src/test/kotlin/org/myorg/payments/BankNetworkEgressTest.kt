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

class BankNetworkEgressTest {

    @AfterEach
    fun tearDown() = AwsGlobalRegistry.reset()

    @Test
    fun `in simulation each bank settles after its delay, B-7 after the deadline, B-8 rejected`() = runTest {
        // Arrange
        val deliveries = mutableListOf<Pair<SettlementLine, Int>>()
        val network = SimulatedBankNetwork(
            { body, delay -> deliveries += Json.decodeFromString(SettlementLine.serializer(), body) to delay },
            delaySeconds = 20,
            lateDelaySeconds = 180,
        )

        // Act
        listOf("B-9", "B-7", "B-8").forEach { account ->
            network.transfer(Fixtures.toBank.copy(id = "p-$account", to = Party(Party.BANK, account)))
        }

        // Assert
        deliveries shouldContainExactly listOf(
            SettlementLine("p-B-9", SettlementLine.SETTLED) to 20,
            SettlementLine("p-B-7", SettlementLine.SETTLED) to 180,
            SettlementLine("p-B-8", SettlementLine.REJECTED, "R03") to 20,
        )
    }

    @Test
    fun `B-6 - the network drops the same settlement file twice`() = runTest {
        // Arrange
        val deliveries = mutableListOf<Pair<String, Int>>()
        val network = SimulatedBankNetwork({ body, delay -> deliveries += body to delay }, delaySeconds = 20, lateDelaySeconds = 180)

        // Act
        network.transfer(Fixtures.toBank.copy(to = Party(Party.BANK, Simulation.DUPLICATE_FILE_BANK)))

        // Assert
        deliveries.map { it.second } shouldContainExactly listOf(20, 20 + Simulation.REDELIVERY_SECONDS)
        deliveries.map { it.first }.toSet().size shouldBe 1
    }

    @Test
    fun `only bank deposits go into the transfer file`() = runTest {
        // Arrange
        GlobalRegistry.setFaultManager(FaultManager(EventPublisherInMemory(), skipErrorLogging = true))
        val transferred = mutableListOf<String>()
        val container = EgressContainer { payment -> transferred += payment.to.account }
        val events = listOf(Fixtures.depositRequested(), Fixtures.depositRequested(Fixtures.toBank), Fixtures.fundsDeposited(Fixtures.toBank))

        // Act
        container.assembler.assemble(events.map { UnitOfWork(event = it) }.asFlow()).toList()

        // Assert
        transferred shouldContainExactly listOf("B-9")
    }
}

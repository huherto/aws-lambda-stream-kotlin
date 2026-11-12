package org.myorg.payments.adapters.inbound

import org.myorg.payments.*

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.testsupport.TestContext
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kopipes.core.sinks.EventsMicrostoreInMemory
import io.kopipes.core.sinks.saveOptions
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

class ListenerContainerTest {

    @Test
    fun `payment events are collected by event id and keyed to their payment, other events are skipped`() {
        // Arrange
        val publisher = EventPublisherInMemory()
        val context = AwsKopipesContext(eventPublisher = publisher, faultManager = FaultManager(publisher, skipErrorLogging = true))
        val microstore = EventsMicrostoreInMemory()
        val listener = Listener { ListenerContainer(context, eventsMicrostore = microstore) }
        val kinesisEvent = StreamRecords.kinesisEvent(
            Fixtures.paymentSubmitted(),
            Fixtures.fundsWithdrawn(),
            Fixtures.accountChanged(),
        )

        // Act
        listener.handleRequest(kinesisEvent, TestContext())

        // Assert
        val stored = microstore.getStored()
        stored.keys shouldContainExactlyInAnyOrder listOf("p-7.submitted", "p-7.withdrawn")
        val options = stored["p-7.withdrawn"].shouldNotBeNull().single().saveOptions.shouldNotBeNull()
        options.sk shouldBe "EVENT"
        options.data shouldBe "p-7"
        publisher.faults().size shouldBe 0
    }
}

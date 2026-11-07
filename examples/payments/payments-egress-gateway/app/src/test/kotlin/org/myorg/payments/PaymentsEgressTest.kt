package org.myorg.payments

import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.JsonRaw
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldNotContain
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class PaymentsEgressTest {

    @AfterEach
    fun tearDown() = AwsGlobalRegistry.reset()

    @Test
    fun `PAYMENT_COMPLETED becomes PAYMENTS_PAYMENT_COMPLETED with only what we choose to share`() {
        // Act
        val external = toExternal(UnitOfWork(event = Fixtures.paymentCompleted())).shouldBeInstanceOf<PaymentsExternalEvent>()

        // Assert
        external.type shouldBe PaymentsExternalEvent.PAYMENT_COMPLETED
        external.id shouldBe "p-7.done.external"
        external.partitionKey shouldBe "p-7"
        external.payment shouldBe ExternalPayment("p-7", "u-42", "100.00", "USD", "card", Fixtures.SUBMITTED_AT)
        external.reason.shouldBeNull()
    }

    @Test
    fun `the external payload never carries raw, triggers, account numbers or the steps`() {
        // Arrange
        val internal = Fixtures.paymentFailed().copy(raw = JsonRaw(Json.parseToJsonElement("""{"secret":"x"}""")))

        // Act
        val external = toExternal(UnitOfWork(event = internal))!!
            .copyEvent(raw = internal.raw, triggers = listOf(io.kopipes.core.EventReference("p-7.undone")))
        val json = external.toString()

        // Assert
        json shouldNotContain "C-1"
        json shouldNotContain "K-2"
        json shouldNotContain "raw"
        json shouldNotContain "triggers"
        Json.parseToJsonElement(json).jsonObject.keys shouldBe setOf("type", "payment", "reason", "id", "timestamp", "partitionKey", "tags")
    }

    @Test
    fun `failure reasons go out from the public list, anything new as rejected`() {
        publicReason(Reasons.ACCOUNT_CLOSED) shouldBe Reasons.ACCOUNT_CLOSED
        publicReason("fraud-suspected") shouldBe Reasons.REJECTED
        (toExternal(UnitOfWork(event = Fixtures.paymentFailed(reason = "fraud-suspected"))) as PaymentsExternalEvent).reason shouldBe Reasons.REJECTED
    }

    @Test
    fun `only outcomes go out, PAYMENT_EXPIRED and the steps stay inside`() = runTest {
        // Arrange
        val publisher = EventPublisherInMemory()
        GlobalRegistry.setFaultManager(FaultManager(publisher, skipErrorLogging = true))
        val container = EgressContainer(publisher)
        val events = listOf(
            Fixtures.paymentSubmitted(), Fixtures.fundsWithdrawn(), Fixtures.paymentExpired(),
            Fixtures.paymentCompleted(), Fixtures.paymentFailed(),
        )

        // Act
        container.assembler.assemble(events.map { UnitOfWork(event = it) }.asFlow()).toList()

        // Assert
        publisher.uows().mapNotNull { it.event?.eventType() } shouldContainExactly
            listOf(PaymentsExternalEvent.PAYMENT_COMPLETED, PaymentsExternalEvent.PAYMENT_FAILED)
    }
}

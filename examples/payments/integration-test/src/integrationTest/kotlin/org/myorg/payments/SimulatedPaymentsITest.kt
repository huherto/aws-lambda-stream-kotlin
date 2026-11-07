package org.myorg.payments

import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.payments.LocalPayments.correlated
import org.myorg.payments.LocalPayments.run
import org.myorg.payments.LocalPayments.status
import org.myorg.payments.LocalPayments.submit
import org.myorg.payments.LocalPayments.view
import org.myorg.payments.PaymentsEvent.Companion.DEPOSIT_FAILED
import org.myorg.payments.PaymentsEvent.Companion.DEPOSIT_REQUESTED
import org.myorg.payments.PaymentsEvent.Companion.FUNDS_DEPOSITED
import org.myorg.payments.PaymentsEvent.Companion.FUNDS_REFUNDED
import org.myorg.payments.PaymentsEvent.Companion.FUNDS_WITHDRAWN
import org.myorg.payments.PaymentsEvent.Companion.PAYMENT_COMPLETED
import org.myorg.payments.PaymentsEvent.Companion.PAYMENT_EXPIRED
import org.myorg.payments.PaymentsEvent.Companion.PAYMENT_FAILED
import org.myorg.payments.PaymentsEvent.Companion.PAYMENT_SUBMITTED
import org.myorg.payments.PaymentsEvent.Companion.REFUND_REQUESTED
import org.myorg.payments.PaymentsEvent.Companion.WITHDRAWAL_FAILED
import org.myorg.payments.PaymentsEvent.Companion.WITHDRAWAL_REQUESTED
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * The teaching scenarios with simulation mode on: nothing is injected, every step comes from the real services.
 *
 *   BFF → bus → control service → egress gateway (simulated system) → ingress gateway → bus → control service → BFF
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimulatedPaymentsITest {

    private fun id(name: String) = "it-$run-$name"

    private val toCard = id("card")
    private val toClosedCard = id("card-closed")
    private val notEnoughMoney = id("nsf")
    private val toBank = id("bank")
    private val toUnknownBank = id("bank-unknown")
    private val toLateBank = id("bank-late")

    @BeforeAll
    fun submitAll(): Unit = runBlocking {
        SeedAccounts.ensure()
        // Arrange: one payment per simulated outcome, all at once.
        listOf(
            submit(toCard, from = "C-1", to = "K-2"),
            submit(toClosedCard, from = "C-1", to = "K-3", amount = "25.50"),
            submit(notEnoughMoney, from = "C-2", to = "K-2", amount = "40.00"),
            submit(toBank, from = "C-1", to = "B-9", amount = "70.00"),
            submit(toUnknownBank, from = "C-1", to = "B-8", amount = "15.00"),
            submit(toLateBank, from = "C-1", to = "B-7", amount = "300.00"),
        ).forEach { it.status shouldBe 202 }
    }

    private suspend fun awaitStatus(id: String, expected: String, timeout: kotlin.time.Duration = 3.minutes) =
        eventually(timeout, what = "$id to be $expected") { status(id)?.takeIf { it == expected } }

    private suspend fun awaitChain(id: String, vararg types: String): List<PaymentsEvent> =
        eventually(2.minutes, what = "the control service to correlate ${types.toList()} for $id") {
            correlated(id).takeIf { events -> events.map { it.eventType() }.containsAll(types.toList()) }
        }

    private fun List<PaymentsEvent>.sourceOf(type: String) = first { it.eventType() == type }.tags?.get("source")

    @Test
    fun `happy path to a card - completed, each step from the gateway that heard it`(): Unit = runBlocking {
        // Act
        awaitStatus(toCard, "COMPLETED")

        // Assert
        val chain = awaitChain(toCard, PAYMENT_SUBMITTED, WITHDRAWAL_REQUESTED, FUNDS_WITHDRAWN, DEPOSIT_REQUESTED, FUNDS_DEPOSITED, PAYMENT_COMPLETED)
        chain.sourceOf(PAYMENT_SUBMITTED) shouldBe "payments-bff"
        chain.sourceOf(FUNDS_WITHDRAWN) shouldBe "payments-core-checking-ingress-gateway"
        chain.sourceOf(FUNDS_DEPOSITED) shouldBe "payments-card-processor-ingress-gateway"
        chain.first { it.eventType() == FUNDS_DEPOSITED }.id shouldBe "evt_$toCard.funds-deposited"
        chain.first { it.eventType() == PAYMENT_COMPLETED }.id shouldBe "$toCard.done"
    }

    @Test
    fun `refund - a closed card is compensated and the payment fails with its reason`(): Unit = runBlocking {
        // Act
        awaitStatus(toClosedCard, "FAILED")

        // Assert
        view(toClosedCard).string("reason") shouldBe Reasons.ACCOUNT_CLOSED
        val chain = awaitChain(toClosedCard, DEPOSIT_FAILED, REFUND_REQUESTED, FUNDS_REFUNDED, PAYMENT_FAILED)
        chain.sourceOf(FUNDS_REFUNDED) shouldBe "payments-core-checking-ingress-gateway"
        (chain.first { it.eventType() == PAYMENT_FAILED } as PaymentFailed).reason shouldBe Reasons.ACCOUNT_CLOSED
    }

    @Test
    fun `not enough money - the withdrawal fails and there is nothing to undo`(): Unit = runBlocking {
        // Act
        awaitStatus(notEnoughMoney, "FAILED")

        // Assert
        view(notEnoughMoney).string("reason") shouldBe Reasons.INSUFFICIENT_FUNDS
        val types = awaitChain(notEnoughMoney, WITHDRAWAL_FAILED, PAYMENT_FAILED).map { it.eventType() }
        types shouldNotContain DEPOSIT_REQUESTED
        types shouldNotContain REFUND_REQUESTED
    }

    @Test
    fun `happy path to a bank - settled from a settlement file`(): Unit = runBlocking {
        // Act
        awaitStatus(toBank, "COMPLETED")

        // Assert
        val chain = awaitChain(toBank, FUNDS_DEPOSITED, PAYMENT_COMPLETED)
        chain.sourceOf(FUNDS_DEPOSITED) shouldBe "payments-bank-network-ingress-gateway"
        chain.first { it.eventType() == FUNDS_DEPOSITED }.id!!.endsWith(":$toBank.funds-deposited") shouldBe true
    }

    @Test
    fun `an unknown bank account is rejected in the settlement file and refunded`(): Unit = runBlocking {
        // Act
        awaitStatus(toUnknownBank, "FAILED")

        // Assert
        view(toUnknownBank).string("reason") shouldBe Reasons.ACCOUNT_NOT_FOUND
        awaitChain(toUnknownBank, DEPOSIT_FAILED, FUNDS_REFUNDED, PAYMENT_FAILED)
    }

    @Test
    fun `late deposit - expired at the deadline, then completed when the bank settles`(): Unit = runBlocking {
        // Act: the deadline is 2 minutes after DEPOSIT_REQUESTED; LocalStack deletes expired items when asked.
        eventually(4.minutes, every = 5.seconds, what = "$toLateBank to expire") {
            LocalPayments.expireNow()
            status(toLateBank)?.takeIf { it == "EXPIRED" || it == "COMPLETED" }
        }

        // Assert: a timeout is not an outcome
        awaitStatus(toLateBank, "COMPLETED", timeout = 3.minutes)
        val chain = awaitChain(toLateBank, PAYMENT_EXPIRED, FUNDS_DEPOSITED, PAYMENT_COMPLETED)
        val expired = chain.first { it.eventType() == PAYMENT_EXPIRED }
        val deposited = chain.first { it.eventType() == FUNDS_DEPOSITED }
        expired.id shouldBe "$toLateBank.timeout"
        (expired.timestamp!! < deposited.timestamp!!) shouldBe true
        chain.map { it.eventType() } shouldContainAll listOf(DEPOSIT_REQUESTED, PAYMENT_COMPLETED)
    }
}

package org.myorg.payments

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.myorg.payments.PaymentsEvent.Companion.DEPOSIT_FAILED
import org.myorg.payments.PaymentsEvent.Companion.DEPOSIT_REQUESTED
import org.myorg.payments.PaymentsEvent.Companion.FUNDS_DEPOSITED
import org.myorg.payments.PaymentsEvent.Companion.FUNDS_REFUNDED
import org.myorg.payments.PaymentsEvent.Companion.FUNDS_WITHDRAWN
import org.myorg.payments.PaymentsEvent.Companion.PAYMENT_COMPLETED
import org.myorg.payments.PaymentsEvent.Companion.PAYMENT_EXPIRED
import org.myorg.payments.PaymentsEvent.Companion.PAYMENT_FAILED
import org.myorg.payments.PaymentsEvent.Companion.REFUND_REQUESTED
import org.myorg.payments.PaymentsEvent.Companion.WITHDRAWAL_FAILED

class PaymentViewTest {

    private fun record(payment: Payment = Fixtures.payment, vararg heard: Pair<String, Long>, reasons: Map<String, String> = emptyMap()) =
        PaymentRecord(payment = payment, requestHash = "h", heard = heard.toMap(), reasons = reasons)

    @Test
    fun `status follows the first matching rule of the table`() {
        // Arrange / Act / Assert: one row each, from the bottom up.
        statusOf(emptySet(), Party.CARD) shouldBe Status.SUBMITTED
        statusOf(setOf(FUNDS_WITHDRAWN), Party.CARD) shouldBe Status.WITHDRAWN
        statusOf(setOf(FUNDS_WITHDRAWN, DEPOSIT_REQUESTED), Party.CARD) shouldBe Status.SENT_TO_CARD
        statusOf(setOf(FUNDS_WITHDRAWN, DEPOSIT_REQUESTED), Party.BANK) shouldBe Status.SENT_TO_BANK
        statusOf(setOf(DEPOSIT_REQUESTED, PAYMENT_EXPIRED), Party.BANK) shouldBe Status.EXPIRED
        statusOf(setOf(DEPOSIT_FAILED), Party.CARD) shouldBe Status.REFUNDING
        statusOf(setOf(REFUND_REQUESTED), Party.CARD) shouldBe Status.REFUNDING
        statusOf(setOf(FUNDS_REFUNDED, PAYMENT_EXPIRED), Party.CARD) shouldBe Status.REFUNDING
        statusOf(setOf(PAYMENT_COMPLETED, PAYMENT_EXPIRED), Party.BANK) shouldBe Status.COMPLETED
        statusOf(setOf(WITHDRAWAL_FAILED), Party.CARD) shouldBe Status.FAILED
        statusOf(setOf(PAYMENT_FAILED, PAYMENT_COMPLETED), Party.CARD) shouldBe Status.FAILED
    }

    @Test
    fun `FUNDS_DEPOSITED alone does not complete the payment`() {
        // Act
        val status = statusOf(setOf(FUNDS_WITHDRAWN, DEPOSIT_REQUESTED, FUNDS_DEPOSITED), Party.CARD)

        // Assert
        status shouldBe Status.SENT_TO_CARD
    }

    @Test
    fun `a view built from events heard in any order is the same`() {
        // Arrange
        val events = listOf(FUNDS_WITHDRAWN to 1L, DEPOSIT_REQUESTED to 2L, FUNDS_DEPOSITED to 3L, PAYMENT_COMPLETED to 4L)

        // Act
        val inOrder = viewOf(record(Fixtures.payment, *events.toTypedArray()))
        val reversed = viewOf(record(Fixtures.payment, *events.reversed().toTypedArray()))

        // Assert
        reversed shouldBe inOrder
        inOrder.status shouldBe Status.COMPLETED
    }

    @Test
    fun `a pending deposit shows the expected time from the destination`() {
        // Act
        val toBank = viewOf(record(Fixtures.toBank, FUNDS_WITHDRAWN to 1L, DEPOSIT_REQUESTED to 2L))
        val toCard = viewOf(record(Fixtures.payment, FUNDS_WITHDRAWN to 1L, DEPOSIT_REQUESTED to 2L))

        // Assert
        toBank.status shouldBe Status.SENT_TO_BANK
        toBank.steps.map { it.step to it.state } shouldContainExactly listOf(
            "submitted" to Step.DONE, "withdrawn" to Step.DONE, "deposited" to Step.PENDING,
        )
        toBank.steps.last().expected shouldBe "next business day"
        toCard.steps.last().expected shouldBe "within minutes"
    }

    @Test
    fun `each step shows the time of the fact`() {
        // Act
        val view = viewOf(record(Fixtures.payment, FUNDS_WITHDRAWN to 1730900580000L))

        // Assert
        view.steps[0].at shouldBe Fixtures.SUBMITTED_AT
        view.steps[1].at shouldBe 1730900580000L
    }

    @Test
    fun `an expired payment says it is delayed and is not final`() {
        // Act
        val expired = viewOf(record(Fixtures.toBank, FUNDS_WITHDRAWN to 1L, DEPOSIT_REQUESTED to 2L, PAYMENT_EXPIRED to 3L))
        val lateDeposit = viewOf(
            record(Fixtures.toBank, FUNDS_WITHDRAWN to 1L, DEPOSIT_REQUESTED to 2L, PAYMENT_EXPIRED to 3L,
                FUNDS_DEPOSITED to 4L, PAYMENT_COMPLETED to 5L)
        )

        // Assert
        expired.status shouldBe Status.EXPIRED
        expired.message shouldBe DELAYED_MESSAGE
        lateDeposit.status shouldBe Status.COMPLETED
        lateDeposit.message.shouldBeNull()
    }

    @Test
    fun `a refunded payment shows the failed deposit, the refund and the outcome with its reason`() {
        // Act
        val view = viewOf(
            record(
                Fixtures.payment,
                FUNDS_WITHDRAWN to 1L, DEPOSIT_REQUESTED to 2L, DEPOSIT_FAILED to 3L, REFUND_REQUESTED to 4L,
                FUNDS_REFUNDED to 5L, PAYMENT_FAILED to 6L,
                reasons = mapOf(DEPOSIT_FAILED to Reasons.ACCOUNT_CLOSED, PAYMENT_FAILED to Reasons.ACCOUNT_CLOSED),
            )
        )

        // Assert
        view.status shouldBe Status.FAILED
        view.reason shouldBe Reasons.ACCOUNT_CLOSED
        view.steps.map { it.step to it.state } shouldContainExactly listOf(
            "submitted" to Step.DONE, "withdrawn" to Step.DONE, "deposited" to Step.FAILED,
            "refunded" to Step.DONE, "failed" to Step.DONE,
        )
    }

    @Test
    fun `a failed withdrawal fails the payment with its reason before PAYMENT_FAILED arrives`() {
        // Act
        val view = viewOf(record(Fixtures.payment, WITHDRAWAL_FAILED to 1L, reasons = mapOf(WITHDRAWAL_FAILED to Reasons.INSUFFICIENT_FUNDS)))

        // Assert
        view.status shouldBe Status.FAILED
        view.reason shouldBe Reasons.INSUFFICIENT_FUNDS
        view.steps[1].state shouldBe Step.FAILED
    }
}

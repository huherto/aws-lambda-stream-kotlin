package org.myorg.payments.domain

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test
import org.myorg.payments.DepositRequested
import org.myorg.payments.Fixtures
import org.myorg.payments.PaymentCompleted
import org.myorg.payments.PaymentExpired
import org.myorg.payments.PaymentFailed
import org.myorg.payments.Reasons
import org.myorg.payments.RefundRequested
import org.myorg.payments.WithdrawalRequested
import kotlin.time.Duration.Companion.minutes

/** The payment process on its own: plain events in, the next event out. No streams, tables or framework. */
class PaymentProcessTest {

    @Test
    fun `start - a submitted payment asks for the withdrawal`() {
        PaymentProcess.start(Fixtures.paymentSubmitted()) shouldBe WithdrawalRequested(payment = Fixtures.payment)
    }

    @Test
    fun `next step - withdrawn funds ask for the deposit`() {
        PaymentProcess.nextStep(Fixtures.fundsWithdrawn()) shouldBe DepositRequested(payment = Fixtures.payment)
    }

    @Test
    fun `done - deposited funds complete the payment`() {
        PaymentProcess.done(Fixtures.fundsDeposited()) shouldBe PaymentCompleted(payment = Fixtures.payment)
    }

    @Test
    fun `nothing to undo - a failed withdrawal fails the payment with its reason`() {
        PaymentProcess.nothingToUndo(Fixtures.withdrawalFailed(reason = Reasons.INSUFFICIENT_FUNDS)) shouldBe
            PaymentFailed(payment = Fixtures.payment, reason = Reasons.INSUFFICIENT_FUNDS)
    }

    @Test
    fun `compensate - a failed deposit asks for the refund`() {
        PaymentProcess.compensate(Fixtures.depositFailed()) shouldBe RefundRequested(payment = Fixtures.payment)
    }

    @Test
    fun `undone - the refund fails the payment with the reason of the deposit failure`() {
        // Arrange
        val history = listOf(Fixtures.depositFailed(reason = Reasons.ACCOUNT_NOT_FOUND), Fixtures.fundsRefunded())

        // Act
        val failed = PaymentProcess.undone(Fixtures.fundsRefunded(), history)

        // Assert
        failed.reason shouldBe Reasons.ACCOUNT_NOT_FOUND
    }

    @Test
    fun `undone - a refund without a deposit failure is something the process can't decide`() {
        shouldThrow<CannotDecide> { PaymentProcess.undone(Fixtures.fundsRefunded(), listOf(Fixtures.fundsRefunded())) }
    }

    @Test
    fun `timeout - the deposit is late only while no deposit outcome has been heard`() {
        PaymentProcess.isDepositLate(listOf(Fixtures.depositRequested())) shouldBe true
        PaymentProcess.isDepositLate(listOf(Fixtures.depositRequested(), Fixtures.fundsDeposited())) shouldBe false
        PaymentProcess.isDepositLate(listOf(Fixtures.depositRequested(), Fixtures.depositFailed())) shouldBe false
    }

    @Test
    fun `timeout - the deadline is the allowed time after the request, and expiring is not an outcome`() {
        // Act
        val deadline = PaymentProcess.deadline(requestedAt = 1_000_000L, allowed = 2.minutes)
        val expired = PaymentProcess.expire(Fixtures.depositRequested())

        // Assert
        deadline shouldBe 1_000L + 120
        expired.shouldBeInstanceOf<PaymentExpired>().payment shouldBe Fixtures.payment
    }

    @Test
    fun `a timeout is not an outcome - a late deposit still completes, a late failure still refunds`() {
        // Arrange: the deadline passed first
        PaymentProcess.isDepositLate(listOf(Fixtures.depositRequested())) shouldBe true

        // Act / Assert: the rules don't look at whether the payment expired
        PaymentProcess.done(Fixtures.fundsDeposited()).shouldBeInstanceOf<PaymentCompleted>()
        PaymentProcess.compensate(Fixtures.depositFailed()).shouldBeInstanceOf<RefundRequested>()
    }
}

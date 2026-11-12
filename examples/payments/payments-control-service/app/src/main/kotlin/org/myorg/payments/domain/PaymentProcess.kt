package org.myorg.payments.domain

import org.myorg.payments.DepositFailed
import org.myorg.payments.DepositRequested
import org.myorg.payments.FundsDeposited
import org.myorg.payments.FundsRefunded
import org.myorg.payments.FundsWithdrawn
import org.myorg.payments.PaymentCompleted
import org.myorg.payments.PaymentExpired
import org.myorg.payments.PaymentFailed
import org.myorg.payments.PaymentSubmitted
import org.myorg.payments.PaymentsEvent
import org.myorg.payments.RefundRequested
import org.myorg.payments.WithdrawalFailed
import org.myorg.payments.WithdrawalRequested
import kotlin.time.Duration

/** The process met a combination of events it has no rule for. The payment needs someone to look at it. */
class CannotDecide(message: String) : IllegalStateException(message)

/**
 * The payment process: which step follows which, the undo, and the timeout. Pure business rules: no streams, no
 * tables, no ids. The inbound adapters feed it the triggering event (and, where a rule needs it, the payment's
 * history); what it returns is published as the next fact.
 *
 * | Rule            | When it has heard…                                 | It emits               |
 * |-----------------|----------------------------------------------------|------------------------|
 * | start           | PAYMENT_SUBMITTED                                  | WITHDRAWAL_REQUESTED   |
 * | next step       | FUNDS_WITHDRAWN                                    | DEPOSIT_REQUESTED      |
 * | done            | FUNDS_DEPOSITED                                    | PAYMENT_COMPLETED      |
 * | nothing to undo | WITHDRAWAL_FAILED                                  | PAYMENT_FAILED         |
 * | compensate      | DEPOSIT_FAILED                                     | REFUND_REQUESTED       |
 * | undone          | FUNDS_REFUNDED (+ the DEPOSIT_FAILED behind it)    | PAYMENT_FAILED         |
 * | timeout         | DEPOSIT_REQUESTED, and by the deadline no outcome  | PAYMENT_EXPIRED        |
 *
 * A timeout is not an outcome: after PAYMENT_EXPIRED, a late FUNDS_DEPOSITED still completes the payment (done) and a
 * late DEPOSIT_FAILED still triggers the refund (compensate). The rules don't depend on the order events arrive in.
 */
object PaymentProcess {

    fun start(submitted: PaymentSubmitted) = WithdrawalRequested(payment = submitted.payment)

    fun nextStep(withdrawn: FundsWithdrawn) = DepositRequested(payment = withdrawn.payment)

    fun done(deposited: FundsDeposited) = PaymentCompleted(payment = deposited.payment)

    fun nothingToUndo(failed: WithdrawalFailed) = PaymentFailed(payment = failed.payment, reason = failed.reason)

    fun compensate(failed: DepositFailed) = RefundRequested(payment = failed.payment)

    /** The payment failed with the reason of the deposit failure that caused the refund. Without one: [CannotDecide]. */
    fun undone(refunded: FundsRefunded, history: List<PaymentsEvent>): PaymentFailed {
        val cause = history.filterIsInstance<DepositFailed>().firstOrNull()
            ?: throw CannotDecide("FUNDS_REFUNDED for ${refunded.payment.id} without a DEPOSIT_FAILED: the refund has no cause")
        return PaymentFailed(payment = refunded.payment, reason = cause.reason)
    }

    /** When a deposit requested at [requestedAt] (epoch ms) is late: [allowed] later, in epoch seconds. */
    fun deadline(requestedAt: Long, allowed: Duration): Long = requestedAt / 1000 + allowed.inWholeSeconds

    /** At the deadline: late if neither FUNDS_DEPOSITED nor DEPOSIT_FAILED has been heard. */
    fun isDepositLate(history: List<PaymentsEvent>): Boolean =
        history.none { it is FundsDeposited || it is DepositFailed }

    fun expire(requested: DepositRequested) = PaymentExpired(payment = requested.payment)
}

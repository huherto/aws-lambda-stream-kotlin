package org.myorg.payments.domain

import kotlinx.serialization.Serializable
import org.myorg.payments.Party
import org.myorg.payments.Payment
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

/**
 * What the BFF knows about a payment: the payment as accepted, and which events it has heard.
 * `heard` maps an event type to the event's own timestamp; `reasons` maps a failure type to its reason.
 * Both only grow, so a late, repeated or out-of-order event never makes the view go backwards.
 */
data class PaymentRecord(
    val payment: Payment,
    val requestHash: String,
    val heard: Map<String, Long> = emptyMap(),
    val reasons: Map<String, String> = emptyMap(),
)

enum class Status {
    SUBMITTED, WITHDRAWN, SENT_TO_CARD, SENT_TO_BANK, EXPIRED, REFUNDING, COMPLETED, FAILED,
}

@Serializable
data class Step(
    val step: String,
    val state: String,
    val at: Long? = null,
    val expected: String? = null,
    val reason: String? = null,
) {
    companion object {
        const val DONE = "done"
        const val PENDING = "pending"
        const val FAILED = "failed"
    }
}

/** The payment as the customer sees it. */
@Serializable
data class PaymentView(
    val id: String,
    val status: Status,
    val amount: String,
    val currency: String,
    val from: Party,
    val to: Party,
    val submittedAt: Long,
    val steps: List<Step>,
    val reason: String? = null,
    val message: String? = null,
)

/**
 * `status` depends only on which events the BFF has heard, never on their order. The first rule that matches wins.
 * `COMPLETED` comes only from the control service's `PAYMENT_COMPLETED`, never from `FUNDS_DEPOSITED` alone.
 */
fun statusOf(heard: Set<String>, toKind: String): Status = when {
    PAYMENT_FAILED in heard || WITHDRAWAL_FAILED in heard -> Status.FAILED
    PAYMENT_COMPLETED in heard -> Status.COMPLETED
    DEPOSIT_FAILED in heard || REFUND_REQUESTED in heard || FUNDS_REFUNDED in heard -> Status.REFUNDING
    PAYMENT_EXPIRED in heard -> Status.EXPIRED
    DEPOSIT_REQUESTED in heard -> if (toKind == Party.CARD) Status.SENT_TO_CARD else Status.SENT_TO_BANK
    FUNDS_WITHDRAWN in heard -> Status.WITHDRAWN
    else -> Status.SUBMITTED
}

/** No event carries the expected deposit time: the BFF works it out from the destination. */
fun expectedDeposit(toKind: String): String = if (toKind == Party.CARD) "within minutes" else "next business day"

const val DELAYED_MESSAGE = "delayed, we're on it"

fun viewOf(record: PaymentRecord): PaymentView {
    val payment = record.payment
    val heard = record.heard
    val reasons = record.reasons
    val status = statusOf(heard.keys, payment.to.kind)

    val steps = buildList {
        add(Step(step = "submitted", state = Step.DONE, at = payment.submittedAt))

        add(
            when {
                FUNDS_WITHDRAWN in heard -> Step("withdrawn", Step.DONE, at = heard[FUNDS_WITHDRAWN])
                WITHDRAWAL_FAILED in heard ->
                    Step("withdrawn", Step.FAILED, at = heard[WITHDRAWAL_FAILED], reason = reasons[WITHDRAWAL_FAILED])
                else -> Step("withdrawn", Step.PENDING)
            }
        )

        when {
            FUNDS_DEPOSITED in heard -> add(Step("deposited", Step.DONE, at = heard[FUNDS_DEPOSITED]))
            DEPOSIT_FAILED in heard ->
                add(Step("deposited", Step.FAILED, at = heard[DEPOSIT_FAILED], reason = reasons[DEPOSIT_FAILED]))
            DEPOSIT_REQUESTED in heard ->
                add(Step("deposited", Step.PENDING, expected = expectedDeposit(payment.to.kind)))
        }

        if (DEPOSIT_FAILED in heard || REFUND_REQUESTED in heard || FUNDS_REFUNDED in heard) {
            add(
                if (FUNDS_REFUNDED in heard) Step("refunded", Step.DONE, at = heard[FUNDS_REFUNDED])
                else Step("refunded", Step.PENDING)
            )
        }

        when {
            PAYMENT_FAILED in heard ->
                add(Step("failed", Step.DONE, at = heard[PAYMENT_FAILED], reason = reasons[PAYMENT_FAILED]))
            PAYMENT_COMPLETED in heard -> add(Step("completed", Step.DONE, at = heard[PAYMENT_COMPLETED]))
        }
    }

    return PaymentView(
        id = payment.id,
        status = status,
        amount = payment.amount,
        currency = payment.currency,
        from = payment.from,
        to = payment.to,
        submittedAt = payment.submittedAt,
        steps = steps,
        reason = if (status == Status.FAILED) reasons[PAYMENT_FAILED] ?: reasons[WITHDRAWAL_FAILED] else null,
        message = if (status == Status.EXPIRED) DELAYED_MESSAGE else null,
    )
}

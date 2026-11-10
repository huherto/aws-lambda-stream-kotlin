package org.myorg.payments.application

import org.myorg.payments.domain.PaymentRecord
import org.myorg.payments.domain.PaymentView
import org.myorg.payments.domain.Rejection
import org.myorg.payments.domain.Status
import org.myorg.payments.domain.checkSource
import org.myorg.payments.domain.viewOf

sealed interface Submission {
    /** Accepted now, or earlier with the same content: either way the customer sees the payment. */
    data class Accepted(val view: PaymentView) : Submission

    data class Invalid(val rejection: Rejection) : Submission

    /** The id was already accepted with another content. */
    data class Conflict(val id: String) : Submission
}

/** Up to the requested number of views, newest first; [next] is where the next page starts, absent on the last. */
data class PaymentsList(val items: List<PaymentView>, val next: PaymentKey? = null)

/**
 * Driving port: what a customer can do with their payments. The REST adapter calls it; nothing in here knows
 * about HTTP, headers, status codes or cursors.
 */
interface Payments {
    suspend fun submit(customerId: String, id: String?, body: String?): Submission

    /** `null` if unknown, or if it is another customer's payment. */
    suspend fun find(customerId: String, id: String): PaymentView?

    suspend fun list(customerId: String, statuses: Set<Status>, limit: Int, after: PaymentKey?): PaymentsList
}

class PaymentsService(
    private val store: PaymentsStore,
    private val clock: () -> Long = System::currentTimeMillis,
) : Payments {

    override suspend fun submit(customerId: String, id: String?, body: String?): Submission {
        val request = when (val validation = validate(id, customerId, body)) {
            is Validation.Invalid -> return Submission.Invalid(validation.error)
            is Validation.Valid -> validation.request
        }

        checkSource(request, store.findAccount(request.from.account))?.let { return Submission.Invalid(it) }

        return when (val outcome = store.submit(request, clock())) {
            is SubmitOutcome.Accepted -> Submission.Accepted(viewOf(outcome.record))
            is SubmitOutcome.AlreadyExists -> {
                val existing = outcome.record
                if (existing != null && existing.requestHash == request.hash) Submission.Accepted(viewOf(existing))
                else Submission.Conflict(request.id)
            }
        }
    }

    override suspend fun find(customerId: String, id: String): PaymentView? {
        val record = store.findPayment(id) ?: return null
        // Another customer's payment is not found, not forbidden, so ids can't be probed.
        return if (record.payment.customerId == customerId) viewOf(record) else null
    }

    /**
     * Collects up to [limit] payments matching [statuses], newest first. `status` is computed, not stored,
     * so the filter runs here, reading as many pages of the store as it takes.
     */
    override suspend fun list(customerId: String, statuses: Set<Status>, limit: Int, after: PaymentKey?): PaymentsList {
        val items = mutableListOf<PaymentView>()
        var from = after
        while (true) {
            val page = store.listByCustomer(customerId, from, limit)
            for ((index, record) in page.records.withIndex()) {
                val view = viewOf(record)
                if (statuses.isEmpty() || view.status in statuses) items += view
                if (items.size == limit) {
                    val more = index < page.records.lastIndex || page.more
                    return PaymentsList(items, next = if (more) record.key() else null)
                }
            }
            if (!page.more || page.records.isEmpty()) return PaymentsList(items)
            from = page.records.last().key()
        }
    }

    private fun PaymentRecord.key() = PaymentKey(payment.id, payment.submittedAt)
}

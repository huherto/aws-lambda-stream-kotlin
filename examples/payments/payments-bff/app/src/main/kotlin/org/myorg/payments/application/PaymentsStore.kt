package org.myorg.payments.application

import org.myorg.payments.Account
import org.myorg.payments.domain.PaymentRecord
import org.myorg.payments.domain.ValidRequest

/** Where a page of a customer's payments starts: the last payment of the previous page. */
data class PaymentKey(val id: String, val submittedAt: Long)

data class PaymentsPage(val records: List<PaymentRecord>, val more: Boolean)

sealed interface SubmitOutcome {
    /** The first successful submission for this id. */
    data class Accepted(val record: PaymentRecord) : SubmitOutcome

    /** The id was already accepted; the caller compares the stored request with its own. */
    data class AlreadyExists(val record: PaymentRecord?) : SubmitOutcome
}

/**
 * Driven port: what the use cases need from storage. The DynamoDB adapter implements it in production,
 * an in-memory one in the tests.
 *
 * Accepting a payment also publishes `PAYMENT_SUBMITTED`: the store's change stream does it (no dual write),
 * so there is no separate publishing port here.
 */
interface PaymentsStore {
    suspend fun findAccount(number: String): Account?
    suspend fun findPayment(id: String): PaymentRecord?
    suspend fun submit(request: ValidRequest, submittedAt: Long): SubmitOutcome
    suspend fun listByCustomer(customerId: String, after: PaymentKey?, limit: Int): PaymentsPage
}

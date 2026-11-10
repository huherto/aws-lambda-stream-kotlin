package org.myorg.payments.application

import org.myorg.payments.Payment

/**
 * Core checking's withdraw and refund API. It never says "it worked": the answer comes from its ledger,
 * through the ingress gateway's change data capture ("act, then hear back").
 */
interface CoreChecking {
    suspend fun debit(payment: Payment)
    suspend fun credit(payment: Payment)

    /** Whether core checking shows a posting (posted or rejected) for this payment and transaction type. */
    suspend fun posted(payment: Payment, txnType: String): Boolean
}

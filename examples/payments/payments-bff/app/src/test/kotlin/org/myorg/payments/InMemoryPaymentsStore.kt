package org.myorg.payments

/** A [PaymentsStore] that behaves like the DynamoDB one, for testing the API without a table. */
class InMemoryPaymentsStore : PaymentsStore {

    val accounts = mutableMapOf<String, Account>()
    val payments = mutableMapOf<String, PaymentRecord>()
    var submits = 0
        private set

    override suspend fun findAccount(number: String): Account? = accounts[number]

    override suspend fun findPayment(id: String): PaymentRecord? = payments[id]

    override suspend fun submit(request: ValidRequest, submittedAt: Long): SubmitOutcome {
        payments[request.id]?.let { return SubmitOutcome.AlreadyExists(it) }
        submits++
        val record = PaymentRecord(request.toPayment(submittedAt), request.hash)
        payments[request.id] = record
        return SubmitOutcome.Accepted(record)
    }

    override suspend fun listByCustomer(customerId: String, after: PaymentKey?, limit: Int): PaymentsPage {
        val sorted = payments.values
            .filter { it.payment.customerId == customerId }
            .sortedWith(compareByDescending<PaymentRecord> { it.payment.submittedAt }.thenByDescending { it.payment.id })
        val start = after?.let { key -> sorted.indexOfFirst { it.payment.id == key.id } + 1 } ?: 0
        val page = sorted.drop(start).take(limit)
        return PaymentsPage(records = page, more = start + page.size < sorted.size)
    }

    fun hear(id: String, type: String, at: Long, reason: String? = null) {
        val record = payments.getValue(id)
        payments[id] = record.copy(
            heard = record.heard + (type to at),
            reasons = if (reason != null) record.reasons + (type to reason) else record.reasons,
        )
    }
}

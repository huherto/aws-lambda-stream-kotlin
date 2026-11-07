package org.myorg.payments

/**
 * The running example: payment `p-7`, 100 from checking account `C-1` to card `K-2`.
 * One event per type in the internal catalog, shared by every service's tests.
 */
object Fixtures {

    const val PAYMENT_ID = "p-7"
    const val CUSTOMER_ID = "u-42"
    const val SUBMITTED_AT = 1730900520000L

    val payment = Payment(
        id = PAYMENT_ID,
        customerId = CUSTOMER_ID,
        from = Party(Party.CHECKING, "C-1"),
        to = Party(Party.CARD, "K-2"),
        amount = "100.00",
        currency = "USD",
        submittedAt = SUBMITTED_AT,
    )

    val toBank = payment.copy(to = Party(Party.BANK, "B-9"))

    private fun at(minutes: Int) = SUBMITTED_AT + minutes * 60_000L

    private fun tags(source: String) = mapOf("source" to source)

    fun paymentSubmitted(p: Payment = payment) = PaymentSubmitted(
        payment = p, id = "${p.id}.submitted", timestamp = at(0), partitionKey = p.id, tags = tags("payments-bff"),
    )

    fun withdrawalRequested(p: Payment = payment) = WithdrawalRequested(
        payment = p, id = "${p.id}.start", timestamp = at(0), partitionKey = p.id, tags = tags("payments-control-service"),
    )

    fun fundsWithdrawn(p: Payment = payment) = FundsWithdrawn(
        payment = p, id = "${p.id}.withdrawn", timestamp = at(1), partitionKey = p.id,
        tags = tags("payments-core-checking-ingress-gateway"),
    )

    fun withdrawalFailed(p: Payment = payment, reason: String = Reasons.INSUFFICIENT_FUNDS) = WithdrawalFailed(
        payment = p, reason = reason, id = "${p.id}.withdrawal-failed", timestamp = at(1), partitionKey = p.id,
        tags = tags("payments-core-checking-ingress-gateway"),
    )

    fun depositRequested(p: Payment = payment) = DepositRequested(
        payment = p, id = "${p.id}.next-step", timestamp = at(1), partitionKey = p.id, tags = tags("payments-control-service"),
    )

    fun fundsDeposited(p: Payment = payment) = FundsDeposited(
        payment = p, id = "${p.id}.deposited", timestamp = at(2), partitionKey = p.id,
        tags = tags("payments-card-processor-ingress-gateway"),
    )

    fun depositFailed(p: Payment = payment, reason: String = Reasons.ACCOUNT_CLOSED) = DepositFailed(
        payment = p, reason = reason, id = "${p.id}.deposit-failed", timestamp = at(2), partitionKey = p.id,
        tags = tags("payments-card-processor-ingress-gateway"),
    )

    fun refundRequested(p: Payment = payment) = RefundRequested(
        payment = p, id = "${p.id}.compensate", timestamp = at(2), partitionKey = p.id, tags = tags("payments-control-service"),
    )

    fun fundsRefunded(p: Payment = payment) = FundsRefunded(
        payment = p, id = "${p.id}.refunded", timestamp = at(3), partitionKey = p.id,
        tags = tags("payments-core-checking-ingress-gateway"),
    )

    fun paymentCompleted(p: Payment = payment) = PaymentCompleted(
        payment = p, id = "${p.id}.done", timestamp = at(2), partitionKey = p.id, tags = tags("payments-control-service"),
    )

    fun paymentFailed(p: Payment = payment, reason: String = Reasons.ACCOUNT_CLOSED) = PaymentFailed(
        payment = p, reason = reason, id = "${p.id}.undone", timestamp = at(3), partitionKey = p.id,
        tags = tags("payments-control-service"),
    )

    fun paymentExpired(p: Payment = payment) = PaymentExpired(
        payment = p, id = "${p.id}.timeout", timestamp = at(3), partitionKey = p.id, tags = tags("payments-control-service"),
    )

    fun accountChanged(
        number: String = "C-1",
        customerId: String = CUSTOMER_ID,
        status: String = Account.OPEN,
        timestamp: Long = SUBMITTED_AT - 86_400_000L,
    ) = AccountChanged(
        account = Account(number = number, kind = Party.CHECKING, customerId = customerId, status = status),
        id = "$number.$timestamp.account-changed", timestamp = timestamp, partitionKey = number,
        tags = tags("payments-accounts-ingress-gateway"),
    )

    /** Every event of the internal catalog, once. */
    fun all(): List<PaymentsEvent> = listOf(
        paymentSubmitted(), withdrawalRequested(), fundsWithdrawn(), withdrawalFailed(), depositRequested(),
        fundsDeposited(), depositFailed(), refundRequested(), fundsRefunded(), paymentCompleted(), paymentFailed(),
        paymentExpired(), accountChanged(),
    )
}

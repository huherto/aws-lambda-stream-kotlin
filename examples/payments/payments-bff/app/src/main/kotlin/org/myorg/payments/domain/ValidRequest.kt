package org.myorg.payments.domain

import kotlinx.serialization.Serializable
import org.myorg.payments.Account
import org.myorg.payments.Party
import org.myorg.payments.Payment
import java.security.MessageDigest

/** Why a request was turned down, and which field is to blame. The REST adapter shows it as the error body. */
@Serializable
data class Rejection(val error: String, val message: String, val field: String? = null)

/** A request that passed the checks that need no data: the payment-to-be, without `submittedAt` yet. */
data class ValidRequest(
    val id: String,
    val customerId: String,
    val from: Party,
    val to: Party,
    val amount: String,
    val currency: String,
) {
    /** Identifies the request's content: a repeat submission with another content is a conflict. */
    val hash: String by lazy {
        val canonical = listOf(id, customerId, from.kind, from.account, to.kind, to.account, amount, currency)
            .joinToString("|")
        MessageDigest.getInstance("SHA-256").digest(canonical.toByteArray())
            .joinToString("") { "%02x".format(it) }
    }

    fun toPayment(submittedAt: Long) = Payment(
        id = id,
        customerId = customerId,
        from = from,
        to = to,
        amount = amount,
        currency = currency,
        submittedAt = submittedAt,
    )
}

/** A known **open** checking account of this customer. "Known" means the BFF heard of it through ACCOUNT_CHANGED. */
fun checkSource(request: ValidRequest, account: Account?): Rejection? {
    val usable = account != null &&
        account.customerId == request.customerId &&
        account.kind == Party.CHECKING &&
        account.status == Account.OPEN
    return if (usable) null
    else Rejection("invalid-request", "from.account is not an open checking account of this customer", "from.account")
}

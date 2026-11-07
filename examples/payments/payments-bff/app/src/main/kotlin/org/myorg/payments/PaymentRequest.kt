package org.myorg.payments

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.math.BigDecimal
import java.security.MessageDigest

/**
 * The `PUT /payments/{id}` body: only what the client decides.
 * The id comes from the path, `customerId` from the caller and `submittedAt` from the BFF's clock;
 * if the body contains any of them, or anything else, it is ignored.
 */
@Serializable
data class PaymentRequest(
    val from: Party? = null,
    val to: Party? = null,
    val amount: String? = null,
    val currency: String? = null,
)

/** The one error shape of the API. */
@Serializable
data class ApiError(val error: String, val message: String, val field: String? = null)

/** A request that passed the checks that need no data: the payment-to-be, without `submittedAt` yet. */
data class ValidRequest(
    val id: String,
    val customerId: String,
    val from: Party,
    val to: Party,
    val amount: String,
    val currency: String,
) {
    /** Identifies the request's content: a repeat `PUT` with another body is a conflict. */
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

sealed interface Validation {
    data class Valid(val request: ValidRequest) : Validation
    data class Invalid(val error: ApiError) : Validation
}

private val requestJson = Json { ignoreUnknownKeys = true; isLenient = false }

private val ID_PATTERN = Regex("^[A-Za-z0-9_-]{1,64}$")
private val AMOUNT_PATTERN = Regex("^[0-9]{1,13}(\\.[0-9]{1,2})?$")
private val CURRENCY_PATTERN = Regex("^[A-Z]{3}$")
private val ACCOUNT_PATTERN = Regex("^[A-Za-z0-9_-]{1,64}$")

private fun invalid(message: String, field: String? = null) =
    Validation.Invalid(ApiError(error = "invalid-request", message = message, field = field))

/** Checks the request at the edge. Nothing here reads data: the source account is checked against the accounts. */
fun validate(id: String?, customerId: String, body: String?): Validation {
    if (id == null || !ID_PATTERN.matches(id)) return invalid("id must be 1-64 letters, digits, '-' or '_'", "id")
    if (body.isNullOrBlank()) return invalid("the body is required")

    val request = try {
        requestJson.decodeFromString<PaymentRequest>(body)
    } catch (e: Exception) {
        return invalid("the body is not a valid payment request")
    }

    val from = request.from ?: return invalid("from is required", "from")
    if (from.kind != Party.CHECKING) return invalid("from.kind must be checking", "from.kind")
    if (!ACCOUNT_PATTERN.matches(from.account)) return invalid("from.account is not a valid account", "from.account")

    val to = request.to ?: return invalid("to is required", "to")
    if (to.kind != Party.CARD && to.kind != Party.BANK) return invalid("to.kind must be card or bank", "to.kind")
    if (!ACCOUNT_PATTERN.matches(to.account)) return invalid("to.account is not a valid account", "to.account")

    val amountText = request.amount ?: return invalid("amount is required", "amount")
    if (!AMOUNT_PATTERN.matches(amountText)) return invalid("amount must be a decimal string such as \"100.00\"", "amount")
    val amount = BigDecimal(amountText)
    if (amount.signum() <= 0) return invalid("amount must be greater than 0", "amount")

    val currency = request.currency ?: return invalid("currency is required", "currency")
    if (!CURRENCY_PATTERN.matches(currency)) return invalid("currency must be an ISO 4217 code", "currency")

    return Validation.Valid(
        ValidRequest(
            id = id,
            customerId = customerId,
            from = from,
            to = to,
            amount = amount.setScale(2).toPlainString(),
            currency = currency,
        )
    )
}

/** A known **open** checking account of this customer. "Known" means the BFF heard of it through ACCOUNT_CHANGED. */
fun checkSource(request: ValidRequest, account: Account?): ApiError? {
    val usable = account != null &&
        account.customerId == request.customerId &&
        account.kind == Party.CHECKING &&
        account.status == Account.OPEN
    return if (usable) null
    else ApiError("invalid-request", "from.account is not an open checking account of this customer", "from.account")
}

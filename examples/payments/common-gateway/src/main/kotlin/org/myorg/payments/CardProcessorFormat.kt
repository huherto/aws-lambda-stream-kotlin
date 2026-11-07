package org.myorg.payments

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.math.BigDecimal
import java.security.MessageDigest
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/** The card processor's dialect: a SaaS webhook, snake case, amounts in cents. */
@Serializable
data class CardWebhook(
    @SerialName("event_id") val eventId: String,
    val type: String,
    /** Epoch seconds. */
    val created: Long,
    val data: CardCredit,
) {
    companion object {
        const val SUCCEEDED = "credit.succeeded"
        const val FAILED = "credit.failed"
    }
}

@Serializable
data class CardCredit(
    @SerialName("credit_id") val creditId: String,
    @SerialName("amount_cents") val amountCents: Long,
    val currency: String,
    val metadata: CardMetadata,
    @SerialName("failure_code") val failureCode: String? = null,
)

@Serializable
data class CardMetadata(val reference: String)

val cardJson = Json { ignoreUnknownKeys = true; explicitNulls = false; encodeDefaults = true }

fun cardProcessorReason(failureCode: String?): String = when (failureCode) {
    "card_closed" -> Reasons.ACCOUNT_CLOSED
    "no_such_card" -> Reasons.ACCOUNT_NOT_FOUND
    "insufficient_limit" -> Reasons.REJECTED
    else -> Reasons.REJECTED
}

fun amountInCents(amount: String): Long = BigDecimal(amount).movePointRight(2).longValueExact()

/** The webhook the card processor would send once it has settled a credit for this payment. */
fun simulatedCardWebhook(payment: Payment, createdSeconds: Long): CardWebhook {
    val failed = payment.to.account == Simulation.CLOSED_CARD
    return CardWebhook(
        // The processor dedups on our idempotency key (the payment id), so a repeat request is the same credit.
        eventId = "evt_${payment.id}",
        type = if (failed) CardWebhook.FAILED else CardWebhook.SUCCEEDED,
        created = createdSeconds,
        data = CardCredit(
            creditId = "cr_${payment.id}",
            amountCents = amountInCents(payment.amount),
            currency = payment.currency.lowercase(),
            metadata = CardMetadata(reference = payment.id),
            failureCode = if (failed) "card_closed" else null,
        ),
    )
}

/**
 * Webhook signatures: header `Card-Signature: t=<epoch seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">`.
 * The timestamp keeps an old, captured request from being replayed.
 */
object CardSignature {
    const val HEADER = "Card-Signature"
    const val TOLERANCE_SECONDS = 300L

    fun sign(secret: String, timestampSeconds: Long, body: String): String =
        "t=$timestampSeconds,v1=${hmac(secret, "$timestampSeconds.$body")}"

    fun verify(secret: String, header: String?, body: String, nowSeconds: Long): Boolean {
        if (header == null) return false
        val parts = header.split(",").mapNotNull { part ->
            part.split("=", limit = 2).takeIf { it.size == 2 }?.let { it[0].trim() to it[1].trim() }
        }.toMap()
        val timestamp = parts["t"]?.toLongOrNull() ?: return false
        val signature = parts["v1"] ?: return false
        if (kotlin.math.abs(nowSeconds - timestamp) > TOLERANCE_SECONDS) return false
        val expected = hmac(secret, "$timestamp.$body")
        return MessageDigest.isEqual(expected.toByteArray(), signature.toByteArray())
    }

    private fun hmac(secret: String, payload: String): String {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(secret.toByteArray(), "HmacSHA256"))
        return mac.doFinal(payload.toByteArray()).joinToString("") { "%02x".format(it) }
    }
}

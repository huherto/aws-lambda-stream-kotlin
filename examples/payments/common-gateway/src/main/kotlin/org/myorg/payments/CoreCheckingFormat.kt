package org.myorg.payments

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Core checking's dialect: a row of its transaction log, as change data capture delivers it.
 * Legacy conventions: upper-case names, every value a string, a signed amount.
 */
@Serializable
data class LedgerEntry(
    val ENTRY_ID: String,
    val ACCT_NO: String,
    /** Signed: a debit is negative. */
    val AMT: String,
    /** `PMT` (a payment debit) or `RFND` (a refund credit). */
    val TXN_TYP: String,
    /** Our payment id. */
    val REF_NO: String,
    /** `POSTED` or `REJECTED`. */
    val STAT: String,
    /** Why a row was rejected: `NSF`, `ACCT_CLSD`, … */
    val REJ_CD: String? = null,
    /** When it was posted, epoch ms. */
    val POST_TS: String,
) {
    companion object {
        const val PAYMENT = "PMT"
        const val REFUND = "RFND"
        const val POSTED = "POSTED"
        const val REJECTED = "REJECTED"
        const val NSF = "NSF"
        const val ACCOUNT_CLOSED = "ACCT_CLSD"

        fun fromAttributes(item: Map<String, String?>): LedgerEntry = LedgerEntry(
            ENTRY_ID = item["ENTRY_ID"] ?: error("ledger row without ENTRY_ID"),
            ACCT_NO = item["ACCT_NO"] ?: error("ledger row without ACCT_NO"),
            AMT = item["AMT"] ?: error("ledger row without AMT"),
            TXN_TYP = item["TXN_TYP"] ?: error("ledger row without TXN_TYP"),
            REF_NO = item["REF_NO"] ?: error("ledger row without REF_NO"),
            STAT = item["STAT"] ?: error("ledger row without STAT"),
            REJ_CD = item["REJ_CD"],
            POST_TS = item["POST_TS"] ?: error("ledger row without POST_TS"),
        )
    }

    fun toAttributes(): Map<String, String> = listOfNotNull(
        "ENTRY_ID" to ENTRY_ID, "ACCT_NO" to ACCT_NO, "AMT" to AMT, "TXN_TYP" to TXN_TYP,
        "REF_NO" to REF_NO, "STAT" to STAT, REJ_CD?.let { "REJ_CD" to it }, "POST_TS" to POST_TS,
    ).toMap()

    fun toJson(): JsonObject = JsonObject(toAttributes().mapValues { JsonPrimitive(it.value) })
}

/** Core checking's rejection codes in our words. Anything new is `rejected` until we decide otherwise. */
fun coreCheckingReason(code: String?): String = when (code) {
    LedgerEntry.NSF -> Reasons.INSUFFICIENT_FUNDS
    LedgerEntry.ACCOUNT_CLOSED -> Reasons.ACCOUNT_CLOSED
    "NO_ACCT" -> Reasons.ACCOUNT_NOT_FOUND
    else -> Reasons.REJECTED
}

/**
 * What core checking would write for a debit or a refund credit of this payment. Core checking assigns its own
 * [entryId] to every call and has no idempotency keys, so calling it twice posts twice: the simulation does the same,
 * which is what makes "called once" testable.
 */
fun simulatedLedgerEntry(payment: Payment, txnType: String, postedAt: Long, entryId: String): LedgerEntry {
    val rejected = txnType == LedgerEntry.PAYMENT && payment.from.account == Simulation.INSUFFICIENT_FUNDS_ACCOUNT
    val signed = if (txnType == LedgerEntry.PAYMENT) "-${payment.amount}" else payment.amount
    return LedgerEntry(
        ENTRY_ID = entryId,
        ACCT_NO = payment.from.account,
        AMT = signed,
        TXN_TYP = txnType,
        REF_NO = payment.id,
        STAT = if (rejected) LedgerEntry.REJECTED else LedgerEntry.POSTED,
        REJ_CD = if (rejected) LedgerEntry.NSF else null,
        POST_TS = postedAt.toString(),
    )
}

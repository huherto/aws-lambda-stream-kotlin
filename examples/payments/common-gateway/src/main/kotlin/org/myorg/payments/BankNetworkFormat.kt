package org.myorg.payments

import kotlinx.serialization.Serializable
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * The bank network's dialect: CSV files. Transfer files go out (`PAY_<date>.csv`:
 * `reference,account,amount,currency`); settlement files come back (`SETTLE_<date>.csv`: `reference,status,code`).
 */
@Serializable
data class SettlementLine(
    val reference: String,
    /** `SETTLED` or `REJECTED`. */
    val status: String,
    /** Why a transfer was rejected: `R02` account closed, `R03` no such account, … */
    val code: String = "",
) {
    companion object {
        const val HEADER = "reference,status,code"
        const val SETTLED = "SETTLED"
        const val REJECTED = "REJECTED"
    }

    fun toCsv(): String = "$reference,$status,$code"
}

/** One file, many lines. A header line is optional; blank lines are skipped. */
fun parseSettlementFile(content: String): List<SettlementLine> =
    content.lineSequence()
        .map { it.trim() }
        .filter { it.isNotEmpty() && !it.equals(SettlementLine.HEADER, ignoreCase = true) }
        .map { line ->
            val cells = line.split(",").map { it.trim() }
            require(cells.size >= 2) { "Not a settlement line: $line" }
            SettlementLine(reference = cells[0], status = cells[1].uppercase(), code = cells.getOrElse(2) { "" })
        }
        .toList()

fun settlementFile(lines: List<SettlementLine>): String =
    (listOf(SettlementLine.HEADER) + lines.map { it.toCsv() }).joinToString("\n", postfix = "\n")

fun bankNetworkReason(code: String): String = when (code) {
    "R02" -> Reasons.ACCOUNT_CLOSED
    "R03" -> Reasons.ACCOUNT_NOT_FOUND
    "R01" -> Reasons.INSUFFICIENT_FUNDS
    else -> Reasons.REJECTED
}

private val fileDate = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)

fun fileDate(epochMillis: Long): String = fileDate.format(Instant.ofEpochMilli(epochMillis))

/** The line the bank network would settle for this payment. */
fun simulatedSettlement(payment: Payment): SettlementLine =
    if (payment.to.account == Simulation.UNKNOWN_BANK_ACCOUNT) {
        SettlementLine(reference = payment.id, status = SettlementLine.REJECTED, code = "R03")
    } else {
        SettlementLine(reference = payment.id, status = SettlementLine.SETTLED)
    }

/** The line our nightly transfer file carries for this payment. */
fun transferLine(payment: Payment): String =
    "${payment.id},${payment.to.account},${payment.amount},${payment.currency}"

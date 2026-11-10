package org.myorg.payments.application

import mu.KotlinLogging

/** Driven port: hands requests back to the listener, as if the stream had delivered them again. */
fun interface Redelivery {
    suspend fun redeliver(claims: List<Claim>)
}

/**
 * Requests in doubt don't wait on the stream, so nothing would look at them again. Every minute, the reconciler
 * redelivers each one to the listener, whose called-once logic asks core checking once: confirmed, still in doubt,
 * or (past the grace period) a fault for an operator. It never calls core checking itself.
 */
class Reconciler(
    private val claims: Claims,
    private val redelivery: Redelivery,
    private val batch: Int = 10,
) {
    private val logger = KotlinLogging.logger { }

    suspend fun reconcile(): Int {
        val inDoubt = claims.inDoubt().filter { it.request != null }
        if (inDoubt.isEmpty()) return 0
        logger.info { "redelivering ${inDoubt.size} request(s) in doubt: ${inDoubt.map { it.key }}" }
        inDoubt.chunked(batch).forEach { redelivery.redeliver(it) }
        return inDoubt.size
    }
}

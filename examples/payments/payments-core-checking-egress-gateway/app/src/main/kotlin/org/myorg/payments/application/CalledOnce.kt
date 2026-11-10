package org.myorg.payments.application

import kotlinx.coroutines.delay
import mu.KotlinLogging
import org.myorg.payments.LedgerEntry
import org.myorg.payments.Payment
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** The call certainly never reached core checking (for example, the connection was refused). */
class CallNotSent(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/** A call has stayed in doubt past the grace period: someone must check core checking. Becomes a fault. */
class CallUnresolved(message: String) : RuntimeException(message)

/** One claim per request: `<payment>#PMT` or `<payment>#RFND`. A claim in doubt keeps its request, for redelivery. */
data class Claim(val key: String, val state: String, val claimedAt: Long, val request: String? = null) {
    companion object {
        const val CALLING = "CALLING"
        const val SENT = "SENT"
    }
}

interface Claims {
    /** The claim for [key], if there is one. */
    suspend fun find(key: String): Claim?

    /** Claims [key] if nobody has, keeping the [request]; otherwise returns the existing claim. */
    suspend fun claim(key: String, now: Long, request: String?): Claim?
    suspend fun markSent(key: String)
    suspend fun release(key: String)

    /** The claims still in doubt: called, and neither confirmed nor released. */
    suspend fun inDoubt(): List<Claim>
}

/** What became of a request. */
enum class Outcome { SENT, ALREADY_SENT, IN_DOUBT, RESOLVED }

/**
 * "Called once" for a system without idempotency keys. It isn't possible to guarantee exactly one call to such a
 * system, so the gateway guarantees the next best thing: **it never calls twice blindly**.
 *
 * 1. Claim the request (`CALLING`) before calling; mark it `SENT` after.
 * 2. A repeat that finds `SENT` does nothing.
 * 3. A call whose outcome is unknown stays `CALLING`: it is *in doubt*. The request moves on: one payment's doubt
 *    never holds up the others. The reconciler redelivers requests in doubt; each redelivery asks core checking
 *    once whether the posting exists. Yes: `SENT`. Not yet: wait for the next round. Past [grace]: a fault.
 * 4. A call that certainly never left ([CallNotSent]) releases the claim, so the next attempt calls normally.
 *
 * Only an open breaker holds requests back: core checking being down concerns them all.
 */
class CalledOnce(
    private val coreChecking: CoreChecking,
    private val claims: Claims,
    private val grace: Duration = 300.seconds,
    private val clock: () -> Long = System::currentTimeMillis,
    /** Calls and inquiries go through it: while core checking is down or closed, nothing is sent and requests wait. */
    private val breaker: Breaker = Breaker.alwaysClosed(),
    /** How long one invocation sleeps for an open breaker before giving the request back to the stream. */
    private val maxWait: Duration = 20.seconds,
) {
    private val logger = KotlinLogging.logger { }

    suspend fun debit(payment: Payment, request: String? = null): Outcome =
        once(payment, LedgerEntry.PAYMENT, request) { coreChecking.debit(payment) }

    suspend fun credit(payment: Payment, request: String? = null): Outcome =
        once(payment, LedgerEntry.REFUND, request) { coreChecking.credit(payment) }

    /**
     * Waits (up to [maxWait]) while the breaker is open. Then either core checking may be called, or the request goes
     * back to the stream as [CoreCheckingUnavailable], which keeps it, in order, until core checking is back.
     */
    private suspend fun admit(key: String) {
        val retryAt = breaker.admit() ?: return
        val wait = (retryAt - clock()).coerceIn(0, maxWait.inWholeMilliseconds)
        if (wait > 0) delay(wait)
        breaker.admit()?.let { throw CoreCheckingUnavailable("$key: core checking is unavailable until $it; waiting") }
    }

    private suspend fun once(payment: Payment, txnType: String, request: String?, call: suspend () -> Unit): Outcome {
        val key = "${payment.id}#$txnType"
        // A request already sent needs no call, so it must not consult the breaker: it could take the trial call
        // after a cool-down and never report how it went, leaving the breaker half-open.
        if (claims.find(key)?.state == Claim.SENT) {
            logger.info { "$key was already sent: not calling core checking again" }
            return Outcome.ALREADY_SENT
        }
        admit(key)
        val existing = claims.claim(key, clock(), request)

        if (existing == null) {
            try {
                call()
            } catch (e: CallNotSent) {
                claims.release(key)
                breaker.failed()
                throw e
            } catch (e: Exception) {
                breaker.failed()
                logger.warn(e) { "$key: core checking call failed, outcome unknown: in doubt until the reconciler resolves it" }
                return Outcome.IN_DOUBT
            }
            breaker.succeeded()
            claims.markSent(key)
            return Outcome.SENT
        }

        return when (existing.state) {
            Claim.SENT -> Outcome.ALREADY_SENT.also { logger.info { "$key was already sent: not calling core checking again" } }
            else -> resolve(existing, payment, txnType)
        }
    }

    /** One look at core checking for a request in doubt; never a second call. */
    private suspend fun resolve(claim: Claim, payment: Payment, txnType: String): Outcome {
        val posted = try {
            coreChecking.posted(payment, txnType)
        } catch (e: Exception) {
            breaker.failed()
            throw CoreCheckingUnavailable("${claim.key}: in doubt, and core checking can't be asked: ${e.message}")
        }
        breaker.succeeded()
        if (posted) {
            logger.info { "${claim.key} was in doubt; core checking shows the posting: not calling again" }
            claims.markSent(claim.key)
            return Outcome.RESOLVED
        }
        // The grace period runs only while core checking is up: a request caught in an outage gets it in full afterwards.
        val since = maxOf(claim.claimedAt, breaker.closedAt())
        if (clock() - since > grace.inWholeMilliseconds) {
            throw CallUnresolved("${claim.key}: in doubt since ${claim.claimedAt} and core checking shows no posting; check it before retrying")
        }
        logger.info { "${claim.key} is still in doubt; core checking shows no posting yet" }
        return Outcome.IN_DOUBT
    }
}

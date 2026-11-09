package org.myorg.payments

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.myorg.payments.LocalPayments.run
import kotlin.time.Duration.Companion.minutes
import kotlin.time.TimeSource

/**
 * Every account the run needs, published before any test submits a payment. The default channel has one shard,
 * so accounts published after a burst of payments would queue behind all of their events.
 */
object SeedAccounts {

    // This run's own customers and accounts, for tests that must not see other runs' data.
    val customer = "u-it-$run"
    val lister = "u-it-$run-lister"
    val open = "C-IT-$run-1"
    val closed = "C-IT-$run-9"
    val toClose = "C-IT-$run-5"
    val othersAccount = "C-IT-$run-OTHER"
    val listerAccount = "C-IT-$run-L"
    val boundaryAccount = "C-IT-$run-B"

    private data class Seed(val number: String, val customer: String, val status: String = "open")

    private val seeds = listOf(
        // The seed accounts of scripts/publish-accounts.sh: same ids and timestamps, so publishing again is a no-op.
        Seed("C-1", "u-42"),
        Seed("C-2", "u-42"),
        Seed("C-5", "u-42"),
        Seed("C-7", "u-42"),
        Seed(open, customer),
        Seed(closed, customer, status = "closed"),
        Seed(toClose, customer),
        Seed(othersAccount, "u-someone-else"),
        Seed(listerAccount, lister),
        Seed(boundaryAccount, customer),
    )

    private val lock = Mutex()
    private var done = false

    suspend fun ensure() = lock.withLock {
        if (done) return@withLock
        seeds.forEach { LocalPayments.publishAccount(it.number, it.customer, it.status) }
        // One budget for all of them: right after a deploy every function cold-starts, and the accounts' path (ingress
        // channel, accounts gateway, bus, default channel, BFF) can take minutes to warm up. That's a one-time cost.
        val deadline = TimeSource.Monotonic.markNow() + 8.minutes
        seeds.forEach { seed ->
            eventually<Any>((-deadline.elapsedNow()).coerceAtLeast(kotlin.time.Duration.ZERO), what = "the BFF to know account ${seed.number}") { LocalPayments.bffAccount(seed.number) }
        }
        done = true
    }
}

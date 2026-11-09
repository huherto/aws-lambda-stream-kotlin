package org.myorg.payments

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.ints.shouldBeLessThanOrEqual
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.myorg.payments.LocalPayments.correlated
import org.myorg.payments.LocalPayments.run
import kotlin.time.Duration.Companion.minutes

/**
 * The card processor allows 5 credits a second (locally). The card ESG sends at that rate and lets the backlog wait,
 * so a burst of payments all complete and the processor never answers 429.
 */
class RateLimitITest {

    @Test
    fun `a burst of card payments is paced at the processor's rate - all complete, no 429`(): Unit = runBlocking {
        // Arrange
        SeedAccounts.ensure()
        val tooManyBefore = LocalPayments.simulatedTooManyRequests()
        val ids = (1..20).map { "it-$run-burst-$it" }

        // Act
        ids.forEach { id -> LocalPayments.submit(id, from = "C-1", to = "K-2").status shouldBe 202 }

        // Assert: everything completes
        ids.forEach { id ->
            eventually<Any>(4.minutes, what = "$id to complete") { LocalPayments.status(id)?.takeIf { it == "COMPLETED" } }
        }
        // ...the processor never said 429...
        LocalPayments.simulatedTooManyRequests() shouldBe tooManyBefore
        // ...and no second saw more credits than allowed (the webhook's `created` is when the processor took the credit).
        val creditSeconds = ids.map { id ->
            correlated(id).single { it.eventType() == PaymentsEvent.FUNDS_DEPOSITED }.timestamp!! / 1000
        }
        creditSeconds shouldHaveSize 20
        creditSeconds.groupingBy { it }.eachCount().values.max() shouldBeLessThanOrEqual 5
    }
}

package org.myorg.payments

import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.myorg.payments.LocalPayments.ledgerPostings
import org.myorg.payments.LocalPayments.run
import kotlin.time.Duration.Companion.minutes

/**
 * While core checking is down the ESG stops calling, and requests wait until it's back: nothing fails, nothing is
 * lost, nothing is debited twice. C-5 takes simulated core checking down for everyone for a minute.
 */
class CircuitBreakerITest {

    @Test
    fun `payments caught in an outage wait for core checking and then complete, each debited once`(): Unit = runBlocking {
        // Arrange
        SeedAccounts.ensure()
        val opensBefore = LocalPayments.breakerOpens()
        val bringsItDown = "it-$run-outage"
        val caughtInIt = "it-$run-outage-bystander"

        // Act: C-5 takes core checking down; another customer's payment arrives during the outage
        LocalPayments.submit(bringsItDown, from = "C-5", to = "K-2").status shouldBe 202
        delay(10_000)
        LocalPayments.submit(caughtInIt, from = "C-1", to = "K-2").status shouldBe 202

        // Assert
        listOf(bringsItDown, caughtInIt).forEach { id ->
            eventually<Any>(4.minutes, what = "$id to complete after the outage") {
                LocalPayments.status(id)?.takeIf { it == "COMPLETED" }
            }
            ledgerPostings(id, "PMT") shouldBe 1
        }
        LocalPayments.breakerOpens() shouldBeGreaterThan opensBefore
    }
}

package org.myorg.payments

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.myorg.payments.LocalPayments.ledgerPostings
import org.myorg.payments.LocalPayments.run
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Faults are events, and someone has to act on them. C-8's first debit call is lost: core checking never gets it.
 * The called-once guard won't call blindly again, so after the grace period the request becomes a fault, stored and
 * alerted by the fault monitor. The operator checks core checking, releases the request, and resubmits the fault:
 * the payment completes, debited once.
 */
class FaultResubmissionITest {

    private val listener = "payments-core-checking-egress-gateway-local-listener"

    @Test
    fun `a lost request becomes a fault - checked, released and resubmitted, the payment completes`(): Unit = runBlocking {
        // Arrange
        SeedAccounts.ensure()
        val operator = Operator.local()
        val id = "it-$run-lost-request"

        // Act 1: the request is lost, and after the grace period it is a fault
        LocalPayments.submit(id, from = "C-8", to = "K-2").status shouldBe 202
        val fault = eventually(5.minutes, every = 10.seconds, what = "a fault for $id") {
            operator.faults().firstOrNull { "$id#PMT" in it.error }
        }

        // Assert 1: stored, alerted, nothing moved
        fault.functionName shouldBe listener
        fault.error shouldContain "check it before retrying"
        eventually<Any>(2.minutes, what = "an alert for $id") { LocalPayments.faultAlerts().firstOrNull { "$id#PMT" in it } }
        LocalPayments.status(id) shouldBe "SUBMITTED"
        ledgerPostings(id, "PMT") shouldBe 0

        // Act 2: fix first. The operator checks core checking (no posting) and releases the request; core checking
        // must also be up, or the resubmission would just be refused again. Then resubmit, checking the outcome.
        operator.releaseClaim(id) shouldContain "claim released"
        eventually<Any>(3.minutes, what = "core checking to be up") { operator.coreCheckingIsUp().takeIf { it } }
        val resubmitted = operator.resubmit(listener)
        resubmitted.failed shouldBe emptyList()

        // Assert 2: the payment completes, debited exactly once
        eventually<Any>(3.minutes, what = "$id to complete") { LocalPayments.status(id)?.takeIf { it == "COMPLETED" } }
        ledgerPostings(id, "PMT") shouldBe 1
    }
}

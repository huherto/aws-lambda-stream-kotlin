package org.myorg.payments

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.payments.LocalPayments.ledgerPostings
import org.myorg.payments.LocalPayments.run
import kotlin.time.Duration.Companion.minutes

/**
 * Core checking has no idempotency keys, so a second call is a second debit. Its egress gateway calls once per
 * request: it claims each request before calling, and resolves a request in doubt by asking core checking for the
 * posting instead of calling again.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CalledOnceITest {

    private val lostAnswer = "it-$run-lost-answer"
    private val redelivered = "it-$run-redelivered"

    @BeforeAll
    fun submit(): Unit = runBlocking {
        SeedAccounts.ensure()
        LocalPayments.submit(lostAnswer, from = "C-7", to = "K-2").status shouldBe 202
        LocalPayments.submit(redelivered, from = "C-1", to = "K-2").status shouldBe 202
    }

    @Test
    fun `C-7 - the debit is applied but the answer is lost, and the retry does not debit again`(): Unit = runBlocking {
        // Act
        eventually(3.minutes, what = "$lostAnswer to complete") { LocalPayments.status(lostAnswer)?.takeIf { it == "COMPLETED" } }

        // Assert
        ledgerPostings(lostAnswer, "PMT") shouldBe 1
    }

    @Test
    fun `a WITHDRAWAL_REQUESTED delivered twice debits once`(): Unit = runBlocking {
        // Arrange
        eventually(3.minutes, what = "$redelivered to complete") { LocalPayments.status(redelivered)?.takeIf { it == "COMPLETED" } }
        ledgerPostings(redelivered, "PMT") shouldBe 1
        val request = LocalPayments.correlated(redelivered).first { it.eventType() == PaymentsEvent.WITHDRAWAL_REQUESTED }

        // Act: the same request event again, as an at-least-once redelivery would bring it
        LocalPayments.putEvent(source = "payments-control-service", detailType = request.eventType(), detail = request.toString())
        delay(30_000)

        // Assert
        ledgerPostings(redelivered, "PMT") shouldBe 1
        LocalPayments.status(redelivered) shouldBe "COMPLETED"
    }
}

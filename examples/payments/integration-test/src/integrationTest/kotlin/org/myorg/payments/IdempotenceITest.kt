package org.myorg.payments

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.myorg.payments.LocalPayments.correlated
import org.myorg.payments.LocalPayments.run
import kotlin.time.Duration.Companion.minutes

/** At least once, any order: a redelivered fact or a retried request changes nothing. */
class IdempotenceITest {

    @Test
    fun `a redelivered event and a retried PUT change nothing once the payment is complete`(): Unit = runBlocking {
        // Arrange
        SeedAccounts.ensure()
        val id = "it-$run-idempotent"
        LocalPayments.submit(id, from = "C-1", to = "K-2").status shouldBe 202
        eventually(3.minutes, what = "$id to complete") { LocalPayments.status(id)?.takeIf { it == "COMPLETED" } }
        val before = eventually(1.minutes, what = "PAYMENT_COMPLETED to be correlated") {
            correlated(id).takeIf { events -> events.any { it.eventType() == PaymentsEvent.PAYMENT_COMPLETED } }
        }
        val viewBefore = LocalPayments.view(id).json
        val deposited = before.first { it.eventType() == PaymentsEvent.FUNDS_DEPOSITED }

        // Act: the card processor's webhook is delivered again (same event, same id), and the client retries.
        LocalPayments.putEvent(source = "payments-card-processor-ingress-gateway", detailType = deposited.eventType(), detail = deposited.toString())
        LocalPayments.submit(id, from = "C-1", to = "K-2").status shouldBe 202
        // Give a redelivery every chance to do something before checking that it did nothing.
        delay(30_000)

        // Assert
        val after = correlated(id)
        after.mapNotNull { it.id }.sorted() shouldBe before.mapNotNull { it.id }.sorted()
        after.count { it.eventType() == PaymentsEvent.PAYMENT_COMPLETED } shouldBe 1
        after.count { it.eventType() == PaymentsEvent.PAYMENT_SUBMITTED } shouldBe 1
        LocalPayments.view(id).json shouldBe viewBefore
    }
}

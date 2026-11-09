package org.myorg.payments

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.payments.LocalPayments.correlated
import org.myorg.payments.LocalPayments.run
import org.myorg.payments.PaymentsEvent.Companion.FUNDS_DEPOSITED
import org.myorg.payments.PaymentsEvent.Companion.PAYMENT_COMPLETED
import kotlin.time.Duration.Companion.minutes

/**
 * Fault accounts whose external system answers twice: the card processor redelivers its webhook (K-6) and the bank
 * network drops the same settlement file again (B-6). Each repeated answer is the same fact, so it gets the same id
 * and changes nothing: one deposit, one outcome, one external event.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DuplicatesITest {

    private val redeliveredWebhook = "it-$run-dup-webhook"
    private val repeatedFile = "it-$run-dup-file"

    @BeforeAll
    fun submit(): Unit = runBlocking {
        SeedAccounts.ensure()
        LocalPayments.submit(redeliveredWebhook, from = "C-1", to = "K-6").status shouldBe 202
        LocalPayments.submit(repeatedFile, from = "C-1", to = "B-6").status shouldBe 202
    }

    private suspend fun assertOnce(paymentId: String, expectedDepositId: (String) -> Boolean) {
        eventually(3.minutes, what = "$paymentId to complete") { LocalPayments.status(paymentId)?.takeIf { it == "COMPLETED" } }
        // The repeat comes a few seconds after the first answer; give it time to arrive and be processed.
        delay(25_000)

        val events = correlated(paymentId)
        val deposits = events.filter { it.eventType() == FUNDS_DEPOSITED }
        deposits shouldHaveSize 1
        expectedDepositId(deposits.single().id!!) shouldBe true
        events.filter { it.eventType() == PAYMENT_COMPLETED } shouldHaveSize 1

        val crossed = LocalPayments.crossedBoundary()
            .map { it["detail"]!!.jsonObject }
            .filter { it["partitionKey"]?.jsonPrimitive?.content == paymentId }
            .map { it["id"]?.jsonPrimitive?.content }
            .toSet()
        crossed shouldBe setOf("$paymentId.done.external")
    }

    @Test
    fun `K-6 - a redelivered webhook is the same FUNDS_DEPOSITED and completes the payment once`(): Unit = runBlocking {
        assertOnce(redeliveredWebhook) { it == "evt_$redeliveredWebhook.funds-deposited" }
    }

    @Test
    fun `B-6 - the same settlement file twice gives the same FUNDS_DEPOSITED and completes the payment once`(): Unit = runBlocking {
        assertOnce(repeatedFile) { it.endsWith(":$repeatedFile.funds-deposited") }
    }
}

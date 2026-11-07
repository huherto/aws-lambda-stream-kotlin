package org.myorg.payments

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldNotContain
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.payments.LocalPayments.run
import kotlin.time.Duration.Companion.minutes

/**
 * What crosses the subsystem boundary, in both directions: only curated PAYMENTS.* events go out; ACCOUNTS.*
 * events come in through the ingress channel and are never forwarded on.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BoundaryITest {

    private val completed = "it-$run-boundary-ok"
    private val failed = "it-$run-boundary-ko"

    @BeforeAll
    fun submit(): Unit = runBlocking {
        SeedAccounts.ensure()
        LocalPayments.submit(completed, from = "C-1", to = "K-2").status shouldBe 202
        LocalPayments.submit(failed, from = "C-2", to = "K-2").status shouldBe 202
    }

    private suspend fun crossedFor(paymentId: String) =
        eventually(3.minutes, what = "an external event for $paymentId downstream") {
            LocalPayments.crossedBoundary().firstOrNull {
                it["detail"]?.jsonObject?.get("partitionKey")?.jsonPrimitive?.content == paymentId
            }
        }

    @Test
    fun `PAYMENT_COMPLETED goes out as PAYMENTS_PAYMENT_COMPLETED with source external and a curated payload`(): Unit = runBlocking {
        // Act
        val message = crossedFor(completed)

        // Assert
        message["source"]?.jsonPrimitive?.content shouldBe "external"
        message["detail-type"]?.jsonPrimitive?.content shouldBe "PAYMENTS.PAYMENT_COMPLETED"
        val detail = message["detail"]!!.jsonObject
        detail["id"]?.jsonPrimitive?.content shouldBe "$completed.done.external"
        detail["payment"]!!.jsonObject.keys shouldBe setOf("id", "customerId", "amount", "currency", "destination", "submittedAt")
        detail["payment"]!!.jsonObject["destination"]?.jsonPrimitive?.content shouldBe "card"
        detail.keys.contains("reason") shouldBe false
    }

    @Test
    fun `PAYMENT_FAILED goes out with a public reason`(): Unit = runBlocking {
        // Act
        val detail = crossedFor(failed)["detail"]!!.jsonObject

        // Assert
        detail["type"]?.jsonPrimitive?.content shouldBe "PAYMENTS.PAYMENT_FAILED"
        detail["reason"]?.jsonPrimitive?.content shouldBe Reasons.INSUFFICIENT_FUNDS
    }

    @Test
    fun `nothing internal, raw or account-level crosses, and incoming ACCOUNTS events are not forwarded`(): Unit = runBlocking {
        // Arrange: this run's ACCOUNTS events reached the BFF (SeedAccounts), so they went through our bus.
        val number = SeedAccounts.boundaryAccount
        crossedFor(completed)
        crossedFor(failed)

        // Act
        val everything = LocalPayments.crossedBoundary()

        // Assert
        everything.forEach { message ->
            val type = message["detail-type"]?.jsonPrimitive?.content!!
            type.startsWith("PAYMENTS.") shouldBe true
            val text = message.toString()
            text shouldNotContain "\"raw\""
            text shouldNotContain "\"triggers\""
            text shouldNotContain "\"from\""
            text shouldNotContain "C-1"
            text shouldNotContain "K-2"
            text shouldNotContain number
        }
    }
}

package org.myorg.payments

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.payments.LocalPayments.paymentBody
import org.myorg.payments.LocalPayments.request
import org.myorg.payments.LocalPayments.run
import kotlin.time.Duration.Companion.minutes

/**
 * The BFF's REST contract through API Gateway, for a customer and accounts of this run only, so the list
 * endpoint sees exactly this run's payments.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BffApiITest {

    private val customer = SeedAccounts.customer
    private val open = SeedAccounts.open
    private val closed = SeedAccounts.closed
    private val toClose = SeedAccounts.toClose
    private val othersAccount = SeedAccounts.othersAccount

    @BeforeAll
    fun seed(): Unit = runBlocking { SeedAccounts.ensure() }

    private fun put(id: String, from: String = open, amount: String = "100.00", who: String? = customer) =
        request("PUT", "/payments/$id", who, paymentBody(from, "K-2", amount))

    @Test
    fun `PUT accepts a payment once - same body again is the stored payment, another body is a conflict`() {
        // Arrange
        val id = "it-$run-api-put"

        // Act
        val first = put(id)
        val again = put(id)
        val different = put(id, amount = "200.00")

        // Assert
        first.status shouldBe 202
        first.string("status") shouldBe "SUBMITTED"
        again.status shouldBe 202
        again.json["submittedAt"] shouldBe first.json["submittedAt"]
        different.status shouldBe 409
        different.string("error") shouldBe "conflict"
    }

    @Test
    fun `bad requests are rejected at the edge and name the culprit`() {
        // Act / Assert
        put("it-$run-api-401", who = null).status shouldBe 401
        put("it-$run-api-closed", from = closed).let { it.status shouldBe 400; it.string("field") shouldBe "from.account" }
        put("it-$run-api-others", from = othersAccount).let { it.status shouldBe 400; it.string("field") shouldBe "from.account" }
        put("it-$run-api-unknown", from = "C-IT-$run-404").status shouldBe 400
        put("it-$run-api-zero", amount = "0").let { it.status shouldBe 400; it.string("field") shouldBe "amount" }
        request("GET", "/payments/it-$run-api-unknown", customer).status shouldBe 404
    }

    @Test
    fun `another customer's payment is not found, not forbidden`() {
        // Arrange
        val id = "it-$run-api-mine"
        put(id).status shouldBe 202

        // Act / Assert
        request("GET", "/payments/$id", customer).status shouldBe 200
        request("GET", "/payments/$id", "u-someone-else").status shouldBe 404
    }

    @Test
    fun `the latest account event wins, so a closed account can't pay and an older event doesn't reopen it`(): Unit = runBlocking {
        // Arrange
        put("it-$run-api-before-close", from = toClose).status shouldBe 202

        // Act
        LocalPayments.publishAccount(toClose, customer, status = "closed", type = "ACCOUNTS.ACCOUNT_CLOSED", timestamp = 1730000005000)
        eventually(3.minutes, what = "the BFF to close $toClose") {
            LocalPayments.bffAccount(toClose)?.takeIf { (it["status"] as? aws.sdk.kotlin.services.dynamodb.model.AttributeValue.S)?.value == "closed" }
        }
        LocalPayments.publishAccount(toClose, customer, status = "open", type = "ACCOUNTS.ACCOUNT_UPDATED", timestamp = 1730000001000)

        // Assert
        put("it-$run-api-after-close", from = toClose).status shouldBe 400
        kotlinx.coroutines.delay(10_000)
        put("it-$run-api-after-old-reopen", from = toClose).status shouldBe 400
    }

    @Test
    fun `GET payments pages the customer's payments newest first and filters by status`(): Unit = runBlocking {
        // Arrange: a customer of its own, so the list holds exactly these
        val lister = SeedAccounts.lister
        val account = SeedAccounts.listerAccount
        val ids = (1..3).map { "it-$run-list-$it" }
        ids.forEach { id ->
            request("PUT", "/payments/$id", lister, paymentBody(account, "K-2")).status shouldBe 202
            kotlinx.coroutines.delay(20)
        }

        // Act
        val first = request("GET", "/payments?limit=2", lister).json
        val next = first["next"]!!.jsonPrimitive.content
        val second = request("GET", "/payments?limit=2&cursor=$next", lister).json

        // Assert
        first["items"]!!.jsonArray.map { it.jsonObject["id"]!!.jsonPrimitive.content } shouldContainExactly ids.reversed().take(2)
        second["items"]!!.jsonArray.map { it.jsonObject["id"]!!.jsonPrimitive.content } shouldContainExactly listOf(ids.first())
        second["next"].shouldBeNull()

        // The simulated card processor completes them; then the filter sees them all as COMPLETED.
        ids.forEach { id -> eventually<Any>(3.minutes, what = "$id to complete") { LocalPayments.status(id, lister)?.takeIf { it == "COMPLETED" } } }
        request("GET", "/payments?status=COMPLETED", lister).json["items"]!!.jsonArray shouldHaveSize 3
        request("GET", "/payments?status=FAILED&status=EXPIRED", lister).json["items"]!!.jsonArray.shouldBeEmpty()
        request("GET", "/payments?limit=0", lister).status shouldBe 400
    }
}

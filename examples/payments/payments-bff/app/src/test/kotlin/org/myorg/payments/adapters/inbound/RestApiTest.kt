package org.myorg.payments.adapters.inbound

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent
import io.kopipes.aws.testsupport.TestContext
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test
import org.myorg.payments.*
import org.myorg.payments.application.InMemoryPaymentsStore
import org.myorg.payments.domain.PaymentView
import org.myorg.payments.domain.Status

class RestApiTest {

    private val store = InMemoryPaymentsStore().apply {
        accounts["C-1"] = Account("C-1", Party.CHECKING, "u-42", Account.OPEN)
        accounts["C-9"] = Account("C-9", Party.CHECKING, "u-42", Account.CLOSED)
        accounts["C-3"] = Account("C-3", Party.CHECKING, "u-77", Account.OPEN)
    }
    private var now = Fixtures.SUBMITTED_AT
    private val api = RestApi { RestApiContainer(store) { now } }

    private val json = Json { ignoreUnknownKeys = true }

    private fun body(from: String = "C-1", amount: String = "100.00") = """
        { "from": { "kind": "checking", "account": "$from" }, "to": { "kind": "card", "account": "K-2" },
          "amount": "$amount", "currency": "USD" }
    """.trimIndent()

    private fun put(id: String, body: String, customer: String? = "u-42"): APIGatewayProxyResponseEvent =
        api.handleRequest(
            APIGatewayProxyRequestEvent()
                .withHttpMethod("PUT")
                .withResource("/payments/{id}")
                .withPath("/payments/$id")
                .withPathParameters(mapOf("id" to id))
                .withHeaders(customer?.let { mapOf("x-customer-id" to it) } ?: emptyMap())
                .withBody(body),
            TestContext(),
        )

    private fun get(id: String, customer: String = "u-42"): APIGatewayProxyResponseEvent =
        api.handleRequest(
            APIGatewayProxyRequestEvent()
                .withHttpMethod("GET")
                .withResource("/payments/{id}")
                .withPathParameters(mapOf("id" to id))
                .withHeaders(mapOf("X-Customer-Id" to customer)),
            TestContext(),
        )

    private fun list(vararg params: Pair<String, String>, customer: String = "u-42"): APIGatewayProxyResponseEvent =
        api.handleRequest(
            APIGatewayProxyRequestEvent()
                .withHttpMethod("GET")
                .withResource("/payments")
                .withHeaders(mapOf("X-Customer-Id" to customer))
                .withMultiValueQueryStringParameters(params.groupBy({ it.first }, { it.second })),
            TestContext(),
        )

    private fun error(response: APIGatewayProxyResponseEvent): String? =
        json.parseToJsonElement(response.body).jsonObject["error"]?.jsonPrimitive?.content

    @Test
    fun `PUT accepts a new payment with 202 and the submitted view`() {
        // Act
        val response = put("p-7", body())

        // Assert
        response.statusCode shouldBe 202
        val view = json.decodeFromString<PaymentView>(response.body)
        view.id shouldBe "p-7"
        view.status shouldBe Status.SUBMITTED
        view.amount shouldBe "100.00"
        store.payments["p-7"].shouldNotBeNull().payment shouldBe Fixtures.payment
    }

    @Test
    fun `a repeated PUT with the same body returns the stored payment and writes nothing`() {
        // Arrange
        put("p-7", body())
        now += 60_000

        // Act
        val response = put("p-7", body())

        // Assert
        response.statusCode shouldBe 202
        json.decodeFromString<PaymentView>(response.body).submittedAt shouldBe Fixtures.SUBMITTED_AT
        store.submits shouldBe 1
    }

    @Test
    fun `a repeated PUT with a different body is a conflict`() {
        // Arrange
        put("p-7", body())

        // Act
        val response = put("p-7", body(amount = "200.00"))

        // Assert
        response.statusCode shouldBe 409
        error(response) shouldBe "conflict"
    }

    @Test
    fun `a request without X-Customer-Id is unauthenticated`() {
        // Act
        val response = put("p-7", body(), customer = null)

        // Assert
        response.statusCode shouldBe 401
        error(response) shouldBe "unauthenticated"
    }

    @Test
    fun `unknown, closed or someone else's source accounts are rejected and nothing is written`() {
        // Act
        val unknown = put("p-1", body(from = "C-404"))
        val closed = put("p-2", body(from = "C-9"))
        val someoneElses = put("p-3", body(from = "C-3"))

        // Assert
        listOf(unknown, closed, someoneElses).map { it.statusCode } shouldContainExactly listOf(400, 400, 400)
        json.parseToJsonElement(unknown.body).jsonObject["field"]?.jsonPrimitive?.content shouldBe "from.account"
        store.payments.isEmpty() shouldBe true
    }

    @Test
    fun `an invalid amount is a bad request`() {
        // Act
        val response = put("p-7", body(amount = "0"))

        // Assert
        response.statusCode shouldBe 400
        error(response) shouldBe "invalid-request"
    }

    @Test
    fun `GET returns the view with the steps heard so far`() {
        // Arrange
        put("p-7", body())
        store.hear("p-7", PaymentsEvent.FUNDS_WITHDRAWN, Fixtures.SUBMITTED_AT + 60_000)

        // Act
        val response = get("p-7")

        // Assert
        response.statusCode shouldBe 200
        json.decodeFromString<PaymentView>(response.body).status shouldBe Status.WITHDRAWN
    }

    @Test
    fun `GET of an unknown payment or another customer's payment is not found`() {
        // Arrange
        put("p-7", body())

        // Act / Assert
        get("p-404").statusCode shouldBe 404
        get("p-7", customer = "u-77").statusCode shouldBe 404
    }

    @Test
    fun `GET payments pages newest first with an opaque cursor`() {
        // Arrange
        (1..5).forEach { i -> now = Fixtures.SUBMITTED_AT + i; put("p-$i", body()) }

        // Act
        val first = json.decodeFromString<PaymentsPageView>(list("limit" to "2").body)
        val second = json.decodeFromString<PaymentsPageView>(list("limit" to "2", "cursor" to first.next!!).body)
        val last = json.decodeFromString<PaymentsPageView>(list("limit" to "2", "cursor" to second.next!!).body)

        // Assert
        first.items.map { it.id } shouldContainExactly listOf("p-5", "p-4")
        second.items.map { it.id } shouldContainExactly listOf("p-3", "p-2")
        last.items.map { it.id } shouldContainExactly listOf("p-1")
        last.next.shouldBeNull()
    }

    @Test
    fun `GET payments filters by one or more statuses and only shows the caller's payments`() {
        // Arrange
        (1..4).forEach { i -> now = Fixtures.SUBMITTED_AT + i; put("p-$i", body()) }
        store.hear("p-2", PaymentsEvent.FUNDS_WITHDRAWN, 1)
        store.hear("p-4", PaymentsEvent.PAYMENT_COMPLETED, 1)
        store.accounts["C-3"]?.let { put("q-1", body(from = "C-3"), customer = "u-77") }

        // Act
        val page = json.decodeFromString<PaymentsPageView>(
            list("status" to "WITHDRAWN", "status" to "COMPLETED", "limit" to "1").body
        )
        val rest = json.decodeFromString<PaymentsPageView>(
            list("status" to "WITHDRAWN", "status" to "COMPLETED", "limit" to "1", "cursor" to page.next!!).body
        )

        // Assert
        page.items.map { it.id } shouldContainExactly listOf("p-4")
        rest.items.map { it.id } shouldContainExactly listOf("p-2")
        json.decodeFromString<PaymentsPageView>(list().body).items.size shouldBe 4
    }

    @Test
    fun `GET payments rejects a bad limit, status or cursor`() {
        list("limit" to "0").statusCode shouldBe 400
        list("limit" to "101").statusCode shouldBe 400
        list("status" to "DONE").statusCode shouldBe 400
        list("cursor" to "###").statusCode shouldBe 400
    }
}

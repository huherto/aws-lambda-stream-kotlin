package org.myorg.payments.application

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.myorg.payments.Account
import org.myorg.payments.Fixtures
import org.myorg.payments.Party
import org.myorg.payments.PaymentsEvent
import org.myorg.payments.domain.Status

/** The use cases behind the driving port, with an in-memory store: no HTTP, no tables. */
class PaymentsServiceTest {

    private val store = InMemoryPaymentsStore().apply {
        accounts["C-1"] = Account("C-1", Party.CHECKING, "u-42", Account.OPEN)
        accounts["C-9"] = Account("C-9", Party.CHECKING, "u-42", Account.CLOSED)
    }
    private var now = Fixtures.SUBMITTED_AT
    private val payments: Payments = PaymentsService(store) { now }

    private fun body(from: String = "C-1", amount: String = "100.00") = """
        { "from": { "kind": "checking", "account": "$from" }, "to": { "kind": "card", "account": "K-2" },
          "amount": "$amount", "currency": "USD" }
    """.trimIndent()

    @Test
    fun `submit accepts a new payment once and keeps the first submission time`() = runBlocking<Unit> {
        // Act
        val first = payments.submit("u-42", "p-7", body())
        now += 60_000
        val repeat = payments.submit("u-42", "p-7", body())

        // Assert
        first.shouldBeInstanceOf<Submission.Accepted>().view.status shouldBe Status.SUBMITTED
        repeat.shouldBeInstanceOf<Submission.Accepted>().view.submittedAt shouldBe Fixtures.SUBMITTED_AT
        store.submits shouldBe 1
    }

    @Test
    fun `submit with another content under an accepted id is a conflict`() = runBlocking<Unit> {
        // Arrange
        payments.submit("u-42", "p-7", body())

        // Act
        val conflict = payments.submit("u-42", "p-7", body(amount = "200.00"))

        // Assert
        conflict shouldBe Submission.Conflict("p-7")
    }

    @Test
    fun `submit rejects an invalid request or an unusable source account and stores nothing`() = runBlocking<Unit> {
        // Act
        val invalid = payments.submit("u-42", "p-1", body(amount = "0"))
        val closed = payments.submit("u-42", "p-2", body(from = "C-9"))

        // Assert
        invalid.shouldBeInstanceOf<Submission.Invalid>().rejection.field shouldBe "amount"
        closed.shouldBeInstanceOf<Submission.Invalid>().rejection.field shouldBe "from.account"
        store.payments.isEmpty() shouldBe true
    }

    @Test
    fun `find shows the customer's own payments only`() = runBlocking<Unit> {
        // Arrange
        payments.submit("u-42", "p-7", body())
        store.hear("p-7", PaymentsEvent.FUNDS_WITHDRAWN, Fixtures.SUBMITTED_AT + 1)

        // Act / Assert
        payments.find("u-42", "p-7").shouldNotBeNull().status shouldBe Status.WITHDRAWN
        payments.find("u-77", "p-7").shouldBeNull()
        payments.find("u-42", "p-404").shouldBeNull()
    }

    @Test
    fun `list filters on the computed status and says where the next page starts`() = runBlocking<Unit> {
        // Arrange
        (1..4).forEach { i -> now = Fixtures.SUBMITTED_AT + i; payments.submit("u-42", "p-$i", body()) }
        store.hear("p-1", PaymentsEvent.PAYMENT_COMPLETED, 1)
        store.hear("p-3", PaymentsEvent.PAYMENT_COMPLETED, 1)

        // Act
        val first = payments.list("u-42", setOf(Status.COMPLETED), limit = 1, after = null)
        val rest = payments.list("u-42", setOf(Status.COMPLETED), limit = 1, after = first.next)

        // Assert
        first.items.map { it.id } shouldContainExactly listOf("p-3")
        first.next shouldBe PaymentKey("p-3", Fixtures.SUBMITTED_AT + 3)
        rest.items.map { it.id } shouldContainExactly listOf("p-1")
        rest.next.shouldBeNull()
    }
}

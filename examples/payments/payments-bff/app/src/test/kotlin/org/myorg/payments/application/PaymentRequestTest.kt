package org.myorg.payments.application

import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test
import org.myorg.payments.*
import org.myorg.payments.domain.checkSource

class PaymentRequestTest {

    private val body = """
        {
          "from": { "kind": "checking", "account": "C-1" },
          "to":   { "kind": "card", "account": "K-2" },
          "amount": "100.00",
          "currency": "USD"
        }
    """.trimIndent()

    private fun invalidField(id: String? = "p-7", body: String?): String? =
        validate(id, Fixtures.CUSTOMER_ID, body).shouldBeInstanceOf<Validation.Invalid>().error.field

    @Test
    fun `a well-formed request is valid and normalizes the amount to two decimals`() {
        // Act
        val result = validate("p-7", "u-42", body.replace("\"100.00\"", "\"100\""))

        // Assert
        val request = result.shouldBeInstanceOf<Validation.Valid>().request
        request.amount shouldBe "100.00"
        request.toPayment(Fixtures.SUBMITTED_AT) shouldBe Fixtures.payment
    }

    @Test
    fun `customerId, submittedAt and unknown fields in the body are ignored`() {
        // Arrange
        val withExtras = body.replace("\"currency\"", "\"customerId\": \"u-99\", \"submittedAt\": 1, \"note\": \"x\", \"currency\"")

        // Act
        val request = validate("p-7", "u-42", withExtras).shouldBeInstanceOf<Validation.Valid>().request

        // Assert
        request.customerId shouldBe "u-42"
        request.hash shouldBe (validate("p-7", "u-42", body) as Validation.Valid).request.hash
    }

    @Test
    fun `bad requests name the culprit field`() {
        invalidField(id = "p 7", body = body) shouldBe "id"
        invalidField(body = null).shouldBeNull()
        invalidField(body = "not json").shouldBeNull()
        invalidField(body = body.replace("\"checking\"", "\"savings\"")) shouldBe "from.kind"
        invalidField(body = body.replace("\"card\"", "\"cash\"")) shouldBe "to.kind"
        invalidField(body = body.replace("\"K-2\"", "\"\"")) shouldBe "to.account"
        invalidField(body = body.replace("\"100.00\"", "\"0.00\"")) shouldBe "amount"
        invalidField(body = body.replace("\"100.00\"", "\"-5\"")) shouldBe "amount"
        invalidField(body = body.replace("\"100.00\"", "\"1.234\"")) shouldBe "amount"
        invalidField(body = body.replace("\"USD\"", "\"usd\"")) shouldBe "currency"
    }

    @Test
    fun `a different body gives a different hash`() {
        // Act
        val first = (validate("p-7", "u-42", body) as Validation.Valid).request.hash
        val second = (validate("p-7", "u-42", body.replace("100.00", "200.00")) as Validation.Valid).request.hash

        // Assert
        first shouldNotBe second
    }

    @Test
    fun `the source must be a known open checking account of the customer`() {
        // Arrange
        val request = (validate("p-7", "u-42", body) as Validation.Valid).request
        val open = Account("C-1", Party.CHECKING, "u-42", Account.OPEN)

        // Act / Assert
        checkSource(request, open).shouldBeNull()
        checkSource(request, null).shouldNotBeNull().field shouldBe "from.account"
        checkSource(request, open.copy(status = Account.CLOSED)).shouldNotBeNull()
        checkSource(request, open.copy(customerId = "u-77")).shouldNotBeNull()
    }
}

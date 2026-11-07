package org.myorg.payments

import io.kopipes.core.JsonRaw
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test

class PaymentsEventCodecTest {

    @Test
    fun `every catalog event survives an encode and decode round trip`() {
        // Arrange
        val events = Fixtures.all()

        // Act
        val decoded = events.map { PaymentsEventCodec.decode(PaymentsEventCodec.encode(it)) }

        // Assert
        decoded shouldBe events
    }

    @Test
    fun `the event type is the envelope's type field`() {
        // Arrange
        val event = Fixtures.fundsWithdrawn()

        // Act
        val json = Json.parseToJsonElement(PaymentsEventCodec.encode(event)).jsonObject

        // Assert
        event.eventType() shouldBe PaymentsEvent.FUNDS_WITHDRAWN
        json["type"]?.jsonPrimitive?.content shouldBe PaymentsEvent.FUNDS_WITHDRAWN
    }

    @Test
    fun `amounts are encoded as decimal strings`() {
        // Arrange
        val event = Fixtures.paymentSubmitted()

        // Act
        val encoded = PaymentsEventCodec.encode(event)

        // Assert
        encoded shouldContain "\"amount\": \"100.00\""
    }

    @Test
    fun `a failure keeps its reason`() {
        // Arrange
        val event = Fixtures.depositFailed(reason = Reasons.ACCOUNT_CLOSED)

        // Act
        val decoded = PaymentsEventCodec.decode(PaymentsEventCodec.encode(event))

        // Assert
        decoded.shouldBeInstanceOf<DepositFailed>()
        decoded.reason shouldBe Reasons.ACCOUNT_CLOSED
    }

    @Test
    fun `unknown fields are ignored`() {
        // Arrange
        val json = """
            {
              "type": "FUNDS_DEPOSITED",
              "id": "p-7.deposited",
              "somethingNew": { "nested": true },
              "payment": {
                "id": "p-7", "customerId": "u-42",
                "from": { "kind": "checking", "account": "C-1" },
                "to": { "kind": "card", "account": "K-2", "network": "visa" },
                "amount": "100.00", "currency": "USD", "submittedAt": 1730900520000
              }
            }
        """.trimIndent()

        // Act
        val decoded = PaymentsEventCodec.decode(json)

        // Assert
        decoded.shouldBeInstanceOf<FundsDeposited>()
        decoded.payment shouldBe Fixtures.payment
    }

    @Test
    fun `raw data from the outside system survives a round trip`() {
        // Arrange
        val raw = JsonRaw(Json.parseToJsonElement("""{"REF_NO":"p-7","AMT":"100.00","STAT":"POSTED"}"""))
        val event = Fixtures.fundsWithdrawn().copy(raw = raw)

        // Act
        val decoded = PaymentsEventCodec.decode(PaymentsEventCodec.encode(event))

        // Assert
        decoded.raw.shouldBeInstanceOf<JsonRaw>()
        ((decoded.raw as JsonRaw).value as JsonObject)["REF_NO"]?.jsonPrimitive?.content shouldBe "p-7"
    }

    @Test
    fun `absent envelope fields are not encoded`() {
        // Arrange
        val event = PaymentSubmitted(payment = Fixtures.payment)

        // Act
        val encoded = PaymentsEventCodec.encode(event)

        // Assert
        encoded shouldNotContain "\"raw\""
        encoded shouldNotContain "\"eem\""
    }
}

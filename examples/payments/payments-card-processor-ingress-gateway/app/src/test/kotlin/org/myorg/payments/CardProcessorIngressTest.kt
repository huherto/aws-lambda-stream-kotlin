package org.myorg.payments

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.testsupport.TestContext
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test

class CardProcessorIngressTest {

    private val secret = "test-secret"
    private val nowSeconds = 1730900600L
    private val publisher = EventPublisherInMemory()
    private val lookup = PaymentLookup { reference ->
        when (reference) {
            "p-7" -> Fixtures.payment
            "p-8" -> Fixtures.payment.copy(id = "p-8", to = Party(Party.CARD, "K-3"))
            else -> throw PaymentNotKnownYet(reference)
        }
    }
    private val webhook = Webhook { WebhookContainer(secret, lookup, publisher, clock = { nowSeconds * 1000 }) }


    private fun post(body: String, signature: String? = CardSignature.sign(secret, nowSeconds, body)) =
        webhook.handleRequest(
            APIGatewayProxyRequestEvent()
                .withHttpMethod("POST")
                .withBody(body)
                .withHeaders(signature?.let { mapOf("card-signature" to it) } ?: emptyMap()),
            TestContext(),
        )

    /** What the egress gateway's simulation sends: the contract between the two halves of the Card ESG. */
    private fun simulated(payment: Payment) =
        cardJson.encodeToString(CardWebhook.serializer(), simulatedCardWebhook(payment, nowSeconds - 1))

    @Test
    fun `a signed credit_succeeded webhook becomes FUNDS_DEPOSITED and is acknowledged`() {
        // Act
        val response = post(simulated(Fixtures.payment))

        // Assert
        response.statusCode shouldBe 200
        val event = publisher.uows().single().event.shouldBeInstanceOf<FundsDeposited>()
        event.id shouldBe "evt_p-7.funds-deposited"
        event.timestamp shouldBe (nowSeconds - 1) * 1000
        event.payment shouldBe Fixtures.payment
    }

    @Test
    fun `a credit_failed webhook becomes DEPOSIT_FAILED with the reason in our words`() {
        // Act
        post(simulated(Fixtures.payment.copy(id = "p-8", to = Party(Party.CARD, "K-3"))))

        // Assert
        publisher.uows().single().event.shouldBeInstanceOf<DepositFailed>().reason shouldBe Reasons.ACCOUNT_CLOSED
    }

    @Test
    fun `a redelivered webhook is the same event`() {
        // Act
        post(simulated(Fixtures.payment))
        post(simulated(Fixtures.payment))

        // Assert
        publisher.uows().map { it.event?.id }.toSet() shouldBe setOf("evt_p-7.funds-deposited")
    }

    @Test
    fun `unsigned, wrongly signed or stale webhooks are refused and publish nothing`() {
        // Arrange
        val body = simulated(Fixtures.payment)

        // Act / Assert
        post(body, signature = null).statusCode shouldBe 401
        post(body, signature = CardSignature.sign("not-the-secret", nowSeconds, body)).statusCode shouldBe 401
        post(body, signature = CardSignature.sign(secret, nowSeconds - 3600, body)).statusCode shouldBe 401
        publisher.uows().shouldBeEmpty()
    }

    @Test
    fun `a webhook for a payment not recorded yet asks the processor to retry`() {
        // Act
        val response = post(simulated(Fixtures.payment.copy(id = "p-99")))

        // Assert
        response.statusCode shouldBe 503
        publisher.uows().shouldBeEmpty()
    }

    @Test
    fun `an unreadable body is a bad request`() {
        post("""{"hello":"world"}""").statusCode shouldBe 400
    }
}

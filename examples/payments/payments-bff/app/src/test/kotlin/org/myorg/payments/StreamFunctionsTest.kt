package org.myorg.payments

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import io.kopipes.aws.from.RecordImage
import io.kopipes.aws.from.RecordPair
import io.kopipes.aws.from.DynamoDbChangeEvent
import io.kopipes.core.UnitOfWork
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as StreamAV

class StreamFunctionsTest {

    private val table = "payments-bff-test-payments"

    // ------------------------------------------------------------------------------------------- listener

    @Test
    fun `heardUpdate records the first time each event was heard`() {
        // Act
        val request = heardUpdate(table, UnitOfWork(event = Fixtures.fundsWithdrawn())).shouldNotBeNull()

        // Assert
        request.key shouldBe Items.key("p-7", Items.PAYMENT)
        request.updateExpression shouldBe "SET #at = if_not_exists(#at, :at)"
        request.expressionAttributeNames shouldBe mapOf("#at" to "at_FUNDS_WITHDRAWN")
        request.expressionAttributeValues shouldBe mapOf(":at" to AttributeValue.N(Fixtures.fundsWithdrawn().timestamp.toString()))
        request.conditionExpression.shouldBeNull()
    }

    @Test
    fun `heardUpdate keeps the reason of a failure`() {
        // Act
        val request = heardUpdate(table, UnitOfWork(event = Fixtures.depositFailed(reason = Reasons.ACCOUNT_CLOSED))).shouldNotBeNull()

        // Assert
        request.expressionAttributeNames?.get("#reason") shouldBe "reason_DEPOSIT_FAILED"
        request.expressionAttributeValues?.get(":reason") shouldBe AttributeValue.S(Reasons.ACCOUNT_CLOSED)
    }

    @Test
    fun `heardUpdate ignores events that do not move the view`() {
        heardUpdate(table, UnitOfWork(event = Fixtures.paymentSubmitted())).shouldBeNull()
        heardUpdate(table, UnitOfWork(event = Fixtures.withdrawalRequested())).shouldBeNull()
        heardUpdate(table, UnitOfWork(event = Fixtures.accountChanged())).shouldBeNull()
    }

    @Test
    fun `accountUpdate lets only a newer event change the account`() {
        // Act
        val request = accountUpdate(table, UnitOfWork(event = Fixtures.accountChanged(status = Account.CLOSED))).shouldNotBeNull()

        // Assert
        request.key shouldBe Items.key("C-1", Items.ACCOUNT)
        request.conditionExpression shouldBe "attribute_not_exists(#ts) OR #ts < :ts"
        request.expressionAttributeValues?.get(":s") shouldBe AttributeValue.S(Account.CLOSED)
        request.expressionAttributeValues?.get(":c") shouldBe AttributeValue.S(Fixtures.CUSTOMER_ID)
    }

    // -------------------------------------------------------------------------------------------- trigger

    private fun s(v: String) = StreamAV().withS(v)
    private fun n(v: Long) = StreamAV().withN(v.toString())

    private val markersOnly = mapOf("pk" to s("p-7"), "sk" to s(Items.PAYMENT), "at_FUNDS_WITHDRAWN" to n(1))

    private val accepted = mapOf(
        "pk" to s("p-7"), "sk" to s(Items.PAYMENT),
        "customerId" to s("u-42"), "fromKind" to s("checking"), "fromAccount" to s("C-1"),
        "toKind" to s("card"), "toAccount" to s("K-2"), "amount" to s("100.00"), "currency" to s("USD"),
        "submittedAt" to n(Fixtures.SUBMITTED_AT), "requestHash" to s("h"),
    )

    private fun change(new: Map<String, StreamAV>?, old: Map<String, StreamAV>?) =
        UnitOfWork(event = DynamoDbChangeEvent(raw = RecordPair(new?.let(::RecordImage), old?.let(::RecordImage))))

    @Test
    fun `an accepted payment publishes PAYMENT_SUBMITTED with a deterministic id`() {
        // Act
        val event = toPaymentSubmitted(change(new = accepted, old = null))

        // Assert
        val submitted = event.shouldBeInstanceOf<PaymentSubmitted>()
        submitted.id shouldBe "p-7.submitted"
        submitted.partitionKey shouldBe "p-7"
        submitted.timestamp shouldBe Fixtures.SUBMITTED_AT
        submitted.payment shouldBe Fixtures.payment
    }

    @Test
    fun `accepting an id whose events were recorded first still publishes PAYMENT_SUBMITTED`() {
        // Act
        val event = toPaymentSubmitted(change(new = accepted + markersOnly, old = markersOnly))

        // Assert
        event.shouldBeInstanceOf<PaymentSubmitted>()
    }

    @Test
    fun `later changes to the payment item publish nothing`() {
        // Act / Assert
        toPaymentSubmitted(change(new = accepted + ("at_FUNDS_WITHDRAWN" to n(1)), old = accepted)).shouldBeNull()
        toPaymentSubmitted(change(new = markersOnly, old = null)).shouldBeNull()
        toPaymentSubmitted(change(new = null, old = accepted)).shouldBeNull()
        toPaymentSubmitted(change(new = mapOf("pk" to s("C-1"), "sk" to s(Items.ACCOUNT)), old = null)).shouldBeNull()
    }
}

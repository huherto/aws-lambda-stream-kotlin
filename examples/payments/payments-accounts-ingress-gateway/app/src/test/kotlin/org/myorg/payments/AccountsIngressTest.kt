package org.myorg.payments

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.testsupport.TestContext
import io.kopipes.core.JsonRaw
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.Date

class AccountsIngressTest {

    private fun external(type: String = AccountsEvent.ACCOUNT_OPENED, customerField: String = "holderId", extra: String = "") = """
        { "id": "acct-C-1-1", "type": "$type", "timestamp": 1730814120000, "partitionKey": "C-1",
          "tags": { "source": "accounts-egress-gateway", "subsystem": "accounts" },
          "account": { "number": "C-1", "kind": "checking", "$customerField": "u-42", "status": "open", "currency": "USD" $extra } }
    """.trimIndent()

    private fun translate(json: String) = toAccountChanged(UnitOfWork(event = AccountsEventCodec.decode(json)))


    @Test
    fun `ACCOUNT_OPENED becomes ACCOUNT_CHANGED with our names and only what the BFF needs`() {
        // Act
        val event = translate(external()).shouldBeInstanceOf<AccountChanged>()

        // Assert
        event.account shouldBe Account(number = "C-1", kind = "checking", customerId = "u-42", status = Account.OPEN)
        event.id shouldBe "acct-C-1-1.account-changed"
        event.partitionKey shouldBe "C-1"
        event.timestamp shouldBe 1730814120000
    }

    @Test
    fun `raw keeps the external event exactly as heard`() {
        // Act
        val event = translate(external(extra = ", \"branch\": \"downtown\"")).shouldBeInstanceOf<AccountChanged>()

        // Assert
        val raw = event.raw.shouldBeInstanceOf<JsonRaw>().value as JsonObject
        raw["type"]?.jsonPrimitive?.content shouldBe AccountsEvent.ACCOUNT_OPENED
        (raw["account"] as JsonObject)["branch"]?.jsonPrimitive?.content shouldBe "downtown"
    }

    @Test
    fun `ACCOUNT_CLOSED always closes the account`() {
        // Act
        val event = translate(external(type = AccountsEvent.ACCOUNT_CLOSED)).shouldBeInstanceOf<AccountChanged>()

        // Assert
        event.account.status shouldBe Account.CLOSED
    }

    @Test
    fun `the holderId to ownerId rename is absorbed here`() {
        // Act
        val renamed = translate(external(customerField = "ownerId")).shouldBeInstanceOf<AccountChanged>()

        // Assert
        renamed.account.customerId shouldBe "u-42"
    }

    @Test
    fun `an unknown ACCOUNTS event type is ignored`() {
        translate(external(type = "ACCOUNTS.ACCOUNT_RENAMED")).shouldBeNull()
    }

    @Test
    fun `a known event without its customer can't be translated`() {
        shouldThrow<IllegalStateException> { translate(external(customerField = "someoneId")) }
    }

    @Test
    fun `the listener publishes ACCOUNT_CHANGED for each external event on the ingress channel`() {
        // Arrange
        val publisher = EventPublisherInMemory()
        val context = AwsKopipesContext(eventPublisher = publisher, faultManager = FaultManager(publisher, skipErrorLogging = true))
        val listener = Listener { ListenerContainer(context) }
        val kinesisEvent = KinesisEvent().apply {
            records = listOf(external(), external(type = AccountsEvent.ACCOUNT_CLOSED)).mapIndexed { i, json ->
                KinesisEvent.KinesisEventRecord().apply {
                    eventID = "shardId-000000000000:$i"
                    kinesis = KinesisEvent.Record().apply {
                        partitionKey = "C-1"
                        sequenceNumber = "$i"
                        approximateArrivalTimestamp = Date()
                        data = ByteBuffer.wrap(json.toByteArray())
                    }
                }
            }
        }

        // Act
        listener.handleRequest(kinesisEvent, TestContext())

        // Assert
        val published = publisher.uows().mapNotNull { it.event as? AccountChanged }
        published shouldHaveSize 2
        published.map { it.account.status }.toSet() shouldBe setOf(Account.OPEN, Account.CLOSED)
        publisher.faults() shouldHaveSize 0
    }
}

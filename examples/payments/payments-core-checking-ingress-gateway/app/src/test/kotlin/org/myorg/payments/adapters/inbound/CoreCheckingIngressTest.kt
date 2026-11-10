package org.myorg.payments.adapters.inbound

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.testsupport.TestContext
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.JsonRaw
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.myorg.payments.*
import java.util.Date

class CoreCheckingIngressTest {

    private val debit = simulatedLedgerEntry(Fixtures.payment, LedgerEntry.PAYMENT, postedAt = 1730900580000, entryId = "E-PMT")

    @AfterEach
    fun tearDown() = AwsGlobalRegistry.reset()

    @Test
    fun `a posted debit is FUNDS_WITHDRAWN with the ledger row as raw`() {
        // Act
        val event = ledgerEvent(debit, Fixtures.payment).shouldBeInstanceOf<FundsWithdrawn>()

        // Assert
        event.id shouldBe "E-PMT.funds-withdrawn"
        event.timestamp shouldBe 1730900580000
        event.partitionKey shouldBe "p-7"
        event.payment shouldBe Fixtures.payment
        ((event.raw as JsonRaw).value as JsonObject)["REF_NO"]?.jsonPrimitive?.content shouldBe "p-7"
    }

    @Test
    fun `a rejected debit is WITHDRAWAL_FAILED with the reason in our words`() {
        // Act
        val event = ledgerEvent(debit.copy(STAT = "REJECTED", REJ_CD = "NSF"), Fixtures.payment).shouldBeInstanceOf<WithdrawalFailed>()

        // Assert
        event.reason shouldBe Reasons.INSUFFICIENT_FUNDS
        event.id shouldBe "E-PMT.withdrawal-failed"
    }

    @Test
    fun `a posted refund credit is FUNDS_REFUNDED, a rejected one has no event`() {
        // Arrange
        val refund = simulatedLedgerEntry(Fixtures.payment, LedgerEntry.REFUND, postedAt = 1, entryId = "E-RFND")

        // Act / Assert
        ledgerEvent(refund, Fixtures.payment).shouldBeInstanceOf<FundsRefunded>().id shouldBe "E-RFND.funds-refunded"
        shouldThrow<IllegalStateException> { ledgerEvent(refund.copy(STAT = "REJECTED"), Fixtures.payment) }
    }

    private fun record(eventName: String, entry: LedgerEntry) = DynamodbEvent.DynamodbStreamRecord().apply {
        this.eventName = eventName
        eventID = "rec-${entry.ENTRY_ID}-$eventName"
        awsRegion = "us-east-1"
        dynamodb = StreamRecord().apply {
            keys = mapOf("ENTRY_ID" to AttributeValue().withS(entry.ENTRY_ID))
            newImage = entry.toAttributes().mapValues { AttributeValue().withS(it.value) }
            approximateCreationDateTime = Date()
            sequenceNumber = "1"
        }
    }

    @Test
    fun `only new ledger rows are read`() {
        newLedgerEntry(UnitOfWork(record = record("INSERT", debit))) shouldBe debit
        newLedgerEntry(UnitOfWork(record = record("MODIFY", debit))).shouldBeNull()
    }

    @Test
    fun `the trigger joins each row to the payment it refers to and publishes one event per row`() {
        // Arrange
        val publisher = EventPublisherInMemory()
        GlobalRegistry.setFaultManager(FaultManager(publisher, skipErrorLogging = true))
        val lookup = PaymentLookup { reference -> if (reference == "p-7") Fixtures.payment else throw PaymentNotKnownYet(reference) }
        val trigger = LedgerTrigger { LedgerContainer(lookup, publisher) }
        val refund = simulatedLedgerEntry(Fixtures.payment, LedgerEntry.REFUND, postedAt = 2, entryId = "E-RFND")

        // Act
        trigger.handleRequest(DynamodbEvent().apply { records = listOf(record("INSERT", debit), record("INSERT", refund)) }, TestContext())

        // Assert
        publisher.uows().mapNotNull { it.event?.eventType() } shouldContainExactly
            listOf(PaymentsEvent.FUNDS_WITHDRAWN, PaymentsEvent.FUNDS_REFUNDED)
        publisher.faults() shouldHaveSize 0
    }
}

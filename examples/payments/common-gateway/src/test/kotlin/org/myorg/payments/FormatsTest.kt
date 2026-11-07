package org.myorg.payments

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.Test

class FormatsTest {

    private val now = 1730900520L

    // ---------------------------------------------------------------------------------------- core checking

    @Test
    fun `simulated debits post, except from C-2, which is rejected for insufficient funds`() {
        // Act
        val posted = simulatedLedgerEntry(Fixtures.payment, LedgerEntry.PAYMENT, postedAt = 5)
        val rejected = simulatedLedgerEntry(Fixtures.payment.copy(from = Party(Party.CHECKING, "C-2")), LedgerEntry.PAYMENT, postedAt = 5)

        // Assert
        posted shouldBe LedgerEntry("p-7-PMT", "C-1", "-100.00", "PMT", "p-7", "POSTED", null, "5")
        rejected.STAT shouldBe LedgerEntry.REJECTED
        rejected.REJ_CD shouldBe LedgerEntry.NSF
    }

    @Test
    fun `a simulated refund is a positive credit to the source account`() {
        // Act
        val refund = simulatedLedgerEntry(Fixtures.payment.copy(from = Party(Party.CHECKING, "C-2")), LedgerEntry.REFUND, postedAt = 5)

        // Assert
        refund.AMT shouldBe "100.00"
        refund.STAT shouldBe LedgerEntry.POSTED
        refund.ENTRY_ID shouldBe "p-7-RFND"
    }

    @Test
    fun `ledger rows survive the attribute round trip`() {
        // Arrange
        val entry = simulatedLedgerEntry(Fixtures.payment, LedgerEntry.PAYMENT, postedAt = 5)

        // Act / Assert
        LedgerEntry.fromAttributes(entry.toAttributes()) shouldBe entry
    }

    @Test
    fun `reason codes from the outside become our reasons, anything new is rejected`() {
        coreCheckingReason("NSF") shouldBe Reasons.INSUFFICIENT_FUNDS
        coreCheckingReason("ACCT_CLSD") shouldBe Reasons.ACCOUNT_CLOSED
        coreCheckingReason("FROZEN") shouldBe Reasons.REJECTED
        cardProcessorReason("card_closed") shouldBe Reasons.ACCOUNT_CLOSED
        cardProcessorReason(null) shouldBe Reasons.REJECTED
        bankNetworkReason("R02") shouldBe Reasons.ACCOUNT_CLOSED
        bankNetworkReason("R03") shouldBe Reasons.ACCOUNT_NOT_FOUND
        bankNetworkReason("R99") shouldBe Reasons.REJECTED
    }

    // ---------------------------------------------------------------------------------------- card processor

    @Test
    fun `simulated webhooks succeed, except for K-3 whose card is closed`() {
        // Act
        val succeeded = simulatedCardWebhook(Fixtures.payment, now)
        val failed = simulatedCardWebhook(Fixtures.payment.copy(to = Party(Party.CARD, "K-3")), now)

        // Assert
        succeeded.type shouldBe CardWebhook.SUCCEEDED
        succeeded.eventId shouldBe "evt_p-7"
        succeeded.data.amountCents shouldBe 10000
        succeeded.data.metadata.reference shouldBe "p-7"
        succeeded.data.failureCode.shouldBeNull()
        failed.type shouldBe CardWebhook.FAILED
        failed.data.failureCode shouldBe "card_closed"
    }

    @Test
    fun `the webhook body is in the processor's own snake case dialect`() {
        // Act
        val body = cardJson.encodeToString(CardWebhook.serializer(), simulatedCardWebhook(Fixtures.payment, now))

        // Assert
        body shouldContain "\"event_id\":\"evt_p-7\""
        body shouldContain "\"amount_cents\":10000"
        body shouldContain "\"metadata\":{\"reference\":\"p-7\"}"
    }

    @Test
    fun `a signature verifies only for the same secret, body and a recent timestamp`() {
        // Arrange
        val body = """{"event_id":"evt_p-7"}"""
        val header = CardSignature.sign("secret", now, body)

        // Act / Assert
        CardSignature.verify("secret", header, body, now) shouldBe true
        CardSignature.verify("other", header, body, now) shouldBe false
        CardSignature.verify("secret", header, body.replace("7", "8"), now) shouldBe false
        CardSignature.verify("secret", header, body, now + CardSignature.TOLERANCE_SECONDS + 1) shouldBe false
        CardSignature.verify("secret", null, body, now) shouldBe false
        CardSignature.verify("secret", "garbage", body, now) shouldBe false
    }

    @Test
    fun `amounts become cents exactly`() {
        amountInCents("100.00") shouldBe 10000
        amountInCents("0.07") shouldBe 7
    }

    // ------------------------------------------------------------------------------------------ bank network

    @Test
    fun `simulated settlements settle, except B-8 which has no such account`() {
        // Act / Assert
        simulatedSettlement(Fixtures.toBank) shouldBe SettlementLine("p-7", SettlementLine.SETTLED, "")
        simulatedSettlement(Fixtures.payment.copy(to = Party(Party.BANK, "B-8"))) shouldBe
            SettlementLine("p-7", SettlementLine.REJECTED, "R03")
    }

    @Test
    fun `a settlement file parses with or without header and skips blank lines`() {
        // Arrange
        val content = "reference,status,code\np-7,SETTLED,\n\np-8,rejected,R02\n"

        // Act
        val lines = parseSettlementFile(content)

        // Assert
        lines shouldContainExactly listOf(SettlementLine("p-7", "SETTLED", ""), SettlementLine("p-8", "REJECTED", "R02"))
        parseSettlementFile(settlementFile(lines)) shouldBe lines
    }

    @Test
    fun `transfer lines carry reference, account, amount and currency`() {
        transferLine(Fixtures.toBank) shouldBe "p-7,B-9,100.00,USD"
        fileDate(Fixtures.SUBMITTED_AT) shouldBe "20241106"
    }
}

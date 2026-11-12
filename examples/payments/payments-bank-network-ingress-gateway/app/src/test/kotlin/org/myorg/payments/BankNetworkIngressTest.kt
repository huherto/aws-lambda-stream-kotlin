package org.myorg.payments

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.testsupport.TestContext
import io.kopipes.core.JsonRaw
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test

class BankNetworkIngressTest {

    private val notification = """
        {"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","eventTime":"2024-11-07T08:00:00.000Z",
          "s3":{"bucket":{"name":"inbound-bucket"},"object":{"key":"inbound/SETTLE_20241107.csv","size":42}}}]}
    """.trimIndent()

    private val file = ArrivedFile("inbound-bucket", "inbound/SETTLE_20241107.csv", 1730966400000)


    @Test
    fun `an S3 notification announces its files, S3's test event none`() {
        arrivedFiles(notification) shouldContainExactly listOf(file)
        arrivedFiles("""{"Service":"Amazon S3","Event":"s3:TestEvent"}""").shouldBeEmpty()
    }

    @Test
    fun `a settled line is FUNDS_DEPOSITED, its id is the file name and the reference`() {
        // Act
        val event = settlementEvent(file, SettlementLine("p-7", "SETTLED"), Fixtures.toBank).shouldBeInstanceOf<FundsDeposited>()

        // Assert
        event.id shouldBe "SETTLE_20241107.csv:p-7.funds-deposited"
        event.timestamp shouldBe 1730966400000
        ((event.raw as JsonRaw).value as JsonObject)["line"]?.jsonPrimitive?.content shouldBe "p-7,SETTLED,"
    }

    @Test
    fun `a rejected line is DEPOSIT_FAILED with the reason in our words`() {
        settlementEvent(file, SettlementLine("p-7", "REJECTED", "R03"), Fixtures.toBank)
            .shouldBeInstanceOf<DepositFailed>().reason shouldBe Reasons.ACCOUNT_NOT_FOUND
    }

    @Test
    fun `each line of a file becomes one event, and reprocessing the file gives the same ids`() {
        // Arrange
        val publisher = EventPublisherInMemory()
        val context = AwsKopipesContext(eventPublisher = publisher, faultManager = FaultManager(publisher, skipErrorLogging = true))
        val content = settlementFile(listOf(simulatedSettlement(Fixtures.toBank.copy(id = "p-1")), SettlementLine("p-2", "REJECTED", "R02")))
        val listener = FileListener {
            FilesContainer(
                context,
                files = { content },
                lookup = { reference -> Fixtures.toBank.copy(id = reference) },
                eventPublisher = publisher,
            )
        }
        val sqs = SQSEvent().apply { records = listOf(SQSEvent.SQSMessage().apply { body = notification }) }

        // Act
        listener.handleRequest(sqs, TestContext())
        listener.handleRequest(sqs, TestContext())

        // Assert
        publisher.uows().mapNotNull { it.event?.id } shouldContainExactly listOf(
            "SETTLE_20241107.csv:p-1.funds-deposited", "SETTLE_20241107.csv:p-2.deposit-failed",
            "SETTLE_20241107.csv:p-1.funds-deposited", "SETTLE_20241107.csv:p-2.deposit-failed",
        )
    }
}

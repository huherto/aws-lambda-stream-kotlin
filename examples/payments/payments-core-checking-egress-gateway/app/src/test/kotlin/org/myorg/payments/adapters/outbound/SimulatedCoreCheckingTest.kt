package org.myorg.payments.adapters.outbound

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test
import org.myorg.payments.*
import org.myorg.payments.application.CallNotSent
import org.myorg.payments.application.Claim
import java.util.Base64

/** The simulated core checking's fault accounts, and the shape of a redelivery to the listener. */
class SimulatedCoreCheckingTest {

    private val payment = Fixtures.payment
    private var now = 1_000_000L

    @Test
    fun `C-8 - the simulated request is lost the first time only, and nothing is posted`() = runTest {
        // Arrange
        val deliveries = mutableListOf<String>()
        val seen = mutableSetOf<String>()
        val simulated = SimulatedCoreChecking({ body, _ -> deliveries += body }, delaySeconds = 2, lost = { seen.add(it) })
        val lostOne = payment.copy(from = Party(Party.CHECKING, Simulation.LOST_REQUEST_ACCOUNT))

        // Act / Assert
        shouldThrow<java.net.SocketTimeoutException> { simulated.debit(lostOne) }
        deliveries.size shouldBe 0
        simulated.debit(lostOne)
        deliveries.size shouldBe 1
    }

    @Test
    fun `C-7 - the simulated core checking applies the debit and then times out`() = runTest {
        // Arrange
        val deliveries = mutableListOf<String>()
        val simulated = SimulatedCoreChecking({ body, _ -> deliveries += body }, delaySeconds = 2)

        // Act / Assert
        shouldThrow<java.net.SocketTimeoutException> {
            simulated.debit(payment.copy(from = Party(Party.CHECKING, Simulation.TIMEOUT_AFTER_DEBIT_ACCOUNT)))
        }
        deliveries.size shouldBe 1
    }

    @Test
    fun `C-5 - the simulated core checking goes down for everyone, once per payment`() = runTest {
        // Arrange
        var downUntil = 0L
        val started = mutableSetOf<String>()
        val outage = object : SimulatedOutage {
            override suspend fun startOnce(paymentId: String, now: Long) {
                if (started.add(paymentId)) downUntil = now + 60_000
            }
            override suspend fun isDown(now: Long) = now < downUntil
        }
        val simulated = SimulatedCoreChecking({ _, _ -> }, delaySeconds = 2, outage = outage, clock = { now })
        val down = Fixtures.payment.copy(id = "p-5", from = Party(Party.CHECKING, Simulation.OUTAGE_ACCOUNT))

        // Act / Assert
        shouldThrow<CallNotSent> { simulated.debit(down) }
        shouldThrow<CallNotSent> { simulated.debit(Fixtures.payment) }
        shouldThrow<CallNotSent> { simulated.posted(Fixtures.payment, LedgerEntry.PAYMENT) }
        now += 60_000
        simulated.debit(down)
        simulated.debit(Fixtures.payment)
    }

    @Test
    fun `a redelivery is a Kinesis event carrying the original request, as the stream would make it`() {
        // Arrange
        val request = """{"type":"WITHDRAWAL_REQUESTED"}"""
        val claim = Claim("p-7#PMT", Claim.CALLING, 1, request)

        // Act
        val payload = redeliveryOf(listOf(claim), now)

        // Assert
        val records = Json.parseToJsonElement(payload).jsonObject["Records"]!!.jsonArray
        records.size shouldBe 1
        val kinesis = records.single().jsonObject["kinesis"]!!.jsonObject
        kinesis["partitionKey"]?.jsonPrimitive?.content shouldBe "p-7"
        String(Base64.getDecoder().decode(kinesis["data"]!!.jsonPrimitive.content)) shouldBe request
    }
}

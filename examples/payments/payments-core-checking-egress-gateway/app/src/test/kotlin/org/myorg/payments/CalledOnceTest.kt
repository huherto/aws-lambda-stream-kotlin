package org.myorg.payments

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** Claims in memory, behaving like the DynamoDB ones. */
class InMemoryClaims : Claims {
    val claims = mutableMapOf<String, Claim>()

    override suspend fun claim(key: String, now: Long): Claim? {
        claims[key]?.let { return it }
        claims[key] = Claim(key, Claim.CALLING, now)
        return null
    }

    override suspend fun markSent(key: String) {
        claims[key] = claims.getValue(key).copy(state = Claim.SENT)
    }

    override suspend fun release(key: String) {
        claims.remove(key)
    }
}

/** A core checking that records every call and can fail the way the real one does. */
class FakeCoreChecking : CoreChecking {
    val calls = mutableListOf<String>()
    val postings = mutableSetOf<String>()
    var failNext: Exception? = null
    /** Applies the debit before failing, like a timeout after core checking did its work. */
    var applyBeforeFailing = false

    override suspend fun debit(payment: Payment) = call("debit", payment, LedgerEntry.PAYMENT)
    override suspend fun credit(payment: Payment) = call("credit", payment, LedgerEntry.REFUND)
    override suspend fun posted(payment: Payment, txnType: String) = "${payment.id}#$txnType" in postings

    private fun call(name: String, payment: Payment, txnType: String) {
        calls += "$name ${payment.id}"
        val failure = failNext
        failNext = null
        if (failure == null || applyBeforeFailing) postings += "${payment.id}#$txnType"
        if (failure != null) throw failure
    }
}

class CalledOnceTest {

    private val coreChecking = FakeCoreChecking()
    private val claims = InMemoryClaims()
    private var now = 1_000_000L
    private val calledOnce = CalledOnce(
        coreChecking, claims,
        grace = 300.seconds, inquiryWait = 0.milliseconds, inquiryEvery = 1.milliseconds,
        clock = { now },
    )
    private val payment = Fixtures.payment

    @Test
    fun `a request is claimed, called once and marked sent`() = runTest {
        // Act
        calledOnce.debit(payment)

        // Assert
        coreChecking.calls shouldContainExactly listOf("debit p-7")
        claims.claims["p-7#PMT"]?.state shouldBe Claim.SENT
    }

    @Test
    fun `a redelivered request that was sent does not call again`() = runTest {
        // Arrange
        calledOnce.debit(payment)

        // Act
        calledOnce.debit(payment)
        calledOnce.debit(payment)

        // Assert
        coreChecking.calls shouldContainExactly listOf("debit p-7")
    }

    @Test
    fun `a debit and a refund of the same payment are different requests`() = runTest {
        // Act
        calledOnce.debit(payment)
        calledOnce.credit(payment)

        // Assert
        coreChecking.calls shouldContainExactly listOf("debit p-7", "credit p-7")
    }

    @Test
    fun `a lost answer leaves the request in doubt, and the retry finds the posting instead of calling again`() = runTest {
        // Arrange: core checking applies the debit, then the call times out (C-7)
        coreChecking.failNext = java.net.SocketTimeoutException("timed out")
        coreChecking.applyBeforeFailing = true

        // Act
        shouldThrow<CallOutcomeUnknown> { calledOnce.debit(payment) }
        calledOnce.debit(payment)

        // Assert
        coreChecking.calls shouldContainExactly listOf("debit p-7")
        claims.claims["p-7#PMT"]?.state shouldBe Claim.SENT
    }

    @Test
    fun `in doubt with no posting yet, the retry waits instead of calling again`() = runTest {
        // Arrange: the call failed and core checking shows nothing (yet)
        coreChecking.failNext = java.net.SocketTimeoutException("timed out")
        shouldThrow<CallOutcomeUnknown> { calledOnce.debit(payment) }
        now += 60_000

        // Act / Assert
        shouldThrow<CallInDoubt> { calledOnce.debit(payment) }
        coreChecking.calls shouldContainExactly listOf("debit p-7")
        claims.claims["p-7#PMT"]?.state shouldBe Claim.CALLING
    }

    @Test
    fun `in doubt past the grace period, it becomes a fault for an operator, still without calling again`() = runTest {
        // Arrange
        coreChecking.failNext = java.net.SocketTimeoutException("timed out")
        shouldThrow<CallOutcomeUnknown> { calledOnce.debit(payment) }
        now += 301_000

        // Act / Assert
        shouldThrow<CallUnresolved> { calledOnce.debit(payment) }
        coreChecking.calls shouldContainExactly listOf("debit p-7")
    }

    @Test
    fun `a call that never left releases the claim, so the next attempt calls normally`() = runTest {
        // Arrange
        coreChecking.failNext = CallNotSent("connection refused")

        // Act
        shouldThrow<CallNotSent> { calledOnce.debit(payment) }
        claims.claims["p-7#PMT"].shouldBeNull()
        calledOnce.debit(payment)

        // Assert
        coreChecking.calls shouldContainExactly listOf("debit p-7", "debit p-7")
        claims.claims["p-7#PMT"]?.state shouldBe Claim.SENT
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
}

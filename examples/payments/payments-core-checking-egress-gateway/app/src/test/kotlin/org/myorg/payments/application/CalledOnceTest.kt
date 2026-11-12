package org.myorg.payments.application

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.myorg.payments.*
import kotlin.time.Duration.Companion.seconds

/** Claims in memory, behaving like the DynamoDB ones. */
class InMemoryClaims : Claims {
    val claims = mutableMapOf<String, Claim>()

    override suspend fun find(key: String): Claim? = claims[key]

    override suspend fun claim(key: String, now: Long, request: String?): Claim? {
        claims[key]?.let { return it }
        claims[key] = Claim(key, Claim.CALLING, now, request)
        return null
    }

    override suspend fun markSent(key: String) {
        claims[key] = claims.getValue(key).copy(state = Claim.SENT)
    }

    override suspend fun release(key: String) {
        claims.remove(key)
    }

    override suspend fun inDoubt(): List<Claim> = claims.values.filter { it.state == Claim.CALLING }
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
    private val calledOnce = CalledOnce(coreChecking, claims, grace = 300.seconds, clock = { now })
    private val payment = Fixtures.payment
    private val request = Fixtures.withdrawalRequested().toString()

    @Test
    fun `a request is claimed, called once and marked sent`() = runTest {
        // Act
        val outcome = calledOnce.debit(payment, request)

        // Assert
        outcome shouldBe Outcome.SENT
        coreChecking.calls shouldContainExactly listOf("debit p-7")
        claims.claims["p-7#PMT"]?.state shouldBe Claim.SENT
    }

    @Test
    fun `a redelivered request that was sent does not call again`() = runTest {
        // Arrange
        calledOnce.debit(payment, request)

        // Act
        val outcomes = listOf(calledOnce.debit(payment, request), calledOnce.debit(payment, request))

        // Assert
        outcomes shouldBe listOf(Outcome.ALREADY_SENT, Outcome.ALREADY_SENT)
        coreChecking.calls shouldContainExactly listOf("debit p-7")
    }

    @Test
    fun `a debit and a refund of the same payment are different requests`() = runTest {
        // Act
        calledOnce.debit(payment, request)
        calledOnce.credit(payment, Fixtures.refundRequested().toString())

        // Assert
        coreChecking.calls shouldContainExactly listOf("debit p-7", "credit p-7")
    }

    @Test
    fun `a lost answer leaves the request in doubt without holding up the stream, and keeps the request`() = runTest {
        // Arrange: core checking applies the debit, then the call times out (C-7)
        coreChecking.failNext = java.net.SocketTimeoutException("timed out")
        coreChecking.applyBeforeFailing = true

        // Act
        val outcome = calledOnce.debit(payment, request)

        // Assert: no exception, so the batch moves on; the claim waits for the reconciler
        outcome shouldBe Outcome.IN_DOUBT
        claims.claims["p-7#PMT"]?.state shouldBe Claim.CALLING
        claims.claims["p-7#PMT"]?.request shouldBe request
    }

    @Test
    fun `redelivered in doubt, it finds the posting instead of calling again`() = runTest {
        // Arrange
        coreChecking.failNext = java.net.SocketTimeoutException("timed out")
        coreChecking.applyBeforeFailing = true
        calledOnce.debit(payment, request)

        // Act
        val outcome = calledOnce.debit(payment, request)

        // Assert
        outcome shouldBe Outcome.RESOLVED
        coreChecking.calls shouldContainExactly listOf("debit p-7")
        claims.claims["p-7#PMT"]?.state shouldBe Claim.SENT
    }

    @Test
    fun `redelivered in doubt with no posting yet, it stays in doubt and still doesn't call`() = runTest {
        // Arrange: the request was lost; core checking shows nothing
        coreChecking.failNext = java.net.SocketTimeoutException("lost")
        calledOnce.debit(payment, request)
        now += 60_000

        // Act
        val outcome = calledOnce.debit(payment, request)

        // Assert
        outcome shouldBe Outcome.IN_DOUBT
        coreChecking.calls shouldContainExactly listOf("debit p-7")
    }

    @Test
    fun `in doubt past the grace period, it becomes a fault for an operator, still without calling again`() = runTest {
        // Arrange
        coreChecking.failNext = java.net.SocketTimeoutException("lost")
        calledOnce.debit(payment, request)
        now += 301_000

        // Act / Assert
        shouldThrow<CallUnresolved> { calledOnce.debit(payment, request) }.message shouldContain "check it before retrying"
        coreChecking.calls shouldContainExactly listOf("debit p-7")
    }

    @Test
    fun `a call that never left releases the claim, so the next attempt calls normally`() = runTest {
        // Arrange
        coreChecking.failNext = CallNotSent("connection refused")

        // Act
        shouldThrow<CallNotSent> { calledOnce.debit(payment, request) }
        claims.claims["p-7#PMT"].shouldBeNull()
        calledOnce.debit(payment, request)

        // Assert
        coreChecking.calls shouldContainExactly listOf("debit p-7", "debit p-7")
        claims.claims["p-7#PMT"]?.state shouldBe Claim.SENT
    }

    @Test
    fun `after an operator releases an unresolved request, the resubmission calls core checking once`() = runTest {
        // Arrange: lost, unresolved, then released by the operator
        coreChecking.failNext = java.net.SocketTimeoutException("lost")
        calledOnce.debit(payment, request)
        now += 301_000
        shouldThrow<CallUnresolved> { calledOnce.debit(payment, request) }
        claims.release("p-7#PMT")

        // Act
        val outcome = calledOnce.debit(payment, request)

        // Assert
        outcome shouldBe Outcome.SENT
        coreChecking.calls shouldContainExactly listOf("debit p-7", "debit p-7")
        coreChecking.postings shouldBe setOf("p-7#PMT")
    }

    @Test
    fun `the reconciler redelivers the requests in doubt, and leaves the rest alone`() = runTest {
        // Arrange
        claims.claims["p-7#PMT"] = Claim("p-7#PMT", Claim.CALLING, 1, request)
        claims.claims["p-8#PMT"] = Claim("p-8#PMT", Claim.SENT, 1, "{}")
        claims.claims["p-9#PMT"] = Claim("p-9#PMT", Claim.CALLING, 1, null)
        val redelivered = mutableListOf<List<Claim>>()
        val reconciler = Reconciler(claims, redelivery = { redelivered += it })

        // Act
        val count = reconciler.reconcile()

        // Assert: only a claim in doubt that kept its request can be redelivered
        count shouldBe 1
        redelivered.single().map { it.key } shouldContainExactly listOf("p-7#PMT")
    }

}
